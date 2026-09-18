"""Bounded temporary artifacts; all mutation runs on the core event loop."""

import asyncio
import hashlib
import mimetypes
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from pydantic import ValidationError
from tyvrana_protocol import (
    MAX_ARTIFACT_CHUNK_SIZE,
    ArtifactBegin,
    ArtifactChunk,
    ArtifactDescriptor,
)

from .config import CoreConfig


class ArtifactError(Exception):
    """A public, path-free artifact failure."""


def _open_source(path: Path) -> tuple[BinaryIO, os.stat_result]:
    """Reject special files and final symlinks before any content read."""
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ArtifactError("Source must be a regular file, not a symlink")
        descriptor = os.open(
            path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            actual = os.fstat(descriptor)
            if not stat.S_ISREG(actual.st_mode) or (actual.st_dev, actual.st_ino) != (
                before.st_dev,
                before.st_ino,
            ):
                raise ArtifactError("Source file changed during admission")
            return os.fdopen(descriptor, "rb"), actual
        except BaseException:
            os.close(descriptor)
            raise
    except (OSError, ValueError) as exc:
        raise ArtifactError("Cannot open a readable regular source file") from exc


def _media_type(path: Path, prefix: bytes, explicit: str | None) -> str:
    inferred, encoding = mimetypes.guess_type(path.name, strict=True)
    detected = (
        "image/png"
        if prefix.startswith(b"\x89PNG\r\n\x1a\n")
        else "image/jpeg"
        if prefix.startswith(b"\xff\xd8\xff")
        else None
    )
    result = (
        explicit
        if explicit is not None
        else detected
        or (inferred if encoding is None else None)
        or "application/octet-stream"
    )
    if result in {"image/png", "image/jpeg"} and result != detected:
        raise ArtifactError("Raster signature does not match its media type")
    return result


class _Entry:
    def __init__(self, owner: str, begin: ArtifactBegin, path: Path) -> None:
        self.owner = owner
        self.begin = begin
        self.path = path
        self.file: BinaryIO | None = path.open("xb")
        self.digest = hashlib.sha256()
        self.offset = 0
        self.complete = False
        self.claimed = False
        self.leases = 0
        self.released = False


class ArtifactStore:
    """Reserve before writing; publish only exact, hash-verified content.

    Successful direct callers own explicit release. MCP releases inline images
    after constructing output; referenced outputs and imports persist until release
    or shutdown. Admission leases keep in-use bytes charged to the quota even
    after release, until their last reader exits. No eviction on a full store.
    """

    def __init__(self, config: CoreConfig) -> None:
        self.config = config
        self._directory: tempfile.TemporaryDirectory[str] | None = None
        self._entries: dict[str, _Entry] = {}
        self._transfers: dict[tuple[str, str], str] = {}

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @property
    def available_ids(self) -> frozenset[str]:
        """Current retrievable descriptors; this does not extend their lifetime."""
        return frozenset(
            key
            for key, entry in self._entries.items()
            if entry.complete and not entry.released
        )

    @property
    def reserved_bytes(self) -> int:
        return sum(entry.begin.descriptor.byte_size for entry in self._entries.values())

    @property
    def active_count(self) -> int:
        return sum(not entry.complete for entry in self._entries.values())

    def start(self) -> None:
        if self._directory is None:
            self._directory = tempfile.TemporaryDirectory(prefix="tyvrana-artifacts-")

    def begin(self, owner: str, message: ArtifactBegin) -> None:
        if self._directory is None:
            raise ArtifactError("Artifact store is closed")
        descriptor = message.descriptor
        if descriptor.artifact_id in self._entries:
            raise ArtifactError("Artifact identifier is already reserved")
        if (owner, message.transfer_id) in self._transfers:
            raise ArtifactError("Transfer identifier is already reserved")
        if descriptor.byte_size > self.config.max_artifact_size:
            raise ArtifactError("Artifact exceeds the individual size limit")
        if (
            self.reserved_bytes + descriptor.byte_size
            > self.config.max_artifact_storage
        ):
            raise ArtifactError("Artifact storage capacity exceeded")
        if self.entry_count >= self.config.max_artifact_entries:
            raise ArtifactError("Artifact entry capacity exceeded")
        entries = [entry for entry in self._entries.values() if entry.owner == owner]
        if (
            sum(not entry.complete for entry in entries)
            >= self.config.max_artifact_transfers
        ):
            raise ArtifactError("Too many concurrent artifact transfers")
        if sum(entry.begin.request_id == message.request_id for entry in entries) >= 8:
            raise ArtifactError("Too many artifacts for one operation")
        path = Path(self._directory.name) / (descriptor.artifact_id + ".partial")
        try:
            entry = _Entry(owner, message, path)
        except OSError as exc:
            raise ArtifactError("Cannot create temporary artifact") from exc
        self._entries[descriptor.artifact_id] = entry
        self._transfers[owner, message.transfer_id] = descriptor.artifact_id

    def request_for(self, owner: str, transfer_id: str) -> str | None:
        artifact_id = self._transfers.get((owner, transfer_id))
        return self._entries[artifact_id].begin.request_id if artifact_id else None

    def _transfer(self, owner: str, transfer_id: str) -> _Entry:
        artifact_id = self._transfers.get((owner, transfer_id))
        if artifact_id is None:
            raise ArtifactError("Unknown artifact transfer")
        entry = self._entries[artifact_id]
        if entry.complete:
            raise ArtifactError("Artifact transfer has already completed")
        return entry

    def write(self, owner: str, chunk: ArtifactChunk) -> None:
        entry = self._transfer(owner, chunk.transfer_id)
        if not 1 <= len(chunk.payload) <= MAX_ARTIFACT_CHUNK_SIZE:
            raise ArtifactError("Invalid artifact chunk size")
        if chunk.offset != entry.offset:
            raise ArtifactError("Artifact chunk offset is out of order")
        if entry.offset + len(chunk.payload) > entry.begin.descriptor.byte_size:
            raise ArtifactError("Artifact contains more bytes than declared")
        assert entry.file is not None
        try:
            entry.file.write(chunk.payload)
        except OSError as exc:
            raise ArtifactError("Cannot write temporary artifact") from exc
        entry.digest.update(chunk.payload)
        entry.offset += len(chunk.payload)

    def complete(self, owner: str, transfer_id: str) -> None:
        entry = self._transfer(owner, transfer_id)
        descriptor = entry.begin.descriptor
        if entry.offset != descriptor.byte_size:
            raise ArtifactError("Artifact size does not match descriptor")
        if entry.digest.hexdigest() != descriptor.sha256:
            raise ArtifactError("Artifact SHA-256 does not match descriptor")
        assert entry.file is not None
        try:
            entry.file.close()
            entry.file = None
            target = entry.path.with_suffix(".complete")
            entry.path.replace(target)
            entry.path = target
        except OSError as exc:
            raise ArtifactError("Cannot finalize temporary artifact") from exc
        entry.complete = True

    def claim(
        self, owner: str, request_id: str, descriptors: tuple[ArtifactDescriptor, ...]
    ) -> None:
        entries = [
            entry
            for entry in self._entries.values()
            if entry.owner == owner and entry.begin.request_id == request_id
        ]
        expected = {entry.begin.descriptor.artifact_id: entry for entry in entries}
        if len(expected) != len(descriptors) or any(
            descriptor.artifact_id not in expected
            or expected[descriptor.artifact_id].begin.descriptor != descriptor
            or not expected[descriptor.artifact_id].complete
            for descriptor in descriptors
        ):
            raise ArtifactError(
                "Operation references incomplete or unrelated artifacts"
            )
        for entry in entries:
            entry.claimed = True
            self._transfers.pop((owner, entry.begin.transfer_id))

    def metadata(self, artifact_id: str) -> ArtifactDescriptor:
        entry = self._entries.get(artifact_id)
        if entry is None or not entry.complete or entry.released:
            raise ArtifactError("Completed artifact is unavailable")
        return entry.begin.descriptor

    def open(self, artifact_id: str) -> BinaryIO:
        self.metadata(artifact_id)
        try:
            return self._entries[artifact_id].path.open("rb")
        except OSError as exc:
            raise ArtifactError("Cannot read temporary artifact") from exc

    def release(self, artifact_id: str) -> bool:
        entry = self._entries.get(artifact_id)
        if entry is None or entry.released:
            return False
        entry.released = True
        if not entry.leases:
            self._delete(artifact_id)
        return True

    def _delete(self, artifact_id: str) -> None:
        entry = self._entries.pop(artifact_id, None)
        if entry is None:
            return
        self._transfers.pop((entry.owner, entry.begin.transfer_id), None)
        try:
            if entry.file is not None:
                entry.file.close()
        finally:
            entry.path.unlink(missing_ok=True)

    @contextmanager
    def lease(
        self, descriptors: tuple[ArtifactDescriptor, ...]
    ) -> Iterator[tuple[tuple[ArtifactDescriptor, BinaryIO], ...]]:
        """Admit a complete immutable set and own every reader until it exits."""
        entries: list[_Entry] = []
        try:
            with ExitStack() as files:
                readers = []
                for descriptor in descriptors:
                    if self.metadata(descriptor.artifact_id) != descriptor:
                        raise ArtifactError("Stored artifact descriptor does not match")
                    entry = self._entries[descriptor.artifact_id]
                    stream = files.enter_context(self.open(descriptor.artifact_id))
                    entry.leases += 1
                    entries.append(entry)
                    readers.append((descriptor, stream))
                yield tuple(readers)
        finally:
            for entry in entries:
                entry.leases -= 1
                if entry.released and not entry.leases:
                    self._delete(entry.begin.descriptor.artifact_id)

    async def import_file(
        self, path: str, *, name: str | None = None, media_type: str | None = None
    ) -> ArtifactDescriptor:
        """Copy one local regular file; source paths terminate at this boundary.

        Bounded reads/writes stay on the owning loop, yielding between chunks.
        Never publish a partial copy or retain the original path as metadata.
        """
        if self._directory is None:
            raise ArtifactError("Artifact store is closed")
        source = Path(path)
        stream, before = _open_source(source)
        artifact_id = uuid4().hex
        try:
            if before.st_size > self.config.max_artifact_size:
                raise ArtifactError("Artifact exceeds the individual size limit")
            display_name = source.name if name is None else name
            if (
                not display_name.strip()
                or display_name in {".", ".."}
                or any(c in "/\\" or ord(c) < 32 or ord(c) == 127 for c in display_name)
            ):
                raise ArtifactError("Artifact name must be a display name, not a path")
            prefix = stream.read(32)
            stream.seek(0)
            try:
                descriptor = ArtifactDescriptor(
                    artifact_id=artifact_id,
                    name=display_name,
                    media_type=_media_type(source, prefix, media_type),
                    byte_size=before.st_size,
                    sha256="0" * 64,
                )
            except ValidationError as exc:
                raise ArtifactError("Invalid artifact name or media type") from exc
            message = ArtifactBegin(
                type="artifact.begin",
                transfer_id=uuid4().hex,
                request_id=uuid4().hex,
                descriptor=descriptor,
            )
            owner = "local-import"
            self.begin(owner, message)
            while chunk := stream.read(MAX_ARTIFACT_CHUNK_SIZE):
                entry = self._entries.get(artifact_id)
                if entry is None:
                    raise ArtifactError("Artifact import was interrupted")
                self.write(
                    owner, ArtifactChunk(message.transfer_id, entry.offset, chunk)
                )
                await asyncio.sleep(0)
            after = os.fstat(stream.fileno())
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                raise ArtifactError("Source file changed during import")
            entry = self._entries.get(artifact_id)
            if entry is None:
                raise ArtifactError("Artifact import was interrupted")
            descriptor = descriptor.model_copy(
                update={"sha256": entry.digest.hexdigest()}
            )
            entry.begin = message.model_copy(update={"descriptor": descriptor})
            self.complete(owner, message.transfer_id)
            self.claim(owner, message.request_id, (descriptor,))
            return descriptor
        except (OSError, ValueError) as exc:
            self.release(artifact_id)
            raise ArtifactError("Cannot copy source artifact") from exc
        except BaseException:
            self.release(artifact_id)
            raise
        finally:
            stream.close()

    async def export_file(
        self, artifact_id: str, path: str, *, overwrite: bool = False
    ) -> ArtifactDescriptor:
        """Atomically export locally; paths never travel to application adapters."""
        destination = Path(path)
        if not destination.is_absolute() or destination.is_symlink():  # noqa: ASYNC240 - Bounded local admission, as import_file.
            raise ArtifactError("Use an absolute destination without a final symlink")
        if not destination.parent.is_dir() or (
            destination.exists() and not destination.is_file()  # noqa: ASYNC240 - Bounded local admission.
        ):
            raise ArtifactError("Destination parent must exist; use a regular file")
        if destination.exists() and not overwrite:  # noqa: ASYNC240 - Bounded local admission.
            raise ArtifactError("Destination exists; explicitly enable overwrite")
        descriptor = self.metadata(artifact_id)
        temporary: Path | None = None
        try:
            with self.lease((descriptor,)) as readers:
                stream = readers[0][1]
                fd, filename = tempfile.mkstemp(
                    prefix=".tyvrana-export-", dir=destination.parent
                )
                temporary = Path(filename)
                digest = hashlib.sha256()
                size = 0
                with os.fdopen(fd, "wb") as output:
                    while chunk := stream.read(MAX_ARTIFACT_CHUNK_SIZE):
                        size += len(chunk)
                        if size > descriptor.byte_size:
                            raise ArtifactError("Stored artifact size changed")
                        output.write(chunk)
                        digest.update(chunk)
                        await asyncio.sleep(0)
                    output.flush()
                    os.fsync(output.fileno())
                if (
                    size != descriptor.byte_size
                    or digest.hexdigest() != descriptor.sha256
                ):
                    raise ArtifactError("Stored artifact integrity changed")
                if destination.is_symlink():  # noqa: ASYNC240 - Recheck before atomic publication.
                    raise ArtifactError("Destination changed to a symlink")
                if overwrite:
                    os.replace(temporary, destination)
                else:
                    os.link(temporary, destination)
            return descriptor
        except OSError as exc:
            raise ArtifactError("Cannot publish artifact at the destination") from exc
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def discard_request(self, owner: str, request_id: str) -> None:
        for artifact_id, entry in list(self._entries.items()):
            if entry.owner == owner and entry.begin.request_id == request_id:
                self.release(artifact_id)

    def disconnect(self, owner: str) -> None:
        for artifact_id, entry in list(self._entries.items()):
            if entry.owner == owner and not entry.claimed:
                self.release(artifact_id)

    def close(self) -> None:
        for artifact_id in list(self._entries):
            self._delete(artifact_id)
        if self._directory is not None:
            self._directory.cleanup()
            self._directory = None
