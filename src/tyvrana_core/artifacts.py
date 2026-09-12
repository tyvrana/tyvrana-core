"""Bounded temporary artifacts; all mutation runs on the core event loop."""

import hashlib
import tempfile
from pathlib import Path
from typing import BinaryIO

from tyvrana_protocol import (
    MAX_ARTIFACT_CHUNK_SIZE,
    ArtifactBegin,
    ArtifactChunk,
    ArtifactDescriptor,
)

from .config import CoreConfig


class ArtifactError(Exception):
    """A public, path-free artifact failure."""


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


class ArtifactStore:
    """Reserve before writing; publish only exact, hash-verified content.

    Successful direct callers own explicit release. MCP releases its artifacts
    after constructing output. Failures/cancellation delete the whole request's
    artifacts. No eviction: full stores reject admission. Shutdown deletes all.
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
        if entry is None or not entry.complete:
            raise ArtifactError("Completed artifact is unavailable")
        return entry.begin.descriptor

    def open(self, artifact_id: str) -> BinaryIO:
        self.metadata(artifact_id)
        try:
            return self._entries[artifact_id].path.open("rb")
        except OSError as exc:
            raise ArtifactError("Cannot read temporary artifact") from exc

    def release(self, artifact_id: str) -> None:
        entry = self._entries.pop(artifact_id, None)
        if entry is None:
            return
        self._transfers.pop((entry.owner, entry.begin.transfer_id), None)
        try:
            if entry.file is not None:
                entry.file.close()
        finally:
            entry.path.unlink(missing_ok=True)

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
            self.release(artifact_id)
        if self._directory is not None:
            self._directory.cleanup()
            self._directory = None
