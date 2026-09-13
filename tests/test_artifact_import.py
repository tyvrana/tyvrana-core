import asyncio
import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from tyvrana_core import ArtifactError, ArtifactStore, CoreConfig

from .test_mcp_artifacts import png


def local_file(root: Path, data: bytes, name: str = "input.bin") -> Path:
    path = root / name
    path.write_bytes(data)
    return path


@pytest.fixture
def store() -> Iterator[ArtifactStore]:
    store = ArtifactStore(CoreConfig())
    store.start()
    assert store._directory is not None
    directory = Path(store._directory.name)
    try:
        yield store
    finally:
        store.close()
        assert not directory.exists()
        assert not store._entries and not store._transfers


@pytest.mark.parametrize("data", [b"", b"abc\x00\xff", bytes(range(256)) * 700, png()])
async def test_import_exact_bytes_and_independent_lifetime(
    store: ArtifactStore, tmp_path: Path, data: bytes
) -> None:
    source = local_file(tmp_path, data)
    descriptor = await store.import_file(str(source), name="Display")
    assert descriptor.byte_size == len(data)
    assert descriptor.sha256 == hashlib.sha256(data).hexdigest()
    assert descriptor.name == "Display"
    assert str(source) not in descriptor.model_dump_json()
    assert set(descriptor.model_dump()) == {
        "artifact_id",
        "name",
        "media_type",
        "byte_size",
        "sha256",
    }
    local_file(tmp_path, b"changed")
    os.unlink(source)
    for _ in range(2):
        with store.open(descriptor.artifact_id) as stream:
            assert stream.read() == data
    assert store.metadata(descriptor.artifact_id) == descriptor
    assert store.active_count == 0 and not store._transfers
    assert store.release(descriptor.artifact_id) is True
    assert store.release(descriptor.artifact_id) is False
    with pytest.raises(ArtifactError, match="unavailable"):
        store.metadata(descriptor.artifact_id)


@pytest.mark.parametrize(
    ("name", "data", "override", "expected"),
    [
        ("image.bin", png(), None, "image/png"),
        ("image.png", png(), "image/png", "image/png"),
        ("image.png", b"\xff\xd8\xff\xe0content", None, "image/jpeg"),
        ("file.txt", b"hello", None, "text/plain"),
        ("file.unknown_extension", b"hello", None, "application/octet-stream"),
        ("file.txt.gz", b"compressed", None, "application/octet-stream"),
        ("file.bin", b"hello", "application/example", "application/example"),
    ],
)
async def test_media_inference_and_override(
    store: ArtifactStore,
    tmp_path: Path,
    name: str,
    data: bytes,
    override: str | None,
    expected: str,
) -> None:
    path = local_file(tmp_path, data, name)
    descriptor = await store.import_file(str(path), media_type=override)
    assert descriptor.media_type == expected and descriptor.name == name


@pytest.mark.parametrize(
    "media_type",
    ["", "image", "Image/PNG", "image/png; charset=utf-8", "image/png", "image/jpeg"],
)
async def test_bad_media_or_signature_rejected(
    store: ArtifactStore, tmp_path: Path, media_type: str
) -> None:
    path = local_file(tmp_path, b"not an image")
    with pytest.raises(ArtifactError):
        await store.import_file(str(path), media_type=media_type)
    assert store.entry_count == 0


@pytest.mark.parametrize(
    "name",
    ["", " ", "../file", "folder/file", "folder\\file", "x\n", ".", "..", "x" * 256],
)
async def test_bad_display_names(
    store: ArtifactStore, tmp_path: Path, name: str
) -> None:
    path = local_file(tmp_path, b"abc")
    with pytest.raises(ArtifactError):
        await store.import_file(str(path), name=name)
    assert store.entry_count == 0


@pytest.mark.parametrize(
    "kind", ["missing", "directory", "fifo", "device", "symlink", "null"]
)
async def test_nonregular_or_invalid_sources_rejected_without_read(
    store: ArtifactStore, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "source"
    if kind == "directory":
        os.mkdir(path)
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "device":
        path = Path(os.devnull)
    elif kind == "symlink":
        target = local_file(tmp_path, b"content")
        os.symlink(target, path)
    value = "invalid\x00path" if kind == "null" else str(path)
    with pytest.raises(ArtifactError) as error:
        await store.import_file(value)
    assert str(path) not in str(error.value)
    assert store.entry_count == 0


@pytest.mark.parametrize("kind", ["size", "storage", "entries"])
async def test_import_obeys_store_limits(tmp_path: Path, kind: str) -> None:
    store = ArtifactStore(
        CoreConfig(
            max_artifact_size=100,
            max_artifact_storage=150,
            max_artifact_entries=1 if kind == "entries" else 8,
        )
    )
    store.start()
    try:
        path = local_file(tmp_path, b"x" * (101 if kind == "size" else 100))
        if kind != "size":
            await store.import_file(str(path))
        count = store.entry_count
        with pytest.raises(ArtifactError):
            await store.import_file(str(path))
        assert store.entry_count == count
    finally:
        store.close()


async def test_multiple_imports_and_cancellation_cleanup(
    store: ArtifactStore, tmp_path: Path
) -> None:
    path = local_file(tmp_path, b"x" * 131072)
    descriptors = await asyncio.gather(
        *(store.import_file(str(path)) for _ in range(3))
    )
    assert len({d.artifact_id for d in descriptors}) == 3
    work = asyncio.create_task(store.import_file(str(path)))
    await asyncio.sleep(0)
    assert store.active_count == 1
    work.cancel()
    with pytest.raises(asyncio.CancelledError):
        await work
    assert store.active_count == 0 and store.entry_count == 3


@pytest.mark.parametrize("change", ["modify", "truncate", "shutdown"])
async def test_source_changes_or_shutdown_during_copy_clean_partial(
    store: ArtifactStore, tmp_path: Path, change: str
) -> None:
    path = local_file(tmp_path, b"x" * 131072)
    work = asyncio.create_task(store.import_file(str(path)))
    await asyncio.sleep(0)
    if change == "shutdown":
        store.close()
    else:
        local_file(tmp_path, b"y" * (1 if change == "truncate" else 131072))
    with pytest.raises(ArtifactError):
        await work
    assert store.entry_count == 0 and not store._transfers


async def test_release_keeps_leased_bytes_charged_until_reader_exits(
    store: ArtifactStore, tmp_path: Path
) -> None:
    descriptor = await store.import_file(str(local_file(tmp_path, b"content")))
    with store.lease((descriptor,)) as first, store.lease((descriptor,)) as second:
        assert store.release(descriptor.artifact_id)
        assert not store.release(descriptor.artifact_id)
        with pytest.raises(ArtifactError):
            store.metadata(descriptor.artifact_id)
        assert store.reserved_bytes == 7
        assert first[0][1].read() == second[0][1].read() == b"content"
    assert store.entry_count == store.reserved_bytes == 0
    assert first[0][1].closed and second[0][1].closed


async def test_lease_rejects_changed_metadata_and_releases_partial_admission(
    store: ArtifactStore, tmp_path: Path
) -> None:
    descriptor = await store.import_file(str(local_file(tmp_path, b"content")))
    changed = descriptor.model_copy(update={"sha256": "0" * 64})
    with pytest.raises(ArtifactError):
        with store.lease((descriptor, changed)):
            pytest.fail("Invalid descriptors must not be admitted")
    assert store._entries[descriptor.artifact_id].leases == 0
    assert store.release(descriptor.artifact_id)


def test_texture_storage_defaults_are_bounded() -> None:
    config = CoreConfig()
    assert config.max_artifact_size == 128 * 1024 * 1024
    assert config.max_artifact_storage == 512 * 1024 * 1024
    assert config.max_artifact_entries == 128
    assert config.max_artifact_transfers == 4
    assert config.max_inline_image_bytes == 4 * 1024 * 1024


async def test_unreadable_regular_file_is_rejected(
    store: ArtifactStore, tmp_path: Path
) -> None:
    if os.getuid() == 0:
        pytest.skip("Root bypasses file read permissions")
    path = local_file(tmp_path, b"content")
    os.chmod(path, 0)
    try:
        with pytest.raises(ArtifactError, match="readable regular"):
            await store.import_file(str(path))
        assert store.entry_count == 0
    finally:
        os.chmod(path, 0o600)
