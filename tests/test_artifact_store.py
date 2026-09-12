from collections.abc import Iterator
from pathlib import Path

import pytest
from tyvrana_protocol import ArtifactChunk

from tyvrana_core import ArtifactError, ArtifactStore, CoreConfig

from .artifact_helpers import begin_message


@pytest.fixture
def store() -> Iterator[ArtifactStore]:
    store = ArtifactStore(
        CoreConfig(
            max_artifact_size=100,
            max_artifact_storage=200,
            max_artifact_entries=3,
            max_artifact_transfers=2,
        )
    )
    store.start()
    assert store._directory is not None
    root = Path(store._directory.name)
    try:
        yield store
    finally:
        store.close()
        assert not root.exists()
        assert store.reserved_bytes == store.entry_count == store.active_count == 0
        assert not store._transfers


@pytest.mark.parametrize("data", [b"", b"hello", b"x" * 100])
def test_atomic_completion_retrieval_release(store: ArtifactStore, data: bytes) -> None:
    message = begin_message(data)
    store.begin("owner", message)
    assert store.entry_count == store.active_count == 1
    assert store.reserved_bytes == len(data)
    with pytest.raises(ArtifactError, match="unavailable"):
        store.open(message.descriptor.artifact_id)
    for offset, byte in enumerate(data):
        store.write("owner", ArtifactChunk(message.transfer_id, offset, bytes([byte])))
    store.complete("owner", message.transfer_id)
    assert store.active_count == 0
    assert store.metadata(message.descriptor.artifact_id) == message.descriptor
    with store.open(message.descriptor.artifact_id) as stream:
        assert stream.read() == data
    store.claim("owner", message.request_id, (message.descriptor,))
    store.disconnect("owner")
    assert store.entry_count == 1
    store.release(message.descriptor.artifact_id)
    store.release(message.descriptor.artifact_id)
    assert store.entry_count == 0


@pytest.mark.parametrize(
    "violation",
    ["size", "storage", "entries", "concurrency", "artifact_id", "transfer_id"],
)
def test_admission_limits_and_collisions(store: ArtifactStore, violation: str) -> None:
    first = begin_message(b"x" * 100)
    store.begin("owner", first)
    candidate = begin_message(b"y" * 100, "other")
    owner = "owner"
    if violation == "size":
        candidate = begin_message(b"x" * 101)
    elif violation == "storage":
        store.begin("second", begin_message(b"x" * 100))
        owner = "third"
    elif violation == "entries":
        store.begin("second", begin_message(b""))
        store.begin("third", begin_message(b""))
        candidate = begin_message(b"")
    elif violation == "concurrency":
        store.begin("owner", begin_message(b""))
        candidate = begin_message(b"")
    elif violation == "artifact_id":
        candidate = candidate.model_copy(update={"descriptor": first.descriptor})
    else:
        candidate = candidate.model_copy(update={"transfer_id": first.transfer_id})
    count, size = store.entry_count, store.reserved_bytes
    with pytest.raises(ArtifactError):
        store.begin(owner, candidate)
    assert (store.entry_count, store.reserved_bytes) == (count, size)


@pytest.mark.parametrize(
    "kind", ["few", "hash", "many", "offset", "empty", "huge", "unknown"]
)
def test_invalid_bytes_never_publish(store: ArtifactStore, kind: str) -> None:
    message = begin_message(b"abc")
    store.begin("owner", message)
    with pytest.raises(ArtifactError):
        if kind == "few":
            store.complete("owner", message.transfer_id)
        elif kind == "hash":
            store.write("owner", ArtifactChunk(message.transfer_id, 0, b"xyz"))
            store.complete("owner", message.transfer_id)
        else:
            chunk = ArtifactChunk(
                "f" * 32 if kind == "unknown" else message.transfer_id,
                1 if kind == "offset" else 0,
                {
                    "many": b"abcd",
                    "offset": b"a",
                    "empty": b"",
                    "huge": b"a" * 65537,
                    "unknown": b"a",
                }[kind],
            )
            store.write("owner", chunk)
    with pytest.raises(ArtifactError):
        store.metadata(message.descriptor.artifact_id)
    store.discard_request("owner", message.request_id)
    assert store.entry_count == 0
    assert store._directory is not None
    assert list(Path(store._directory.name).iterdir()) == []


@pytest.mark.parametrize("completed", [False, True])
def test_disconnect_cleans_unclaimed_artifacts(
    store: ArtifactStore, completed: bool
) -> None:
    message = begin_message(b"")
    store.begin("owner", message)
    if completed:
        store.complete("owner", message.transfer_id)
    store.disconnect("unrelated")
    assert store.entry_count == 1
    store.disconnect("owner")
    assert store.entry_count == 0


def test_claim_rejects_incomplete_missing_foreign_or_changed_descriptors(
    store: ArtifactStore,
) -> None:
    message = begin_message(b"")
    store.begin("owner", message)
    for owner, request_id, descriptors in [
        ("owner", message.request_id, (message.descriptor,)),
        ("owner", message.request_id, ()),
        ("other", message.request_id, (message.descriptor,)),
        ("owner", "other", (message.descriptor,)),
    ]:
        with pytest.raises(ArtifactError):
            store.claim(owner, request_id, descriptors)
    store.complete("owner", message.transfer_id)
    with pytest.raises(ArtifactError):
        store.claim(
            "owner",
            message.request_id,
            (message.descriptor.model_copy(update={"name": "changed"}),),
        )


def test_store_restart_and_closed_admission(store: ArtifactStore) -> None:
    store.close()
    store.close()
    with pytest.raises(ArtifactError, match="closed"):
        store.begin("owner", begin_message(b""))
    store.start()
    store.begin("owner", begin_message(b""))


def test_maximum_artifacts_per_request() -> None:
    store = ArtifactStore(CoreConfig())
    store.start()
    try:
        for _ in range(8):
            message = begin_message(b"")
            store.begin("owner", message)
            store.complete("owner", message.transfer_id)
        with pytest.raises(ArtifactError, match="one operation"):
            store.begin("owner", begin_message(b""))
    finally:
        store.close()


def test_completed_transfer_does_not_accept_more_bytes(store: ArtifactStore) -> None:
    message = begin_message(b"")
    store.begin("owner", message)
    store.complete("owner", message.transfer_id)
    with pytest.raises(ArtifactError, match="completed"):
        store.write("owner", ArtifactChunk(message.transfer_id, 0, b"x"))


def test_file_failure_is_sanitized(
    store: ArtifactStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_open(*args: object, **kwargs: object) -> None:
        raise OSError("private/path")

    monkeypatch.setattr(Path, "open", fail_open)
    with pytest.raises(ArtifactError, match="^Cannot create temporary artifact$"):
        store.begin("owner", begin_message(b"x"))
    assert store.entry_count == 0
