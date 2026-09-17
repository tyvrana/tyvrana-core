"""Durable semantics, transactions, bounded retrieval and revision correctness."""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from pydantic import ValidationError

from tyvrana_core.projects.models import (
    ApplyInput,
    BindingObservation,
    CheckpointInput,
    CreateInput,
    DeltaInput,
    Entity,
    Issue,
    ProjectPatch,
    SearchInput,
)
from tyvrana_core.projects.store import ProjectError, ProjectStore


@pytest.fixture
def store(tmp_path: Path) -> ProjectStore:
    return ProjectStore(tmp_path / "state/projects.sqlite3")


def fixture_batch(revision: int = 1) -> ApplyInput:
    return ApplyInput.model_validate(
        {
            "expected_revision": revision,
            "upsert": [
                {"kind": "entity", "id": "part", "label": "Source component"},
                {
                    "kind": "entity",
                    "id": "runtime",
                    "label": "Runtime asset",
                    "entity_type": "runtime",
                },
                {
                    "kind": "relationship",
                    "id": "mapping",
                    "label": "Source maps to runtime",
                    "source_id": "part",
                    "target_id": "runtime",
                    "relation": "maps_to",
                },
                {
                    "kind": "document",
                    "id": "source-file",
                    "label": "Source document",
                    "application": "modeler",
                    "application_project_id": "source-uuid",
                },
                {
                    "kind": "document",
                    "id": "runtime-file",
                    "label": "Runtime document",
                    "application": "engine",
                    "application_project_id": "runtime-uuid",
                },
                {
                    "kind": "binding",
                    "id": "source-binding",
                    "label": "Source resource",
                    "entity_id": "part",
                    "document_id": "source-file",
                    "resource_kind": "mesh",
                    "resource_id": "resource-uuid",
                },
                {
                    "kind": "binding",
                    "id": "runtime-binding",
                    "label": "Runtime resource",
                    "entity_id": "runtime",
                    "document_id": "runtime-file",
                    "resource_kind": "prefab",
                    "resource_id": "prefab-uuid",
                },
                {
                    "kind": "validation",
                    "id": "check",
                    "label": "Structure checked",
                    "validation_type": "inspection",
                    "status": "passed",
                    "freshness": "current",
                    "entity_ids": ["part"],
                },
                {
                    "kind": "milestone",
                    "id": "structure",
                    "label": "Structure accepted",
                    "status": "accepted",
                    "entity_ids": ["part"],
                    "validation_ids": ["check"],
                },
                {
                    "kind": "issue",
                    "id": "clearance",
                    "label": "Check clearance",
                    "entity_ids": ["part"],
                    "importance": 5,
                },
            ],
            "project": {"stage": "clearance verification"},
            "checkpoint": {"id": "structure-ready", "label": "Structure ready"},
        }
    )


def populated(store: ProjectStore) -> str:
    project = store.create(
        CreateInput(
            title="Portable assembly", goal="Verify a reusable articulated assembly"
        )
    )
    store.apply(project.id, fixture_batch(), {"source-file": "connection-a"})
    return project.id


def test_restart_compact_continuation_and_cross_application_relationship(
    store: ProjectStore,
) -> None:
    key = populated(store)
    restarted = ProjectStore(store.path)
    before = store.continuation(key, None, {"source-file": "connection-a"}, set())
    after = restarted.continuation(key, None, {"source-file": "connection-a"}, set())
    assert before == after
    assert after.project.stage == "clearance verification"
    assert after.checkpoint and after.checkpoint.revision == 2
    assert after.project.next_action == ""  # No invented plan.
    assert after.counts["relationship"] == 1
    assert len(after.model_dump_json().encode()) < 12000
    assert restarted.select(None, {("engine", "runtime-uuid")}) == key
    assert restarted.select(None, set()) == key


@pytest.mark.parametrize(
    "change",
    [
        {
            "upsert": [
                {
                    "kind": "relationship",
                    "id": "bad",
                    "label": "Dangling",
                    "source_id": "part",
                    "target_id": "unknown",
                    "relation": "depends_on",
                }
            ]
        },
        {"upsert": [{"kind": "issue", "id": "part", "label": "Cannot change kind"}]},
        {
            "upsert": [
                {
                    "kind": "validation",
                    "id": "bad",
                    "label": "Bad validation",
                    "validation_type": "inspection",
                    "evidence_ids": ["missing-evidence"],
                }
            ]
        },
        {"remove": ["part"]},
        {"checkpoint": {"id": "structure-ready", "label": "Cannot replace marker"}},
    ],
)
def test_failed_batch_preserves_all_state_and_revision(
    store: ProjectStore, change: dict[str, object]
) -> None:
    key = populated(store)
    before = store.continuation(key, 1, {}, set())
    batch = {"expected_revision": 2, "project": {"stage": "must roll back"}, **change}
    with pytest.raises(ProjectError):
        store.apply(key, ApplyInput.model_validate(batch))
    assert store.continuation(key, 1, {}, set()) == before
    with store.transaction() as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not db.execute("PRAGMA foreign_key_check").fetchall()


def test_concurrent_writers_conflict_delta_and_one_retry(store: ProjectStore) -> None:
    key = populated(store)
    barrier = Barrier(2)

    def write(stage: str) -> str:
        other = ProjectStore(store.path)
        barrier.wait()
        try:
            other.apply(
                key, ApplyInput(expected_revision=2, project=ProjectPatch(stage=stage))
            )
            return "success"
        except ProjectError as exc:
            assert exc.code == "revision_conflict"
            assert exc.details["current_revision"] == 3
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(write, ["first", "second"]))
    assert sorted(results) == ["conflict", "success"]
    delta = store.delta(key, DeltaInput(since_revision=2))
    assert delta.counts == {"project:updated": 1}
    repaired = store.apply(
        key,
        ApplyInput(
            expected_revision=delta.revision,
            project=ProjectPatch(next_action="Inspect the changed stage before work"),
        ),
    )
    assert repaired.project.revision == 4


def test_identity_observation_and_content_freshness_are_separate(
    store: ProjectStore,
) -> None:
    key = populated(store)
    store.apply(
        key,
        ApplyInput(expected_revision=2, project=ProjectPatch()),
        observations={
            "source-binding": BindingObservation(
                state="verified",
                connection_id="connection-a",
                name="Original",
                fingerprint="a",
            )
        },
    )
    view = store.search(
        key,
        SearchInput(ids=["source-binding", "check"]),
        {"source-file": "connection-a"},
    )
    observation = next(
        v for v in view.records if v.record.id == "source-binding"
    ).binding
    assert observation is not None and observation.state == "verified"
    assert (
        next(v for v in view.records if v.record.id == "check").freshness == "current"
    )
    reconnected = store.search(
        key,
        SearchInput(ids=["source-binding", "check"]),
        {"source-file": "connection-b"},
    )
    observation = next(
        v for v in reconnected.records if v.record.id == "source-binding"
    ).binding
    assert observation is not None and observation.state == "unverified"
    assert (
        next(v for v in reconnected.records if v.record.id == "check").freshness
        == "unverified"
    )
    store.invalidate("modeler", "source-uuid")
    stale = store.search(
        key,
        SearchInput(ids=["source-binding", "check"]),
        {"source-file": "connection-a"},
    )
    observation = next(
        v for v in stale.records if v.record.id == "source-binding"
    ).binding
    assert observation is not None and observation.state == "stale"
    assert next(v for v in stale.records if v.record.id == "check").freshness == "stale"
    revision = stale.revision
    store.invalidate("modeler", "source-uuid")
    assert store.search(key, SearchInput()).revision == revision  # Coalesced.


def test_entity_change_stales_validation_and_atomic_revalidation(
    store: ProjectStore,
) -> None:
    key = populated(store)
    store.apply(
        key,
        ApplyInput(
            expected_revision=2, upsert=[Entity(id="part", label="Revised component")]
        ),
    )
    assert store.search(key, SearchInput(ids=["check"])).records[0].freshness == "stale"
    validation = store.search(key, SearchInput(ids=["check"])).records[0].record
    store.apply(
        key,
        ApplyInput(
            expected_revision=3,
            upsert=[
                Entity(id="part", label="Accepted component"),
                validation.model_copy(update={"freshness": "current"}),
            ],
        ),
        {"source-file": "session"},
    )
    assert (
        store.search(key, SearchInput(ids=["check"]), {"source-file": "session"})
        .records[0]
        .freshness
        == "current"
    )


def test_search_pagination_filters_and_delta_tombstones(store: ProjectStore) -> None:
    key = populated(store)
    first = store.search(key, SearchInput(limit=3))
    assert first.next_offset is not None
    second = store.search(
        key, SearchInput(limit=3, offset=first.next_offset, at_revision=2)
    )
    assert not {v.record.id for v in first.records}.intersection(
        v.record.id for v in second.records
    )
    assert store.search(key, SearchInput(related_to="part")).matched_count == 5
    assert store.search(key, SearchInput(application="engine")).matched_count == 2
    assert store.search(key, SearchInput(relation="maps_to")).matched_count == 1
    assert (
        store.search(key, SearchInput(query="clearance", status="open")).matched_count
        == 1
    )
    store.apply(
        key,
        ApplyInput(
            expected_revision=2, remove=["mapping", "runtime-binding", "runtime"]
        ),
    )
    delta = store.delta(
        key, DeltaInput(checkpoint_id="structure-ready", limit=2, details=True)
    )
    assert delta.matched_count == 4 and delta.next_offset == 2
    assert delta.counts == {
        "validation:staled": 1,
        "relationship:removed": 1,
        "binding:removed": 1,
        "entity:removed": 1,
    }
    assert delta.records[0].freshness == "stale"
    with pytest.raises(ProjectError, match="current revision"):
        store.search(key, SearchInput(at_revision=2))


def test_bounded_history_preserves_named_marker(
    store: ProjectStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("tyvrana_core.projects.store.HISTORY_REVISIONS", 4)
    key = populated(store)
    for revision in range(2, 12):
        store.apply(
            key,
            ApplyInput(
                expected_revision=revision,
                project=ProjectPatch(stage=f"stage {revision}"),
            ),
        )
    marker = store.search(key, SearchInput(kind="checkpoint")).checkpoints[0]
    assert marker.revision == 2
    with pytest.raises(ProjectError) as error:
        store.delta(key, DeltaInput(checkpoint_id=marker.id))
    assert error.value.code == "history_expired"
    packet = store.continuation(key, None, {}, set())
    assert packet.notices and packet.checkpoint == marker
    assert store.delta(key, DeltaInput(since_revision=10)).matched_count == 2


def test_ambiguous_selection_never_silently_chooses_and_removal_is_semantic_only(
    store: ProjectStore, tmp_path: Path
) -> None:
    first = populated(store)
    second = store.create(
        CreateInput(title="Different project", goal="A different goal")
    )
    with pytest.raises(ProjectError) as error:
        store.select(None, set())
    assert error.value.code == "project_ambiguous"
    native_file = tmp_path / "native.file"
    native_file.write_text("authoritative application content")
    with pytest.raises(ProjectError):
        store.remove(first, 2, second.id)
    store.remove(first, 2, first)
    assert native_file.read_text() == "authoritative application content"
    assert store.select(None, set()) == second.id


def test_no_opaque_metadata_and_bounded_mutations() -> None:
    with pytest.raises(ValidationError):
        Entity.model_validate(
            {"id": "e", "label": "Entity", "metadata": {"anything": True}}
        )
    with pytest.raises(ValidationError):
        ApplyInput(
            expected_revision=1,
            upsert=[Entity(id=str(i), label="Entity") for i in range(257)],
        )
    with pytest.raises(ValidationError):
        Issue(id="issue", label="Issue", summary="x" * 801)


def test_ephemeral_artifact_expiry_is_not_durable_evidence(store: ProjectStore) -> None:
    key = populated(store)
    artifact = "a" * 32
    store.apply(
        key,
        ApplyInput.model_validate(
            {
                "expected_revision": 2,
                "upsert": [
                    {
                        "kind": "evidence",
                        "id": "image",
                        "label": "Inspection image",
                        "storage": "ephemeral_artifact",
                        "artifact_id": artifact,
                    }
                ],
            }
        ),
    )
    assert (
        store.search(key, SearchInput(ids=["image"]), artifacts={artifact})
        .records[0]
        .evidence_availability
        == "available"
    )
    assert (
        ProjectStore(store.path)
        .search(key, SearchInput(ids=["image"]))
        .records[0]
        .evidence_availability
        == "expired"
    )


def test_failure_after_writes_rolls_back(
    store: ProjectStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = populated(store)

    def fail(*args: object) -> None:
        raise OSError("injected failure before commit")

    monkeypatch.setattr(store, "_checkpoint", fail)
    with pytest.raises(OSError):
        store.apply(
            key,
            ApplyInput(
                expected_revision=2,
                upsert=[Entity(id="new", label="New")],
                checkpoint=CheckpointInput(id="new", label="New"),
            ),
        )
    assert store.search(key, SearchInput(ids=["new"])).matched_count == 0
    assert store.continuation(key, None, {}, set()).project.revision == 2


def test_large_state_deterministic_bounded_packet(store: ProjectStore) -> None:
    key = populated(store)
    for batch in range(4):
        store.apply(
            key,
            ApplyInput(
                expected_revision=batch + 2,
                upsert=[
                    Entity(
                        id=f"part-{batch * 200 + i:04}",
                        label=f"Component {i}",
                        summary="bounded detail " * 20,
                    )
                    for i in range(200)
                ],
            ),
        )
    packet = store.continuation(key, None, {}, set())
    assert packet.counts["entity"] == 802
    assert len(packet.model_dump_json().encode()) <= 32768
    assert packet.omitted_counts["entity"] >= 796
    assert packet == store.continuation(key, None, {}, set())
    page = store.search(key, SearchInput(kind="entity", limit=50))
    assert page.next_offset == 50 and len(page.records) == 50
    assert len(json.dumps(page.model_dump())) < 40000


def test_interrupted_transaction_recovers_without_partial_state(
    store: ProjectStore,
) -> None:
    import subprocess
    import sys

    key = populated(store)
    script = """
import sys
from pathlib import Path
from tyvrana_core.projects.store import ProjectStore
from tyvrana_core.projects.models import Entity
store = ProjectStore(Path(sys.argv[1]))
with store.transaction(write=True) as db:
    store._put(db, sys.argv[2], Entity(id="interrupted", label="Uncommitted"), 3)
    print("transaction-open", flush=True)
    sys.stdin.read()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(store.path), key],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "transaction-open"
        child.kill()
        child.wait(timeout=5)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        if child.stdin:
            child.stdin.close()
        if child.stdout:
            child.stdout.close()
    recovered = ProjectStore(store.path)
    assert recovered.search(key, SearchInput(ids=["interrupted"])).matched_count == 0
    assert recovered.continuation(key, None, {}, set()).project.revision == 2
    with recovered.transaction() as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_explicit_checkpoint_cleanup_and_unknown_marker_rollback(
    store: ProjectStore,
) -> None:
    key = populated(store)
    with pytest.raises(ProjectError):
        store.apply(
            key,
            ApplyInput(
                expected_revision=2, forget_checkpoints=["structure-ready", "unknown"]
            ),
        )
    assert store.search(key, SearchInput(kind="checkpoint")).matched_count == 1
    store.apply(
        key, ApplyInput(expected_revision=2, forget_checkpoints=["structure-ready"])
    )
    assert store.search(key, SearchInput(kind="checkpoint")).matched_count == 0
    assert store.delta(key, DeltaInput(since_revision=2)).counts == {
        "checkpoint:removed": 1
    }


def test_open_concern_prioritizes_its_low_importance_entity(
    store: ProjectStore,
) -> None:
    key = populated(store)
    store.apply(
        key,
        ApplyInput(
            expected_revision=2,
            upsert=[
                *[
                    Entity(id=f"a-{i}", label="Background entity", importance=5)
                    for i in range(12)
                ],
                Entity(id="z-critical", label="Important to current issue"),
                Issue(
                    id="concern",
                    label="Current concern",
                    importance=5,
                    severity="critical",
                    entity_ids=["z-critical"],
                ),
            ],
        ),
    )
    packet = store.continuation(key, None, {}, set())
    assert "z-critical" in {v.record.id for v in packet.records}
