"""Authored prerequisite contracts, provisional work and selective invalidation."""

from pathlib import Path
from typing import Any

import pytest

from tyvrana_core.projects.models import (
    ApplyInput,
    ApplyResult,
    Binding,
    BindingObservation,
    CreateInput,
    Document,
    Entity,
    Evidence,
    Issue,
    Milestone,
    ProjectPatch,
    SearchInput,
    Validation,
)
from tyvrana_core.projects.store import ProjectError, ProjectStore


@pytest.fixture
def contract(tmp_path: Path) -> tuple[ProjectStore, str]:
    store = ProjectStore(tmp_path / "projects.sqlite3")
    project = store.create(
        CreateInput(title="Assembly", goal="Verified reusable result")
    )
    store.apply(
        project.id,
        ApplyInput(
            expected_revision=1,
            upsert=[
                Entity(id="foundation", label="Foundation", tags=["provisional"]),
                Entity(id="surface", label="Surface"),
                Entity(id="independent", label="Independent output"),
                Milestone(
                    id="a",
                    label="Foundation ready",
                    entity_ids=["foundation"],
                    status="in_progress",
                    validation_ids=["qa"],
                    acceptance="Measured within tolerance",
                ),
                Milestone(
                    id="b",
                    label="Surface ready",
                    entity_ids=["surface"],
                    prerequisite_ids=["a"],
                    validation_ids=["qb"],
                    acceptance="Inspected fit",
                ),
                Milestone(
                    id="c",
                    label="Separate output",
                    entity_ids=["independent"],
                    validation_ids=["qc"],
                    acceptance="Measured dimensions",
                ),
                *[
                    Validation(
                        id=f"q{x}",
                        label=f"Check {x}",
                        validation_type="inspection",
                        entity_ids=[entity],
                    )
                    for x, entity in [
                        ("a", "foundation"),
                        ("b", "surface"),
                        ("c", "independent"),
                    ]
                ],
            ],
            project=ProjectPatch(stage="a"),
        ),
    )
    return store, project.id


def current(store: ProjectStore, key: str, record_id: str) -> Any:
    return store.search(key, SearchInput(ids=[record_id])).records[0].record


def apply(store: ProjectStore, key: str, **kwargs: Any) -> ApplyResult:
    revision = store.search(key, SearchInput()).revision
    assert revision is not None
    return store.apply(key, ApplyInput(expected_revision=revision, **kwargs))


def accept(store: ProjectStore, key: str, stage: str) -> None:
    milestone = current(store, key, stage)
    validation = current(store, key, f"q{stage}")
    apply(
        store,
        key,
        upsert=[
            Evidence(
                id=f"e{stage}",
                label="Measurement report",
                storage="external",
                uri=f"https://example.test/reports/{stage}",
                summary="Observed dimensions and motion meet the recorded tolerances.",
            ),
            validation.model_copy(
                update={
                    "status": "passed",
                    "freshness": "current",
                    "evidence_ids": [f"e{stage}"],
                    "summary": "Measurements meet acceptance criteria.",
                }
            ),
            milestone.model_copy(update={"status": "accepted"}),
        ],
        project=ProjectPatch(stage=""),
    )


def test_prerequisites_persist_and_provisional_work_is_not_acceptance(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    restarted = ProjectStore(store.path)
    assert current(restarted, key, "b").prerequisite_ids == ["a"]
    assert current(restarted, key, "foundation").tags == ["provisional"]
    for status in ["in_progress", "accepted"]:
        before = store.continuation(key, None, {}, set())
        with pytest.raises(ProjectError) as error:
            apply(
                store,
                key,
                upsert=[current(store, key, "b").model_copy(update={"status": status})],
            )
        assert error.value.code == "milestone_blocked"
        assert any(b["record_id"] == "a" for b in error.value.details["blockers"])
        assert store.continuation(key, None, {}, set()) == before


@pytest.mark.parametrize(
    "status,freshness",
    [
        ("failed", "current"),
        ("unknown", "current"),
        ("passed", "stale"),
        ("passed", "unverified"),
    ],
)
def test_required_validation_must_pass_and_be_current(
    contract: tuple[ProjectStore, str], status: str, freshness: str
) -> None:
    store, key = contract
    accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[
            current(store, key, "qa").model_copy(
                update={"status": status, "freshness": freshness}
            )
        ],
    )
    assert current(store, key, "a").status == "invalidated"
    with pytest.raises(ProjectError) as error:
        apply(
            store,
            key,
            upsert=[
                current(store, key, "b").model_copy(update={"status": "in_progress"})
            ],
            project=ProjectPatch(stage="b"),
        )
    assert any(b["record_id"] == "qa" for b in error.value.details["blockers"])


def test_existence_or_status_assertion_cannot_pass_evidence_gate(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    with pytest.raises(ProjectError) as error:
        apply(
            store,
            key,
            upsert=[
                current(store, key, "qa").model_copy(
                    update={"status": "passed", "freshness": "current"}
                ),
                current(store, key, "a").model_copy(update={"status": "accepted"}),
            ],
            project=ProjectPatch(stage=""),
        )
    assert any(
        b["reason"] == "validation:observations_and_evidence_required"
        for b in error.value.details["blockers"]
    )
    # A declaration cannot omit all checks and still call the milestone accepted.
    with pytest.raises(ProjectError):
        apply(
            store,
            key,
            upsert=[
                Milestone(id="name-only", label="Object exists", status="accepted")
            ],
        )


def test_current_evidence_allows_progress_and_upstream_change_is_selective(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[current(store, key, "b").model_copy(update={"status": "in_progress"})],
        project=ProjectPatch(stage="b"),
    )
    accept(store, key, "b")
    accept(store, key, "c")
    apply(store, key, upsert=[Entity(id="foundation", label="Changed foundation")])
    assert current(store, key, "a").status == "invalidated"
    assert current(store, key, "b").status == "invalidated"
    assert current(store, key, "qa").freshness == "stale"
    assert current(store, key, "qb").freshness == "stale"
    assert current(store, key, "c").status == "accepted"
    assert current(store, key, "qc").freshness == "current"
    # Merely reaccepting the prerequisite does not resurrect downstream acceptance.
    accept(store, key, "a")
    assert current(store, key, "b").status == "invalidated"


def test_evidence_change_invalidates_dependents(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    accept(store, key, "a")
    accept(store, key, "b")
    apply(
        store,
        key,
        upsert=[
            current(store, key, "ea").model_copy(update={"summary": "Revised report"})
        ],
    )
    assert current(store, key, "qa").freshness == "stale"
    assert current(store, key, "b").status == "invalidated"
    assert current(store, key, "qb").freshness == "stale"


def test_open_issue_allows_own_repair_but_blocks_acceptance_and_downstream(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[Issue(id="defect", label="Repair needed", entity_ids=["foundation"])],
    )
    assert current(store, key, "a").status == "invalidated"
    apply(
        store,
        key,
        upsert=[current(store, key, "a").model_copy(update={"status": "in_progress"})],
        project=ProjectPatch(stage="a"),
    )
    with pytest.raises(ProjectError):
        accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[
            current(store, key, "defect").model_copy(update={"status": "resolved"})
        ],
    )
    accept(store, key, "a")
    accept(store, key, "b")


def test_cycles_and_dangling_references_are_atomic(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    for prerequisite, code in [
        ("b", "prerequisite_cycle"),
        ("missing", "invalid_reference"),
    ]:
        with pytest.raises(ProjectError) as error:
            apply(
                store,
                key,
                upsert=[
                    current(store, key, "a").model_copy(
                        update={"prerequisite_ids": [prerequisite]}
                    )
                ],
            )
        assert error.value.code == code
    assert current(store, key, "a").prerequisite_ids == []


def test_continuation_reports_stale_prerequisite_and_blocker_compactly(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[current(store, key, "b").model_copy(update={"status": "in_progress"})],
        project=ProjectPatch(stage="b"),
    )
    apply(
        store,
        key,
        upsert=[
            Entity(id="foundation", label="Changed foundation"),
            Issue(id="fix", label="Repair prerequisite", entity_ids=["foundation"]),
        ],
    )
    packet = store.continuation(key, None, {}, set())
    assert packet.project.stage == packet.stage_state.milestone_id == "b"
    assert {b.record_id for b in packet.stage_state.blockers} >= {"a", "qa", "fix"}
    assert any(b.reason == "validation:stale" for b in packet.stage_state.blockers)
    assert len(packet.model_dump_json().encode()) < 10000


def test_bound_scope_preserves_accepted_prerequisite_and_rejects_switches(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[
            Document(
                id="doc",
                label="Document",
                application="editor",
                application_project_id="saved",
                adapter_id="intended",
            ),
            current(store, key, "b").model_copy(
                update={"status": "in_progress", "document_ids": ["doc"]}
            ),
        ],
        project=ProjectPatch(stage="b"),
    )
    for document_id, adapter_id in [("different", "intended"), ("saved", "other")]:
        with pytest.raises(ProjectError) as error:
            store.prepare_mutation("editor", document_id, adapter_id, {}, set())
        assert error.value.code == "project_binding_mismatch"
    store.prepare_mutation("editor", "saved", "intended", {"doc": "connection"}, set())
    assert current(store, key, "a").status == "accepted"
    assert current(store, key, "qa").freshness == "current"
    apply(store, key, upsert=[Entity(id="foundation", label="Changed")])
    with pytest.raises(ProjectError) as error:
        store.prepare_mutation(
            "editor", "saved", "intended", {"doc": "connection"}, set()
        )
    assert error.value.code == "milestone_blocked"
    # Unbound one-step work has no setup requirement.
    store.prepare_mutation("another-editor", None, "unbound", {}, set())


def test_reconnect_does_not_reuse_accepted_application_evidence(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    apply(
        store,
        key,
        upsert=[
            Document(
                id="doc",
                label="Document",
                application="editor",
                application_project_id="saved",
                adapter_id="intended",
            ),
            Binding(
                id="binding",
                label="Resource",
                entity_id="foundation",
                document_id="doc",
                resource_kind="mesh",
                resource_id="opaque",
            ),
            Evidence(
                id="ea",
                label="Inspected geometry",
                storage="application",
                binding_id="binding",
                summary="Observed measured geometry.",
            ),
        ],
    )
    revision = store.search(key, SearchInput()).revision
    assert revision is not None
    store.apply(
        key,
        ApplyInput(
            expected_revision=revision,
            upsert=[
                current(store, key, "qa").model_copy(
                    update={
                        "status": "passed",
                        "freshness": "current",
                        "evidence_ids": ["ea"],
                        "summary": "Measurements meet criteria",
                    }
                ),
                current(store, key, "a").model_copy(update={"status": "accepted"}),
            ],
            project=ProjectPatch(stage=""),
        ),
        {"doc": "first"},
        observations={
            "binding": BindingObservation(
                state="verified", connection_id="first", fingerprint="x"
            )
        },
    )
    packet = store.continuation(key, None, {"doc": "second"}, set())
    observed = next(v.record for v in packet.records if v.record.id == "a")
    assert isinstance(observed, Milestone) and observed.status == "invalidated"
    assert (
        store.search(
            key, SearchInput(status="accepted"), {"doc": "second"}
        ).matched_count
        == 0
    )
    assert (
        store.search(
            key, SearchInput(status="invalidated"), {"doc": "second"}
        ).matched_count
        == 1
    )
    with pytest.raises(ProjectError):
        store.apply(
            key,
            ApplyInput(
                expected_revision=packet.project.revision,
                upsert=[
                    current(store, key, "b").model_copy(
                        update={"status": "in_progress"}
                    )
                ],
                project=ProjectPatch(stage="b"),
            ),
            {"doc": "second"},
        )


def test_pause_and_scope_are_enforced_without_blocking_own_repairs(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    apply(
        store,
        key,
        upsert=[
            Document(
                id="doc",
                label="Document",
                application="editor",
                application_project_id="saved",
                adapter_id="intended",
            )
        ],
    )
    with pytest.raises(ProjectError) as error:
        store.prepare_mutation("editor", "saved", "intended", {}, set())
    assert error.value.code == "stage_scope_required"
    apply(
        store,
        key,
        upsert=[
            current(store, key, "a").model_copy(update={"document_ids": ["doc"]}),
            Issue(id="fix", label="Fix foundation", entity_ids=["foundation"]),
        ],
    )
    store.prepare_mutation("editor", "saved", "intended", {"doc": "connection"}, set())
    apply(store, key, project=ProjectPatch(stage=""))
    with pytest.raises(ProjectError) as error:
        store.prepare_mutation("editor", "saved", "intended", {}, set())
    assert error.value.code == "active_stage_required"


def test_expired_artifact_cannot_support_required_validation(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    accept(store, key, "a")
    apply(
        store,
        key,
        upsert=[
            Evidence(
                id="ea",
                label="Released image",
                storage="ephemeral_artifact",
                artifact_id="a" * 32,
                summary="Inspected image before release.",
            )
        ],
    )
    assert current(store, key, "a").status == "invalidated"
    with pytest.raises(ProjectError) as error:
        apply(
            store,
            key,
            upsert=[
                current(store, key, "qa").model_copy(update={"freshness": "current"}),
                current(store, key, "a").model_copy(update={"status": "accepted"}),
            ],
        )
    assert any(
        b["reason"] == "evidence:expired" for b in error.value.details["blockers"]
    )


def test_semantic_contract_has_no_domain_pipeline_or_obsolete_stage_fields() -> None:
    import json

    from pydantic import ValidationError

    from tyvrana_core.projects.catalog import CONTRACTS
    from tyvrana_core.projects.models import CreateInput, Record, SearchInput

    schemas = json.dumps([c.arguments_schema for c in CONTRACTS]).lower()
    for domain in [
        "mallard",
        "duck",
        "bird",
        "skeleton",
        "muscle",
        "feather",
        "biological",
    ]:
        assert domain not in schemas
    assert len(CONTRACTS) == 7
    assert {c.name for c in CONTRACTS} == {
        "project." + suffix
        for suffix in [
            "create",
            "apply",
            "continue",
            "search",
            "delta",
            "verify",
            "remove",
        ]
    }
    for fields in [
        Record.model_fields,
        CreateInput.model_fields,
        SearchInput.model_fields,
    ]:
        assert "stage" not in fields
    with pytest.raises(ValidationError):
        Entity.model_validate(
            {"id": "old", "label": "Old free-text grouping", "stage": "phase"}
        )


def test_required_checks_inherit_scope_and_criteria_changes_require_revalidation(
    contract: tuple[ProjectStore, str],
) -> None:
    store, key = contract
    apply(
        store,
        key,
        upsert=[current(store, key, "qa").model_copy(update={"entity_ids": []})],
    )
    accept(store, key, "a")
    accept(store, key, "b")
    apply(store, key, upsert=[Entity(id="foundation", label="Revised")])
    assert current(store, key, "qa").freshness == "stale"
    accept(store, key, "a")
    with pytest.raises(ProjectError) as error:
        apply(
            store,
            key,
            upsert=[
                current(store, key, "a").model_copy(
                    update={"acceptance": "Stricter tolerance"}
                )
            ],
        )
    assert any(
        b["record_id"] == "qa" and b["reason"] == "validation:stale"
        for b in error.value.details["blockers"]
    )
    # Criteria may change provisionally, preserving the need for new evidence.
    apply(
        store,
        key,
        upsert=[
            current(store, key, "a").model_copy(
                update={"acceptance": "Stricter tolerance", "status": "in_progress"}
            )
        ],
        project=ProjectPatch(stage="a"),
    )
    assert current(store, key, "qa").freshness == "stale"
    assert current(store, key, "b").status == "invalidated"
