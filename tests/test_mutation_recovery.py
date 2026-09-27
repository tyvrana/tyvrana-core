"""Native proof survives failed semantic publication without mutation replay."""

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.models import MutationStatusInput
from tyvrana_core.projects.mutations import WorkingMutations
from tyvrana_core.projects.reconcile_models import ReconcileResult
from tyvrana_core.projects.store import ProjectError

from .test_continuity import establish
from .test_working_lineage import editor


async def working(core: AdapterServer) -> str:
    key, revision = await establish(core)
    await core.projects.execute(
        "project.apply",
        dict(
            project_id=key,
            expected_revision=revision,
            project=dict(stage="working"),
            upsert=[
                dict(kind="entity", id="mechanism", label="Mechanism"),
                dict(
                    kind="milestone",
                    id="working",
                    label="Mechanism work",
                    entity_ids=["mechanism"],
                    document_ids=["doc"],
                    prerequisite_ids=["stage"],
                    status="in_progress",
                ),
            ],
        ),
    )
    return key


@pytest.mark.parametrize(
    "case,expected",
    [
        ("recover", None),
        ("interrupted", None),
        ("external", "reconciliation_delta_mismatch"),
        ("document", "reconciliation_lineage"),
        ("resource", "reconciliation_delta_mismatch"),
        ("receipt_id", "reconciliation_receipt"),
        ("receipt_before", "reconciliation_receipt"),
        ("receipt_after", "reconciliation_receipt"),
        ("receipt_resources", "reconciliation_receipt"),
        ("wrong_mutation", "reconciliation_mutation"),
        ("stale_head", "reconciliation_prior_head"),
        ("wrong_delta", "reconciliation_mutation"),
    ],
)
async def test_retained_receipt_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str, expected: str | None
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            key = await working(core)
            baseline = core.projects.continuity.baseline(key, "doc")
            assert baseline is not None

            def fail_publication(*args: Any, **kwargs: Any) -> None:
                raise ProjectError("application_changed", "Publication rejected")

            with monkeypatch.context() as patch:
                patch.setattr(core.projects.store, "_invalidate", fail_publication)
                with pytest.raises(ProjectError) as rejected:
                    await core.dispatcher.execute(
                        adapter_id="initial",
                        operation="editor.change",
                        arguments={"mode": "downstream"},
                    )
            assert rejected.value.code == "application_changed"
            assert isinstance(rejected.value.details, dict)
            mutation_id = rejected.value.details["mutation_id"]
            assert isinstance(mutation_id, str)
            assert native["digest"] == "d" * 64
            coordinator = WorkingMutations(core.projects)
            status = await coordinator.status(
                key, MutationStatusInput(project_id=key, mutation_id=mutation_id)
            )
            assert status.state == "uncommitted"
            assert status.arguments == {"mode": "downstream"}
            compact = await core.projects.mutations.continuation(key)
            assert (
                next(m for m in compact if m.mutation_id == mutation_id).arguments
                is None
            )
            assert status.native_execution == "completed"
            assert status.post_state_attested and not status.replay_safe
            assert status.recovery_operation == "project.reconcile"
            assert status.recovery_proof == "receipt"
            assert status.before_digest == baseline.digest
            assert status.after_digest == native["digest"]
            assert core.projects.continuity.baseline(key, "doc") == baseline
            with core.projects.store.transaction() as db:
                revision = core.projects.store.project(db, key).revision
            request: dict[str, Any] = dict(
                project_id=key,
                reconciliation_id="recover",
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                stage_id="working",
                prior_digest=baseline.digest,
                expected_digest="d" * 64,
                provenance="Recover completed native edit",
                mutation_id=mutation_id,
            )
            if case == "external":
                native["digest"] = "e" * 64
            if case == "document":
                native["document_session_id"] = "other"
            if case == "resource":
                native["resources"][0]["resource_id"] = "other"
            if case == "wrong_mutation":
                request["mutation_id"] = "other"
            if case == "stale_head":
                request["prior_digest"] = "f" * 64
            if case == "wrong_delta":
                request["delta"] = [
                    dict(
                        operation="editor.change",
                        arguments={"mode": "upstream"},
                        owner_entity_id="mechanism",
                    )
                ]
            if case.startswith("receipt_") or case == "interrupted":
                with core.projects.store.transaction(write=True) as db:
                    intent = json.loads(
                        db.execute(
                            "SELECT data FROM document_mutations WHERE id=?",
                            (mutation_id,),
                        ).fetchone()[0]
                    )
                    if case == "interrupted":
                        intent["state"] = "pending"
                    receipt = intent["receipt"]
                    if case == "receipt_id":
                        receipt["mutation_id"] = "other"
                    if case == "receipt_before":
                        receipt["before"]["digest"] = "f" * 64
                    if case == "receipt_after":
                        receipt["after"]["digest"] = "f" * 64
                    if case == "receipt_resources":
                        receipt["before"]["resources"][0]["fingerprint"] = "other"
                    db.execute(
                        "UPDATE document_mutations SET data=? WHERE id=?",
                        (json.dumps(intent), mutation_id),
                    )
            untouched = copy.deepcopy(native)
            result = await core.projects.execute("project.reconcile", request)
            assert isinstance(result, ReconcileResult)
            assert native == untouched
            assert not core.projects.proofs.leases
            if expected:
                assert result.state == "failed"
                assert result.error_code == expected
                assert core.projects.continuity.baseline(key, "doc") == baseline
                return
            assert result.state == "completed"
            assert result.revision == revision + 1
            repeated = await core.projects.execute("project.reconcile", request)
            assert isinstance(repeated, ReconcileResult)
            assert repeated.already_reconciled and repeated.revision == result.revision
            recovered = await coordinator.status(
                key, MutationStatusInput(project_id=key, mutation_id=mutation_id)
            )
            assert (
                recovered.state == "completed" and recovered.reconciled_by == "recover"
            )
            assert recovered.error_code is None
            assert native == untouched
            head = core.projects.continuity.baseline(key, "doc")
            assert head is not None and head.digest == native["digest"]


@pytest.mark.parametrize(
    "case,expected",
    [
        ("recover", None),
        ("missing", "proof_artifact_missing"),
        ("snapshot", "reconciliation_snapshot"),
        ("inverse", "reconciliation_prior_head"),
        ("resource", "reconciliation_prior_head"),
        ("forward", "reconciliation_delta_mismatch"),
    ],
)
async def test_inverse_proof_of_unsaved_head(
    tmp_path: Path, case: str, expected: str | None
) -> None:
    from .proof_fixture import proof_artifact

    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            key = await working(core)
            baseline = core.projects.continuity.baseline(key, "doc")
            assert baseline is not None
            baseline = baseline.model_copy(update={"artifact_sha256": None})
            native["digest"] = "e" * 64 if case == "forward" else "d" * 64
            value = copy.deepcopy(native)
            value["host_session_id"] = "proof-host"
            if case == "snapshot":
                value["digest"] = "f" * 64
            if case == "resource":
                value["resources"][0]["fingerprint"] = "wrong"
                native["resources"][0]["fingerprint"] = "wrong"
            with core.projects.store.transaction(write=True) as db:
                revision = core.projects.store.project(db, key).revision
                db.execute(
                    "UPDATE document_attestations SET data=? "
                    "WHERE project_id=? AND document_id='doc'",
                    (baseline.model_dump_json(), key),
                )
                db.execute(
                    "INSERT INTO document_mutations VALUES (?,?,?,?)",
                    (
                        "edit",
                        key,
                        "doc",
                        json.dumps(
                            dict(
                                state="uncommitted",
                                operation="editor.change",
                                arguments={"mode": "downstream"},
                                stage="working",
                                before=baseline.digest,
                                native_execution="completed",
                                error_code="application_changed",
                            )
                        ),
                    ),
                )
            request = dict(
                project_id=key,
                reconciliation_id="recover",
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                stage_id="working",
                prior_digest=baseline.digest,
                expected_digest=native["digest"],
                provenance="Prove the exact missing transition",
                mutation_id="edit",
                delta=[
                    dict(
                        operation="editor.change",
                        arguments={"mode": "downstream"},
                        owner_entity_id="mechanism",
                    )
                ],
                inverse_delta=[]
                if case == "missing"
                else [
                    dict(
                        operation="editor.change",
                        arguments={
                            "mode": "downstream" if case == "inverse" else "inverse"
                        },
                        owner_entity_id="mechanism",
                    )
                ],
            )
            untouched = copy.deepcopy(native)
            async with proof_artifact(core, value):
                result = await core.projects.execute("project.reconcile", request)
            assert isinstance(result, ReconcileResult)
            assert native == untouched
            assert not core.projects.proofs.leases
            if expected:
                assert result.state == "failed" and result.error_code == expected
                assert core.projects.continuity.baseline(key, "doc") == baseline
            else:
                assert result.state == "completed"
                assert result.revision == revision + 1
                again = await core.projects.execute("project.reconcile", request)
                assert isinstance(again, ReconcileResult)
                assert again.already_reconciled
