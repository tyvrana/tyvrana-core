"""Reconciliation owns asynchronous evidence through terminal publication."""

import asyncio
import copy
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.models import Continuation, ReconcileResult
from tyvrana_core.projects.reconcile import Reconciliation
from tyvrana_core.projects.store import ProjectError

from .helpers import eventually
from .test_mutation_recovery import working
from .test_working_lineage import editor


@pytest.mark.parametrize(
    "outcome",
    ["complete", "failure", "wrong_id", "reconnect", "restart", "timeout", "cancel"],
)
async def test_pending_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        faults: dict[str, Any] = {}
        async with AsyncExitStack() as stack:
            native = await stack.enter_async_context(editor(core, faults=faults))
            key = await working(core)
            baseline = core.projects.continuity.baseline(key, "doc")
            assert baseline is not None

            def fail_publication(*args: Any, **kwargs: Any) -> None:
                raise ProjectError(
                    "publication_failed", "No semantic receipt published"
                )

            with monkeypatch.context() as patch:
                patch.setattr(core.projects.store, "_invalidate", fail_publication)
                with pytest.raises(ProjectError) as rejected:
                    await core.dispatcher.execute(
                        adapter_id="initial",
                        operation="editor.change",
                        arguments={"mode": "downstream"},
                    )
            assert isinstance(rejected.value.details, dict)
            mid = rejected.value.details["mutation_id"]
            with core.projects.store.transaction() as db:
                revision = core.projects.store.project(db, key).revision
            untouched = copy.deepcopy(native)
            owner = core.projects.reconciliation
            owner.caller_wait_seconds = 0.02
            if outcome == "timeout":
                owner.execution_seconds = 1.0
            faults["pending"] = True
            request = dict(
                project_id=key,
                reconciliation_id="recovery",
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                stage_id="working",
                prior_digest=baseline.digest,
                expected_digest=native["digest"],
                provenance="Recover completed native edit",
                mutation_id=mid,
            )
            pending = await core.projects.execute("project.reconcile", request)
            assert isinstance(pending, ReconcileResult)
            await eventually(lambda: owner.status(key, "recovery").state == "pending")
            pending = owner.status(key, "recovery")
            assert (
                pending.state == "pending" and pending.next_action == "observe_status"
            )
            assert (
                pending.attestation is not None
                and pending.attestation.job_id == "attestation-job"
            )
            assert pending.native_execution == "completed" and not pending.replay_safe
            assert pending.publication == "uncommitted" and pending.error_code is None
            assert pending.status_operation == "project.reconcile_status"
            assert len(pending.model_dump_json()) < 1500
            for _ in range(3):
                assert owner.status(key, "recovery") == pending
            packet = await core.projects.execute(
                "project.continue", {"project_id": key}
            )
            assert isinstance(packet, Continuation) and packet.reconciliations == [
                pending
            ]
            assert len(packet.model_dump_json().encode()) <= 32768
            assert (
                faults["starts"] == 1
            )  # Continuation cannot start duplicate evidence.
            assert core.projects.continuity.baseline(key, "doc") == baseline
            assert faults["native_calls"] == 1
            if outcome == "reconnect":
                await stack.aclose()
                await asyncio.sleep(0.15)  # Poll crosses a disconnected transport.
                await stack.enter_async_context(
                    editor(core, faults=faults, value_override=native)
                )
                assert owner.status(key, "recovery").attestation == pending.attestation
            if outcome == "restart":
                # A restarted coordinator has no task to resume.
                interrupted = owner._read(key, "recovery")
                assert interrupted is not None
                await owner.shutdown()
                # Restore the durable row as seen on abrupt process loss.
                owner._write("recovery", interrupted)
                restarted = Reconciliation(core.projects)
                restart_result = restarted.status(key, "recovery")
                assert (
                    restart_result.state == "failed"
                    and restart_result.error_code == "reconciliation_interrupted"
                )
                assert restart_result.attestation == pending.attestation
                assert restarted.status(key, "recovery") == restart_result
                return
            if outcome == "cancel":
                result = await core.projects.execute(
                    "project.reconcile_cancel",
                    dict(project_id=key, reconciliation_id="recovery"),
                )
            else:
                faults[outcome if outcome in {"failure", "wrong_id"} else "release"] = (
                    True
                )
                if outcome == "timeout":
                    faults["release"] = False
                result = await core.projects.execute(
                    "project.reconcile_status",
                    dict(project_id=key, reconciliation_id="recovery", wait_seconds=2),
                )
            assert isinstance(result, ReconcileResult)
            assert result.state == (
                "completed" if outcome in {"complete", "reconnect"} else "failed"
            )
            assert (
                result.attestation is not None
                and result.attestation.job_id == "attestation-job"
            )
            for _ in range(3):
                assert owner.status(key, "recovery") == result
            repeated = await core.projects.execute("project.reconcile", request)
            assert (
                isinstance(repeated, ReconcileResult)
                and repeated.revision == result.revision
            )
            assert faults["starts"] == 1 and faults["native_calls"] == 1
            assert native == untouched
            if result.state == "completed":
                assert (
                    result.revision == revision + 1
                    and result.publication == "committed"
                )
                assert (
                    result.next_action == "save"
                    and result.attestation.state == "completed"
                )
            else:
                assert (
                    result.next_action == "inspect_failure"
                    and result.publication == "uncommitted"
                )
                assert core.projects.continuity.baseline(key, "doc") == baseline
                expected = {
                    "failure": "attestation_incomplete",
                    "wrong_id": "attestation_identity",
                    "timeout": "reconciliation_timeout",
                    "cancel": "reconciliation_interrupted",
                }[outcome]
                assert result.error_code == expected
                if outcome == "failure":
                    assert isinstance(result.error_details, dict)
                    assert result.error_details["error"] == dict(
                        code="content_changed",
                        message="Hash input changed",
                        details={"resource": "mesh"},
                    )
