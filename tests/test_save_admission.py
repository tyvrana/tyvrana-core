"""Matching slow evidence admits one save; missing/changed evidence never does."""

import asyncio
import json
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.models import (
    MutationStatusInput,
    ReconcileResult,
)
from tyvrana_core.projects.store import ProjectError

from .test_mutation_recovery import working
from .test_working_lineage import editor


@pytest.mark.parametrize(
    "case",
    [
        "clean",
        "reconciled",
        "metadata",
        "digest",
        "resource",
        "document",
        "format",
        "generation",
        "wrong_job",
        "reconnect",
        "timeout",
        "native_guard",
    ],
)
async def test_save_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        faults: dict[str, Any] = {}
        async with AsyncExitStack() as stack:
            native = await stack.enter_async_context(editor(core, faults=faults))
            key = await working(core)
            owner = core.projects.mutations
            baseline = core.projects.continuity.baseline(key, "doc")
            assert baseline is not None
            if case != "clean":

                def fail(*args: Any, **kwargs: Any) -> None:
                    raise ProjectError("publication_failed", "Native work completed")

                with monkeypatch.context() as patch:
                    patch.setattr(core.projects.store, "_invalidate", fail)
                    with pytest.raises(ProjectError) as rejected:
                        await core.dispatcher.execute(
                            adapter_id="initial",
                            operation="editor.change",
                            arguments={"mode": "downstream"},
                        )
                assert isinstance(rejected.value.details, dict)
                with core.projects.store.transaction() as db:
                    revision = core.projects.store.project(db, key).revision
                reconciled = await core.projects.execute(
                    "project.reconcile",
                    dict(
                        project_id=key,
                        reconciliation_id="recover",
                        expected_revision=revision,
                        document_id="doc",
                        adapter_id="initial",
                        stage_id="working",
                        prior_digest=baseline.digest,
                        expected_digest=native["digest"],
                        provenance="Recover completed work",
                        mutation_id=rejected.value.details["mutation_id"],
                    ),
                )
                assert (
                    isinstance(reconciled, ReconcileResult)
                    and reconciled.state == "completed"
                )
                assert reconciled.next_action == "save"
                baseline = core.projects.continuity.baseline(key, "doc")
                assert baseline is not None and baseline.digest == native["digest"]
                if case == "metadata":
                    await core.projects.execute(
                        "project.apply",
                        dict(
                            project_id=key,
                            expected_revision=reconciled.revision,
                            project=dict(next_action="Preserve current work"),
                        ),
                    )
            if case == "timeout":
                owner.admission_seconds = 0.2
            faults["pending"] = True
            with pytest.raises(ProjectError) as pending:
                await core.dispatcher.execute(
                    adapter_id="initial",
                    operation="editor.file.save",
                    arguments={},
                    timeout=0.02,
                )
            assert pending.value.code == "mutation_pending"
            assert isinstance(pending.value.details, dict)
            mid = pending.value.details["mutation_id"]
            assert isinstance(mid, str)
            query = MutationStatusInput(project_id=key, mutation_id=mid)
            before = await owner.status(key, query)
            assert before.state == "pending" and before.next_action == "observe_status"
            assert before.native_execution == "not_started" and not before.replay_safe
            assert before.recovery_operation is None
            assert before.before_digest == baseline.digest
            assert before.admission_attestation is not None
            assert before.admission_attestation.job_id == "attestation-job"
            assert len(before.model_dump_json().encode()) < 1800
            for _ in range(3):
                assert await owner.status(key, query) == before
            assert not faults.get("save_calls")
            if case == "digest":
                native["digest"] = "e" * 64
            if case == "resource":
                native["resources"][0]["resource_id"] = "foreign"
            if case == "document":
                native["document_session_id"] = "foreign"
            if case == "format":
                native["format"] = "foreign"
            if case == "generation":
                faults.update(failure=True, job_error="application_changed")
            if case == "wrong_job":
                faults["wrong_id"] = True
            if case == "native_guard":
                faults["guard_failure"] = True
            if case == "reconnect":
                await stack.aclose()
                await asyncio.sleep(0.15)
                await stack.enter_async_context(
                    editor(core, faults=faults, value_override=native)
                )
            faults["release"] = case != "timeout"
            final = await owner.status(
                key, query.model_copy(update={"wait_seconds": 2.0})
            )
            success = case in {"clean", "reconciled", "metadata", "reconnect"}
            assert final.state == ("completed" if success else "uncommitted"), final
            assert faults.get("save_calls", 0) == (1 if success else 0)
            assert final.admission_attestation is not None
            assert (
                final.admission_attestation.job_id
                == before.admission_attestation.job_id
            )
            assert faults["starts"] == 1
            for _ in range(3):
                assert await owner.status(key, query) == final
            if success:
                assert (
                    final.next_action == "inspect_result"
                    and final.after_digest == baseline.digest
                )
            else:
                assert final.native_execution == "not_started" and final.replay_safe
                assert final.recovery_operation is None and final.recovery_proof is None
                assert final.next_action == "inspect_failure"
                assert final.error_code == {
                    "generation": "attestation_incomplete",
                    "wrong_job": "attestation_identity",
                    "timeout": "admission_timeout",
                }.get(case, "content_diverged")
                assert core.projects.continuity.baseline(key, "doc") == baseline
            with core.projects.store.transaction() as db:
                entries = [
                    json.loads(row[0])
                    for row in db.execute(
                        "SELECT data FROM document_mutations WHERE project_id=?", (key,)
                    )
                ]
            assert sum(e.get("kind") == "reconciliation" for e in entries) == (
                0 if case == "clean" else 1
            )
            assert sum(e.get("operation") == "editor.change" for e in entries) == (
                0 if case == "clean" else 1
            )
