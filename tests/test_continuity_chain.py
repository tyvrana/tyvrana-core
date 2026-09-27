"""Exact reattachment, retained checkpoint admission and fresh continuation."""

from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.continuity import AttestJob, AttestResult
from tyvrana_core.projects.models import ApplyResult, Continuation, ProjectOperation
from tyvrana_core.projects.store import ProjectError

from .test_mutation_recovery import working
from .test_working_lineage import editor


async def revision(core: AdapterServer, key: str) -> int:
    with core.projects.store.transaction() as db:
        return core.projects.store.project(db, key).revision


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "slow_reattach",
        "slow_checkpoint",
        "slow_continue",
        "reconnect",
        "core_restart",
    ],
)
async def test_saved_reopened_checkpoint(tmp_path: Path, case: str) -> None:
    config = CoreConfig(port=0, state_directory=str(tmp_path))
    async with AsyncExitStack() as stack:
        core = await stack.enter_async_context(AdapterServer(config))
        faults: dict[str, Any] = {}
        native = await stack.enter_async_context(editor(core, faults=faults))
        key = await working(core)
        await core.dispatcher.execute(
            adapter_id="initial",
            operation="editor.change",
            arguments={"mode": "downstream"},
        )
        await core.dispatcher.execute(
            adapter_id="initial", operation="editor.file.save", arguments={}
        )
        saved = core.projects.continuity.baseline(key, "doc")
        assert saved is not None and faults["save_calls"] == 1
        rev = await revision(core, key)
        native["document_session_id"] = "reopened"
        if case in {"reconnect", "core_restart"}:
            await stack.aclose()
            core = await stack.enter_async_context(AdapterServer(config))
            await stack.enter_async_context(
                editor(core, faults=faults, value_override=native)
            )
        arguments: dict[str, Any] = dict(
            project_id=key,
            document_id="doc",
            adapter_id="initial",
            expected_revision=rev,
            mode="reattach",
        )
        core.projects.continuity.caller_wait_seconds = 0.2
        core.projects.operations.caller_wait_seconds = 0.2
        if case == "slow_reattach":
            faults.update(pending=True, release=False)
        result = await core.projects.execute("project.attest", arguments)
        if case == "slow_reattach":
            assert isinstance(result, AttestJob) and result.state == "running"
            repeat = await core.projects.execute("project.attest", arguments)
            assert (
                isinstance(repeat, AttestJob)
                and repeat.attestation_id == result.attestation_id
            )
            packet = await core.projects.execute(
                "project.continue", dict(project_id=key)
            )
            assert isinstance(packet, Continuation)
            assert packet.applications[0].trust == "pending"
            assert packet.attestations[0].attestation_id == result.attestation_id
            assert faults["starts"] == 1
            assert core.projects.continuity.baseline(key, "doc") == saved
            faults["release"] = True
            completed = await core.projects.execute(
                "project.attest_status",
                dict(
                    project_id=key, attestation_id=result.attestation_id, wait_seconds=2
                ),
            )
            assert isinstance(completed, AttestJob) and completed.state == "completed"
            assert completed.result and completed.result.trust == "verified"
            for _ in range(3):
                assert (
                    await core.projects.execute(
                        "project.attest_status",
                        dict(project_id=key, attestation_id=result.attestation_id),
                    )
                    == completed
                )
        else:
            assert isinstance(result, AttestResult)
        assert await revision(core, key) == rev
        rebound = core.projects.continuity.baseline(key, "doc")
        assert (
            rebound
            and rebound.document_session_id == "reopened"
            and rebound.digest == saved.digest
        )
        faults.update(pending=case == "slow_checkpoint", release=False)
        request: dict[str, Any] = dict(
            project_id=key,
            expected_revision=rev,
            checkpoint=dict(id="durable", label="Durable work"),
        )
        applied = await core.projects.execute("project.apply", request)
        if case == "slow_checkpoint":
            assert isinstance(applied, ProjectOperation) and applied.state == "pending"
            duplicate = await core.projects.execute("project.apply", request)
            assert (
                isinstance(duplicate, ProjectOperation)
                and duplicate.operation_id == applied.operation_id
            )
            assert await revision(core, key) == rev
            faults["release"] = True
            status = await core.projects.execute(
                "project.operation_status",
                dict(project_id=key, operation_id=applied.operation_id, wait_seconds=2),
            )
            assert isinstance(status, ProjectOperation) and status.state == "completed"
            for _ in range(3):
                assert (
                    await core.projects.execute(
                        "project.operation_status",
                        dict(project_id=key, operation_id=applied.operation_id),
                    )
                    == status
                )
            assert isinstance(status.result, ApplyResult)
            applied = status.result
        assert isinstance(applied, ApplyResult) and applied.checkpoint
        assert applied.project.revision == rev + 1
        assert applied.checkpoint.document_states["doc"]["digest"] == saved.digest
        faults.update(pending=case == "slow_continue", release=False)
        packet = await core.projects.execute("project.continue", dict(project_id=key))
        if case == "slow_continue":
            assert isinstance(packet, ProjectOperation) and packet.state == "pending"
            faults["release"] = True
            status = await core.projects.execute(
                "project.operation_status",
                dict(project_id=key, operation_id=packet.operation_id, wait_seconds=2),
            )
            assert isinstance(status, ProjectOperation) and status.state == "completed"
            assert isinstance(status.result, Continuation)
            packet = status.result
        assert isinstance(packet, Continuation)
        assert packet.applications[0].trust == "current"
        assert packet.checkpoint and packet.checkpoint.id == "durable"
        assert not packet.operations
        assert faults["save_calls"] == 1


@pytest.mark.parametrize(
    "fault",
    [
        "digest",
        "resource",
        "file",
        "project",
        "generation",
        "wrong_job",
        "baseline_identity",
        "revision",
        "interrupted",
    ],
)
async def test_reattachment_never_promotes_invalid_evidence(
    tmp_path: Path, fault: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        faults: dict[str, Any] = {}
        async with editor(core, faults=faults) as native:
            key = await working(core)
            await core.dispatcher.execute(
                adapter_id="initial", operation="editor.file.save", arguments={}
            )
            baseline = core.projects.continuity.baseline(key, "doc")
            rev = await revision(core, key)
            if fault == "baseline_identity":
                assert baseline is not None
                baseline = baseline.model_copy(
                    update={"application_project_id": "foreign"}
                )
                with core.projects.store.transaction(write=True) as db:
                    db.execute(
                        "UPDATE document_attestations SET data=? WHERE project_id=?",
                        (baseline.model_dump_json(), key),
                    )
                with pytest.raises(ProjectError) as rejected:
                    await core.projects.execute(
                        "project.attest",
                        dict(
                            project_id=key,
                            document_id="doc",
                            adapter_id="initial",
                            expected_revision=rev,
                            mode="reattach",
                        ),
                    )
                assert rejected.value.code == "document_mismatch"
                assert core.projects.continuity.baseline(key, "doc") == baseline
                return
            native["document_session_id"] = "reopened"
            faults.update(pending=True, release=False)
            core.projects.continuity.caller_wait_seconds = 0.2
            result = await core.projects.execute(
                "project.attest",
                dict(
                    project_id=key,
                    document_id="doc",
                    adapter_id="initial",
                    expected_revision=rev,
                    mode="reattach",
                ),
            )
            assert isinstance(result, AttestJob)
            if fault == "digest":
                native["digest"] = "f" * 64
            elif fault == "resource":
                native["resources"][0]["resource_id"] = "wrong"
            elif fault == "file":
                native["file_sha256"] = "f" * 64
            elif fault == "project":
                native["project_id"] = "wrong"
            elif fault == "generation":
                faults.update(failure=True, job_error="application_changed")
            elif fault == "wrong_job":
                faults["wrong_id"] = True
            elif fault == "revision":
                from tyvrana_core.projects.models import ApplyInput, ProjectPatch

                core.projects.store.apply(
                    key,
                    ApplyInput(
                        expected_revision=rev,
                        project=ProjectPatch(next_action="Changed metadata"),
                    ),
                )
            elif fault == "interrupted":
                await core.projects.continuity.shutdown()
            faults["release"] = True
            final = await core.projects.execute(
                "project.attest_status",
                dict(
                    project_id=key, attestation_id=result.attestation_id, wait_seconds=2
                ),
            )
            assert isinstance(final, AttestJob) and final.state == "failed"
            assert final.next_action == "inspect_failure"
            assert core.projects.continuity.baseline(key, "doc") == baseline
            faults.update(pending=False, failure=False, wrong_id=False)
            with pytest.raises(ProjectError):
                await core.projects.execute(
                    "project.apply",
                    dict(
                        project_id=key,
                        expected_revision=await revision(core, key),
                        checkpoint=dict(id="unsafe", label="Must not commit"),
                    ),
                )
            with pytest.raises(ProjectError):
                await core.dispatcher.execute(
                    adapter_id="initial", operation="editor.file.save", arguments={}
                )
            assert faults["save_calls"] == 1


@pytest.mark.parametrize("fault", ["external_edit", "revision", "interruption"])
async def test_pending_checkpoint_never_commits_stale_state(
    tmp_path: Path, fault: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        faults: dict[str, Any] = {}
        async with editor(core, faults=faults) as native:
            key = await working(core)
            rev = await revision(core, key)
            owner = core.projects.operations
            owner.caller_wait_seconds = 0.2
            faults.update(pending=True, release=False)
            result = await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=rev,
                    checkpoint=dict(id="stale", label="Must not commit"),
                ),
            )
            assert isinstance(result, ProjectOperation) and result.state == "pending"
            if fault == "external_edit":
                native["digest"] = "f" * 64
            elif fault == "revision":
                from tyvrana_core.projects.models import ApplyInput, ProjectPatch

                core.projects.store.apply(
                    key,
                    ApplyInput(
                        expected_revision=rev,
                        project=ProjectPatch(next_action="Metadata edit"),
                    ),
                )
            else:
                await owner.shutdown()
            faults["release"] = True
            failed = await owner.observe_status(key, result.operation_id, 2)
            assert failed.state == "failed" and failed.next_action == "inspect_failure"
            assert (
                failed.error_code
                == {
                    "external_edit": "content_diverged",
                    "revision": "revision_conflict",
                    "interruption": "operation_interrupted",
                }[fault]
            )
            with core.projects.store.transaction() as db:
                assert (
                    db.execute(
                        "SELECT count(*) FROM checkpoints WHERE project_id=? "
                        "AND id='stale'",
                        (key,),
                    ).fetchone()[0]
                    == 0
                )
            assert await owner.observe_status(key, result.operation_id, 0) == failed


async def test_pending_binding_verification_waits_before_inspection(
    tmp_path: Path,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        faults: dict[str, Any] = {}
        async with editor(core, faults=faults):
            key = await working(core)
            rev = await revision(core, key)
            core.projects.operations.caller_wait_seconds = 0.2
            faults.update(pending=True, release=False)
            result = await core.projects.execute(
                "project.verify",
                dict(project_id=key, expected_revision=rev, binding_ids=["binding"]),
            )
            assert isinstance(result, ProjectOperation) and result.state == "pending"
            assert not faults.get("inspections")
            assert await revision(core, key) == rev
            faults["release"] = True
            done = await core.projects.operations.observe_status(
                key, result.operation_id, 3
            )
            assert done.state == "completed" and isinstance(done.result, ApplyResult)
            assert done.result.project.revision == rev + 1
            assert faults["inspections"] == 1
            faults["pending"] = False
            packet = await core.projects.execute(
                "project.continue", dict(project_id=key)
            )
            assert isinstance(packet, Continuation)
            binding = next(r for r in packet.records if r.record.id == "binding")
            assert binding.binding and binding.binding.state == "verified"
