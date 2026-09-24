"""Lease identity, routing, cancellation and bounded cleanup across real transport."""

import asyncio
from pathlib import Path

import pytest
from tyvrana_protocol import DocumentAttestation, ProofLease

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.errors import InvalidAdapterBehavior, UnsupportedOperation
from tyvrana_core.projects.store import ProjectError
from tyvrana_core.registry import AdapterInfo

from .test_working_lineage import editor


@pytest.mark.parametrize("outcome", ["success", "error", "cancel", "timeout"])
async def test_owned_proof_cleanup_and_isolation(tmp_path: Path, outcome: str) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core):
            parent = core.registry.get("initial")
            artifact = core.projects.proofs.artifact(
                locator="/fixture.blend", sha256="b" * 64, project_id="saved"
            )
            entered = asyncio.Event()

            async def operation() -> None:
                async with core.projects.proofs.acquire(parent, artifact) as proof:
                    assert [a.instance_id for a in core.registry.list()] == ["initial"]
                    assert len(core.registry.list(include_proofs=True)) == 2
                    assert core.registry.supporting("editor.change") == (parent,)
                    with pytest.raises(UnsupportedOperation):
                        await core.dispatcher.execute(
                            adapter_id=proof.instance_id,
                            operation="editor.change",
                            arguments={},
                        )
                    evidence = await core.projects.continuity.observe(proof)
                    assert evidence.host_session_id == "proof-host"
                    runtime = proof.registration.runtime
                    assert runtime and runtime.proof_lease
                    with pytest.raises(ProjectError, match="already owns"):
                        async with core.projects.proofs.acquire(parent, artifact):
                            raise AssertionError("Concurrent proof admitted")
                    variants: list[dict[str, object]] = [
                        dict(process_id=1),
                        dict(build="f" * 64),
                        dict(
                            proof_lease=ProofLease(
                                lease_id=runtime.proof_lease.lease_id,
                                token="f" * 64,
                                parent_adapter_id="initial",
                            )
                        ),
                    ]
                    for changes in variants:
                        with pytest.raises(InvalidAdapterBehavior):
                            core.projects.proofs.admit(
                                proof.registration.model_copy(
                                    update=dict(
                                        runtime=runtime.model_copy(update=changes)
                                    )
                                )
                            )
                    entered.set()
                    if outcome == "error":
                        raise ValueError("Workflow failed")
                    if outcome in {"cancel", "timeout"}:
                        await asyncio.Event().wait()

            if outcome == "timeout":
                core.projects.proofs.workflow_timeout = 0.3
            task = asyncio.create_task(operation())
            await asyncio.wait_for(entered.wait(), timeout=3)
            if outcome == "cancel":
                task.cancel()
            if outcome == "success":
                await task
            else:
                expected = {
                    "error": ValueError,
                    "cancel": asyncio.CancelledError,
                    "timeout": TimeoutError,
                }[outcome]
                with pytest.raises(expected):
                    await task
            assert [a.instance_id for a in core.registry.list(include_proofs=True)] == [
                "initial"
            ]
            assert not core.projects.proofs.leases
            assert core.projects.proofs.metrics[-1]["cleanup_ok"] is True
            assert core.registry.get("initial").connection_id == parent.connection_id


async def test_public_attestation_cancellation_cleans_owned_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from .test_continuity import establish

    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core):
            project, revision = await establish(core, capture=False)
            core.projects.continuity.caller_wait_seconds = 0.01
            entered = asyncio.Event()
            observe = core.projects.continuity.observe

            async def delayed(adapter: AdapterInfo) -> DocumentAttestation:
                if (
                    adapter.registration.runtime
                    and adapter.registration.runtime.role == "proof"
                ):
                    entered.set()
                    await asyncio.Event().wait()
                return await observe(adapter)

            monkeypatch.setattr(core.projects.continuity, "observe", delayed)
            job = await core.projects.execute(
                "project.attest",
                dict(
                    project_id=project,
                    document_id="doc",
                    adapter_id="initial",
                    expected_revision=revision,
                    mode="bootstrap",
                    trusted_artifact_sha256="b" * 64,
                    provenance="Known artifact",
                ),
            )
            assert job.model_dump()["state"] == "running"
            await asyncio.wait_for(entered.wait(), 3)
            cancelled = await core.projects.execute(
                "project.attest_cancel",
                dict(
                    project_id=project,
                    attestation_id=job.model_dump()["attestation_id"],
                ),
            )
            assert cancelled.model_dump()["state"] == "failed"
            assert core.projects.continuity.baseline(project, "doc") is None
            assert [a.instance_id for a in core.registry.list(include_proofs=True)] == [
                "initial"
            ]
            assert not core.projects.proofs.leases
