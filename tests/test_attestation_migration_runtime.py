"""Migration pins document identity while proof hosts and telemetry change."""

import copy
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest
from tyvrana_protocol import DocumentAttestation

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.errors import OperationTimeout
from tyvrana_core.projects.store import ProjectError
from tyvrana_core.registry import AdapterInfo

from .helpers import eventually
from .proof_fixture import proof_artifact
from .test_attestation_migration import accepted_chain, new_format, request, snapshot
from .test_continuity import state
from .test_working_lineage import editor


def work(elapsed: float) -> dict[str, Any]:
    cost = dict(
        category="meshes",
        resource="Mesh",
        resources=1,
        stream_bytes=1,
        stream_items=1,
        bulk_elements=1,
        elapsed_ms=elapsed,
    )
    return dict(
        exceeded=None,
        stream_bytes=1,
        stream_items=1,
        bulk_elements=1,
        resources_completed=1,
        peak_buffer_bytes=1,
        current_category="meshes",
        current_resource="Mesh",
        categories=[cost],
        heaviest=[cost],
        limits=dict(
            stream_bytes=100,
            stream_items=100,
            bulk_elements=100,
            buffer_bytes=100,
            resources=10,
            elapsed_ms=10000,
            nesting=10,
        ),
    )


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "registry_churn",
        "content",
        "document",
        "host",
        "transport",
        "instance",
        "proof_failure",
        "proof_timeout",
        "repeat",
    ],
)
async def test_migration_runtime_continuity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with AsyncExitStack() as stack:
            live_stack = await stack.enter_async_context(AsyncExitStack())
            live = await live_stack.enter_async_context(editor(core))
            project, revision = await accepted_chain(core)
            new_format(live)
            proof_value = copy.deepcopy(live)
            proof_value["host_session_id"] = "proof-host"
            if case == "proof_failure":
                proof_value.update(
                    status="unsupported", digest=None, omissions=["Unavailable proof"]
                )
            await stack.enter_async_context(proof_artifact(core, proof_value))
            live["work"] = work(1.25)
            baseline = core.projects.continuity.baseline(project, "doc")
            before = snapshot(core)
            observe = core.projects.continuity.observe
            calls: list[str] = []
            proof_started = False
            proof_finished = False

            async def observed(adapter: AdapterInfo) -> DocumentAttestation:
                nonlocal proof_started, proof_finished
                is_proof = (
                    adapter.registration.runtime is not None
                    and adapter.registration.runtime.role == "proof"
                )
                calls.append("proof" if is_proof else adapter.instance_id)
                if is_proof and case == "proof_timeout":
                    raise OperationTimeout("proof", "attest", 1)
                result = await observe(adapter)
                if adapter.instance_id == "initial" and not proof_started:
                    proof_started = True
                elif is_proof and not proof_finished:
                    proof_finished = True
                    live["work"] = work(9.75)
                    if case == "content":
                        live["digest"] = "d" * 64
                    elif case == "document":
                        live["document_session_id"] = "new-document"
                    elif case == "host":
                        live["host_session_id"] = "new-host"
                    elif case in {"transport", "instance"}:
                        preserved = copy.deepcopy(live)
                        await live_stack.aclose()
                        await eventually(
                            lambda: all(
                                a.instance_id != "initial" for a in core.registry.list()
                            )
                        )
                        replacement = await stack.enter_async_context(
                            editor(
                                core,
                                "initial" if case == "transport" else "replacement",
                            )
                        )
                        replacement.update(preserved)
                    if case == "registry_churn":
                        distractor = await stack.enter_async_context(
                            editor(core, "latest-proof")
                        )
                        new_format(distractor)
                return result

            monkeypatch.setattr(core.projects.continuity, "observe", observed)
            if case in {
                "content",
                "document",
                "host",
                "proof_failure",
                "proof_timeout",
            }:
                expected = OperationTimeout if case == "proof_timeout" else ProjectError
                with pytest.raises(expected) as error:
                    await core.projects.execute(
                        "project.attest", request(project, revision)
                    )
                if isinstance(error.value, ProjectError):
                    assert error.value.code == (
                        "attestation_incomplete"
                        if case == "proof_failure"
                        else "application_changed"
                    )
                assert core.projects.continuity.baseline(project, "doc") == baseline
                assert snapshot(core) == before
                return
            result = (
                await core.projects.execute(
                    "project.attest", request(project, revision)
                )
            ).model_dump()
            assert result["baseline_migrated"]
            assert result["adapter_id"] == (
                "replacement" if case == "instance" else "initial"
            )
            assert calls[:2] == ["initial", "proof"]
            assert calls[-1] == result["adapter_id"]
            assert snapshot(core) == before
            assert await state(core, project) == ("accepted", revision)
            if case == "repeat":
                migrated = core.projects.continuity.baseline(project, "doc")
                repeated = (
                    await core.projects.execute(
                        "project.attest", request(project, revision)
                    )
                ).model_dump()
                assert repeated["already_migrated"]
                assert not repeated["semantic_revision_changed"]
                assert core.projects.continuity.baseline(project, "doc") == migrated
                assert snapshot(core) == before
