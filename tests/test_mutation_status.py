"""Bounded observation of guarded work without replay or caller cancellation."""

import asyncio
import json
from pathlib import Path

import pytest
from mcp import Client
from tyvrana_protocol import (
    AdapterRegistration,
    JsonValue,
    OperationRequest,
    OperationSuccess,
)

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.mcp import create_mcp_server
from tyvrana_core.projects.store import ProjectError

from .test_continuity import connected, establish, evidence


@pytest.mark.parametrize("outcome", ["completed", "failed", "cancelled"])
async def test_pending_status_wait_retains_terminal_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    config = CoreConfig(port=0, state_directory=str(tmp_path))
    async with AdapterServer(config) as core:
        async with connected(core, "initial", evidence()):
            key, _ = await establish(core)
            release = asyncio.Event()
            calls = 0

            async def work(
                registration: AdapterRegistration,
                request: OperationRequest,
                project_id: str,
            ) -> OperationSuccess:
                nonlocal calls
                calls += 1
                await release.wait()
                if outcome == "failed":
                    raise ProjectError("fixture_failure", "Native work failed")
                with core.projects.store.transaction(write=True) as db:
                    row = db.execute(
                        "SELECT data FROM document_mutations WHERE id=?",
                        (request.request_id,),
                    ).fetchone()
                    intent = json.loads(row[0])
                    intent.update(state="completed", revision=3)
                    db.execute(
                        "UPDATE document_mutations SET data=? WHERE id=?",
                        (json.dumps(intent), request.request_id),
                    )
                return OperationSuccess(
                    type="operation.success", request_id=request.request_id, result={}
                )

            monkeypatch.setattr(core.projects.mutations, "_execute", work)
            with pytest.raises(ProjectError) as pending:
                await core.projects.mutations.execute(
                    core.registry.get("initial").registration,
                    OperationRequest(
                        type="operation.request",
                        request_id="retained",
                        operation="editor.change",
                        arguments={},
                    ),
                    0.001,
                )
            assert pending.value.code == "mutation_pending"
            assert pending.value.details == {
                "mutation_id": "retained",
                "project_id": key,
                "status_operation": "project.mutation_status",
            }
            arguments: dict[str, JsonValue] = {
                "project_id": key,
                "mutation_id": "retained",
            }
            status = await core.projects.execute("project.mutation_status", arguments)
            assert status.model_dump()["state"] == "pending"
            waiter = asyncio.create_task(
                core.projects.execute(
                    "project.mutation_status", {**arguments, "wait_seconds": 20}
                )
            )
            await asyncio.sleep(0)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            task = core.projects.mutations.tasks["retained"][1]
            assert not task.done()
            if outcome == "cancelled":
                task.cancel()
            else:
                release.set()
            terminal = await core.projects.execute(
                "project.mutation_status", {**arguments, "wait_seconds": 1}
            )
            result = terminal.model_dump()
            assert result["state"] == (
                "completed" if outcome == "completed" else "uncommitted"
            )
            assert result["revision"] == (3 if outcome == "completed" else None)
            assert (
                result["error_code"]
                == {
                    "completed": None,
                    "failed": "fixture_failure",
                    "cancelled": "cancelled",
                }[outcome]
            )
            assert calls == 1
    # A new Core has no task cache: the same terminal truth is durable.
    async with Client(create_mcp_server(AdapterServer(config))) as client:
        schemas = await client.call_tool(
            "tyvrana_list_operations",
            {
                "adapter_id": "core",
                "names": ["project.mutation_status"],
                "schemas": "full",
            },
        )
        assert not schemas.is_error
        recovered = await client.call_tool(
            "tyvrana_execute_operation",
            {"operation": "project.mutation_status", "arguments": arguments},
        )
        assert not recovered.is_error
        assert recovered.structured_content["result"] == result


async def test_status_scope_and_interrupted_receipt(tmp_path: Path) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence()):
            key, _ = await establish(core)
        other = await core.projects.execute(
            "project.create", {"title": "Other", "goal": "Separate project"}
        )
        with core.projects.store.transaction(write=True) as db:
            intent = dict(state="pending", operation="editor.change", revision=2)
            db.execute(
                "INSERT INTO document_mutations VALUES (?,?,?,?)",
                ("abandoned", key, "doc", json.dumps(intent)),
            )
        for project_id, mutation_id in [
            (key, "missing"),
            (other.model_dump()["id"], "abandoned"),
        ]:
            with pytest.raises(ProjectError, match="No mutation"):
                await core.projects.execute(
                    "project.mutation_status",
                    {"project_id": project_id, "mutation_id": mutation_id},
                )
        observed = await core.projects.execute(
            "project.mutation_status", {"project_id": key, "mutation_id": "abandoned"}
        )
        assert observed.model_dump()["state"] == "interrupted"
        assert observed.model_dump()["revision"] is None
        with core.projects.store.transaction() as db:
            raw = db.execute(
                "SELECT data FROM document_mutations WHERE id='abandoned'"
            ).fetchone()[0]
            assert json.loads(raw) == intent  # Observation does not rewrite history.


async def test_immediate_pre_dispatch_failure_has_terminal_receipt(
    tmp_path: Path,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence()):
            key, _ = await establish(core)
            # This adapter attests but has no guarded mutation contract. Failure
            # happens before a native intent can be dispatched.
            with pytest.raises(ProjectError) as failure:
                await core.projects.mutations.execute(
                    core.registry.get("initial").registration,
                    OperationRequest(
                        type="operation.request",
                        request_id="unsupported",
                        operation="editor.change",
                        arguments={},
                    ),
                    20,
                )
            assert failure.value.code == "mutation_unsupported"
            result = await core.projects.execute(
                "project.mutation_status",
                {"project_id": key, "mutation_id": "unsupported", "wait_seconds": 20},
            )
            assert result.model_dump()["state"] == "uncommitted"
            assert result.model_dump()["error_code"] == "mutation_unsupported"
            assert result.model_dump()["revision"] is None
