"""Bound native failures retain the same machine-readable application evidence."""

import json
from pathlib import Path
from typing import Any

import pytest
from tyvrana_protocol import (
    AdapterRegistration,
    JsonValue,
    OperationSuccess,
    ProtocolError,
)
from tyvrana_protocol.mutations import DocumentMutationRequest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.mcp.tools import _failure
from tyvrana_core.projects.store import ProjectError

from .helpers import contract


@pytest.mark.parametrize(
    "code,details",
    [
        ("surface_degenerate", {"patch_id": "sheet", "thicknesses": [0.3, 0.3, 0.3]}),
        ("form_limit", {"component": "socket", "measured": 1024, "limit": 512}),
        (
            "invalid_arguments",
            [{"field": "forms.0.voxel_size", "reason": "greater_than"}],
        ),
        ("form_revision_conflict", None),
    ],
)
async def test_guarded_failure_retains_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: str, details: JsonValue
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        registration = AdapterRegistration(
            type="adapter.register",
            instance_id="editor",
            application="editor",
            application_version="1",
            operations=(
                contract("editor.mutate").model_copy(
                    update={"tags": ("document_mutation",)}
                ),
                contract("editor.status").model_copy(
                    update={"tags": ("document_mutation_status",)}
                ),
            ),
        )
        error = ProtocolError(code=code, message="Authoring rejected", details=details)

        async def dispatch(**kwargs: Any) -> OperationSuccess:
            return OperationSuccess(
                type="operation.success",
                request_id="wire",
                result=dict(
                    job_id="edit", state="failed", error=error.model_dump(mode="json")
                ),
            )

        monkeypatch.setattr(core.dispatcher, "execute", dispatch)
        args = DocumentMutationRequest(
            mutation_id="edit",
            operation="editor.construct",
            arguments={},
            host_session_id="host",
            document_session_id="doc",
            project_id="project",
            format="fixture",
            digest="a" * 64,
        )
        with pytest.raises(ProjectError) as raised:
            await core.projects.mutations.guarded(registration, args)
        bound = _failure(
            raised.value.code, str(raised.value), raised.value.details, args.operation
        )
        unbound = _failure(error.code, error.message, error.details, args.operation)
        assert bound == unbound
        assert "Traceback" not in str(bound)


def test_error_payload_bound() -> None:
    result = _failure("geometry_invalid", "Rejected", {"samples": ["x" * 1000] * 1000})
    assert len(result.model_dump_json()) < 1024
    value = json.loads(result.content[0].text)  # type: ignore[union-attr]
    assert value["details"] == {"diagnostics_truncated": True, "byte_limit": 16384}


def test_compact_representation_and_recovery_guidance() -> None:
    from tyvrana_core.mcp.server import INSTRUCTIONS

    assert len(INSTRUCTIONS.encode()) < 8000
    assert len(INSTRUCTIONS.split()) < 1150
    for concept in (
        "semantic intent",
        "shells",
        "lofts",
        "constructive",
        "assemblies",
        "invalid_arguments",
        "geometric",
        "stale",
        "Unsupported",
        "internal",
        "evidence-led",
        "downstream",
    ):
        assert concept in INSTRUCTIONS


@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        ProtocolError(
            code="construction_invalid",
            message="Frame.member.03: tangent is parallel to its reference axis",
            details={
                "component": "Frame.member.03",
                "family": "member",
                "index": 3,
                "cause": {
                    "field": "templates.1.spec.x_reference",
                    "reason": "parallel_to_tangent",
                    "value": [0, 1, 0],
                },
            },
        ),
        ProtocolError(
            code="invalid_arguments",
            message="Invalid field",
            details=[{"field": "items.3.depth", "reason": "greater_than", "limit": 0}],
        ),
        ProtocolError(code="rejected", message="Rejected"),
        ProtocolError(
            code="large_diagnostic", message="é" * 10000, details={"data": "x" * 20000}
        ),
        RuntimeError("private internal data" * 10000),
    ],
)
async def test_terminal_diagnostic_matches_immediate_mcp_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pending: bool,
    failure: ProtocolError | RuntimeError,
) -> None:
    import asyncio

    from mcp import Client
    from tyvrana_protocol import OperationRequest

    from tyvrana_core.mcp import create_mcp_server

    from .test_continuity import connected, establish, evidence

    config = CoreConfig(port=0, state_directory=str(tmp_path))
    async with AdapterServer(config) as core:
        async with connected(core, "initial", evidence()):
            project_id, _ = await establish(core)
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
                if isinstance(failure, ProtocolError):
                    raise ProjectError.from_operation(failure)
                raise failure

            monkeypatch.setattr(core.projects.mutations, "_execute", work)
            if not pending:
                release.set()
            with pytest.raises((ProjectError, RuntimeError)) as raised:
                await core.projects.mutations.execute(
                    core.registry.get("initial").registration,
                    OperationRequest(
                        type="operation.request",
                        request_id="construction",
                        operation="editor.change",
                        arguments={},
                    ),
                    0.001 if pending else 20,
                )
            if pending:
                assert isinstance(raised.value, ProjectError)
                assert raised.value.code == "mutation_pending"
            release.set()
            args: dict[str, JsonValue] = dict(
                project_id=project_id, mutation_id="construction", wait_seconds=1
            )
            terminal = await core.projects.execute("project.mutation_status", args)
            result = terminal.model_dump(mode="json")
            assert result["state"] == "uncommitted" and result["revision"] is None
            immediate = (
                _failure(failure.code, failure.message, failure.details)
                if isinstance(failure, ProtocolError)
                else _failure(
                    "internal_error", "Tool execution failed; see the server logs."
                )
            )
            expected = json.loads(immediate.content[0].text)  # type: ignore[union-attr]
            assert result["error_code"] == expected["code"]
            assert result["error_message"] == expected["message"]
            assert result["error_details"] == expected.get("details")
            assert len(json.dumps(result).encode()) < 18000
            assert "private internal data" not in json.dumps(result)
            with core.projects.store.transaction() as db:
                stored = json.loads(
                    db.execute(
                        "SELECT data FROM document_mutations WHERE id='construction'"
                    ).fetchone()[0]
                )
            for name in ("error_code", "error_message", "error_details"):
                assert stored[name] == result[name]
            for _ in range(3):
                assert (
                    await core.projects.execute("project.mutation_status", args)
                ).model_dump(mode="json") == result
            assert calls == 1
    # Read the durable terminal response through a fresh MCP server, without an
    # adapter connection or the task/exception object still in memory.
    async with Client(create_mcp_server(AdapterServer(config))) as client:
        for _ in range(2):
            response = await client.call_tool(
                "tyvrana_execute_operation",
                {"operation": "project.mutation_status", "arguments": args},
            )
            assert not response.is_error
            assert response.structured_content["result"] == result
