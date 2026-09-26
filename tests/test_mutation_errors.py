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
