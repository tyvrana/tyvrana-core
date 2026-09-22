"""Working content advances only through qualified native receipts."""

import asyncio
import copy
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import pytest
from tyvrana_protocol import (
    AdapterEvent,
    AdapterRegistration,
    DocumentAttestation,
    OperationContract,
    OperationRequest,
    OperationSuccess,
)
from tyvrana_protocol.mutations import DocumentMutationJob, DocumentMutationRequest
from websockets.asyncio.client import connect

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.store import ProjectError

from .helpers import FakeAdapter
from .test_continuity import establish, evidence


@asynccontextmanager
async def editor(
    core: AdapterServer, instance: str = "initial"
) -> AsyncIterator[dict[str, Any]]:
    value = evidence(host="host" if instance == "initial" else "proof-host")
    value.update(
        resource_scope="strong-closure",
        resources=[
            dict(
                resource_kind="mesh",
                resource_id="mesh",
                state="present",
                name="Base",
                fingerprint="base-original",
            ),
        ],
    )
    async with connect(core.uri, proxy=None) as socket:
        fake = FakeAdapter(socket)
        contracts = [
            OperationContract(
                name="editor.document.attest",
                description="Strong evidence",
                tags=("document_attestation",),
                arguments_schema={"type": "object"},
                result_schema=DocumentAttestation.model_json_schema(),
                effect="read_only",
                execution="synchronous",
            ),
            OperationContract(
                name="editor.change",
                tags=("recovery_replay",),
                description="Change authored content",
                arguments_schema={"type": "object"},
                result_schema={"type": "object"},
                effect="mutating",
                execution="synchronous",
            ),
            OperationContract(
                name="editor.document.mutate",
                description="Guard publication",
                tags=("document_mutation",),
                arguments_schema=DocumentMutationRequest.model_json_schema(),
                result_schema=DocumentMutationJob.model_json_schema(),
                effect="mutating",
                execution="job_start",
            ),
            OperationContract(
                name="editor.document.status",
                description="Observe publication",
                tags=("document_mutation_status",),
                arguments_schema={"type": "object"},
                result_schema=DocumentMutationJob.model_json_schema(),
                effect="read_only",
                execution="job_status",
            ),
        ]
        with core.events.subscribe() as events:
            await fake.send(
                AdapterRegistration(
                    type="adapter.register",
                    instance_id=instance,
                    application="editor",
                    project_id="saved",
                    operations=tuple(contracts),
                )
            )
            await fake.send(
                AdapterEvent(type="adapter.event", event="test.ready", payload=None)
            )
            await anext(events)

        async def respond() -> None:
            while True:
                request = await fake.receive()
                assert isinstance(request, OperationRequest)
                if request.operation == "editor.document.attest":
                    result = copy.deepcopy(value)
                else:
                    guard = DocumentMutationRequest.model_validate(request.arguments)
                    assert guard.digest == value["digest"]
                    assert isinstance(guard.arguments, dict)
                    mode = guard.arguments.get("mode")
                    if mode == "failure":
                        result = dict(
                            job_id=guard.mutation_id,
                            state="failed",
                            error=dict(code="fixture_rollback", message="Rolled back"),
                        )
                    else:
                        before = copy.deepcopy(value)
                        value["digest"] = ("c" if mode == "upstream" else "d") * 64
                        if mode == "upstream":
                            value["resources"][0]["fingerprint"] = "changed-base"
                        result = dict(
                            job_id=guard.mutation_id,
                            state="completed",
                            result=dict(
                                mutation_id=guard.mutation_id,
                                result={"changed": True},
                                before=before,
                                after=copy.deepcopy(value),
                            ),
                        )
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result=result,
                    )
                )

        task = asyncio.create_task(respond())
        try:
            yield value
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


async def test_scoped_working_head_and_failed_transaction(tmp_path: Path) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            key, revision = await establish(core)
            await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=revision,
                    project=dict(stage="downstream"),
                    upsert=[
                        dict(kind="entity", id="harness", label="Harness"),
                        dict(
                            kind="milestone",
                            id="downstream",
                            label="Working mechanics",
                            status="in_progress",
                            entity_ids=["harness"],
                            document_ids=["doc"],
                            prerequisite_ids=["stage"],
                        ),
                    ],
                ),
            )
            head = core.projects.continuity.baseline(key, "doc")
            assert head is not None
            with pytest.raises(ProjectError, match="Rolled back"):
                await core.dispatcher.execute(
                    adapter_id="initial",
                    operation="editor.change",
                    arguments={"mode": "failure"},
                )
            assert core.projects.continuity.baseline(key, "doc") == head
            await core.dispatcher.execute(
                adapter_id="initial",
                operation="editor.change",
                arguments={"mode": "downstream"},
            )
            packet = (
                await core.projects.execute("project.continue", {"project_id": key})
            ).model_dump()
            stages = {
                r["record"]["id"]: r
                for r in packet["records"]
                if r["record"]["kind"] == "milestone"
            }
            assert stages["stage"]["record"]["status"] == "accepted"
            assert stages["downstream"]["record"]["status"] == "in_progress"
            checkpoint = (
                await core.projects.execute(
                    "project.apply",
                    dict(
                        project_id=key,
                        expected_revision=packet["project"]["revision"],
                        checkpoint=dict(id="working", label="Working state"),
                    ),
                )
            ).model_dump()
            assert (
                checkpoint["checkpoint"]["document_states"]["doc"]["digest"]
                == native["digest"]
            )
            await core.dispatcher.execute(
                adapter_id="initial",
                operation="editor.change",
                arguments={"mode": "upstream"},
            )
            packet = (
                await core.projects.execute("project.continue", {"project_id": key})
            ).model_dump()
            stages = {
                r["record"]["id"]: r
                for r in packet["records"]
                if r["record"]["kind"] == "milestone"
            }
            assert stages["stage"]["record"]["status"] == "invalidated"
            assert stages["stage"]["historical_status"] == "accepted"
            with pytest.raises(ProjectError) as blocked:
                await core.dispatcher.execute(
                    adapter_id="initial",
                    operation="editor.change",
                    arguments={"mode": "downstream"},
                )
            assert blocked.value.code == "milestone_blocked"


async def test_external_change_cannot_advance_working_head(tmp_path: Path) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            key, _ = await establish(core)
            original = core.projects.continuity.baseline(key, "doc")
            native["digest"] = "f" * 64
            with pytest.raises(ProjectError) as failed:
                await core.dispatcher.execute(
                    adapter_id="initial", operation="editor.change", arguments={}
                )
            assert failed.value.code == "content_diverged"
            assert core.projects.continuity.baseline(key, "doc") == original
