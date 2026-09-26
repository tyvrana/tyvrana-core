"""Real MCP contracts, restart and portable resource binding orchestration."""

import asyncio
from pathlib import Path

import pytest
from mcp import Client
from tyvrana_protocol import (
    AdapterEvent,
    AdapterRegistration,
    JsonValue,
    OperationContract,
    OperationRequest,
    OperationSuccess,
    ResourceInspectionRequest,
    ResourceInspectionResult,
)
from websockets.asyncio.client import connect

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.mcp import create_mcp_server
from tyvrana_core.projects.models import ApplicationStatus

from .helpers import FakeAdapter
from .mcp_helpers import failure
from .test_projects import fixture_batch


async def test_fresh_clients_restart_and_conflict_over_mcp(tmp_path: Path) -> None:
    config = CoreConfig(port=0, state_directory=str(tmp_path))
    core = AdapterServer(config)
    async with Client(create_mcp_server(core), raise_exceptions=True) as client:
        schemas = await client.call_tool(
            "tyvrana_list_operations",
            {"query": "continuation", "schemas": "full"},
        )
        assert not schemas.is_error
        assert any(
            o["name"] == "project.continue"
            for o in schemas.structured_content["operations"]
        )
        created = await client.call_tool(
            "tyvrana_execute_operation",
            {
                "operation": "project.create",
                "arguments": {
                    "title": "Durable assembly",
                    "goal": "Verify structure and motion",
                },
            },
        )
        assert not created.is_error
        key = created.structured_content["result"]["id"]
        changed = await client.call_tool(
            "tyvrana_execute_operation",
            {
                "operation": "project.apply",
                "arguments": {
                    "project_id": key,
                    **fixture_batch().model_dump(mode="json"),
                },
            },
        )
        assert not changed.is_error
        conflict = await client.call_tool(
            "tyvrana_execute_operation",
            {
                "operation": "project.apply",
                "arguments": {
                    "project_id": key,
                    "expected_revision": 1,
                    "project": {"stage": "wrong"},
                },
            },
        )
        assert failure(conflict)["code"] == "revision_conflict"
        invalid = await client.call_tool(
            "tyvrana_execute_operation",
            {
                "operation": "project.apply",
                "arguments": {"expected_revision": 2, "metadata": {"opaque": True}},
            },
        )
        assert failure(invalid)["code"] == "invalid_arguments"
    async with Client(
        create_mcp_server(AdapterServer(config)), raise_exceptions=True
    ) as fresh:
        packet = await fresh.call_tool(
            "tyvrana_execute_operation",
            {"operation": "project.continue", "arguments": {}},
        )
        assert not packet.is_error
        assert packet.structured_content["result"]["project"]["id"] == key
        assert packet.structured_content["result"]["project"]["revision"] == 2
        assert (
            packet.structured_content["result"]["applications"][0]["state"]
            == "unavailable"
        )
        removed = await fresh.call_tool(
            "tyvrana_execute_operation",
            {
                "operation": "project.remove",
                "arguments": {
                    "project_id": key,
                    "expected_revision": 2,
                    "confirm_project_id": key,
                },
            },
        )
        assert (
            not removed.is_error
            and not removed.structured_content["result"]["application_files_affected"]
        )


async def test_portable_inspection_records_missing_and_repair(tmp_path: Path) -> None:
    core = AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path)))
    async with Client(create_mcp_server(core), raise_exceptions=True) as client:
        project = await core.projects.execute(
            "project.create", {"title": "Assembly", "goal": "Inspect"}
        )
        key = project.model_dump()["id"]
        await core.projects.execute(
            "project.apply",
            {"project_id": key, **fixture_batch().model_dump(mode="json")},
        )
        inspector = OperationContract(
            name="editor.resource.inspect",
            description="Resolve resource identities",
            effect="read_only",
            execution="synchronous",
            arguments_schema=ResourceInspectionRequest.model_json_schema(),
            result_schema=ResourceInspectionResult.model_json_schema(),
        )
        async with connect(core.uri, proxy=None) as websocket:
            fake = FakeAdapter(websocket)
            with core.events.subscribe() as events:
                await fake.send(
                    AdapterRegistration(
                        type="adapter.register",
                        instance_id="native",
                        application="modeler",
                        project_id="source-uuid",
                        resource_inspection=inspector.name,
                        operations=(inspector,),
                    )
                )
                await fake.send(
                    AdapterEvent(type="adapter.event", event="ready.test", payload=None)
                )
                await anext(events)
            for revision, state, name in [
                (2, "missing", None),
                (3, "present", "Renamed resource"),
            ]:
                pending = asyncio.create_task(
                    client.call_tool(
                        "tyvrana_execute_operation",
                        {
                            "operation": "project.verify",
                            "arguments": {
                                "project_id": key,
                                "expected_revision": revision,
                                "binding_ids": ["source-binding"],
                            },
                        },
                    )
                )
                request = await fake.receive()
                assert isinstance(request, OperationRequest)
                args = ResourceInspectionRequest.model_validate(request.arguments)
                assert (
                    args.project_id == "source-uuid"
                    and args.resources[0].resource_id == "resource-uuid"
                )
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result={
                            "project_id": "source-uuid",
                            "resources": [
                                {
                                    "resource_kind": "mesh",
                                    "resource_id": "resource-uuid",
                                    "state": state,
                                    "name": name,
                                    "fingerprint": "summary" if name else None,
                                }
                            ],
                            "fingerprint_scope": "Test structural summary",
                        },
                    )
                )
                result = await pending
                assert not result.is_error
                view = await core.projects.execute(
                    "project.search", {"project_id": key, "ids": ["source-binding"]}
                )
                observation = view.model_dump()["records"][0]["binding"]
                assert observation["state"] == (
                    "verified" if state == "present" else "missing"
                )
                assert observation["name"] == name


async def test_many_bindings_share_document_continuity_check(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
) -> None:
    core = AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path)))
    async with Client(create_mcp_server(core), raise_exceptions=True) as client:
        project = await core.projects.execute(
            "project.create", {"title": "Reference set", "goal": "Verify together"}
        )
        key = project.model_dump()["id"]
        bindings: list[dict[str, JsonValue]] = [
            {
                "kind": "binding",
                "id": f"binding-{i}",
                "label": f"Reference {i}",
                "entity_id": "set",
                "document_id": "document",
                "resource_kind": "reference",
                "resource_id": f"resource-{i}",
            }
            for i in range(40)
        ]
        await core.projects.execute(
            "project.apply",
            {
                "project_id": key,
                "expected_revision": 1,
                "upsert": [
                    {"kind": "entity", "id": "set", "label": "Reference set"},
                    {
                        "kind": "document",
                        "id": "document",
                        "label": "References",
                        "application": "modeler",
                        "application_project_id": "reference-document",
                        "adapter_id": "native",
                    },
                    *bindings,
                ],
            },
        )
        inspector = OperationContract(
            name="editor.resource.inspect",
            description="Inspect resources together",
            effect="read_only",
            execution="synchronous",
            arguments_schema=ResourceInspectionRequest.model_json_schema(),
            result_schema=ResourceInspectionResult.model_json_schema(),
        )
        async with connect(core.uri, proxy=None) as websocket:
            fake = FakeAdapter(websocket)
            with core.events.subscribe() as events:
                await fake.send(
                    AdapterRegistration(
                        type="adapter.register",
                        instance_id="native",
                        application="modeler",
                        project_id="reference-document",
                        resource_inspection=inspector.name,
                        operations=(inspector,),
                    )
                )
                await fake.send(
                    AdapterEvent(type="adapter.event", event="ready.test", payload=None)
                )
                await anext(events)
            original = core.projects.environment
            checks = 0
            lose_continuity = False

            async def environment(
                project_id: str,
            ) -> tuple[dict[str, str], list[ApplicationStatus]]:
                nonlocal checks
                checks += 1
                connections, statuses = await original(project_id)
                if lose_continuity and checks >= 3:
                    connections = {}
                return connections, statuses

            monkeypatch.setattr(core.projects, "environment", environment)
            for revision, expected in [(2, "verified"), (3, "unverified")]:
                checks = 0
                lose_continuity = revision == 3
                pending = asyncio.create_task(
                    client.call_tool(
                        "tyvrana_execute_operation",
                        {
                            "operation": "project.verify",
                            "arguments": {
                                "project_id": key,
                                "expected_revision": revision,
                                "binding_ids": [b["id"] for b in bindings],
                            },
                        },
                    )
                )
                request = await fake.receive()
                assert isinstance(request, OperationRequest)
                arguments = ResourceInspectionRequest.model_validate(request.arguments)
                assert len(arguments.resources) == 40
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result={
                            "project_id": "reference-document",
                            "resources": [
                                {
                                    **r.model_dump(mode="json"),
                                    "state": "present",
                                    "name": r.resource_id,
                                    "fingerprint": "unchanged",
                                }
                                for r in arguments.resources
                            ],
                            "fingerprint_scope": "Reference identity",
                        },
                    )
                )
                result = await pending
                assert not result.is_error
                assert result.structured_content["result"]["counts"] == {
                    "binding:verified": 40
                }
                # Document evidence cost must stay bounded as the set grows;
                # the final continuity check still invalidates stale bindings.
                assert checks <= 3
                recovered = await core.projects.execute(
                    "project.search",
                    {"project_id": key, "kind": "binding", "limit": 40},
                )
                records = recovered.model_dump()["records"]
                assert len(records) == 40
                assert all(r["binding"]["state"] == expected for r in records)
