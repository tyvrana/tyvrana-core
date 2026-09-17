import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Literal

import anyio
import pytest
from mcp import Client
from mcp.types import TextContent
from tyvrana_protocol import (
    CancelRequest,
    JsonValue,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
    ProtocolError,
)

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.mcp import create_mcp_server
from tyvrana_core.mcp.server import INSTRUCTIONS

from .helpers import adapter
from .mcp_helpers import (
    assert_application_control_policy,
    assert_workflow_guidance,
    execute,
    failure,
)


@asynccontextmanager
async def session(
    *, operation_timeout: float = 1.0
) -> AsyncIterator[tuple[AdapterServer, Client]]:
    core = AdapterServer(
        CoreConfig(port=0, operation_timeout=operation_timeout, close_timeout=0.1)
    )
    async with Client(create_mcp_server(core), raise_exceptions=True) as client:
        yield core, client
    assert core.registry.list() == ()
    assert core.dispatcher.pending_count == 0
    assert core.events.subscriber_count == 0


async def test_discovery_and_empty_adapter_list() -> None:
    async with session() as (core, client):
        assert core.uri.startswith("ws://127.0.0.1:")
        listed = await client.list_tools()
        assert [tool.name for tool in listed.tools] == [
            "tyvrana_list_adapters",
            "tyvrana_list_operations",
            "tyvrana_execute_operation",
            "tyvrana_import_artifact",
            "tyvrana_release_artifact",
        ]
        discover, _, execute_tool = listed.tools[:3]
        assert "wait_seconds" in discover.input_schema["properties"]
        assert discover.input_schema["additionalProperties"] is False
        assert (
            execute_tool.input_schema["properties"]["adapter_id"]["default"] == "core"
        )
        assert execute_tool.input_schema["required"] == [
            "operation",
            "arguments",
        ]
        assert execute_tool.input_schema["additionalProperties"] is False
        assert execute_tool.output_schema is not None
        assert execute_tool.output_schema["required"] == ["result"]
        assert "$defs" in execute_tool.input_schema
        result = await client.call_tool("tyvrana_list_adapters", {})
        assert not result.is_error
        assert result.structured_content == {"adapters": [], "revision": 0}


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_agent_guidance_reaches_official_client(
    mode: Literal["auto", "legacy"],
) -> None:
    core = AdapterServer(CoreConfig(port=0))
    server = create_mcp_server(core)
    assert server.create_initialization_options().instructions == INSTRUCTIONS
    assert_application_control_policy(INSTRUCTIONS)
    async with Client(server, mode=mode, raise_exceptions=True) as client:
        assert client.instructions == INSTRUCTIONS
        assert_application_control_policy(client.instructions)
        assert_workflow_guidance(client.instructions)
        assert client.server_capabilities.prompts is None
        assert len(INSTRUCTIONS.encode()) < 5500
        for topic in (
            "advertised operation",
            "structured",
            "visual verification",
            "raycasting",
            "snapshot",
            "unrelated user state",
            "partial mutation",
            "Runtime validation",
        ):
            assert topic in " ".join(client.instructions.split())
        descriptions = {
            tool.name: tool.description or ""
            for tool in (await client.list_tools()).tools
        }
        assert set(descriptions) == {
            "tyvrana_list_adapters",
            "tyvrana_list_operations",
            "tyvrana_execute_operation",
            "tyvrana_import_artifact",
            "tyvrana_release_artifact",
        }
        assert "currently connected" in descriptions["tyvrana_list_adapters"]
        assert "catalog" in descriptions["tyvrana_list_adapters"]
        assert "tyvrana_list_adapters" in descriptions["tyvrana_execute_operation"]
        for name in ("tyvrana_list_adapters", "tyvrana_execute_operation"):
            description = descriptions[name].lower()
            assert "application mutations must use" in description
            assert "typed operations" in description
            assert "report missing capabilities" in description
            assert "do not bypass a connected adapter" in description
            assert len(description) < 1000
        assert "arguments contract" in descriptions["tyvrana_execute_operation"]
        assert "MCP tool errors" in descriptions["tyvrana_execute_operation"]
        assert "artifact bytes" in descriptions["tyvrana_import_artifact"]
        assert "source path" in descriptions["tyvrana_import_artifact"]
        assert "temporary artifact" in descriptions["tyvrana_release_artifact"]
        for specific in ("codex", "chatgpt", "blender", "unity", "unreal", "godot"):
            assert specific not in " ".join(descriptions.values()).lower()
    assert core.registry.list() == ()
    assert core.events.subscriber_count == 0


async def test_adapters_listed_with_metadata_in_deterministic_order() -> None:
    async with session() as (core, client):
        async with (
            adapter(core, "z-last"),
            adapter(core, "a-first", operations=("document.inspect", "asset.describe")),
        ):
            result = await client.call_tool("tyvrana_list_adapters")
            assert not result.is_error
            assert result.structured_content == {
                "adapters": [
                    {
                        "instance_id": "a-first",
                        "application": "Example Editor",
                        "application_version": "2026.9",
                        "project_path": "projects/example.project",
                        "operation_count": 2,
                        "catalog_sha256": core.registry.get("a-first").catalog_sha256,
                    },
                    {
                        "instance_id": "z-last",
                        "application": "Example Editor",
                        "application_version": "2026.9",
                        "project_path": "projects/example.project",
                        "operation_count": 1,
                        "catalog_sha256": core.registry.get("z-last").catalog_sha256,
                    },
                ],
                "revision": 2,
            }
            result.structured_content["adapters"][0]["operation_count"] = 0
            again = await client.call_tool("tyvrana_list_adapters")
            assert again.structured_content["adapters"][0]["operation_count"] == 2
            assert core.registry.get("a-first").registration.operation_names == (
                "document.inspect",
                "asset.describe",
            )


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [],
        True,
        42,
        1.5,
        "null",
        "[]",
        "{}",
        "42",
        "true",
        {"🌍": [None, False, 2.5, {"nested": []}]},
    ],
)
async def test_mcp_dispatch_round_trip_preserves_json(value: JsonValue) -> None:
    async with session() as (core, client):
        async with adapter(core) as fake:
            task = execute(client, value)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            assert request.arguments == value
            assert type(request.arguments) is type(value)
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result=value,
                )
            )
            result = await task
            assert not result.is_error
            assert result.structured_content == {"result": value}
            assert isinstance(result.content[0], TextContent)
            assert json.loads(result.content[0].text) == {"result": value}
            assert core.dispatcher.pending_count == 0


async def test_remote_error_preserves_code_message_details_without_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with session() as (core, client):
        async with adapter(core) as fake:
            task = execute(client)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            error = ProtocolError(
                code="document_not_found",
                message='Document "Sample" does not exist.',
                details={"name": "Sample", "nested": [None, {}]},
            )
            await fake.send(
                OperationFailure(
                    type="operation.failure", request_id=request.request_id, error=error
                )
            )
            assert failure(await task) == {
                **error.model_dump(mode="json"),
                "operation": "document.inspect",
            }
            assert all(record.exc_info is None for record in caplog.records)


async def test_missing_adapter_is_tool_failure() -> None:
    async with session() as (_, client):
        assert failure(await execute(client))["code"] == "adapter_not_found"


async def test_unsupported_operation_is_tool_failure() -> None:
    async with session() as (core, client):
        async with adapter(core, operations=()):
            assert failure(await execute(client))["code"] == "operation_unsupported"
            assert core.dispatcher.pending_count == 0


async def test_core_default_timeout_maps_to_tool_failure_and_cancel() -> None:
    async with session(operation_timeout=0.04) as (core, client):
        async with adapter(core) as fake:
            task = execute(client)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            assert failure(await task)["code"] == "operation_timeout"
            assert await fake.receive() == CancelRequest(
                type="operation.cancel", request_id=request.request_id
            )
            assert core.dispatcher.pending_count == 0


async def test_disconnect_maps_to_tool_failure() -> None:
    async with session() as (core, client):
        async with adapter(core) as fake:
            task = execute(client)
            assert isinstance(await fake.receive(), OperationRequest)
            await fake.websocket.close()
            assert failure(await task)["code"] == "adapter_disconnected"


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"adapter_id": "a", "operation": "a.b"},
        {"adapter_id": 5, "operation": "a.b", "arguments": {}},
        {"adapter_id": "a", "operation": "unqualified", "arguments": {}},
        {"adapter_id": "a", "operation": "a.b", "arguments": {}, "timeout": 1},
    ],
)
async def test_invalid_tool_input_is_a_clear_failure(
    arguments: dict[str, JsonValue],
) -> None:
    async with session() as (_, client):
        error = failure(await client.call_tool("tyvrana_execute_operation", arguments))
        assert error["code"] == "invalid_arguments"
        assert error["message"] == "Invalid tool arguments"
        assert error["details"]


async def test_discovery_rejects_extra_arguments() -> None:
    async with session() as (_, client):
        assert (
            failure(await client.call_tool("tyvrana_list_adapters", {"extra": True}))[
                "code"
            ]
            == "invalid_arguments"
        )


async def test_unexpected_failure_is_sanitized_and_logged_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async with session() as (core, client):

        async def broken(
            *,
            adapter_id: str,
            operation: str,
            arguments: JsonValue,
            artifact_ids: tuple[str, ...] = (),
        ) -> JsonValue:
            raise RuntimeError("Private diagnostic")

        monkeypatch.setattr(core.dispatcher, "execute", broken)
        error = failure(await execute(client))
        assert error["code"] == "internal_error"
        assert "Private diagnostic" not in str(error)
        assert "Private diagnostic" in caplog.text
        records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(records) == 1
        assert records[0].exc_info


@pytest.mark.parametrize("cancel_scope", [True, False])
async def test_mcp_client_cancellation_reaches_dispatcher(cancel_scope: bool) -> None:
    async with session() as (core, client):
        async with adapter(core) as fake:
            scope_ready: asyncio.Queue[anyio.CancelScope] = asyncio.Queue()

            async def call() -> None:
                with anyio.CancelScope() as scope:
                    scope_ready.put_nowait(scope)
                    await client.call_tool(
                        "tyvrana_execute_operation",
                        {
                            "adapter_id": "adapter-a",
                            "operation": "document.inspect",
                            "arguments": {},
                        },
                    )

            task = asyncio.create_task(call())
            scope = await scope_ready.get()
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            if cancel_scope:
                scope.cancel()
                await task
            else:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert await fake.receive() == CancelRequest(
                type="operation.cancel", request_id=request.request_id
            )
            assert core.dispatcher.pending_count == 0
            assert core.registry.get("adapter-a").connected


async def test_mcp_session_exit_closes_adapters_subscriptions_and_pending_work() -> (
    None
):
    core = AdapterServer(CoreConfig(port=0))
    async with AsyncExitStack() as resources:
        async with Client(create_mcp_server(core), raise_exceptions=True) as client:
            fake = await resources.enter_async_context(adapter(core))
            subscription = core.events.subscribe()
            task = execute(client)
            assert isinstance(await fake.receive(), OperationRequest)
        async with asyncio.timeout(2):
            assert failure(await task)["code"] == "adapter_disconnected"
        assert fake.websocket.close_code == 1001
        assert subscription.closed
        assert core.registry.list() == ()
        assert core.dispatcher.pending_count == 0
        with pytest.raises(RuntimeError):
            _ = core.uri


async def test_mcp_lifespan_can_restart_without_import_side_effects() -> None:
    core = AdapterServer(CoreConfig(port=0))
    mcp = create_mcp_server(core)
    with pytest.raises(RuntimeError):
        _ = core.uri
    for _ in range(3):
        async with Client(mcp, raise_exceptions=True) as client:
            async with adapter(core):
                result = await client.call_tool("tyvrana_list_adapters")
                assert len(result.structured_content["adapters"]) == 1
        assert core.registry.list() == ()
