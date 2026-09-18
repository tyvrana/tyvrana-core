import asyncio
import os
import shutil
import signal
import sys
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import anyio
import pytest
from mcp import Client, MCPError, StdioServerParameters
from mcp.client.stdio import stdio_client
from tyvrana_protocol import (
    AdapterRegistration,
    CancelRequest,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
    ProtocolError,
)
from websockets.asyncio.client import connect

from tyvrana_core.mcp.server import INSTRUCTIONS

from .helpers import FakeAdapter, contract
from .mcp_helpers import (
    assert_application_control_policy,
    assert_workflow_guidance,
    execute,
    failure,
)

ROOT = Path(__file__).resolve().parents[1]


def command() -> list[str]:
    uv = shutil.which("uv")
    assert uv is not None
    return [
        uv,
        "run",
        "--locked",
        "--directory",
        str(ROOT),
        "tyvrana-core",
        "mcp",
        "--port",
        "0",
    ]


def environment() -> dict[str, str]:
    return {
        "UV_CACHE_DIR": str(ROOT / ".uv-cache"),
        "UV_PYTHON_DOWNLOADS": "never",
        "PYTHONASYNCIODEBUG": "1",
    }


def listening_uri(log: str) -> str:
    return next(
        line.split()[-1]
        for line in log.splitlines()
        if "Adapter server listening on" in line
    )


@asynccontextmanager
async def stdio_session(tmp_path: Path) -> AsyncIterator[tuple[Client, str]]:
    cmd = command()
    params = StdioServerParameters(
        command=cmd[0], args=cmd[1:], env={**environment(), "TMPDIR": str(tmp_path)}
    )
    log_path = tmp_path / "server.log"
    with log_path.open("w") as errors:
        async with Client(
            stdio_client(params, errlog=errors), read_timeout_seconds=5
        ) as client:
            assert client.server_info is not None
            assert client.server_info.name == "Tyvrana"
            assert client.instructions == INSTRUCTIONS
            yield client, listening_uri(log_path.read_text())
    log = log_path.read_text()
    assert "Adapter server stopped" in log
    assert "Traceback" not in log
    assert "was destroyed" not in log
    assert "was never awaited" not in log
    assert "ResourceWarning" not in log
    assert not list(tmp_path.glob("tyvrana-artifacts-*"))  # noqa: ASYNC240 - Bounded test directory.


async def register(fake: FakeAdapter, client: Client) -> None:
    await fake.send(
        AdapterRegistration(
            type="adapter.register",
            instance_id="adapter-a",
            application="Example",
            operations=(contract("document.inspect"),),
        )
    )
    result = await client.call_tool("tyvrana_list_adapters", {"wait_seconds": 2})
    assert not result.is_error
    summary = result.structured_content["adapters"][0]
    assert summary["instance_id"] == "adapter-a"
    assert summary["application"] == "Example"
    assert summary["operation_count"] == 1
    assert len(summary["catalog_sha256"]) == 64


async def test_stdio_initialization_discovery_execution_and_failures(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with stdio_session(tmp_path) as (client, uri):
        assert client.instructions is not None
        assert_application_control_policy(client.instructions)
        assert_workflow_guidance(client.instructions)
        tools = await client.list_tools()
        assert [tool.name for tool in tools.tools] == [
            "tyvrana_list_adapters",
            "tyvrana_list_operations",
            "tyvrana_execute_operation",
            "tyvrana_import_artifact",
            "tyvrana_export_artifact",
            "tyvrana_release_artifact",
        ]
        assert (await client.call_tool("tyvrana_list_adapters")).structured_content == {
            "adapters": [],
            "revision": 0,
        }
        assert failure(await execute(client))["code"] == "adapter_not_found"
        async with connect(uri, proxy=None) as websocket:
            fake = FakeAdapter(websocket)
            await register(fake, client)
            task = execute(client, {"values": [None, {"🌍": "[]"}]})
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            assert request.arguments == {"values": [None, {"🌍": "[]"}]}
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result=request.arguments,
                )
            )
            assert (await task).structured_content == {"result": request.arguments}
            failed = execute(client)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            await fake.send(
                OperationFailure(
                    type="operation.failure",
                    request_id=request.request_id,
                    error=ProtocolError(
                        code="custom.failure",
                        message="Could not complete",
                        details={"reason": "example"},
                    ),
                )
            )
            assert failure(await failed) == {
                "code": "custom.failure",
                "operation": "document.inspect",
                "message": "Could not complete",
                "details": {"reason": "example"},
            }
    # The SDK parses every stdout line; any non-MCP output is logged as a parse error.
    assert "Failed to parse" not in caplog.text


async def test_stdio_client_cancellation_sends_adapter_cancel(tmp_path: Path) -> None:
    async with stdio_session(tmp_path) as (client, uri):
        async with connect(uri, proxy=None) as websocket:
            fake = FakeAdapter(websocket)
            await register(fake, client)
            ready: asyncio.Queue[anyio.CancelScope] = asyncio.Queue()

            async def call() -> None:
                with anyio.CancelScope() as scope:
                    ready.put_nowait(scope)
                    await client.call_tool(
                        "tyvrana_execute_operation",
                        {
                            "adapter_id": "adapter-a",
                            "operation": "document.inspect",
                            "arguments": {},
                        },
                    )

            task = asyncio.create_task(call())
            scope = await ready.get()
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            scope.cancel()
            async with asyncio.timeout(3):
                await task
            assert await fake.receive() == CancelRequest(
                type="operation.cancel", request_id=request.request_id
            )
            # Late responses remain harmless and the same adapter serves another call.
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result="late",
                )
            )
            following = execute(client)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result="current",
                )
            )
            assert (await following).structured_content == {"result": "current"}


async def test_stdio_exit_cleans_pending_operation_and_active_adapter(
    tmp_path: Path,
) -> None:
    async with AsyncExitStack() as resources:
        async with stdio_session(tmp_path) as (client, uri):
            websocket = await resources.enter_async_context(connect(uri, proxy=None))
            fake = FakeAdapter(websocket)
            await register(fake, client)
            task = execute(client)
            assert isinstance(await fake.receive(), OperationRequest)
        async with asyncio.timeout(3):
            await websocket.wait_closed()
            with pytest.raises(MCPError):
                await task
        assert websocket.close_code == 1001


@pytest.mark.skipif(os.name != "posix", reason="POSIX process signal handling")
@pytest.mark.parametrize("stop", ["eof", "sigint", "sigterm"])
async def test_stdio_logs_stay_on_stderr_and_shutdown_is_clean(stop: str) -> None:
    process = await asyncio.create_subprocess_exec(
        # Signal the core process directly so its exit status is observable.
        # The SDK sessions above exercise the documented uv launch command.
        str(Path(sys.executable).with_name("tyvrana-core")),
        "mcp",
        "--port",
        "0",
        env={**os.environ, **environment()},
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stderr is not None
        assert process.stdin is not None
        async with asyncio.timeout(5):
            while True:
                line = await process.stderr.readline()
                assert line, "Server exited before startup"
                if b"Adapter server listening on" in line:
                    uri = line.decode().split()[-1]
                    break
        async with connect(uri, proxy=None) as websocket:
            if stop == "eof":
                process.stdin.close()
            else:
                process.send_signal(
                    signal.SIGINT if stop == "sigint" else signal.SIGTERM
                )
            async with asyncio.timeout(5):
                stdout, stderr = await process.communicate()
                await websocket.wait_closed()
            assert stdout == b""
            assert process.returncode == 0, stderr.decode()
            assert b"Adapter server stopped" in stderr
            assert b"Traceback" not in stderr
            assert websocket.close_code == 1001
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
