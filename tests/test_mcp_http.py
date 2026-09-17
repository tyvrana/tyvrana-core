"""Independent MCP clients share one local core and cleanly release listeners."""

import asyncio
import os
import signal
import socket
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from websockets.asyncio.client import connect

from tyvrana_core.cli import main
from tyvrana_core.mcp.server import INSTRUCTIONS

from .mcp_helpers import assert_workflow_guidance


@pytest.fixture
async def http_core(
    tmp_path: Path,
) -> AsyncIterator[tuple[asyncio.subprocess.Process, str, str]]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "from tyvrana_core.cli import main; main()",
        "mcp-http",
        "--port",
        "0",
        "--mcp-port",
        "0",
        "--state-directory",
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stderr is not None
        adapter_uri = ""
        async with asyncio.timeout(8):
            while True:
                line = await process.stderr.readline()
                assert line, "HTTP core exited before startup"
                if b"Adapter server listening on" in line:
                    adapter_uri = line.decode().strip().split()[-1]
                if b"Uvicorn running on" in line:
                    uri = line.decode().split("Uvicorn running on ")[1].split()[0]
                    break
        assert adapter_uri
        yield process, uri + "/mcp/", adapter_uri
    finally:
        if process.returncode is None:
            process.terminate()
        async with asyncio.timeout(8):
            await process.communicate()


async def test_separate_clients_keep_shared_core_and_project_state(
    http_core: tuple[asyncio.subprocess.Process, str, str],
) -> None:
    process, uri, adapter_uri = http_core
    async with Client(streamable_http_client(uri)) as first:
        assert first.instructions == INSTRUCTIONS
        assert_workflow_guidance(first.instructions)
        result = await first.call_tool(
            "tyvrana_execute_operation",
            {
                "operation": "project.create",
                "arguments": {"title": "Shared", "goal": "Continue across clients"},
            },
        )
        assert not result.is_error
        identifier = result.structured_content["result"]["id"]
        async with Client(streamable_http_client(uri)) as second:
            recovered = await second.call_tool(
                "tyvrana_execute_operation",
                {"operation": "project.continue", "arguments": {}},
            )
            assert recovered.structured_content["result"]["project"]["id"] == identifier
    # Closing every client must not close the application's core listener.
    assert process.returncode is None
    async with connect(adapter_uri, proxy=None):
        async with Client(streamable_http_client(uri)) as fresh:
            assert fresh.instructions == INSTRUCTIONS
            assert_workflow_guidance(fresh.instructions)
            recovered = await fresh.call_tool(
                "tyvrana_execute_operation",
                {"operation": "project.continue", "arguments": {}},
            )
            assert recovered.structured_content["result"]["project"]["id"] == identifier


async def test_http_rejects_foreign_host_and_origin(
    http_core: tuple[asyncio.subprocess.Process, str, str],
) -> None:
    _, uri, _ = http_core
    async with httpx2.AsyncClient() as client:
        for headers in (
            {"Host": "attacker.invalid"},
            {"Origin": "https://attacker.invalid"},
        ):
            result = await client.post(uri, json={}, headers=headers)
            assert result.status_code in (403, 421)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process signal handling")
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
async def test_http_signal_stops_mcp_and_adapter_lifetimes(
    http_core: tuple[asyncio.subprocess.Process, str, str], signum: signal.Signals
) -> None:
    process, uri, adapter_uri = http_core
    async with connect(adapter_uri, proxy=None) as websocket:
        process.send_signal(signum)
        async with asyncio.timeout(8):
            _, stderr = await process.communicate()
            await websocket.wait_closed()
        assert websocket.close_code == 1001
        assert b"Adapter server stopped" in stderr
        assert b"Traceback" not in stderr
    async with httpx2.AsyncClient() as client:
        with pytest.raises(httpx2.ConnectError):
            await client.get(uri)


def test_http_port_validation(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        main(["mcp-http", "--mcp-port", "65536"])
    assert error.value.code == 2
    assert "65535" in capsys.readouterr().err


async def test_adapter_startup_failure_returns_nonzero(tmp_path: Path) -> None:
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "from tyvrana_core.cli import main; main()",
            "mcp-http",
            "--port",
            str(occupied.getsockname()[1]),
            "--mcp-port",
            "0",
            "--state-directory",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        async with asyncio.timeout(8):
            _, stderr = await process.communicate()
        assert process.returncode != 0
        assert b"Uvicorn running on" not in stderr
