import asyncio
import os
import signal
import sys

import pytest
from websockets.asyncio.client import connect

from tyvrana_core.cli import main


def test_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        main(["--help"])
    assert caught.value.code == 0
    output = capsys.readouterr().out
    assert "serve" in output
    assert "mcp" in output


def test_invalid_cli_port(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        main(["serve", "--port", "65536"])
    assert caught.value.code == 2
    assert "65535" in capsys.readouterr().err


@pytest.mark.parametrize("mode", ["serve", "mcp"])
def test_mode_help(mode: str, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        main([mode, "--help"])
    assert caught.value.code == 0
    output = capsys.readouterr().out
    assert "--host" in output
    assert "--port" in output


def test_cli_requires_explicit_mode(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        main([])
    assert caught.value.code == 2
    assert "required" in capsys.readouterr().err


@pytest.mark.skipif(os.name != "posix", reason="POSIX process signal handling")
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
async def test_cli_runs_and_shuts_down_on_signal(signum: signal.Signals) -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "from tyvrana_core.cli import main; main()",
        "serve",
        "--port",
        "0",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stderr is not None
        async with asyncio.timeout(3):
            while True:
                line = await process.stderr.readline()
                assert line, "CLI exited without starting"
                if b"Adapter server listening on" in line:
                    uri = line.decode().strip().split()[-1]
                    break
        async with connect(uri, proxy=None) as websocket:
            process.send_signal(signum)
            async with asyncio.timeout(3):
                _, stderr = await process.communicate()
                await websocket.wait_closed()
            assert process.returncode == 0, stderr.decode()
            assert websocket.close_code == 1001
            assert b"Adapter server stopped" in stderr
            assert b"Traceback" not in stderr
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
