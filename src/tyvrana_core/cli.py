"""Run a manual adapter listener or an MCP stdio session."""

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

from pydantic import ValidationError

from .config import CoreConfig
from .mcp import run_stdio
from .server import AdapterServer


async def _serve(core: AdapterServer) -> None:
    async with core:
        await asyncio.Event().wait()


async def _run(config: CoreConfig, mode: str) -> None:
    core = AdapterServer(config)
    work = asyncio.create_task(run_stdio(core) if mode == "mcp" else _serve(core))
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    signalled = False

    def stop() -> None:
        nonlocal signalled
        if not signalled:
            signalled = True
            work.cancel()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop)
            except NotImplementedError:
                continue
            installed.append(signum)
        try:
            await work
        except asyncio.CancelledError:
            if not signalled:
                raise
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run Tyvrana core with a manual listener or MCP stdio."
    )
    defaults = CoreConfig()
    modes = parser.add_subparsers(dest="mode", required=True)
    for name, help_text in (
        ("serve", "Run the adapter WebSocket server"),
        ("mcp", "Run MCP stdio and the adapter WebSocket server"),
    ):
        mode = modes.add_parser(name, help=help_text)
        mode.add_argument(
            "--host",
            default=defaults.host,
            help="Adapter bind host (default: loopback)",
        )
        mode.add_argument(
            "--port",
            type=int,
            default=defaults.port,
            help="Adapter bind port (0 selects a free port)",
        )
        mode.add_argument(
            "--state-directory",
            default=defaults.state_directory,
            help="Local durable semantic project store directory",
        )
    args = parser.parse_args(argv)
    try:
        config = CoreConfig(
            host=args.host, port=args.port, state_directory=args.state_directory
        )
    except ValidationError as exc:
        parser.error(str(exc))
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
        force=True,
    )
    try:
        asyncio.run(_run(config, args.mode))
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        parser.exit(1, f"Could not start adapter server: {exc}\n")
