"""Run the local adapter listener with standard-library command-line tools."""

import argparse
import asyncio
import logging
import signal
from collections.abc import Sequence

from pydantic import ValidationError

from .config import CoreConfig
from .server import AdapterServer


async def _run(config: CoreConfig) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop.set)
            except NotImplementedError:
                continue
            installed.append(signum)
        async with AdapterServer(config):
            await stop.wait()
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the local Tyvrana adapter server."
    )
    defaults = CoreConfig()
    parser.add_argument(
        "--host", default=defaults.host, help="Bind host (default: loopback)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=defaults.port,
        help="Bind port (0 selects a free port)",
    )
    args = parser.parse_args(argv)
    try:
        config = CoreConfig(host=args.host, port=args.port)
    except ValidationError as exc:
        parser.error(str(exc))
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    try:
        asyncio.run(_run(config))
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        parser.exit(1, f"Could not start adapter server: {exc}\n")
