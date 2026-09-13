"""Compose MCP stdio with the existing adapter server lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..server import AdapterServer
from .tools import call_tool, list_tools

INSTRUCTIONS = """Tyvrana controls connected professional applications through adapters.
List connected adapters first; use their advertised operation names and do not
assume an application or unsupported operation is available. Inspect structured
state before significant changes. Prefer typed adapter operations for application
state changes rather than bypassing them through shell commands or direct editor
access. Operation arguments are application-specific; follow their contracts and
validation errors.

For visual work, render or capture a baseline, inspect it, make a controlled
change, render/capture again, inspect, correct, and repeat until verified. API
success is not visual verification. Use advertised render-to-surface raycasting
before targeted sculpt work instead of guessing 3D coordinates. After topology
changes, inspect/query again: mesh element indices belong to the current snapshot.

Prefer reversible, non-destructive workflows where appropriate and preserve
unrelated user state. If an operation reports possible partial mutation,
reinspect state and rerender before retrying; do not assume rollback. Runtime
validation and safety limits remain authoritative. Attach imported artifacts by
ID; source paths belong only to core's local ingestion boundary. Release imported
artifacts when finished.
"""


def create_mcp_server(core: AdapterServer) -> Server[AdapterServer]:
    """Create an unstarted MCP server that owns core for its lifespan.

    The low-level SDK API preserves JsonValue strings such as "null" and "[]";
    the high-level function helper pre-parses them into other JSON types.
    """

    @asynccontextmanager
    async def lifespan(server: Server[AdapterServer]) -> AsyncIterator[AdapterServer]:
        await core.start()
        try:
            yield core
        finally:
            with anyio.CancelScope(shield=True):
                await core.stop()

    return Server(
        "Tyvrana",
        instructions=INSTRUCTIONS,
        lifespan=lifespan,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


async def run_stdio(core: AdapterServer) -> None:
    """Serve one local MCP session; stdin/stdout belong only to MCP."""
    server = create_mcp_server(core)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, write_stream, server.create_initialization_options()
        )
