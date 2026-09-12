"""Compose MCP stdio with the existing adapter server lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..server import AdapterServer
from .tools import call_tool, list_tools


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
