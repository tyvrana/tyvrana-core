"""Local MCP access whose lifetime is independent of individual clients."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from mcp.server.streamable_http_manager import (
    StreamableHTTPASGIApp,
    StreamableHTTPSessionManager,
)
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.routing import Mount

from ..server import AdapterServer
from .server import create_mcp_server


def create_http_app(core: AdapterServer) -> Starlette:
    """Share the normal MCP server/core across independent local sessions."""
    manager = StreamableHTTPSessionManager(
        create_mcp_server(core),
        json_response=True,
        security_settings=TransportSecuritySettings(
            allowed_hosts=["127.0.0.1:*", "localhost:*"],
            allowed_origins=["http://127.0.0.1:*", "http://localhost:*"],
        ),
    )

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with manager.run():
            yield

    return Starlette(
        routes=[Mount("/mcp", app=StreamableHTTPASGIApp(manager))],
        lifespan=lifespan,
    )


async def run_http(core: AdapterServer, port: int) -> None:
    """Serve only on loopback; Uvicorn owns graceful process signal handling."""
    server = uvicorn.Server(
        uvicorn.Config(
            create_http_app(core),
            host="127.0.0.1",
            port=port,
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=5,
        )
    )
    await server.serve()
