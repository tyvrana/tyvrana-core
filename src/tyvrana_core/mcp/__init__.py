"""Local MCP access to core services."""

from .server import create_mcp_server, run_stdio

__all__ = ["create_mcp_server", "run_stdio"]
