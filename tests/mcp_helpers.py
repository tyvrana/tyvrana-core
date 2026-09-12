import asyncio
import json

from mcp import Client
from mcp.types import CallToolResult, TextContent
from pydantic import TypeAdapter
from tyvrana_protocol import JsonValue


def failure(result: CallToolResult) -> dict[str, JsonValue]:
    assert result.is_error
    assert result.structured_content is None
    assert len(result.content) == 1
    content = result.content[0]
    assert isinstance(content, TextContent)
    assert "Traceback" not in content.text
    return TypeAdapter[dict[str, JsonValue]](dict[str, JsonValue]).validate_python(
        json.loads(content.text)
    )


def execute(
    client: Client, arguments: JsonValue = None, adapter_id: str = "adapter-a"
) -> asyncio.Task[CallToolResult]:
    return asyncio.create_task(
        client.call_tool(
            "tyvrana_execute_operation",
            {
                "adapter_id": adapter_id,
                "operation": "document.inspect",
                "arguments": arguments,
            },
        )
    )
