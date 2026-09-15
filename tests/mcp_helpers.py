import asyncio
import json

from mcp import Client
from mcp.types import CallToolResult, TextContent
from pydantic import TypeAdapter
from tyvrana_protocol import JsonValue


def assert_workflow_guidance(instructions: str) -> None:
    """Check durable decisions, not one verbatim instruction paragraph."""
    guidance = " ".join(instructions.lower().split())
    concepts = (
        ("before", "mutation", "requested result", "current state"),
        ("infer", "workflow", "dependencies", "hidden/internal"),
        ("motion", "deformation", "assembly", "acceptance"),
        ("references", "measurements", "materials", "downstream", "runtime"),
        ("authoritative", "actually inspect", "images", "diagrams"),
        ("discover", "typed tyvrana", "capabilities"),
        ("tyvrana capability gap", "authorized development", "unsupported"),
        ("do not bypass", "arbitrary scripts", "source-development permission"),
        ("batched", "filtered", "bounded", "comparison", "argument guessing"),
        ("biology", "birds", "arthropods", "mechanical", "vehicles", "buildings"),
        ("static props", "not a fixed pipeline", "external ai"),
    )
    for concept in concepts:
        assert all(term in guidance for term in concept), concept
    assert len(instructions.encode()) < 5500


def assert_application_control_policy(instructions: str) -> None:
    policy = " ".join(instructions.lower().split())
    for required in (
        "when a tyvrana adapter is connected for an application",
        "all meaningful mutations",
        "project/editor state must use tyvrana's advertised typed operations",
        "do not bypass",
        "report the capability gap",
        "non-authoritative observation/window management",
        "must not mutate project/editor state",
        "source-code/file editing with software-development tools remains allowed",
        "does not allow direct scene/asset/prefab state edits",
        "compilation inspection",
    ):
        assert required in policy
    for bypass in (
        "computer use",
        "mouse",
        "keyboard",
        "menus",
        "shortcuts",
        "gizmos",
        "console commands",
        "arbitrary scripts",
        "direct application apis",
        "another editor-control/mcp integration",
    ):
        assert bypass in policy
    for specific in ("codex", "chatgpt", "blender", "unity", "unreal", "godot"):
        assert specific not in policy


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
