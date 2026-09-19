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
        ("tyvrana tooling gap", "authorized development", "unsupported"),
        ("do not bypass", "arbitrary", "scripts", "source-development permission"),
        ("batched", "filtered", "bounded", "comparison", "argument guessing"),
        ("intended use", "only complexity", "not a fixed pipeline", "external ai"),
        ("validate", "before dependent detail", "acceptance gate"),
        ("ranges and transitions", "simple geometry"),
        ("domain structure", "control/deformation/procedural proxies"),
        ("rigs, guides, cages", "do not establish physical structure"),
        ("skeletal geometry", "separately from the armature"),
        ("influence or validate", "representation roles", "dependencies"),
        ("observed structures", "behavior evidence", "not proxy existence"),
        ("deformable biology", "construct", "muscle/soft-tissue", "bare-body"),
        ("validate its deformation", "before finalizing", "production topology"),
        ("dependent exterior systems", "not body-deformation acceptance"),
        ("approximations", "static likeness", "not require hidden anatomy"),
        ("query terms", "compact summaries", "schemas=arguments", "alternate terms"),
        ("result schemas are opt-in", "multiview", "completed image once"),
        ("concise", "assumptions from evidence"),
    )
    for concept in concepts:
        assert all(term in guidance for term in concept), concept
    assert all(
        term in guidance
        for term in (
            "substantial multi-stage",
            "persist a compact contract",
            "project.create/apply",
            "milestone prerequisites",
            "required validations",
            "bind the intended document/adapter",
            "active in_progress milestone id",
            "exploratory work stays provisional",
            "accept only after observing required evidence",
            "repair stale prerequisites",
            "simple unbound one-step edits need no contract",
            "accept that anatomical foundation",
        )
    )
    assert len(instructions.encode()) < 6100
    assert "Examples, when relevant:" not in instructions
    for benchmark in ("mallard", "duck", "greyhound"):
        assert benchmark not in guidance


def assert_application_control_policy(instructions: str) -> None:
    policy = " ".join(instructions.lower().split())
    for required in (
        "when a tyvrana adapter is connected",
        "all meaningful",
        "advertised typed operations",
        "do not bypass",
        "tyvrana tooling gap",
        "shell/application cli",
        "direct host apis",
        "another application process",
        "background/headless",
        "intended interactive application instance",
        "explicit adapter target",
        "deliberately rebinding",
        "meaningful checkpoints",
        "typed selection/viewport framing",
        "visible editor progress",
        "window presence proves neither monitor visibility",
        "ui automation",
        "console commands",
        "another editor-control integration",
        "source-code/file editing remains allowed",
        "does not authorize direct",
        "non-authoritative observation/window management must not mutate",
        "compilation inspection",
        "api success is not task success",
    ):
        assert required in policy

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
