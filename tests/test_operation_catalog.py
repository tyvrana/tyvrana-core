"""Contracts and event-driven discovery through the official MCP client."""

import asyncio

from .helpers import adapter
from .mcp_helpers import failure
from .test_mcp import session


async def test_wait_filters_and_registration_revisions() -> None:
    async with session() as (core, client):
        waiting = asyncio.create_task(
            client.call_tool(
                "tyvrana_list_adapters",
                {"application": "Example Editor", "wait_seconds": 2},
            )
        )
        async with adapter(core):
            result = await waiting
            assert result.structured_content["revision"] == 1
            summary = result.structured_content["adapters"][0]
            assert summary["operation_count"] == 1
            assert "operations" not in summary
            changed = asyncio.create_task(
                client.call_tool(
                    "tyvrana_list_adapters", {"after_revision": 1, "wait_seconds": 2}
                )
            )
        assert (await changed).structured_content == {"adapters": [], "revision": 2}
        absent = await client.call_tool(
            "tyvrana_list_adapters",
            {
                "adapter_id": "absent",
                "wait_seconds": 0.01,
            },
        )
        assert absent.structured_content["adapters"] == []


async def test_catalog_pages_selected_schemas_and_stable_hash() -> None:
    async with session() as (core, client):
        names = tuple(f"asset.action_{i:02}" for i in range(7))
        async with adapter(core, operations=names):
            digest = core.registry.get("adapter-a").catalog_sha256
            args = {"adapter_id": "adapter-a", "prefix": "asset.", "limit": 5}
            first = await client.call_tool("tyvrana_list_operations", args)
            page = first.structured_content
            assert page["catalog_sha256"] == digest
            assert page["matched_count"] == 7 and page["next_offset"] == 5
            assert [item["name"] for item in page["operations"]] == list(names[:5])
            assert all("arguments_schema" not in item for item in page["operations"])
            selected = await client.call_tool(
                "tyvrana_list_operations",
                {
                    "adapter_id": "adapter-a",
                    "names": [names[3]],
                    "include_schemas": True,
                },
            )
            contract = selected.structured_content["operations"][0]
            assert contract["name"] == names[3] and contract["effect"] == "read_only"
            assert contract["arguments_schema"]["additionalProperties"] is False
            contract["arguments_schema"]["type"] = "string"
            assert (
                core.registry.get("adapter-a")
                .registration.operations[3]
                .arguments_schema["type"]
                == "object"
            )
            detailed = await client.call_tool(
                "tyvrana_list_operations", {**args, "include_schemas": True}
            )
            assert len(detailed.structured_content["operations"]) == 4
            assert detailed.structured_content["next_offset"] == 4
            missing = await client.call_tool(
                "tyvrana_list_operations",
                {
                    "adapter_id": "adapter-a",
                    "names": ["absent.operation"],
                },
            )
            assert failure(missing)["code"] == "operation_unsupported"
        async with adapter(core, operations=tuple(reversed(names))):
            assert core.registry.get("adapter-a").catalog_sha256 == digest
        async with adapter(core, operations=names[:-1]):
            assert core.registry.get("adapter-a").catalog_sha256 != digest


async def test_cancelled_registry_wait_has_no_owned_tasks() -> None:
    async with session() as (core, client):
        waiting = asyncio.create_task(core.registry.wait_for_change(0, 30))
        await asyncio.sleep(0)
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        async with adapter(core):
            # Cancellation must not consume future notifications.
            await core.registry.wait_for_change(0, 0.1)
        error = await client.call_tool(
            "tyvrana_list_operations",
            {
                "adapter_id": "absent",
                "limit": 999,
            },
        )
        details = failure(error)["details"]
        assert isinstance(details, list)
        detail = details[0]
        assert isinstance(detail, dict)
        assert detail["field"] == "limit" and detail["reason"] == "less_than_equal"
