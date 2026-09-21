"""Conditional contracts work across queries without per-client server state."""

import pytest

from tyvrana_core.mcp import tools

from .helpers import contract
from .test_mcp import session


async def test_overlapping_contracts_refresh_and_changed_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, second, third = (
        contract("asset." + n) for n in ("create", "edit", "inspect")
    )
    monkeypatch.setattr(tools, "CONTRACTS", (first, second, third))
    async with session() as (_, client):
        initial = await client.call_tool(
            "tyvrana_list_operations",
            {
                "names": [first.name, second.name],
                "schemas": "arguments",
            },
        )
        rows = initial.structured_content["operations"]
        known = {row["name"]: row["contract_sha256"] for row in rows}
        assert all(row["schema_status"] == "included" for row in rows)
        overlap = await client.call_tool(
            "tyvrana_list_operations",
            {
                "prefix": "asset.",
                "schemas": "arguments",
                "known_contracts": known,
            },
        )
        rows = {row["name"]: row for row in overlap.structured_content["operations"]}
        assert rows[first.name]["schema_status"] == "unchanged"
        assert "arguments_schema" not in rows[first.name]
        assert rows[third.name]["schema_status"] == "included"
        assert "arguments_schema" in rows[third.name]
        changed = first.model_copy(
            update={
                "arguments_schema": {
                    "type": "object",
                    "properties": {"label": {"type": "string"}},
                }
            }
        )
        # Even unchanged catalog metadata must not hide a changed contract.
        monkeypatch.setattr(tools, "CONTRACTS", (changed, second, third))
        refreshed = await client.call_tool(
            "tyvrana_list_operations",
            {
                "names": [first.name, second.name],
                "schemas": "arguments",
                "known_contracts": known,
            },
        )
        rows = {row["name"]: row for row in refreshed.structured_content["operations"]}
        assert rows[first.name]["schema_status"] == "included"
        assert rows[first.name]["contract_sha256"] != known[first.name]
        assert rows[second.name]["schema_status"] == "unchanged"
        full = await client.call_tool(
            "tyvrana_list_operations",
            {
                "names": [second.name],
                "schemas": "full",
                "known_contracts": known,
            },
        )
        assert "result_schema" in full.structured_content["operations"][0]
        forced = await client.call_tool(
            "tyvrana_list_operations",
            {
                "names": [second.name],
                "schemas": "arguments",
            },
        )
        assert "arguments_schema" in forced.structured_content["operations"][0]


@pytest.mark.parametrize(
    "known",
    [
        {"asset.inspect": "bad"},
        {"asset.op" + str(i): "a" * 64 for i in range(65)},
    ],
)
def test_known_contracts_are_bounded(known: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        tools.ListOperationsInput(known_contracts=known)
