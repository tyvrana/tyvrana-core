import asyncio
import hashlib
import os
from pathlib import Path

import pytest
from mcp import Client
from mcp.types import TextContent
from tyvrana_protocol import ArtifactDescriptor, OperationRequest, OperationSuccess
from websockets.asyncio.client import connect

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.mcp import create_mcp_server

from .helpers import FakeAdapter, adapter, eventually
from .mcp_helpers import failure
from .test_artifact_import import local_file
from .test_input_transfer import receive_input
from .test_mcp import session
from .test_mcp_artifacts import png
from .test_mcp_stdio import register, stdio_session


async def import_and_execute(client: Client, fake: FakeAdapter, source: Path) -> None:
    imported = await client.call_tool(
        "tyvrana_import_artifact", {"path": str(source), "name": "Texture"}
    )
    assert not imported.is_error
    descriptor = ArtifactDescriptor.model_validate(imported.structured_content)
    assert descriptor.byte_size == len(png())
    assert descriptor.sha256 == hashlib.sha256(png()).hexdigest()
    assert descriptor.media_type == "image/png" and descriptor.name == "Texture"
    assert all(isinstance(content, TextContent) for content in imported.content)
    assert str(source) not in imported.model_dump_json()
    assert (
        "data" not in imported.structured_content
        and "path" not in imported.structured_content
    )
    os.unlink(source)
    work = asyncio.create_task(
        client.call_tool(
            "tyvrana_execute_operation",
            {
                "adapter_id": "adapter-a",
                "operation": "document.inspect",
                "arguments": {},
                "artifact_ids": [descriptor.artifact_id],
            },
        )
    )
    begin, content = await receive_input(fake)
    assert content == png() and begin.descriptor == descriptor
    assert str(source) not in begin.model_dump_json()
    request = await fake.receive()
    assert isinstance(request, OperationRequest) and request.artifacts == (descriptor,)
    await fake.send(
        OperationSuccess(
            type="operation.success",
            request_id=request.request_id,
            result={"loaded": True},
        )
    )
    response = await work
    assert not response.is_error and response.structured_content == {
        "result": {"loaded": True}
    }
    for expected in (True, False):
        released = await client.call_tool(
            "tyvrana_release_artifact", {"artifact_id": descriptor.artifact_id}
        )
        assert not released.is_error and released.structured_content == {
            "released": expected
        }


async def test_mcp_import_execute_release(tmp_path: Path) -> None:
    source = local_file(tmp_path, png(), "source.png")
    async with session() as (core, client), adapter(core) as fake:
        await import_and_execute(client, fake, source)
        assert core.artifacts.entry_count == 0


async def test_stdio_import_and_attached_execution(tmp_path: Path) -> None:
    source = local_file(tmp_path, png(), "source.png")
    async with (
        stdio_session(tmp_path) as (client, uri),
        connect(uri, proxy=None) as websocket,
    ):
        fake = FakeAdapter(websocket)
        await register(fake, client)
        await import_and_execute(client, fake, source)


@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "directory",
        "oversize",
        "bad_mime",
        "bad_name",
        "null",
        "unknown",
        "wrong_path_type",
    ],
)
async def test_import_errors_are_sanitized_tool_failures(
    tmp_path: Path, kind: str
) -> None:
    source = local_file(tmp_path, png(), "private_source.png")
    arguments: dict[str, object] = {"path": str(source)}
    if kind == "missing":
        os.unlink(source)
    elif kind == "directory":
        arguments["path"] = str(tmp_path)
    elif kind == "bad_mime":
        arguments["media_type"] = "Image/PNG"
    elif kind == "bad_name":
        arguments["name"] = "folder/image"
    elif kind == "null":
        arguments["name"] = None
    elif kind == "unknown":
        arguments["bytes"] = "unsupported"
    elif kind == "wrong_path_type":
        arguments["path"] = True
    core = AdapterServer(
        CoreConfig(port=0, max_artifact_size=1 if kind == "oversize" else 100)
    )
    async with Client(create_mcp_server(core)) as client:
        result = await client.call_tool("tyvrana_import_artifact", arguments)
        error = failure(result)
        assert error["code"] in {"artifact_import_failed", "invalid_arguments"}
        assert str(source) not in result.model_dump_json()
        assert core.artifacts.entry_count == 0


@pytest.mark.parametrize(
    "ids", [None, "a", ["bad"], ["1" * 32] * 2, [f"{i:032x}" for i in range(9)], [1]]
)
async def test_invalid_attachment_ids(ids: object) -> None:
    async with session() as (core, client):
        result = await client.call_tool(
            "tyvrana_execute_operation",
            {
                "adapter_id": "adapter-a",
                "operation": "document.inspect",
                "arguments": {},
                "artifact_ids": ids,
            },
        )
        assert failure(result)["code"] == "invalid_arguments"
        assert core.dispatcher.pending_count == 0


async def test_unreleased_import_is_cleaned_at_mcp_shutdown(tmp_path: Path) -> None:
    source = local_file(tmp_path, png())
    async with session() as (core, client):
        response = await client.call_tool(
            "tyvrana_import_artifact", {"path": str(source)}
        )
        assert not response.is_error
        assert core.artifacts.entry_count == 1
    assert core.artifacts.entry_count == 0


async def test_cancelled_mcp_import_cleans_partial_file(tmp_path: Path) -> None:
    source = local_file(tmp_path, b"x" * (8 * 1024 * 1024))
    async with session() as (core, client):
        work = asyncio.create_task(
            client.call_tool("tyvrana_import_artifact", {"path": str(source)})
        )
        await eventually(lambda: core.artifacts.active_count == 1)
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await work
        await eventually(lambda: core.artifacts.active_count == 0)
        assert core.artifacts.entry_count == 0 and not core.artifacts._transfers
