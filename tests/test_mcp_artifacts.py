import asyncio
import base64
import hashlib
import struct
import zlib
from pathlib import Path

import anyio
import pytest
from mcp import Client
from mcp.types import ImageContent
from tyvrana_protocol import (
    ArtifactAbort,
    ArtifactComplete,
    CancelRequest,
    OperationRequest,
    OperationSuccess,
)
from websockets.asyncio.client import connect

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.mcp import create_mcp_server

from .artifact_helpers import begin, begin_message, upload
from .helpers import FakeAdapter, adapter
from .mcp_helpers import execute, failure
from .test_mcp import session
from .test_mcp_stdio import register, stdio_session


def png() -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack("!I", len(data))
            + kind
            + data
            + struct.pack("!I", zlib.crc32(kind + data))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack("!2I5B", 2, 1, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\x00\xff\x00\x00\x00\xff\x00"))
        + chunk(b"IEND", b"")
    )


@pytest.mark.parametrize(
    "media_type", ["image/png", "image/jpeg", "application/octet-stream"]
)
async def test_mcp_image_and_structured_metadata_release(media_type: str) -> None:
    async with session() as (core, client), adapter(core) as fake:
        task = execute(client)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        data = png() if media_type == "image/png" else b"opaque-test-bytes"
        message = begin_message(data, request.request_id, media_type=media_type)
        await upload(fake, message, data)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=request.request_id,
                result={"width": 2},
                artifacts=(message.descriptor,),
            )
        )
        result = await task
        assert not result.is_error
        expected = {
            "result": {"width": 2},
            "artifacts": [message.descriptor.model_dump(mode="json")],
        }
        if not media_type.startswith("image/"):
            expected["retained_artifact_ids"] = [message.descriptor.artifact_id]
        assert result.structured_content == expected
        images = [c for c in result.content if isinstance(c, ImageContent)]
        if media_type.startswith("image/"):
            assert len(images) == 1
            assert images[0].mime_type == media_type
            assert base64.b64decode(images[0].data, validate=True) == data
        else:
            assert images == []
        wire = result.model_dump_json(by_alias=True)
        assert (
            "file:" not in wire
            and "tyvrana-artifacts-" not in wire
            and "/tmp/" not in wire
        )
        assert "data" not in result.structured_content
        if not media_type.startswith("image/"):
            assert (
                core.artifacts.metadata(message.descriptor.artifact_id)
                == message.descriptor
            )
            core.artifacts.release(message.descriptor.artifact_id)
        assert core.artifacts.entry_count == 0


@pytest.mark.parametrize("count", [1, 2])
async def test_mcp_inline_total_bound_retains_references(count: int) -> None:
    data = png()
    limit = len(data) * count - 1
    core = AdapterServer(CoreConfig(port=0, max_inline_image_bytes=limit))
    async with Client(create_mcp_server(core)) as client, adapter(core) as fake:
        task = execute(client)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        messages = [
            begin_message(data, request.request_id, media_type="image/png")
            for _ in range(count)
        ]
        for message in messages:
            await upload(fake, message, data)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=request.request_id,
                result=None,
                artifacts=tuple(m.descriptor for m in messages),
            )
        )
        result = await task
        assert not result.is_error
        assert not any(isinstance(c, ImageContent) for c in result.content)
        assert result.structured_content["retained_artifact_ids"] == [
            m.descriptor.artifact_id for m in messages
        ]
        assert core.artifacts.entry_count == count


async def test_transfer_failure_is_mcp_tool_error() -> None:
    async with session() as (core, client), adapter(core) as fake:
        task = execute(client)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        message = begin_message(png(), request.request_id, media_type="image/png")
        await begin(fake, message)
        await fake.send(
            ArtifactComplete(type="artifact.complete", transfer_id=message.transfer_id)
        )
        assert isinstance(await fake.receive(), ArtifactAbort)
        assert isinstance(await fake.receive(), CancelRequest)
        assert failure(await task)["code"] == "artifact_transfer_failed"
        assert core.artifacts.entry_count == 0


async def test_real_stdio_client_receives_png(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with (
        stdio_session(tmp_path) as (client, uri),
        connect(uri, proxy=None) as socket,
    ):
        fake = FakeAdapter(socket)
        await register(fake, client)
        task = execute(client)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        data = png()
        message = begin_message(data, request.request_id, media_type="image/png")
        await upload(fake, message, data)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=request.request_id,
                result={"width": 2, "height": 1},
                artifacts=(message.descriptor,),
            )
        )
        result = await task
        images = [c for c in result.content if isinstance(c, ImageContent)]
        assert not result.is_error and len(images) == 1
        decoded = base64.b64decode(images[0].data, validate=True)
        assert decoded == data
        assert (
            hashlib.sha256(decoded).hexdigest()
            == result.structured_content["artifacts"][0]["sha256"]
        )
        assert "tyvrana-artifacts-" not in result.model_dump_json()
    assert "Failed to parse" not in caplog.text


async def test_cancelled_mcp_call_cleans_partial_transfer() -> None:
    async with session() as (core, client), adapter(core) as fake:
        scopes: asyncio.Queue[anyio.CancelScope] = asyncio.Queue()

        async def call() -> None:
            with anyio.CancelScope() as scope:
                scopes.put_nowait(scope)
                await client.call_tool(
                    "tyvrana_execute_operation",
                    {
                        "adapter_id": "adapter-a",
                        "operation": "document.inspect",
                        "arguments": {},
                    },
                )

        task = asyncio.create_task(call())
        scope = await scopes.get()
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        await begin(fake, begin_message(png(), request.request_id))
        assert core.artifacts.entry_count == 1
        scope.cancel()
        await task
        assert isinstance(await fake.receive(), CancelRequest)
        assert core.artifacts.entry_count == 0
