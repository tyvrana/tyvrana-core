"""Referenced outputs survive MCP delivery and export with integrity/atomicity."""

import asyncio
from pathlib import Path

import pytest
from mcp.types import ImageContent
from tyvrana_protocol import OperationRequest, OperationSuccess

from tyvrana_core import ArtifactError, ArtifactStore, CoreConfig

from .artifact_helpers import begin_message, upload
from .helpers import adapter
from .test_mcp import session
from .test_mcp_artifacts import png


async def test_reference_delivery_export_and_release(tmp_path: Path) -> None:
    async with session() as (core, client), adapter(core) as fake:
        task = asyncio.create_task(
            client.call_tool(
                "tyvrana_execute_operation",
                {
                    "adapter_id": "adapter-a",
                    "operation": "document.inspect",
                    "arguments": {},
                    "artifact_delivery": "reference",
                },
            )
        )
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        data = png()
        message = begin_message(data, request.request_id, media_type="image/png")
        await upload(fake, message, data)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=request.request_id,
                result={},
                artifacts=(message.descriptor,),
            )
        )
        result = await task
        assert not result.is_error
        assert not any(isinstance(c, ImageContent) for c in result.content)
        identifier = message.descriptor.artifact_id
        assert result.structured_content["retained_artifact_ids"] == [identifier]
        destination = tmp_path / "captured.png"
        exported = await client.call_tool(
            "tyvrana_export_artifact",
            {"artifact_id": identifier, "path": str(destination)},
        )
        assert not exported.is_error, exported.content
        assert destination.read_bytes() == data
        assert exported.structured_content["released"]
        assert core.artifacts.entry_count == 0


async def test_export_rejects_overwrite_symlink_and_corruption(tmp_path: Path) -> None:
    store = ArtifactStore(CoreConfig())
    store.start()
    try:
        source = tmp_path / "source"
        source.write_bytes(b"retained binary")
        descriptor = await store.import_file(str(source))
        target = tmp_path / "target"
        target.write_bytes(b"preserved")
        with pytest.raises(ArtifactError, match="overwrite"):
            await store.export_file(descriptor.artifact_id, str(target))
        link = tmp_path / "link"
        link.symlink_to(target)
        with pytest.raises(ArtifactError, match="symlink"):
            await store.export_file(descriptor.artifact_id, str(link), overwrite=True)
        assert target.read_bytes() == b"preserved"
        await store.export_file(descriptor.artifact_id, str(target), overwrite=True)
        assert target.read_bytes() == source.read_bytes()
        entry = store._entries[descriptor.artifact_id]
        entry.path.write_bytes(b"corrupted")
        with pytest.raises(ArtifactError, match="integrity"):
            await store.export_file(descriptor.artifact_id, str(target), overwrite=True)
        assert target.read_bytes() == source.read_bytes()
        assert not list(tmp_path.glob(".tyvrana-export-*"))  # noqa: ASYNC240 - Isolated test directory.
    finally:
        store.close()


async def test_cancelled_export_cleans_partial_and_keeps_source(tmp_path: Path) -> None:
    store = ArtifactStore(CoreConfig())
    store.start()
    try:
        source = tmp_path / "large"
        source.write_bytes(b"q" * 200000)
        descriptor = await store.import_file(str(source))
        destination = tmp_path / "destination"
        task = asyncio.create_task(
            store.export_file(descriptor.artifact_id, str(destination))
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not destination.exists()
        assert not list(tmp_path.glob(".tyvrana-export-*"))  # noqa: ASYNC240 - Isolated test directory.
        assert store.metadata(descriptor.artifact_id) == descriptor
    finally:
        store.close()
