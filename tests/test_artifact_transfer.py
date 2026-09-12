import asyncio
from pathlib import Path

import pytest
from tyvrana_protocol import (
    ArtifactAbort,
    ArtifactAccepted,
    ArtifactComplete,
    CancelRequest,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
    ProtocolError,
    encode_artifact_chunk,
)

from tyvrana_core import (
    AdapterDisconnected,
    AdapterServer,
    ArtifactError,
    CoreConfig,
    RemoteOperationError,
)

from .artifact_helpers import begin, begin_message, upload
from .helpers import adapter, eventually, execute


@pytest.mark.parametrize("size", [0, 1, 65536, 131089])
async def test_successful_transfer(server: AdapterServer, size: int) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        data = b"x" * size
        message = begin_message(data, request.request_id)
        await upload(fake, message, data)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=request.request_id,
                result={"ok": True},
                artifacts=(message.descriptor,),
            )
        )
        response = await task
        assert response.artifacts == (message.descriptor,)
        with server.artifacts.open(message.descriptor.artifact_id) as stream:
            assert stream.read() == data
    assert server.artifacts.entry_count == 1
    server.artifacts.release(message.descriptor.artifact_id)
    assert server.artifacts.entry_count == 0


@pytest.mark.parametrize("same_request", [False, True])
async def test_concurrent_interleaved_transfers_and_normal_requests(
    server: AdapterServer, same_request: bool
) -> None:
    async with adapter(server) as fake:
        tasks = [execute(server) for _ in range(3)]
        requests = [await fake.receive() for _ in tasks]
        assert all(isinstance(r, OperationRequest) for r in requests)
        ids = [r.request_id for r in requests if isinstance(r, OperationRequest)]
        messages = [
            begin_message(b"abcd", ids[0]),
            begin_message(b"wxyz", ids[0] if same_request else ids[1]),
        ]
        for message in messages:
            await begin(fake, message)
        for offset in range(4):
            for message, data in zip(messages, [b"abcd", b"wxyz"], strict=True):
                await fake.websocket.send(
                    encode_artifact_chunk(
                        message.transfer_id, offset, data[offset : offset + 1]
                    )
                )
        await fake.send(
            OperationSuccess(
                type="operation.success", request_id=ids[2], result="normal"
            )
        )
        assert (await tasks[2]).result == "normal"
        for message in reversed(messages):
            await fake.send(
                ArtifactComplete(
                    type="artifact.complete", transfer_id=message.transfer_id
                )
            )
            assert isinstance(await fake.receive(), ArtifactAccepted)
        for request_id in ids[:2]:
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request_id,
                    result=None,
                    artifacts=tuple(
                        m.descriptor for m in messages if m.request_id == request_id
                    ),
                )
            )
        results = await asyncio.gather(*tasks)
        assert sum(len(r.artifacts) for r in results) == 2


@pytest.mark.parametrize(
    "kind",
    [
        "hash",
        "few",
        "many",
        "offset",
        "repeat",
        "abort",
        "missing",
        "changed",
        "incomplete",
    ],
)
async def test_transfer_failure_isolated_and_partial_files_removed(
    server: AdapterServer, kind: str
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        message = begin_message(b"abc", request.request_id)
        await begin(fake, message)
        if kind == "abort":
            await fake.send(
                ArtifactAbort(
                    type="artifact.abort",
                    transfer_id=message.transfer_id,
                    error=ProtocolError(
                        code="render_failed", message="Rendering failed"
                    ),
                )
            )
        elif kind in ("missing", "changed", "incomplete"):
            if kind != "incomplete":
                await fake.websocket.send(
                    encode_artifact_chunk(message.transfer_id, 0, b"abc")
                )
                await fake.send(
                    ArtifactComplete(
                        type="artifact.complete", transfer_id=message.transfer_id
                    )
                )
                assert isinstance(await fake.receive(), ArtifactAccepted)
            descriptors = (
                ()
                if kind == "missing"
                else (
                    message.descriptor.model_copy(update={"name": "changed"})
                    if kind == "changed"
                    else message.descriptor,
                )
            )
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result={},
                    artifacts=descriptors,
                )
            )
        else:
            if kind != "few":
                data = (
                    b"wrong" if kind == "many" else b"xyz" if kind == "hash" else b"a"
                )
                await fake.websocket.send(
                    encode_artifact_chunk(
                        message.transfer_id, 1 if kind == "offset" else 0, data
                    )
                )
                if kind == "repeat":
                    await fake.websocket.send(
                        encode_artifact_chunk(message.transfer_id, 0, b"a")
                    )
            if kind in ("hash", "few"):
                await fake.send(
                    ArtifactComplete(
                        type="artifact.complete", transfer_id=message.transfer_id
                    )
                )
            assert isinstance(await fake.receive(), ArtifactAbort)
        with pytest.raises(RemoteOperationError) as caught:
            await task
        assert caught.value.error.code == (
            "render_failed" if kind == "abort" else "artifact_transfer_failed"
        )
        if kind not in ("missing", "changed", "incomplete"):
            assert isinstance(await fake.receive(), CancelRequest)
        assert server.artifacts.entry_count == 0
        assert server.artifacts._directory is not None
        assert list(Path(server.artifacts._directory.name).iterdir()) == []  # noqa: ASYNC240 - Bounded test directory.
        next_task = execute(server)
        following = await fake.receive()
        assert isinstance(following, OperationRequest)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=following.request_id,
                result="healthy",
            )
        )
        assert (await next_task).result == "healthy"


@pytest.mark.parametrize(
    "action",
    [
        "disconnect",
        "shutdown",
        "cancel",
        "failure",
        "cancel_after_complete",
        "cancel_after_response",
    ],
)
async def test_cleanup_during_operation(server: AdapterServer, action: str) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        message = begin_message(b"abc", request.request_id)
        if action.startswith("cancel_after"):
            await upload(fake, message, b"abc")
        else:
            await begin(fake, message)
            await fake.websocket.send(
                encode_artifact_chunk(message.transfer_id, 0, b"a")
            )
        if action == "disconnect":
            await fake.websocket.close()
        elif action == "shutdown":
            await server.stop()
        elif action == "failure":
            await fake.send(
                OperationFailure(
                    type="operation.failure",
                    request_id=request.request_id,
                    error=ProtocolError(code="failed", message="Failed"),
                )
            )
        else:
            if action == "cancel_after_response":
                server.registry._get_connection("adapter-a").resolve(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result=None,
                        artifacts=(message.descriptor,),
                    )
                )
            task.cancel()
        with pytest.raises(
            (AdapterDisconnected, RemoteOperationError, asyncio.CancelledError)
        ):
            await task
        assert server.artifacts.entry_count == server.artifacts.reserved_bytes == 0
        with pytest.raises(ArtifactError):
            server.artifacts.open(message.descriptor.artifact_id)


async def test_unknown_late_chunks_do_not_disrupt_normal_requests(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        await fake.websocket.send(encode_artifact_chunk("1" * 32, 0, b"x"))
        assert isinstance(await fake.receive(), ArtifactAbort)
        await fake.send(begin_message(b"x", "unknown"))
        assert isinstance(await fake.receive(), ArtifactAbort)
        assert isinstance(await fake.receive(), CancelRequest)
        assert server.artifacts.entry_count == 0
        assert server.registry.get("adapter-a").connected


@pytest.mark.parametrize("kind", ["individual", "storage", "entries", "concurrency"])
async def test_receiver_admission_bounds(kind: str) -> None:
    config = CoreConfig(
        port=0,
        max_artifact_size=10,
        max_artifact_storage=10,
        max_artifact_entries=1 if kind == "entries" else 10,
        max_artifact_transfers=1 if kind == "concurrency" else 4,
    )
    async with AdapterServer(config) as server, adapter(server) as fake:
        first = execute(server)
        r1 = await fake.receive()
        assert isinstance(r1, OperationRequest)
        await begin(fake, begin_message(b"x", r1.request_id))
        second = execute(server)
        r2 = await fake.receive()
        assert isinstance(r2, OperationRequest)
        size = 11 if kind == "individual" else 10 if kind == "storage" else 0
        await fake.send(begin_message(b"x" * size, r2.request_id))
        assert isinstance(await fake.receive(), ArtifactAbort)
        assert isinstance(await fake.receive(), CancelRequest)
        with pytest.raises(RemoteOperationError):
            await second
        assert not first.done()
        assert server.artifacts.entry_count == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first


async def test_binary_json_registration_is_rejected(server: AdapterServer) -> None:
    from tyvrana_protocol import AdapterRegistration, encode_message
    from websockets.asyncio.client import connect

    async with connect(server.uri, proxy=None) as socket:
        await socket.send(
            encode_message(
                AdapterRegistration(
                    type="adapter.register",
                    instance_id="binary",
                    application="App",
                    operations=(),
                )
            )
        )
        await eventually(lambda: socket.close_code is not None)
        assert socket.close_code == 1008


async def test_fragmented_websocket_chunk_reassembles_before_validation(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        message = begin_message(b"abc", request.request_id)
        await begin(fake, message)
        wire = encode_artifact_chunk(message.transfer_id, 0, b"abc")
        await fake.websocket.send([wire[:7], wire[7:24], wire[24:]])
        await fake.send(
            ArtifactComplete(type="artifact.complete", transfer_id=message.transfer_id)
        )
        assert isinstance(await fake.receive(), ArtifactAccepted)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=request.request_id,
                result=None,
                artifacts=(message.descriptor,),
            )
        )
        assert (await task).artifacts == (message.descriptor,)


@pytest.mark.parametrize("collision", ["transfer", "artifact"])
async def test_collision_cannot_overwrite_reserved_data(
    server: AdapterServer, collision: str
) -> None:
    async with adapter(server) as fake:
        first = execute(server)
        r1 = await fake.receive()
        assert isinstance(r1, OperationRequest)
        original = begin_message(b"original", r1.request_id)
        await begin(fake, original)
        second = execute(server)
        r2 = await fake.receive()
        assert isinstance(r2, OperationRequest)
        candidate = begin_message(b"replacement", r2.request_id)
        candidate = candidate.model_copy(
            update={"transfer_id": original.transfer_id}
            if collision == "transfer"
            else {"descriptor": original.descriptor}
        )
        await fake.send(candidate)
        if collision == "transfer":
            outcomes = await asyncio.gather(first, second, return_exceptions=True)
            assert all(isinstance(outcome, AdapterDisconnected) for outcome in outcomes)
            assert server.artifacts.entry_count == 0
        else:
            assert isinstance(await fake.receive(), ArtifactAbort)
            assert isinstance(await fake.receive(), CancelRequest)
            with pytest.raises(RemoteOperationError):
                await second
            assert not first.done()
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first


async def test_operation_deadline_cleans_incoming_artifact(
    server: AdapterServer,
) -> None:
    from tyvrana_core import OperationTimeout

    async with adapter(server) as fake:
        task = asyncio.create_task(
            server.dispatcher.execute(
                adapter_id="adapter-a",
                operation="document.inspect",
                arguments=None,
                timeout=0.1,
            )
        )
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        await begin(fake, begin_message(b"abc", request.request_id))
        with pytest.raises(OperationTimeout):
            await task
        assert isinstance(await fake.receive(), CancelRequest)
        assert server.artifacts.entry_count == 0
