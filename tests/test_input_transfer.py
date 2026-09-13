import asyncio
import hashlib
from pathlib import Path

import pytest
from tyvrana_protocol import (
    ArtifactAbort,
    ArtifactAccepted,
    ArtifactBegin,
    ArtifactComplete,
    ArtifactDescriptor,
    ArtifactReady,
    CancelRequest,
    OperationRequest,
    OperationSuccess,
    ProtocolError,
    decode_artifact_chunk,
    decode_message,
)

from tyvrana_core import (
    AdapterDisconnected,
    AdapterServer,
    ArtifactError,
    OperationTimeout,
    RemoteOperationError,
)

from .helpers import FakeAdapter, adapter
from .test_artifact_import import local_file


async def receive_input(fake: FakeAdapter) -> tuple[ArtifactBegin, bytes]:
    begin = await fake.receive()
    assert isinstance(begin, ArtifactBegin)
    await fake.send(ArtifactReady(type="artifact.ready", transfer_id=begin.transfer_id))
    content = bytearray()
    while True:
        wire = await fake.websocket.recv()
        if isinstance(wire, bytes):
            chunk = decode_artifact_chunk(wire)
            assert chunk.transfer_id == begin.transfer_id
            assert chunk.offset == len(content)
            content.extend(chunk.payload)
        else:
            assert decode_message(wire.encode()) == ArtifactComplete(
                type="artifact.complete", transfer_id=begin.transfer_id
            )
            break
    assert len(content) == begin.descriptor.byte_size
    assert hashlib.sha256(content).hexdigest() == begin.descriptor.sha256
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            await fake.websocket.recv()
    await fake.send(
        ArtifactAccepted(type="artifact.accepted", transfer_id=begin.transfer_id)
    )
    return begin, bytes(content)


def attached_request(
    server: AdapterServer,
    descriptors: tuple[ArtifactDescriptor, ...],
    *,
    timeout: float = 1.0,
) -> asyncio.Task[OperationSuccess]:
    return asyncio.create_task(
        server.dispatcher.execute(
            adapter_id="adapter-a",
            operation="document.inspect",
            arguments={},
            artifact_ids=tuple(d.artifact_id for d in descriptors),
            timeout=timeout,
        )
    )


@pytest.mark.parametrize("size", [0, 1, 65536, 131089])
async def test_input_precedes_operation_and_remains_reusable(
    server: AdapterServer, tmp_path: Path, size: int
) -> None:
    data = b"x" * size
    source = local_file(tmp_path, data)
    descriptor = await server.artifacts.import_file(str(source))
    transfer_ids = []
    async with adapter(server) as fake:
        connection = server.registry._get_connection("adapter-a")
        for _ in range(2):
            work = attached_request(server, (descriptor,))
            begin, received = await receive_input(fake)
            transfer_ids.append(begin.transfer_id)
            assert received == data
            assert str(source) not in begin.model_dump_json()
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            assert request.artifacts == (descriptor,)
            assert request.request_id == begin.request_id
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result={"ok": True},
                )
            )
            assert (await work).result == {"ok": True}
            assert server.artifacts.metadata(descriptor.artifact_id) == descriptor
            assert not connection._outgoing and connection.pending_count == 0
    assert len(set(transfer_ids)) == 2
    assert server.artifacts.entry_count == 1


async def test_two_inputs_complete_before_request(
    server: AdapterServer, tmp_path: Path
) -> None:
    descriptors = tuple(
        [
            await server.artifacts.import_file(
                str(local_file(tmp_path, b"data" + bytes([i]), f"{i}.bin"))
            )
            for i in range(2)
        ]
    )
    async with adapter(server) as fake:
        work = attached_request(server, descriptors)
        inputs = [await receive_input(fake) for _ in descriptors]
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        assert request.artifacts == descriptors
        assert all(begin.request_id == request.request_id for begin, data in inputs)
        await fake.send(
            OperationSuccess(
                type="operation.success", request_id=request.request_id, result=None
            )
        )
        await work
    assert server.artifacts.entry_count == 2


@pytest.mark.parametrize("same_artifact", [True, False])
async def test_concurrent_inputs_and_unattached_operation(
    server: AdapterServer, tmp_path: Path, same_artifact: bool
) -> None:
    first = await server.artifacts.import_file(str(local_file(tmp_path, b"a" * 131089)))
    second = (
        first
        if same_artifact
        else await server.artifacts.import_file(str(local_file(tmp_path, b"b" * 65539)))
    )
    async with adapter(server) as fake:
        work = [
            attached_request(server, (first,)),
            attached_request(server, (second,)),
            attached_request(server, ()),
        ]
        transfers: dict[str, tuple[ArtifactBegin, bytearray]] = {}
        received: set[str] = set()
        while len(received) < 3:
            wire = await fake.websocket.recv()
            if isinstance(wire, bytes):
                chunk = decode_artifact_chunk(wire)
                begin, content = transfers[chunk.transfer_id]
                assert chunk.offset == len(content)
                content.extend(chunk.payload)
                continue
            message = decode_message(wire.encode())
            if isinstance(message, ArtifactBegin):
                assert message.transfer_id not in transfers
                transfers[message.transfer_id] = (message, bytearray())
                await fake.send(
                    ArtifactReady(
                        type="artifact.ready", transfer_id=message.transfer_id
                    )
                )
            elif isinstance(message, ArtifactComplete):
                begin, content = transfers[message.transfer_id]
                assert hashlib.sha256(content).hexdigest() == begin.descriptor.sha256
                await fake.send(
                    ArtifactAccepted(
                        type="artifact.accepted", transfer_id=message.transfer_id
                    )
                )
            else:
                assert isinstance(message, OperationRequest)
                received.add(message.request_id)
                assert len(
                    [
                        b
                        for b, c in transfers.values()
                        if b.request_id == message.request_id
                    ]
                ) == len(message.artifacts)
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=message.request_id,
                        result=None,
                    )
                )
        await asyncio.gather(*work)
        assert len(transfers) == 2
        assert not server.registry._get_connection("adapter-a")._outgoing


@pytest.mark.parametrize(
    "action",
    [
        "bad_ready",
        "bad_accepted",
        "abort",
        "disconnect",
        "cancel",
        "timeout",
        "unknown_ack",
        "early_response",
    ],
)
async def test_failed_input_never_executes_and_releases_transfer_state(
    server: AdapterServer, tmp_path: Path, action: str
) -> None:
    descriptor = await server.artifacts.import_file(str(local_file(tmp_path, b"input")))
    async with adapter(server) as fake:
        connection = server.registry._get_connection("adapter-a")
        work = attached_request(
            server,
            (descriptor,),
            timeout=0.15 if action in {"timeout", "unknown_ack"} else 1,
        )
        begin = await fake.receive()
        assert isinstance(begin, ArtifactBegin)
        expected: type[BaseException]
        if action == "bad_ready":
            expected = ArtifactError
            await fake.send(
                ArtifactAccepted(
                    type="artifact.accepted", transfer_id=begin.transfer_id
                )
            )
        elif action == "bad_accepted":
            expected = ArtifactError
            await fake.send(
                ArtifactReady(type="artifact.ready", transfer_id=begin.transfer_id)
            )
            assert isinstance(await fake.websocket.recv(), bytes)
            assert isinstance(await fake.receive(), ArtifactComplete)
            await fake.send(
                ArtifactReady(type="artifact.ready", transfer_id=begin.transfer_id)
            )
        elif action == "abort":
            expected = RemoteOperationError
            await fake.send(
                ArtifactAbort(
                    type="artifact.abort",
                    transfer_id=begin.transfer_id,
                    error=ProtocolError(code="capacity", message="No capacity"),
                )
            )
        elif action == "disconnect":
            expected = AdapterDisconnected
            await fake.websocket.close()
        elif action == "early_response":
            expected = AdapterDisconnected
            await fake.send(
                OperationSuccess(
                    type="operation.success", request_id=begin.request_id, result=None
                )
            )
        elif action == "cancel":
            expected = asyncio.CancelledError
            work.cancel()
        else:
            expected = OperationTimeout
            if action == "unknown_ack":
                await fake.send(
                    ArtifactReady(type="artifact.ready", transfer_id="f" * 32)
                )
        with pytest.raises(expected):
            await work
        assert not connection._outgoing and connection.pending_count == 0
        assert not connection._started
        assert server.artifacts._entries[descriptor.artifact_id].leases == 0
        assert server.artifacts.metadata(descriptor.artifact_id) == descriptor
        if action not in {"disconnect", "early_response"}:
            assert isinstance(await fake.receive(), CancelRequest)


@pytest.mark.parametrize("change", ["hash", "size"])
async def test_store_corruption_cannot_reach_operation(
    server: AdapterServer, tmp_path: Path, change: str
) -> None:
    descriptor = await server.artifacts.import_file(str(local_file(tmp_path, b"input")))
    path = server.artifacts._entries[descriptor.artifact_id].path
    local_file(path.parent, b"other" if change == "hash" else b"longer", path.name)
    async with adapter(server) as fake:
        work = attached_request(server, (descriptor,))
        begin = await fake.receive()
        assert isinstance(begin, ArtifactBegin)
        await fake.send(
            ArtifactReady(type="artifact.ready", transfer_id=begin.transfer_id)
        )
        if change == "hash":
            assert isinstance(await fake.websocket.recv(), bytes)
        assert isinstance(await fake.receive(), CancelRequest)
        with pytest.raises(ArtifactError):
            await work
        assert not server.registry._get_connection("adapter-a")._outgoing


async def test_released_artifact_rejected_before_dispatch(
    server: AdapterServer, tmp_path: Path
) -> None:
    descriptor = await server.artifacts.import_file(str(local_file(tmp_path, b"input")))
    server.artifacts.release(descriptor.artifact_id)
    async with adapter(server) as fake:
        with pytest.raises(ArtifactError):
            await attached_request(server, (descriptor,))
        assert server.dispatcher.pending_count == 0
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.01):
                await fake.websocket.recv()


async def test_release_during_admitted_transfer_preserves_current_use(
    server: AdapterServer, tmp_path: Path
) -> None:
    descriptor = await server.artifacts.import_file(str(local_file(tmp_path, b"input")))
    async with adapter(server) as fake:
        work = attached_request(server, (descriptor,))
        begin = await fake.receive()
        assert isinstance(begin, ArtifactBegin)
        assert server.artifacts.release(descriptor.artifact_id)
        assert server.artifacts.reserved_bytes == 5
        await fake.send(
            ArtifactReady(type="artifact.ready", transfer_id=begin.transfer_id)
        )
        assert decode_artifact_chunk(await fake.websocket.recv()).payload == b"input"  # type: ignore[arg-type]
        assert isinstance(await fake.receive(), ArtifactComplete)
        await fake.send(
            ArtifactAccepted(type="artifact.accepted", transfer_id=begin.transfer_id)
        )
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        assert server.artifacts.entry_count == 0
        await fake.send(
            OperationSuccess(
                type="operation.success", request_id=request.request_id, result=None
            )
        )
        await work


@pytest.mark.parametrize("during_next_input", [True, False])
async def test_abort_after_acceptance_still_fails_its_request(
    server: AdapterServer, tmp_path: Path, during_next_input: bool
) -> None:
    descriptors = tuple(
        [
            await server.artifacts.import_file(str(local_file(tmp_path, b"input")))
            for _ in range(2 if during_next_input else 1)
        ]
    )
    async with adapter(server) as fake:
        connection = server.registry._get_connection("adapter-a")
        work = attached_request(server, descriptors)
        first, _ = await receive_input(fake)
        following = await fake.receive()
        assert isinstance(
            following, ArtifactBegin if during_next_input else OperationRequest
        )
        await fake.send(
            ArtifactAbort(
                type="artifact.abort",
                transfer_id=first.transfer_id,
                error=ProtocolError(
                    code="input_lost", message="Accepted input was lost"
                ),
            )
        )
        with pytest.raises(RemoteOperationError) as error:
            await work
        assert error.value.error.code == "input_lost"
        assert isinstance(await fake.receive(), CancelRequest)
        assert not connection._input_ids and not connection._outgoing
        assert connection.pending_count == 0
