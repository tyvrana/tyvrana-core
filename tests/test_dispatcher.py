import asyncio

import pytest
from tyvrana_protocol import (
    AdapterEvent,
    CancelRequest,
    JsonValue,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
    ProtocolError,
)

from tyvrana_core import (
    AdapterDisconnected,
    AdapterNotFound,
    AdapterServer,
    CoreConfig,
    OperationTimeout,
    RemoteOperationError,
    UnsupportedOperation,
)

from .helpers import adapter, eventually, execute


@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        [],
        False,
        42,
        1.5,
        "text",
        {"nested": [1, True, None, {"🌍": [[], {}]}]},
    ],
)
async def test_success_and_recursive_json(
    server: AdapterServer, result: JsonValue
) -> None:
    async with adapter(server) as fake:
        task = execute(server, arguments=result)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        assert request.arguments == result
        assert server.dispatcher.pending_count == 1
        await fake.send(
            OperationSuccess(
                type="operation.success", request_id=request.request_id, result=result
            )
        )
        assert await task == result
        assert server.dispatcher.pending_count == 0


async def test_remote_failure_preserves_error(server: AdapterServer) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        error = ProtocolError(
            code="document.unavailable",
            message="Document unavailable",
            details={"reason": ["closed"]},
        )
        await fake.send(
            OperationFailure(
                type="operation.failure", request_id=request.request_id, error=error
            )
        )
        with pytest.raises(RemoteOperationError) as caught:
            await task
        assert caught.value.error == error
        assert caught.value.adapter_id == "adapter-a"
        assert caught.value.request_id == request.request_id
        assert server.dispatcher.pending_count == 0


async def test_missing_adapter(server: AdapterServer) -> None:
    with pytest.raises(AdapterNotFound) as caught:
        await execute(server)
    assert caught.value.adapter_id == "adapter-a"
    assert server.dispatcher.pending_count == 0


async def test_unsupported_operation_is_never_sent(server: AdapterServer) -> None:
    async with adapter(server) as fake:
        with pytest.raises(UnsupportedOperation):
            await server.dispatcher.execute(
                adapter_id="adapter-a", operation="asset.describe", arguments={}
            )
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        assert request.operation == "document.inspect"
        await fake.send(
            OperationSuccess(
                type="operation.success", request_id=request.request_id, result={}
            )
        )
        assert await task == {}


@pytest.mark.parametrize("timeout", [0.0, -1.0, float("nan"), float("inf")])
async def test_invalid_operation_timeout(server: AdapterServer, timeout: float) -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        await server.dispatcher.execute(
            adapter_id="missing",
            operation="document.inspect",
            arguments={},
            timeout=timeout,
        )
    assert server.dispatcher.pending_count == 0


async def test_concurrent_responses_correlate_out_of_order(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        tasks = [execute(server, arguments={"index": index}) for index in range(8)]
        requests = [await fake.receive() for _ in tasks]
        assert all(isinstance(request, OperationRequest) for request in requests)
        assert (
            len(
                {
                    request.request_id
                    for request in requests
                    if isinstance(request, OperationRequest)
                }
            )
            == 8
        )
        for request in reversed(requests):
            assert isinstance(request, OperationRequest)
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result=request.arguments,
                )
            )
        assert await asyncio.gather(*tasks) == [{"index": index} for index in range(8)]
        assert server.dispatcher.pending_count == 0


async def test_routes_to_selected_adapter_and_rejects_cross_connection_response(
    server: AdapterServer, caplog: pytest.LogCaptureFixture
) -> None:
    async with adapter(server, "first") as first, adapter(server, "second") as second:
        task = execute(server, adapter_id="second")
        request = await second.receive()
        assert isinstance(request, OperationRequest)
        with server.events.subscribe() as events:
            await first.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result="wrong",
                )
            )
            await first.send(
                AdapterEvent(type="adapter.event", event="test.barrier", payload=None)
            )
            async with asyncio.timeout(2):
                assert (await anext(events)).adapter_id == "first"
        assert not task.done()
        assert "unknown or stale response" in caplog.text
        await second.send(
            OperationSuccess(
                type="operation.success", request_id=request.request_id, result="right"
            )
        )
        assert await task == "right"


async def test_timeout_sends_cancel_and_late_response_does_not_break_connection(
    server: AdapterServer, caplog: pytest.LogCaptureFixture
) -> None:
    async with adapter(server) as fake:
        task = asyncio.create_task(
            server.dispatcher.execute(
                adapter_id="adapter-a",
                operation="document.inspect",
                arguments={},
                timeout=0.05,
            )
        )
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        with pytest.raises(OperationTimeout) as caught:
            await task
        assert caught.value.request_id == request.request_id
        assert caught.value.adapter_id == "adapter-a"
        assert caught.value.timeout == 0.05
        assert await fake.receive() == CancelRequest(
            type="operation.cancel", request_id=request.request_id
        )
        assert server.dispatcher.pending_count == 0
        await fake.send(
            OperationSuccess(
                type="operation.success", request_id=request.request_id, result="late"
            )
        )
        following = execute(server)
        next_request = await fake.receive()
        assert isinstance(next_request, OperationRequest)
        await fake.send(
            OperationSuccess(
                type="operation.success",
                request_id=next_request.request_id,
                result="current",
            )
        )
        assert await following == "current"
        assert "unknown or stale response" in caplog.text


@pytest.mark.parametrize(
    "response",
    [
        OperationSuccess(type="operation.success", request_id="unknown", result={}),
        OperationFailure(
            type="operation.failure",
            request_id="unknown",
            error=ProtocolError(code="failed", message="Failure"),
        ),
    ],
)
async def test_unknown_responses_are_logged_and_ignored(
    server: AdapterServer,
    caplog: pytest.LogCaptureFixture,
    response: OperationSuccess | OperationFailure,
) -> None:
    async with adapter(server) as fake:
        with server.events.subscribe() as events:
            await fake.send(response)
            await fake.send(
                AdapterEvent(type="adapter.event", event="test.barrier", payload=None)
            )
            async with asyncio.timeout(2):
                await anext(events)
        assert "unknown or stale response" in caplog.text
        assert server.registry.get("adapter-a").connected
        assert server.dispatcher.pending_count == 0


@pytest.mark.parametrize("abrupt", [False, True])
async def test_disconnect_fails_all_pending_requests_promptly(
    server: AdapterServer, abrupt: bool
) -> None:
    async with adapter(server) as fake:
        tasks = [execute(server) for _ in range(3)]
        for _ in tasks:
            assert isinstance(await fake.receive(), OperationRequest)
        if abrupt:
            fake.websocket.transport.abort()
        else:
            await fake.websocket.close()
        async with asyncio.timeout(0.5):
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(outcome, AdapterDisconnected) for outcome in outcomes)
        assert server.dispatcher.pending_count == 0


async def test_shutdown_fails_pending_before_operation_deadline(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        assert isinstance(await fake.receive(), OperationRequest)
        async with asyncio.timeout(0.5):
            await server.stop()
            with pytest.raises(AdapterDisconnected, match="shutting down"):
                await task
        assert server.dispatcher.pending_count == 0


async def test_explicit_task_cancellation_sends_matching_cancel(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await fake.receive() == CancelRequest(
            type="operation.cancel", request_id=request.request_id
        )
        assert server.dispatcher.pending_count == 0
        assert server.registry.get("adapter-a").connected


@pytest.mark.parametrize("response_first", [False, True])
async def test_response_cancellation_race(
    server: AdapterServer, response_first: bool
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        request = await fake.receive()
        assert isinstance(request, OperationRequest)
        response = OperationSuccess(
            type="operation.success", request_id=request.request_id, result="done"
        )
        if response_first:
            await fake.send(response)
        task.cancel()
        if not response_first:
            await fake.send(response)
        outcome = (await asyncio.gather(task, return_exceptions=True))[0]
        assert outcome == "done" or isinstance(outcome, asyncio.CancelledError)
        with server.events.subscribe() as events:
            await fake.send(
                AdapterEvent(type="adapter.event", event="test.barrier", payload=None)
            )
            async with asyncio.timeout(2):
                await anext(events)
        assert server.dispatcher.pending_count == 0
        assert server.registry.get("adapter-a").connected


@pytest.mark.parametrize("disconnect_first", [False, True])
async def test_disconnect_cancellation_race(
    server: AdapterServer, disconnect_first: bool
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        assert isinstance(await fake.receive(), OperationRequest)
        if disconnect_first:
            await fake.websocket.close()
            await eventually(task.done)
        task.cancel()
        if not disconnect_first:
            fake.websocket.transport.abort()
        outcome = (await asyncio.gather(task, return_exceptions=True))[0]
        if disconnect_first:
            assert isinstance(outcome, AdapterDisconnected)
        else:
            assert isinstance(outcome, asyncio.CancelledError)
        assert server.dispatcher.pending_count == 0


@pytest.mark.parametrize("failure", ["error", "timeout", "cancel"])
async def test_failed_or_interrupted_send_cleans_pending(
    server: AdapterServer, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    async with adapter(server):
        connection = server.registry._get_connection("adapter-a")
        entered = asyncio.Event()
        never = asyncio.Event()

        async def failing_send(wire: bytes) -> None:
            entered.set()
            if failure == "error":
                raise OSError("Injected send failure")
            await never.wait()

        monkeypatch.setattr(connection._websocket, "send", failing_send)
        task = execute(server)
        async with asyncio.timeout(2):
            await entered.wait()
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(AdapterDisconnected):
                await task
        assert connection.pending_count == 0
        assert not connection.connected


async def test_default_timeout_is_used() -> None:
    async with AdapterServer(CoreConfig(port=0, operation_timeout=0.03)) as server:
        async with adapter(server) as fake:
            task = execute(server)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            with pytest.raises(OperationTimeout) as caught:
                await task
            assert caught.value.timeout == 0.03
            assert await fake.receive() == CancelRequest(
                type="operation.cancel", request_id=request.request_id
            )


async def test_invalid_adapter_traffic_fails_pending_operations(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        task = execute(server)
        assert isinstance(await fake.receive(), OperationRequest)
        await fake.send(
            CancelRequest(type="operation.cancel", request_id="invalid-direction")
        )
        async with asyncio.timeout(0.5):
            with pytest.raises(AdapterDisconnected, match="Invalid adapter behavior"):
                await task
        assert server.dispatcher.pending_count == 0


@pytest.mark.parametrize("cancel_caller", [False, True])
async def test_cancellation_send_failure_preserves_local_outcome(
    server: AdapterServer, monkeypatch: pytest.MonkeyPatch, cancel_caller: bool
) -> None:
    async with adapter(server) as fake:
        task = asyncio.create_task(
            server.dispatcher.execute(
                adapter_id="adapter-a",
                operation="document.inspect",
                arguments={},
                timeout=0.05,
            )
        )
        assert isinstance(await fake.receive(), OperationRequest)
        connection = server.registry._get_connection("adapter-a")

        async def fail_cancel(wire: bytes) -> None:
            raise OSError("Injected cancellation delivery failure")

        monkeypatch.setattr(connection._websocket, "send", fail_cancel)
        if cancel_caller:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(OperationTimeout):
                await task
        assert connection.pending_count == 0
        assert not connection.connected
