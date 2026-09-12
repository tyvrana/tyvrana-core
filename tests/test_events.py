import asyncio

import pytest
from tyvrana_protocol import AdapterEvent, OperationRequest, OperationSuccess

from tyvrana_core import AdapterServer, EventBroker, EventSubscriptionOverflow

from .helpers import adapter, execute


async def test_event_retains_source_and_reaches_all_subscribers(
    server: AdapterServer,
) -> None:
    async with adapter(server, "first") as first, adapter(server, "second") as second:
        with server.events.subscribe() as one, server.events.subscribe() as two:
            event = AdapterEvent(
                type="adapter.event", event="document.changed", payload={"items": [1]}
            )
            await first.send(event)
            async with asyncio.timeout(2):
                received_one = await anext(one)
                received_two = await anext(two)
            assert received_one.adapter_id == received_two.adapter_id == "first"
            assert received_one.message == received_two.message == event
            assert isinstance(received_one.message.payload, dict)
            received_one.message.payload["items"] = "changed by subscriber"
            assert received_two.message.payload == {"items": [1]}
            await second.send(event)
            async with asyncio.timeout(2):
                assert (await anext(one)).adapter_id == "second"
                assert (await anext(two)).adapter_id == "second"
        assert server.events.subscriber_count == 0


async def test_slow_subscriber_overflow_does_not_block_receive_loop(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake:
        with (
            server.events.subscribe(capacity=1) as slow,
            server.events.subscribe(capacity=4) as fast,
        ):
            for index in range(3):
                await fake.send(
                    AdapterEvent(
                        type="adapter.event", event="document.changed", payload=index
                    )
                )
            task = execute(server)
            request = await fake.receive()
            assert isinstance(request, OperationRequest)
            await fake.send(
                OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result="responsive",
                )
            )
            assert await task == "responsive"
            assert slow.closed
            with pytest.raises(EventSubscriptionOverflow):
                await anext(slow)
            async with asyncio.timeout(2):
                assert [(await anext(fast)).message.payload for _ in range(3)] == [
                    0,
                    1,
                    2,
                ]
            assert server.events.subscriber_count == 1


async def test_unsubscribe_wakes_waiter_and_is_idempotent() -> None:
    broker = EventBroker()
    subscription = broker.subscribe()
    waiting = asyncio.create_task(anext(subscription))
    await asyncio.sleep(0)
    subscription.close()
    subscription.close()
    async with asyncio.timeout(2):
        with pytest.raises(StopAsyncIteration):
            await waiting
    assert broker.subscriber_count == 0


async def test_shutdown_closes_subscriptions_and_allows_new_ones_on_restart(
    server: AdapterServer,
) -> None:
    subscription = server.events.subscribe()
    waiting = asyncio.create_task(anext(subscription))
    await asyncio.sleep(0)
    await server.stop()
    with pytest.raises(StopAsyncIteration):
        await waiting
    await server.start()
    async with adapter(server) as fake:
        with server.events.subscribe() as new_subscription:
            await fake.send(
                AdapterEvent(
                    type="adapter.event", event="document.changed", payload=None
                )
            )
            async with asyncio.timeout(2):
                assert (await anext(new_subscription)).message.payload is None


async def test_context_exit_unsubscribes_after_consumer_error() -> None:
    broker = EventBroker()
    with pytest.raises(ValueError, match="consumer failed"), broker.subscribe():
        raise ValueError("consumer failed")
    assert broker.subscriber_count == 0


@pytest.mark.parametrize("capacity", [0, -1])
def test_subscription_capacity_must_be_bounded_and_positive(capacity: int) -> None:
    with pytest.raises(ValueError):
        EventBroker().subscribe(capacity=capacity)


async def test_cancelled_consumer_can_continue_using_subscription() -> None:
    broker = EventBroker()
    with broker.subscribe() as subscription:
        waiting = asyncio.create_task(anext(subscription))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        event = AdapterEvent(type="adapter.event", event="document.changed", payload={})
        broker._publish("adapter-a", event)
        async with asyncio.timeout(2):
            assert (await anext(subscription)).message == event
