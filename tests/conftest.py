import asyncio
import gc
from collections.abc import AsyncIterator

import pytest

from tyvrana_core import AdapterServer, CoreConfig


@pytest.fixture(autouse=True)
async def no_async_leaks() -> AsyncIterator[None]:
    loop = asyncio.get_running_loop()
    original_handler = loop.get_exception_handler()
    errors: list[dict[str, object]] = []
    baseline = asyncio.all_tasks()

    def capture_error(
        event_loop: asyncio.AbstractEventLoop, context: dict[str, object]
    ) -> None:
        errors.append(context)

    loop.set_exception_handler(capture_error)
    try:
        yield
        gc.collect()
        await asyncio.sleep(0)
        leaked = asyncio.all_tasks() - baseline - {asyncio.current_task()}
        if leaked:
            for task in leaked:
                task.cancel()
            await asyncio.gather(*leaked, return_exceptions=True)
        assert not leaked, f"Leaked tasks: {leaked}"
        assert not errors, f"Unhandled asynchronous errors: {errors}"
    finally:
        loop.set_exception_handler(original_handler)


@pytest.fixture
async def server() -> AsyncIterator[AdapterServer]:
    config = CoreConfig(
        port=0,
        registration_timeout=0.3,
        operation_timeout=1.0,
        send_timeout=0.2,
        close_timeout=0.1,
    )
    async with AdapterServer(config) as running:
        yield running
    assert running.registry.list() == ()
    assert running.dispatcher.pending_count == 0
    assert running.events.subscriber_count == 0
