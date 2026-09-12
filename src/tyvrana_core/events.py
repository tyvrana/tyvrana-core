"""Bounded subscriptions for unsolicited adapter events."""

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import TracebackType
from typing import Self

from tyvrana_protocol import AdapterEvent

from .errors import EventSubscriptionOverflow

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourcedEvent:
    adapter_id: str
    message: AdapterEvent


class EventSubscription(AsyncIterator[SourcedEvent]):
    """Single-consumer iterator; use a context manager or call close()."""

    def __init__(self, broker: "EventBroker", capacity: int) -> None:
        self._broker = broker
        self._queue: asyncio.Queue[SourcedEvent | None] = asyncio.Queue(capacity)
        self._closed = False
        self._error: EventSubscriptionOverflow | None = None

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> SourcedEvent:
        if self._error is not None:
            raise self._error
        if self._closed:
            raise StopAsyncIteration
        event = await self._queue.get()
        if self._error is not None:
            raise self._error
        if event is None:
            raise StopAsyncIteration
        return event

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._broker._unsubscribe(self)
        while not self._queue.empty():
            self._queue.get_nowait()
        self._queue.put_nowait(None)

    def _offer(self, adapter_id: str, event: AdapterEvent) -> None:
        if self._queue.full():
            self._error = EventSubscriptionOverflow(
                "Event subscription capacity exceeded"
            )
            logger.warning("Closing slow event subscription: capacity exceeded")
            self.close()
            return
        # JsonValue containers are mutable, so subscribers receive independent data.
        self._queue.put_nowait(SourcedEvent(adapter_id, event.model_copy(deep=True)))


class EventBroker:
    def __init__(self) -> None:
        self._subscriptions: set[EventSubscription] = set()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscriptions)

    def subscribe(self, *, capacity: int = 128) -> EventSubscription:
        if capacity <= 0:
            raise ValueError("Event subscription capacity must be positive")
        subscription = EventSubscription(self, capacity)
        self._subscriptions.add(subscription)
        return subscription

    def _publish(self, adapter_id: str, event: AdapterEvent) -> None:
        for subscription in tuple(self._subscriptions):
            subscription._offer(adapter_id, event)

    def _unsubscribe(self, subscription: EventSubscription) -> None:
        self._subscriptions.discard(subscription)

    def close(self) -> None:
        """Close current subscriptions; new subscriptions may be created later."""
        for subscription in tuple(self._subscriptions):
            subscription.close()
