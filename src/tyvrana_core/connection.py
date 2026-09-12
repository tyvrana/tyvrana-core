"""One registered adapter and its outstanding operation responses."""

import asyncio
import logging

from tyvrana_protocol import (
    AdapterRegistration,
    CancelRequest,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
    encode_message,
)
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from .errors import AdapterDisconnected, OperationTimeout

logger = logging.getLogger(__name__)
type OperationResponse = OperationSuccess | OperationFailure


class AdapterConnection:
    """Own pending futures; all methods run on the server's event loop."""

    def __init__(
        self,
        websocket: ServerConnection,
        registration: AdapterRegistration,
        send_timeout: float,
    ) -> None:
        self.registration = registration
        self._websocket = websocket
        self._send_timeout = send_timeout
        self._disconnected = False
        self._pending: dict[str, asyncio.Future[OperationResponse]] = {}

    @property
    def connected(self) -> bool:
        return not self._disconnected and self._websocket.state is State.OPEN

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    async def _send(self, message: OperationRequest | CancelRequest) -> None:
        if not self.connected:
            raise AdapterDisconnected(self.registration.instance_id)
        wire = encode_message(message)
        try:
            async with asyncio.timeout(self._send_timeout):
                await self._websocket.send(wire)
        except (ConnectionClosed, OSError, TimeoutError) as exc:
            self.disconnect("Send failed or timed out")
            self._websocket.transport.abort()
            raise AdapterDisconnected(
                self.registration.instance_id, "Send failed or timed out"
            ) from exc
        except asyncio.CancelledError:
            # An interrupted send has uncertain delivery. Retire the connection
            # instead of allowing later frames to overtake unfinished output.
            self.disconnect("Send interrupted")
            self._websocket.transport.abort()
            raise

    async def request(
        self, message: OperationRequest, timeout: float
    ) -> OperationResponse:
        future: asyncio.Future[OperationResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[message.request_id] = future
        try:
            async with asyncio.timeout(timeout):
                await self._send(message)
                return await asyncio.shield(future)
        except TimeoutError as exc:
            if self._discard(message.request_id, future):
                await self._cancel_remote(message.request_id)
            raise OperationTimeout(
                self.registration.instance_id, message.request_id, timeout
            ) from exc
        except asyncio.CancelledError:
            if self._discard(message.request_id, future):
                await self._cancel_remote(message.request_id)
            raise
        finally:
            self._discard(message.request_id, future)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                # A disconnect can fail the future while a send is still
                # unwinding; retrieve that exception even if it wasn't awaited.
                future.exception()

    def _discard(
        self, request_id: str, future: asyncio.Future[OperationResponse]
    ) -> bool:
        if self._pending.get(request_id) is not future:
            return False
        del self._pending[request_id]
        return True

    async def _cancel_remote(self, request_id: str) -> None:
        if not self.connected:
            return
        try:
            await self._send(
                CancelRequest(type="operation.cancel", request_id=request_id)
            )
        except AdapterDisconnected:
            logger.info("Connection closed while cancelling request %s", request_id)

    def resolve(self, response: OperationResponse) -> None:
        future = self._pending.pop(response.request_id, None)
        if future is None:
            logger.warning(
                "Ignoring unknown or stale response %s from adapter %s",
                response.request_id,
                self.registration.instance_id,
            )
            return
        future.set_result(response)

    def disconnect(self, reason: str = "Connection closed") -> None:
        self._disconnected = True
        pending, self._pending = self._pending, {}
        for future in pending.values():
            future.set_exception(
                AdapterDisconnected(self.registration.instance_id, reason)
            )
