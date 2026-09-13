"""One registered adapter and its outstanding operation responses."""

import asyncio
import hashlib
import logging
from typing import BinaryIO
from uuid import uuid4

from tyvrana_protocol import (
    MAX_ARTIFACT_CHUNK_SIZE,
    AdapterRegistration,
    ArtifactAbort,
    ArtifactAccepted,
    ArtifactBegin,
    ArtifactChunk,
    ArtifactComplete,
    ArtifactDescriptor,
    ArtifactReady,
    CancelRequest,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
    ProtocolError,
    encode_artifact_chunk,
    encode_message,
)
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from .artifacts import ArtifactError, ArtifactStore
from .errors import (
    AdapterDisconnected,
    InvalidAdapterBehavior,
    OperationTimeout,
    RemoteOperationError,
)

logger = logging.getLogger(__name__)
type OperationResponse = OperationSuccess | OperationFailure


class AdapterConnection:
    """Own pending futures; all methods run on the server's event loop."""

    def __init__(
        self,
        websocket: ServerConnection,
        registration: AdapterRegistration,
        send_timeout: float,
        artifacts: ArtifactStore,
    ) -> None:
        self.registration = registration
        self._websocket = websocket
        self._send_timeout = send_timeout
        self._disconnected = False
        self._pending: dict[str, asyncio.Future[OperationResponse]] = {}
        self._artifacts = artifacts
        self._owner = uuid4().hex
        self._started: set[str] = set()
        self._outgoing: dict[
            str,
            tuple[str, asyncio.Queue[ArtifactReady | ArtifactAccepted | ArtifactAbort]],
        ] = {}
        self._input_slots = asyncio.Semaphore(artifacts.config.max_artifact_transfers)
        self._input_ids: dict[str, str] = {}

    @property
    def connected(self) -> bool:
        return not self._disconnected and self._websocket.state is State.OPEN

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    async def _send(
        self,
        message: OperationRequest
        | CancelRequest
        | ArtifactReady
        | ArtifactAccepted
        | ArtifactAbort
        | ArtifactBegin
        | ArtifactComplete
        | bytes,
    ) -> None:
        if not self.connected:
            raise AdapterDisconnected(self.registration.instance_id)
        wire = (
            message
            if isinstance(message, bytes)
            else encode_message(message).decode("utf-8")
        )
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
        delivered = False
        try:
            async with asyncio.timeout(timeout):
                if message.artifacts:
                    async with self._input_slots:
                        with self._artifacts.lease(message.artifacts) as sources:
                            for descriptor, stream in sources:
                                if future.done():
                                    future.result()
                                await self._send_input(
                                    message.request_id, descriptor, stream
                                )
                if future.done():
                    future.result()
                self._started.add(message.request_id)
                await self._send(message)
                response = await asyncio.shield(future)
                delivered = True
                return response
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
        except (ArtifactError, RemoteOperationError):
            if self._discard(message.request_id, future):
                await self._cancel_remote(message.request_id)
            raise
        finally:
            self._started.discard(message.request_id)
            for transfer_id, request_id in list(self._input_ids.items()):
                if request_id == message.request_id:
                    del self._input_ids[transfer_id]
            if not delivered:
                self._artifacts.discard_request(self._owner, message.request_id)
            self._discard(message.request_id, future)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                # A disconnect can fail the future while a send is still
                # unwinding; retrieve that exception even if it wasn't awaited.
                future.exception()

    async def _send_input(
        self, request_id: str, descriptor: ArtifactDescriptor, stream: BinaryIO
    ) -> None:
        transfer_id = uuid4().hex
        queue: asyncio.Queue[ArtifactReady | ArtifactAccepted | ArtifactAbort] = (
            asyncio.Queue(maxsize=1)
        )
        self._outgoing[transfer_id] = request_id, queue
        self._input_ids[transfer_id] = request_id

        async def acknowledge(
            expected: type[ArtifactReady] | type[ArtifactAccepted],
        ) -> None:
            message = await queue.get()
            if not self.connected:
                raise AdapterDisconnected(self.registration.instance_id)
            if isinstance(message, ArtifactAbort):
                raise RemoteOperationError(
                    self.registration.instance_id, request_id, message.error
                )
            if not isinstance(message, expected):
                raise ArtifactError("Unexpected input artifact acknowledgement")

        try:
            await self._send(
                ArtifactBegin(
                    type="artifact.begin",
                    transfer_id=transfer_id,
                    request_id=request_id,
                    descriptor=descriptor,
                )
            )
            await acknowledge(ArtifactReady)
            offset = 0
            digest = hashlib.sha256()
            while chunk := stream.read(MAX_ARTIFACT_CHUNK_SIZE):
                if offset + len(chunk) > descriptor.byte_size:
                    raise ArtifactError("Stored artifact size changed")
                await self._send(encode_artifact_chunk(transfer_id, offset, chunk))
                offset += len(chunk)
                digest.update(chunk)
                await asyncio.sleep(0)
                if not queue.empty():
                    await acknowledge(ArtifactAccepted)
                    raise ArtifactError("Premature input artifact acknowledgement")
            if (
                offset != descriptor.byte_size
                or digest.hexdigest() != descriptor.sha256
            ):
                raise ArtifactError("Stored artifact integrity check failed")
            await self._send(
                ArtifactComplete(type="artifact.complete", transfer_id=transfer_id)
            )
            await acknowledge(ArtifactAccepted)
        except OSError as exc:
            raise ArtifactError("Cannot read stored input artifact") from exc
        finally:
            self._outgoing.pop(transfer_id, None)

    def _discard(
        self, request_id: str, future: asyncio.Future[OperationResponse]
    ) -> bool:
        if self._pending.get(request_id) is not future:
            return False
        del self._pending[request_id]
        self._artifacts.discard_request(self._owner, request_id)
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
        if (
            response.request_id in self._pending
            and response.request_id not in self._started
        ):
            raise InvalidAdapterBehavior("Operation responded before input delivery")
        future = self._pending.pop(response.request_id, None)
        if future is None:
            logger.warning(
                "Ignoring unknown or stale response %s from adapter %s",
                response.request_id,
                self.registration.instance_id,
            )
            return
        try:
            if isinstance(response, OperationSuccess):
                self._artifacts.claim(
                    self._owner, response.request_id, response.artifacts
                )
            else:
                self._artifacts.discard_request(self._owner, response.request_id)
        except ArtifactError as exc:
            self._artifacts.discard_request(self._owner, response.request_id)
            response = OperationFailure(
                type="operation.failure",
                request_id=response.request_id,
                error=ProtocolError(code="artifact_transfer_failed", message=str(exc)),
            )
        future.set_result(response)

    async def artifact(
        self,
        message: ArtifactBegin
        | ArtifactComplete
        | ArtifactAbort
        | ArtifactChunk
        | ArtifactReady
        | ArtifactAccepted,
    ) -> None:
        transfer_id = message.transfer_id
        if isinstance(message, (ArtifactReady, ArtifactAccepted, ArtifactAbort)):
            outgoing = self._outgoing.get(transfer_id)
            if outgoing is not None:
                _, queue = outgoing
                if queue.full():
                    raise InvalidAdapterBehavior(
                        "Unexpected input artifact acknowledgement"
                    )
                queue.put_nowait(message)
                return
            if not isinstance(message, ArtifactAbort):
                return  # Acknowledgement may have raced cancellation.
            related = self._input_ids.get(transfer_id)
            if related is not None:
                future = self._pending.get(related)
                if future is not None and not future.done():
                    future.set_exception(
                        RemoteOperationError(
                            self.registration.instance_id, related, message.error
                        )
                    )
                    for active_id, (active_request, queue) in self._outgoing.items():
                        if active_request == related:
                            if not queue.empty():
                                queue.get_nowait()
                            queue.put_nowait(
                                ArtifactAbort(
                                    type="artifact.abort",
                                    transfer_id=active_id,
                                    error=message.error,
                                )
                            )
                return
        if transfer_id in self._input_ids:
            raise InvalidAdapterBehavior("Transfer ID conflicts with an input transfer")
        request_id = self._artifacts.request_for(self._owner, transfer_id)
        try:
            if isinstance(message, ArtifactBegin):
                if request_id is not None:
                    raise InvalidAdapterBehavior("Duplicate transfer identifier")
                request_id = message.request_id
                if request_id not in self._pending:
                    raise ArtifactError("Artifact request is not outstanding")
                if request_id not in self._started:
                    raise InvalidAdapterBehavior(
                        "Output transfer began before operation delivery"
                    )
                self._artifacts.begin(self._owner, message)
                await self._send(
                    ArtifactReady(type="artifact.ready", transfer_id=transfer_id)
                )
            elif isinstance(message, ArtifactChunk):
                self._artifacts.write(self._owner, message)
            elif isinstance(message, ArtifactComplete):
                self._artifacts.complete(self._owner, transfer_id)
                await self._send(
                    ArtifactAccepted(type="artifact.accepted", transfer_id=transfer_id)
                )
            else:
                if request_id is not None:
                    self.resolve(
                        OperationFailure(
                            type="operation.failure",
                            request_id=request_id,
                            error=message.error,
                        )
                    )
                    await self._cancel_remote(request_id)
        except ArtifactError as exc:
            error = ProtocolError(code="artifact_transfer_failed", message=str(exc))
            if request_id in self._pending:
                assert request_id is not None
                self.resolve(
                    OperationFailure(
                        type="operation.failure", request_id=request_id, error=error
                    )
                )
            await self._send(
                ArtifactAbort(
                    type="artifact.abort", transfer_id=transfer_id, error=error
                )
            )
            if request_id is not None:
                await self._cancel_remote(request_id)

    def disconnect(self, reason: str = "Connection closed") -> None:
        self._disconnected = True
        for transfer_id, (_, queue) in self._outgoing.items():
            if not queue.empty():
                queue.get_nowait()
            queue.put_nowait(
                ArtifactAbort(
                    type="artifact.abort",
                    transfer_id=transfer_id,
                    error=ProtocolError(
                        code="adapter_disconnected", message="Adapter disconnected"
                    ),
                )
            )
        self._artifacts.disconnect(self._owner)
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(
                    AdapterDisconnected(self.registration.instance_id, reason)
                )
