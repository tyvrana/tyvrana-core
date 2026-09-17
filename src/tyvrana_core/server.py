"""Local WebSocket listener enforcing the canonical adapter message directions."""

import asyncio
import logging
from pathlib import Path
from types import TracebackType
from typing import Self

from tyvrana_protocol import (
    AdapterEvent,
    AdapterRegistration,
    ArtifactAbort,
    ArtifactAccepted,
    ArtifactBegin,
    ArtifactComplete,
    ArtifactReady,
    Message,
    OperationFailure,
    OperationSuccess,
    decode_artifact_chunk,
    decode_message,
)
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from .artifacts import ArtifactStore
from .config import CoreConfig
from .connection import AdapterConnection
from .dispatcher import OperationDispatcher
from .errors import AdapterDisconnected, DuplicateAdapter, InvalidAdapterBehavior
from .events import EventBroker
from .projects.service import ProjectService
from .registry import AdapterRegistry

logger = logging.getLogger(__name__)


class AdapterServer:
    def __init__(self, config: CoreConfig | None = None) -> None:
        self.config = config if config is not None else CoreConfig()
        self.registry = AdapterRegistry()
        self.events = EventBroker()
        self.artifacts = ArtifactStore(self.config)
        self.projects = ProjectService(
            self, Path(self.config.state_directory) / "projects.sqlite3"
        )
        self.dispatcher = OperationDispatcher(
            self.registry,
            self.config.operation_timeout,
            self.artifacts,
            before_mutation=self.projects.before_mutation,
        )
        self._server: Server | None = None
        self._stopping = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def uri(self) -> str:
        """The first bound address, including the actual ephemeral port."""
        if self._server is None:
            raise RuntimeError("Adapter server is not running")
        address = self._server.sockets[0].getsockname()
        host = str(address[0])
        if ":" in host:
            host = f"[{host}]"
        return f"ws://{host}:{address[1]}"

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._server is not None:
                return
            self._stopping = False
            self._server = await serve(
                self._handle,
                self.config.host,
                self.config.port,
                origins=[None],
                max_size=self.config.max_message_size,
                close_timeout=self.config.close_timeout,
                open_timeout=self.config.registration_timeout,
                compression=None,
                max_queue=8,
            )
            try:
                self.artifacts.start()
            except BaseException:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
                raise
            logger.info("Adapter server listening on %s", self.uri)

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if self._server is None:
                self.events.close()
                self.artifacts.close()
                return
            self._stopping = True
            self._server.close()
            for connection in self.registry._all_connections():
                connection.disconnect("Server shutting down")
                self.registry._remove(connection)
            self.events.close()
            try:
                await self._server.wait_closed()
            except asyncio.CancelledError:
                await self._server.wait_closed()
                raise
            finally:
                self._server = None
                self.artifacts.close()
            logger.info("Adapter server stopped")

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.stop()

    @staticmethod
    def _decode(data: str | bytes) -> Message:
        if not isinstance(data, str):
            raise InvalidAdapterBehavior("Control messages must use WebSocket text")
        try:
            return decode_message(data.encode("utf-8"))
        except (ValueError, RecursionError) as exc:
            raise InvalidAdapterBehavior("Malformed protocol message") from exc

    async def _handle(self, websocket: ServerConnection) -> None:
        connection: AdapterConnection | None = None
        try:
            try:
                async with asyncio.timeout(self.config.registration_timeout):
                    registration = self._decode(await websocket.recv())
            except TimeoutError as exc:
                raise InvalidAdapterBehavior("Registration timed out") from exc
            if not isinstance(registration, AdapterRegistration):
                raise InvalidAdapterBehavior("First message must register the adapter")
            if registration.instance_id == "core":
                raise InvalidAdapterBehavior("Adapter ID core is reserved")
            if self._stopping:
                await websocket.close(code=1001, reason="Server shutting down")
                return
            connection = AdapterConnection(
                websocket, registration, self.config.send_timeout, self.artifacts
            )
            self.registry._add(connection)
            logger.info("Adapter registered: %s", registration.instance_id)
            async for data in websocket:
                if self._stopping:
                    break
                if isinstance(data, bytes):
                    try:
                        chunk = decode_artifact_chunk(data)
                    except ValueError as exc:
                        raise InvalidAdapterBehavior(
                            "Malformed artifact chunk"
                        ) from exc
                    await connection.artifact(chunk)
                    continue
                message = self._decode(data)
                if isinstance(message, (OperationSuccess, OperationFailure)):
                    connection.resolve(message)
                elif isinstance(message, AdapterEvent):
                    self.events._publish(registration.instance_id, message)
                elif isinstance(
                    message,
                    (
                        ArtifactBegin,
                        ArtifactComplete,
                        ArtifactAbort,
                        ArtifactReady,
                        ArtifactAccepted,
                    ),
                ):
                    await connection.artifact(message)
                else:
                    raise InvalidAdapterBehavior(
                        "Message direction is not adapter to core"
                    )
        except (InvalidAdapterBehavior, DuplicateAdapter) as exc:
            logger.warning("Rejecting adapter connection: %s", exc)
            if connection is not None:
                connection.disconnect("Invalid adapter behavior")
                self.registry._remove(connection)
            await websocket.close(code=1008, reason="Invalid adapter behavior")
        except (ConnectionClosed, AdapterDisconnected):
            pass
        finally:
            if connection is not None:
                connection.disconnect()
                self.registry._remove(connection)
                logger.info(
                    "Adapter disconnected: %s", connection.registration.instance_id
                )
