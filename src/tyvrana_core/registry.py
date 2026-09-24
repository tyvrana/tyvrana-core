"""Typed snapshots of registered connections, without exposing mutable maps."""

import asyncio
from dataclasses import dataclass

from tyvrana_protocol import AdapterRegistration

from .connection import AdapterConnection
from .errors import AdapterDisconnected, AdapterNotFound, DuplicateAdapter


@dataclass(frozen=True, slots=True)
class AdapterInfo:
    registration: AdapterRegistration
    connected: bool
    catalog_sha256: str = ""
    connection_id: str = ""

    @property
    def instance_id(self) -> str:
        return self.registration.instance_id


class AdapterRegistry:
    def __init__(self) -> None:
        self._connections: dict[str, AdapterConnection] = {}
        self._revision = 0
        self._changed = asyncio.Event()

    @property
    def revision(self) -> int:
        return self._revision

    def _notify(self) -> None:
        self._revision += 1
        changed, self._changed = self._changed, asyncio.Event()
        changed.set()

    async def wait_for_change(self, revision: int, timeout: float) -> None:
        if self._revision != revision:
            return
        changed = self._changed
        try:
            async with asyncio.timeout(timeout):
                await changed.wait()
        except TimeoutError:
            pass

    def list(self, *, include_proofs: bool = False) -> tuple[AdapterInfo, ...]:
        return tuple(
            AdapterInfo(
                connection.registration,
                connection.connected,
                connection.catalog_sha256,
                connection.connection_id,
            )
            for connection in self._connections.values()
            if connection.connected
            and (
                include_proofs
                or connection.registration.runtime is None
                or connection.registration.runtime.role == "work"
            )
        )

    def get(self, instance_id: str) -> AdapterInfo:
        connection = self._get_connection(instance_id)
        return AdapterInfo(
            connection.registration,
            connection.connected,
            connection.catalog_sha256,
            connection.connection_id,
        )

    def supporting(self, operation: str) -> tuple[AdapterInfo, ...]:
        return tuple(
            info
            for info in self.list()
            if operation in info.registration.operation_names
        )

    def _get_connection(self, instance_id: str) -> AdapterConnection:
        connection = self._connections.get(instance_id)
        if connection is None:
            raise AdapterNotFound(instance_id)
        if not connection.connected:
            raise AdapterDisconnected(instance_id)
        return connection

    def _add(self, connection: AdapterConnection) -> None:
        instance_id = connection.registration.instance_id
        existing = self._connections.get(instance_id)
        if existing is not None and existing.connected:
            raise DuplicateAdapter(instance_id)
        self._connections[instance_id] = connection
        self._notify()

    def _remove(self, connection: AdapterConnection) -> None:
        instance_id = connection.registration.instance_id
        if self._connections.get(instance_id) is connection:
            del self._connections[instance_id]
            self._notify()

    def _all_connections(self) -> tuple[AdapterConnection, ...]:
        return tuple(self._connections.values())
