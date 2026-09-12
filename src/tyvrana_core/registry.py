"""Typed snapshots of registered connections, without exposing mutable maps."""

from dataclasses import dataclass

from tyvrana_protocol import AdapterRegistration

from .connection import AdapterConnection
from .errors import AdapterDisconnected, AdapterNotFound, DuplicateAdapter


@dataclass(frozen=True, slots=True)
class AdapterInfo:
    registration: AdapterRegistration
    connected: bool

    @property
    def instance_id(self) -> str:
        return self.registration.instance_id


class AdapterRegistry:
    def __init__(self) -> None:
        self._connections: dict[str, AdapterConnection] = {}

    def list(self) -> tuple[AdapterInfo, ...]:
        return tuple(
            AdapterInfo(connection.registration, connection.connected)
            for connection in self._connections.values()
            if connection.connected
        )

    def get(self, instance_id: str) -> AdapterInfo:
        connection = self._get_connection(instance_id)
        return AdapterInfo(connection.registration, connection.connected)

    def supporting(self, operation: str) -> tuple[AdapterInfo, ...]:
        return tuple(
            info for info in self.list() if operation in info.registration.operations
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

    def _remove(self, connection: AdapterConnection) -> None:
        instance_id = connection.registration.instance_id
        if self._connections.get(instance_id) is connection:
            del self._connections[instance_id]

    def _all_connections(self) -> tuple[AdapterConnection, ...]:
        return tuple(self._connections.values())
