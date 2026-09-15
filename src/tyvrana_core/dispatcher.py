"""Route operations to a registered adapter and preserve remote failures."""

import math
from uuid import uuid4

from tyvrana_protocol import (
    JsonValue,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
)

from .artifacts import ArtifactStore
from .errors import RemoteOperationError, UnsupportedOperation
from .registry import AdapterRegistry


class OperationDispatcher:
    def __init__(
        self,
        registry: AdapterRegistry,
        default_timeout: float,
        artifacts: ArtifactStore,
    ) -> None:
        self._registry = registry
        self._default_timeout = default_timeout
        self._artifacts = artifacts

    @property
    def pending_count(self) -> int:
        return sum(c.pending_count for c in self._registry._all_connections())

    async def execute(
        self,
        *,
        adapter_id: str,
        operation: str,
        arguments: JsonValue,
        artifact_ids: tuple[str, ...] = (),
        timeout: float | None = None,
    ) -> OperationSuccess:
        """Execute an operation; cancelling the caller requests remote cancellation.

        The timeout includes sending and waiting for a response. Cancellation
        delivery is best effort and bounded by the connection's send timeout.
        """
        limit = self._default_timeout if timeout is None else timeout
        if not math.isfinite(limit) or limit <= 0:
            raise ValueError("Operation timeout must be positive and finite")
        connection = self._registry._get_connection(adapter_id)
        if operation not in connection.registration.operation_names:
            raise UnsupportedOperation(adapter_id, operation)
        request = OperationRequest(
            type="operation.request",
            request_id=str(uuid4()),
            operation=operation,
            arguments=arguments,
            artifacts=tuple(
                self._artifacts.metadata(identifier) for identifier in artifact_ids
            ),
        )
        response = await connection.request(request, limit)
        if isinstance(response, OperationFailure):
            raise RemoteOperationError(adapter_id, request.request_id, response.error)
        return response
