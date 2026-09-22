"""Route operations to a registered adapter and preserve remote failures."""

import asyncio
import math
from collections.abc import Awaitable, Callable
from uuid import uuid4

from tyvrana_protocol import (
    AdapterRegistration,
    JsonValue,
    OperationFailure,
    OperationRequest,
    OperationSuccess,
)

from .artifacts import ArtifactStore
from .errors import OperationTimeout, RemoteOperationError, UnsupportedOperation
from .registry import AdapterRegistry


class OperationDispatcher:
    def __init__(
        self,
        registry: AdapterRegistry,
        default_timeout: float,
        artifacts: ArtifactStore,
        before_mutation: Callable[[AdapterRegistration], Awaitable[None]] | None = None,
    ) -> None:
        self._registry = registry
        self._default_timeout = default_timeout
        self._artifacts = artifacts
        self._before_mutation = before_mutation

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
        contract = next(
            c for c in connection.registration.operations if c.name == operation
        )
        if contract.effect == "mutating" and self._before_mutation is not None:
            started = asyncio.get_running_loop().time()
            try:
                async with asyncio.timeout(limit):
                    await self._before_mutation(connection.registration)
            except TimeoutError as exc:
                raise OperationTimeout(adapter_id, request.request_id, limit) from exc
            limit = max(0.000001, limit - (asyncio.get_running_loop().time() - started))
        response = await connection.request(request, limit)
        if isinstance(response, OperationFailure):
            raise RemoteOperationError(adapter_id, request.request_id, response.error)
        return response
