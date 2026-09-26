"""Failures that core callers can handle without parsing log messages."""

import json

from tyvrana_protocol import JsonValue, ProtocolError


class CoreError(Exception):
    """Base class for core service errors."""


class AdapterNotFound(CoreError):
    def __init__(self, adapter_id: str) -> None:
        self.adapter_id = adapter_id
        super().__init__(f"Adapter not found: {adapter_id}")


class UnsupportedOperation(CoreError):
    def __init__(self, adapter_id: str, operation: str) -> None:
        self.adapter_id = adapter_id
        self.operation = operation
        super().__init__(f"Adapter {adapter_id} does not advertise {operation}")


class AdapterDisconnected(CoreError):
    def __init__(self, adapter_id: str, reason: str = "Connection closed") -> None:
        self.adapter_id = adapter_id
        super().__init__(f"Adapter {adapter_id} disconnected: {reason}")


class OperationTimeout(CoreError):
    def __init__(self, adapter_id: str, request_id: str, timeout: float) -> None:
        self.adapter_id = adapter_id
        self.request_id = request_id
        self.timeout = timeout
        super().__init__(f"Request {request_id} to {adapter_id} exceeded {timeout}s")


class RemoteOperationError(CoreError):
    def __init__(self, adapter_id: str, request_id: str, error: ProtocolError) -> None:
        self.adapter_id = adapter_id
        self.request_id = request_id
        self.error = error
        super().__init__(f"Adapter {adapter_id}: {error.code}: {error.message}")


class DuplicateAdapter(CoreError):
    def __init__(self, adapter_id: str) -> None:
        self.adapter_id = adapter_id
        super().__init__(f"Duplicate active adapter instance: {adapter_id}")


class InvalidAdapterBehavior(CoreError):
    """A peer violated the adapter protocol or message direction."""


class EventSubscriptionOverflow(CoreError):
    """A subscriber fell behind and must explicitly resubscribe."""


def bounded_diagnostics(details: JsonValue) -> JsonValue:
    """Keep valid JSON evidence or explicitly mark an oversized diagnostic."""
    encoded = json.dumps(details, ensure_ascii=False, allow_nan=False)
    if len(encoded.encode()) <= 16384:
        return details
    return {"diagnostics_truncated": True, "byte_limit": 16384}
