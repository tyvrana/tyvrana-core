"""Application fixture owns its proof transport, opened only by lifecycle requests."""

import copy
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from typing import Any
from weakref import WeakKeyDictionary

from tyvrana_protocol import (
    AdapterRuntime,
    OperationContract,
    OperationRequest,
    ProofHostControl,
    ProofHostStart,
    ProofHostStatus,
)

from tyvrana_core import AdapterServer

_templates: WeakKeyDictionary[AdapterServer, dict[str, Any]] = WeakKeyDictionary()


@asynccontextmanager
async def proof_artifact(
    core: AdapterServer, value: dict[str, Any]
) -> AsyncIterator[dict[str, Any]]:
    _templates[core] = value
    try:
        yield value
    finally:
        _templates.pop(core, None)


class ManagedProofFixture:
    def __init__(
        self,
        core: AdapterServer,
        initial: dict[str, Any],
        factory: Callable[
            [str, dict[str, Any], ProofHostStart], AbstractAsyncContextManager[Any]
        ],
    ) -> None:
        self.core = core
        self.initial = copy.deepcopy(initial)
        self.factory = factory
        self.stack = AsyncExitStack()
        self.request: ProofHostStart | None = None

    @staticmethod
    def contracts() -> list[OperationContract]:
        return [
            OperationContract(
                name=f"editor.proof.{name}",
                description="Owned proof lifecycle",
                tags=(f"proof_host_{name}",),
                arguments_schema=(
                    ProofHostStart if name == "start" else ProofHostControl
                ).model_json_schema(),
                result_schema=ProofHostStatus.model_json_schema(),
                effect="read_only" if name == "status" else "lifecycle",
                execution="job_status" if name == "status" else "lifecycle",
            )
            for name in ("start", "status", "stop")
        ]

    @staticmethod
    def runtime(lease: ProofHostStart | None) -> AdapterRuntime:
        return AdapterRuntime(
            role="proof" if lease else "work",
            build="a" * 64,
            process_id=2 if lease else 1,
            background=lease is not None,
            proof_lease=lease.lease if lease else None,
        )

    async def respond(self, request: OperationRequest) -> dict[str, Any]:
        if request.operation.endswith("start"):
            intent = ProofHostStart.model_validate(request.arguments)
            self.request = intent
            value = copy.deepcopy(_templates.get(self.core, self.initial))
            value["host_session_id"] = value.get("host_session_id", "proof-host")
            if self.core not in _templates:
                value["host_session_id"] = "proof-host"
            await self.stack.enter_async_context(
                self.factory(intent.lease.lease_id, value, intent)
            )
            return dict(lease_id=intent.lease.lease_id, state="ready", process_id=2)
        control = ProofHostControl.model_validate(request.arguments)
        if request.operation.endswith("stop"):
            await self.stack.aclose()
            return dict(lease_id=control.lease.lease_id, state="stopped")
        return dict(lease_id=control.lease.lease_id, state="ready", process_id=2)
