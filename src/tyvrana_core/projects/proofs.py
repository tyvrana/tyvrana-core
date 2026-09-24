"""Core-owned leases; application adapters own disposable proof processes."""

import asyncio
import json
import logging
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from tyvrana_protocol import (
    AdapterRegistration,
    ProofArtifact,
    ProofHostControl,
    ProofHostStart,
    ProofHostStatus,
    ProofLease,
)

from ..errors import AdapterDisconnected, AdapterNotFound, InvalidAdapterBehavior
from ..registry import AdapterInfo
from .store import ProjectError

if TYPE_CHECKING:
    from .service import ProjectService


PROOF_WORKFLOW: ContextVar[bool] = ContextVar("proof_workflow", default=False)


@dataclass
class Lease:
    request: ProofHostStart
    parent: AdapterInfo
    adapter_id: str | None = None


class ProofHosts:
    def __init__(self, service: "ProjectService") -> None:
        self.service = service
        self.leases: dict[str, Lease] = {}
        self.metrics: list[dict[str, float | str | bool]] = []
        self.start_timeout = 60.0
        self.workflow_timeout = 600.0

    def admit(self, registration: AdapterRegistration) -> None:
        runtime = registration.runtime
        if runtime is None or runtime.role != "proof":
            return
        identity = runtime.proof_lease
        assert identity is not None
        lease = self.leases.get(identity.lease_id)
        if (
            lease is None
            or not secrets.compare_digest(identity.token, lease.request.lease.token)
            or identity.parent_adapter_id != lease.parent.instance_id
            or runtime.build != lease.request.expected_build
            or registration.application != lease.parent.registration.application
            or registration.project_id != lease.request.artifact.project_id
            or (
                lease.parent.registration.runtime is not None
                and runtime.process_id == lease.parent.registration.runtime.process_id
            )
            or not runtime.background
            or lease.adapter_id not in {None, registration.instance_id}
        ):
            raise InvalidAdapterBehavior("Unrecognized or incompatible proof lease")
        lease.adapter_id = registration.instance_id

    @staticmethod
    def _operation(parent: AdapterInfo, tag: str) -> str:
        names = [c.name for c in parent.registration.operations if tag in c.tags]
        if len(names) != 1:
            raise ProjectError(
                "proof_unsupported",
                "Application lacks managed proof lifecycle",
                phase=tag,
            )
        return names[0]

    async def _control(self, lease: Lease, tag: str) -> ProofHostStatus:
        parent = lease.parent
        try:
            self.service.core.registry.get(parent.instance_id)
        except (AdapterNotFound, AdapterDisconnected):
            runtime = parent.registration.runtime
            candidates = [
                a
                for a in self.service.core.registry.list()
                if a.registration.application == parent.registration.application
                and a.registration.runtime
                and runtime
                and a.registration.runtime.process_id == runtime.process_id
                and a.registration.runtime.build == runtime.build
            ]
            if len(candidates) != 1:
                raise ProjectError(
                    "proof_cleanup", "Proof owner disconnected before release"
                ) from None
            parent = candidates[0]
        response = await self.service.core.dispatcher.execute(
            adapter_id=parent.instance_id,
            operation=self._operation(lease.parent, tag),
            arguments=ProofHostControl(lease=lease.request.lease).model_dump(
                mode="json"
            ),
            timeout=10,
            _internal=True,
        )
        status = ProofHostStatus.model_validate(response.result)
        if status.lease_id != lease.request.lease.lease_id:
            raise ProjectError(
                "proof_identity", "Proof lifecycle returned another lease"
            )
        return status

    async def _release(self, lease: Lease) -> None:
        status = await self._control(lease, "proof_host_stop")
        if status.state != "stopped":
            raise ProjectError(
                "proof_cleanup", "Application did not stop its proof process"
            )
        deadline = time.monotonic() + 10
        while any(
            a.instance_id == lease.adapter_id
            for a in self.service.core.registry.list(include_proofs=True)
        ):
            if time.monotonic() >= deadline:
                raise ProjectError(
                    "proof_cleanup", "Proof connection survived process shutdown"
                )
            await asyncio.sleep(0.05)

    @asynccontextmanager
    async def acquire(
        self, parent: AdapterInfo, artifact: ProofArtifact
    ) -> AsyncIterator[AdapterInfo]:
        runtime = parent.registration.runtime
        if runtime is None or runtime.role != "work":
            raise ProjectError(
                "proof_unsupported", "Select a work host with typed build identity"
            )
        if len(self.leases) >= 4 or any(
            item.parent.instance_id == parent.instance_id
            for item in self.leases.values()
        ):
            raise ProjectError(
                "proof_busy",
                "A proof workflow already owns this host or the proof capacity is full",
            )
        for tag in ("proof_host_start", "proof_host_status", "proof_host_stop"):
            self._operation(parent, tag)
        request = ProofHostStart(
            lease=ProofLease(
                lease_id=uuid4().hex,
                token=secrets.token_hex(32),
                parent_adapter_id=parent.instance_id,
            ),
            artifact=artifact,
            expected_build=runtime.build,
            ttl_seconds=min(900, int(self.workflow_timeout) + 60),
        )
        lease = Lease(request, parent)
        self.leases[request.lease.lease_id] = lease
        started = time.monotonic()
        context_token = PROOF_WORKFLOW.set(True)
        metrics: dict[str, float | str | bool] = {
            "startup_seconds": 0.0,
            "failed": True,
            "cleanup_ok": False,
        }
        try:
            async with asyncio.timeout(self.workflow_timeout):
                response = await self.service.core.dispatcher.execute(
                    adapter_id=parent.instance_id,
                    operation=self._operation(parent, "proof_host_start"),
                    arguments=request.model_dump(mode="json"),
                    timeout=30,
                    _internal=True,
                )
                status = ProofHostStatus.model_validate(response.result)
                if status.lease_id != request.lease.lease_id:
                    raise ProjectError(
                        "proof_identity", "Proof start returned another lease"
                    )
                deadline = time.monotonic() + self.start_timeout
                while True:
                    if status.state in {"failed", "stopped"}:
                        raise ProjectError(
                            "proof_start_failed",
                            status.error.message
                            if status.error
                            else "Proof host stopped during startup",
                        )
                    candidates = [
                        a
                        for a in self.service.core.registry.list(include_proofs=True)
                        if a.instance_id == lease.adapter_id
                    ]
                    if status.state == "ready" and len(candidates) == 1:
                        proof = candidates[0]
                        self.admit(proof.registration)
                        if (
                            proof.registration.runtime is None
                            or status.process_id
                            != proof.registration.runtime.process_id
                        ):
                            raise ProjectError(
                                "proof_identity",
                                "Proof registration differs from the owned process",
                            )
                        break
                    if time.monotonic() >= deadline:
                        raise ProjectError(
                            "proof_start_timeout", "Proof host did not become ready"
                        )
                    await asyncio.sleep(0.1)
                    status = await self._control(lease, "proof_host_status")
                metrics["startup_seconds"] = time.monotonic() - started
                yield proof
                metrics["failed"] = False
        finally:
            cleanup = time.monotonic()
            task = asyncio.create_task(self._release(lease))
            try:
                await asyncio.shield(task)
                metrics["cleanup_ok"] = True
            except asyncio.CancelledError:
                await task
                metrics["cleanup_ok"] = True
                raise
            finally:
                metrics["cleanup_seconds"] = time.monotonic() - cleanup
                metrics["lifetime_seconds"] = time.monotonic() - started
                logging.getLogger(__name__).info(
                    "Proof lifecycle metrics %s", json.dumps(metrics)
                )
                self.metrics.append(metrics)
                del self.metrics[:-128]
                self.leases.pop(request.lease.lease_id, None)
                PROOF_WORKFLOW.reset(context_token)

    def artifact(
        self, *, locator: str | None, sha256: str | None, project_id: str
    ) -> ProofArtifact:
        if not locator or not sha256:
            raise ProjectError(
                "proof_artifact_missing",
                "Independent proof requires a durable artifact locator and SHA256",
            )
        return ProofArtifact(locator=locator, sha256=sha256, project_id=project_id)
