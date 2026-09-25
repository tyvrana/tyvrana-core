"""Advance trusted working content only through correlated native receipts."""

import asyncio
import json
from typing import TYPE_CHECKING, Any

from tyvrana_protocol import (
    AdapterRegistration,
    OperationRequest,
    OperationSuccess,
    ResourceReference,
)
from tyvrana_protocol.mutations import (
    DocumentMutationJob,
    DocumentMutationRequest,
    DocumentMutationResult,
)

from ..errors import AdapterDisconnected, AdapterNotFound
from .continuity import AttestInput, Baseline
from .models import Binding, Change
from .store import ProjectError, now

if TYPE_CHECKING:
    from .service import ProjectService


class WorkingMutations:
    def __init__(self, service: "ProjectService") -> None:
        self.service = service
        self.lock = asyncio.Lock()
        self.tasks: set[asyncio.Task[OperationSuccess]] = set()

    async def shutdown(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def execute(
        self, registration: AdapterRegistration, request: OperationRequest, limit: float
    ) -> OperationSuccess | None:
        if not registration.project_id:
            return None
        project = self.service.store.document_project(
            registration.application, registration.project_id
        )
        if project is None:
            return None
        if not any("document_attestation" in c.tags for c in registration.operations):
            return None
        task = asyncio.create_task(self._execute(registration, request, project.id))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        # Drain exceptions even if the original caller has left after a pending result.
        task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
        try:
            async with asyncio.timeout(min(20.0, limit)):
                return await asyncio.shield(task)
        except TimeoutError:
            raise ProjectError(
                "mutation_pending",
                "Guarded work continues; observe the working project",
                mutation_id=request.request_id,
            ) from None

    async def _execute(
        self,
        registration: AdapterRegistration,
        request: OperationRequest,
        project_id: str,
    ) -> OperationSuccess:
        async with self.lock:
            service, store = self.service, self.service.store
            mutations = [
                c for c in registration.operations if "document_mutation" in c.tags
            ]
            statuses = [
                c
                for c in registration.operations
                if "document_mutation_status" in c.tags
            ]
            if len(mutations) != 1 or len(statuses) != 1:
                raise ProjectError(
                    "mutation_unsupported", "Adapter lacks guarded mutation receipts"
                )
            document = next(
                d
                for d in store.documents(project_id)
                if d.application == registration.application
                and d.application_project_id == registration.project_id
            )
            with store.transaction() as db:
                project = store.project(db, project_id)
            baseline = service.continuity.baseline(project_id, document.id)
            if baseline is None:
                await service.continuity.attest(
                    project_id,
                    AttestInput(
                        project_id=project_id,
                        document_id=document.id,
                        adapter_id=registration.instance_id,
                        expected_revision=project.revision,
                    ),
                )
                baseline = service.continuity.baseline(project_id, document.id)
            assert baseline is not None
            connections, _ = await service.environment(project_id)
            if document.id not in connections:
                raise ProjectError(
                    "content_diverged", "Working document differs from its trusted head"
                )
            store.prepare_mutation(
                registration.application,
                registration.project_id,
                registration.instance_id,
                connections,
                set(service.core.artifacts.available_ids),
                attached_documents={
                    d: a for (p, d), a in service.attachments.items() if p == project_id
                },
                invalidate=False,
            )
            with store.transaction() as db:
                project = store.project(db, project_id)
                bindings = [
                    Binding.model_validate_json(r[0])
                    for r in db.execute(
                        "SELECT data FROM records WHERE project_id=? AND "
                        "kind='binding' "
                        "AND json_extract(data,'$.document_id')=?",
                        (project_id, document.id),
                    )
                ]
            intent: dict[str, Any] = dict(
                state="pending",
                operation=request.operation,
                stage=project.stage,
                revision=project.revision,
                before=baseline.digest,
                after=None,
            )
            with store.transaction(write=True) as db:
                db.execute(
                    "INSERT INTO document_mutations VALUES (?,?,?,?)",
                    (request.request_id, project_id, document.id, json.dumps(intent)),
                )
            try:
                args = DocumentMutationRequest(
                    mutation_id=request.request_id,
                    operation=request.operation,
                    arguments=request.arguments,
                    host_session_id=baseline.host_session_id,
                    document_session_id=baseline.document_session_id,
                    project_id=document.application_project_id,
                    format=baseline.format,
                    digest=baseline.digest,
                    resources=[
                        ResourceReference(
                            resource_kind=b.resource_kind, resource_id=b.resource_id
                        )
                        for b in bindings
                    ],
                )
                receipt = await self.guarded(registration, args)
                if receipt.mutation_id != request.request_id:
                    raise ProjectError(
                        "mutation_receipt_invalid", "Receipt correlation mismatch"
                    )
                for evidence in (receipt.before, receipt.after):
                    if (
                        evidence.status != "complete"
                        or any(
                            getattr(evidence, field) != getattr(baseline, field)
                            for field in (
                                "host_session_id",
                                "document_session_id",
                                "format",
                            )
                        )
                        or evidence.project_id != document.application_project_id
                    ):
                        raise ProjectError(
                            "mutation_receipt_invalid", "Receipt identity mismatch"
                        )
                if (
                    receipt.before.digest != baseline.digest
                    or not receipt.before.resource_scope
                ):
                    raise ProjectError(
                        "mutation_receipt_invalid",
                        "Receipt does not extend the trusted head",
                    )
                before = {
                    (r.resource_kind, r.resource_id): r
                    for r in receipt.before.resources
                }
                after = {
                    (r.resource_kind, r.resource_id): r for r in receipt.after.resources
                }
                changed = {
                    b.entity_id
                    for b in bindings
                    if before.get((b.resource_kind, b.resource_id))
                    != after.get((b.resource_kind, b.resource_id))
                }
                with store.transaction(write=True) as db:
                    current = store.project(db, project_id)
                    store.expect(current, project.revision)
                    old = db.execute(
                        "SELECT data FROM document_attestations WHERE "
                        "project_id=? AND document_id=?",
                        (project_id, document.id),
                    ).fetchone()
                    if old is None or Baseline.model_validate_json(old[0]) != baseline:
                        raise ProjectError(
                            "mutation_conflict",
                            "Working head changed before receipt commit",
                        )
                    if receipt.after.digest != receipt.before.digest:
                        active = store._gates(
                            db,
                            project_id,
                            connections,
                            set(service.core.artifacts.available_ids),
                        ).milestones[project.stage]
                        changed.update(active.entity_ids)
                        store._invalidate(
                            db,
                            registration.application,
                            document.application_project_id,
                            changed,
                            connections,
                        )
                    is_save = any(
                        c.name == request.operation and "document_save" in c.tags
                        for c in registration.operations
                    )
                    updated = baseline.model_copy(
                        update={
                            "digest": receipt.after.digest,
                            "artifact_locator": receipt.after.file_locator
                            if is_save
                            else baseline.artifact_locator,
                            "artifact_sha256": receipt.after.file_sha256
                            if is_save
                            else (
                                baseline.artifact_sha256
                                if receipt.after.digest == receipt.before.digest
                                else None
                            ),
                            "resources": [
                                r.model_dump(mode="json")
                                for r in receipt.after.resources
                            ],
                            "resource_scope": receipt.after.resource_scope,
                        }
                    )
                    db.execute(
                        "UPDATE document_attestations SET data=? WHERE "
                        "project_id=? AND document_id=?",
                        (updated.model_dump_json(), project_id, document.id),
                    )
                    revision = project.revision + 1
                    current = store.project(db, project_id).model_copy(
                        update={"revision": revision, "updated_at": now()}
                    )
                    store._change(
                        db,
                        project_id,
                        Change(
                            revision=revision,
                            kind="document",
                            id=document.id,
                            action="updated",
                            label="Authorized working content",
                            status=project.stage,
                        ),
                    )
                    db.execute(
                        "UPDATE projects SET data=? WHERE id=?",
                        (current.model_dump_json(), project_id),
                    )
                    intent.update(
                        state="completed", after=receipt.after.digest, revision=revision
                    )
                    db.execute(
                        "UPDATE document_mutations SET data=? WHERE id=?",
                        (json.dumps(intent), request.request_id),
                    )
                return OperationSuccess(
                    type="operation.success",
                    request_id=request.request_id,
                    result=receipt.result,
                )
            except BaseException:
                intent["state"] = "uncommitted"
                with store.transaction(write=True) as db:
                    db.execute(
                        "UPDATE document_mutations SET data=? WHERE id=?",
                        (json.dumps(intent), request.request_id),
                    )
                raise

    async def guarded(
        self, registration: AdapterRegistration, args: DocumentMutationRequest
    ) -> DocumentMutationResult:
        mutations = [
            c for c in registration.operations if "document_mutation" in c.tags
        ]
        statuses = [
            c for c in registration.operations if "document_mutation_status" in c.tags
        ]
        if (
            len(mutations) != 1
            or len(statuses) != 1
            or statuses[0].effect != "read_only"
        ):
            raise ProjectError(
                "mutation_unsupported", "Adapter lacks guarded mutation receipts"
            )
        response = await self.service.core.dispatcher.execute(
            adapter_id=registration.instance_id,
            operation=mutations[0].name,
            arguments=args.model_dump(mode="json"),
            _internal=True,
        )
        job = DocumentMutationJob.model_validate(response.result)
        delay = job.poll_after_seconds
        async with asyncio.timeout(180):
            while job.state in {"queued", "running"}:
                await asyncio.sleep(delay)
                # A retained native job can change document/path metadata and
                # reconnect while it runs. Wait for routing to return; retry only
                # its read-only status, never the mutation that already started.
                registry = self.service.core.registry
                while True:
                    revision = registry.revision
                    try:
                        response = await self.service.core.dispatcher.execute(
                            adapter_id=registration.instance_id,
                            operation=statuses[0].name,
                            arguments={"job_id": job.job_id},
                            _internal=True,
                        )
                        break
                    except (AdapterNotFound, AdapterDisconnected):
                        await registry.wait_for_change(revision, 180)
                job = DocumentMutationJob.model_validate(response.result)
                delay = min(2.0, delay * 1.5)
        if job.state != "completed" or job.result is None:
            raise ProjectError(
                job.error.code if job.error else "mutation_incomplete",
                job.error.message if job.error else "Native mutation did not complete",
            )
        return job.result
