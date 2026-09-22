"""Prove a missing working transition by replay on an independent prior document."""

import asyncio
import hashlib
import json
import logging
import sqlite3
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from tyvrana_protocol import (
    DocumentAttestation,
    DocumentMutationRequest,
    ResourceReference,
)

from .continuity import Baseline
from .models import Binding, BindingObservation, Change, Document, Milestone, Validation
from .reconcile_models import ReconcileInput, ReconcileResult
from .store import ProjectError, now

if TYPE_CHECKING:
    from .service import ProjectService

logger = logging.getLogger(__name__)


class Reconciliation:
    def __init__(self, service: "ProjectService") -> None:
        self.service = service
        self.tasks: dict[str, asyncio.Task[None]] = {}

    async def shutdown(self) -> None:
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _read(self, project: str, key: str) -> dict[str, Any] | None:
        with self.service.store.transaction() as db:
            row = db.execute(
                "SELECT data FROM document_mutations WHERE id=? AND project_id=?",
                (key, project),
            ).fetchone()
            data = json.loads(row[0]) if row else None
            return data if data and data.get("kind") == "reconciliation" else None

    def _write(self, key: str, value: dict[str, Any]) -> None:
        with self.service.store.transaction(write=True) as db:
            db.execute(
                "UPDATE document_mutations SET data=? WHERE id=?",
                (json.dumps(value), key),
            )

    def status(self, project: str, key: str) -> ReconcileResult:
        data = self._read(project, key)
        if data is None:
            raise ProjectError("reconciliation_missing", "Unknown reconciliation")
        result = ReconcileResult.model_validate(data["result"])
        if result.state == "running" and key not in self.tasks:
            result = result.model_copy(
                update=dict(
                    state="failed",
                    error_code="reconciliation_interrupted",
                    error_message="Recovery stopped before commit; no head adopted",
                )
            )
            data["result"] = result.model_dump()
            self._write(key, data)
        return result

    async def start(self, project: str, request: ReconcileInput) -> ReconcileResult:
        encoded = request.model_dump_json()
        if len(encoded.encode()) > 128 * 1024:
            raise ProjectError(
                "reconciliation_limit", "Declared typed delta exceeds 128 KiB"
            )
        signature = hashlib.sha256(encoded.encode()).hexdigest()
        prior = self._read(project, request.reconciliation_id)
        if prior:
            if prior["signature"] != signature:
                raise ProjectError(
                    "reconciliation_conflict",
                    "Reconciliation ID already names a different request",
                )
            result = self.status(project, request.reconciliation_id)
            return result.model_copy(
                update={"already_reconciled": result.state == "completed"}
            )
        with self.service.store.transaction(write=True) as db:
            self.service.store.expect(
                self.service.store.project(db, project), request.expected_revision
            )
            if db.execute(
                "SELECT 1 FROM document_mutations WHERE id=?",
                (request.reconciliation_id,),
            ).fetchone():
                raise ProjectError(
                    "reconciliation_conflict", "Lineage record ID is already used"
                )
            data: dict[str, Any] = dict(
                kind="reconciliation",
                signature=signature,
                request=request.model_dump(mode="json"),
                receipts=[],
                result=ReconcileResult(
                    reconciliation_id=request.reconciliation_id, state="running"
                ).model_dump(),
            )
            db.execute(
                "INSERT INTO document_mutations VALUES (?,?,?,?)",
                (
                    request.reconciliation_id,
                    project,
                    request.document_id,
                    json.dumps(data),
                ),
            )
        task = asyncio.create_task(self._run(project, request, data))
        self.tasks[request.reconciliation_id] = task
        task.add_done_callback(
            lambda _: self.tasks.pop(request.reconciliation_id, None)
        )
        try:
            async with asyncio.timeout(20):
                await asyncio.shield(task)
        except TimeoutError:
            pass  # Retained work; repeating the request does not duplicate replay.
        return self.status(project, request.reconciliation_id)

    def _claims(
        self,
        db: sqlite3.Connection,
        project: str,
        request: ReconcileInput,
        baseline: Baseline,
    ) -> tuple[Milestone, set[str], set[str], list[Binding]]:
        store = self.service.store
        target = store._record(db, project, request.stage_id)
        if (
            not isinstance(target, Milestone)
            or target.status == "accepted"
            or not target.entity_ids
            or request.document_id not in target.document_ids
        ):
            raise ProjectError(
                "reconciliation_stage",
                "Choose an unaccepted working stage owning this document",
            )
        if any(step.owner_entity_id not in target.entity_ids for step in request.delta):
            raise ProjectError(
                "reconciliation_ownership",
                "Every replay step must belong to the target stage",
            )
        stages: set[str] = set()
        pending = list(target.prerequisite_ids)
        while pending:
            key = pending.pop()
            if key in stages:
                continue
            stage = store._record(db, project, key)
            if not isinstance(stage, Milestone) or stage.status not in {
                "accepted",
                "invalidated",
            }:
                raise ProjectError(
                    "reconciliation_prerequisite",
                    "Only historically accepted prerequisites can be restored",
                )
            stages.add(key)
            pending.extend(stage.prerequisite_ids)
        if not stages:
            raise ProjectError(
                "reconciliation_prerequisite",
                "Recovery requires an accepted prerequisite",
            )
        protected: set[str] = set()
        floor = store.project(db, project).history_floor
        for key in stages:
            row = db.execute(
                "SELECT * FROM records WHERE project_id=? AND id=?", (project, key)
            ).fetchone()
            view = store._view(
                db,
                project,
                row,
                {request.document_id: baseline.context_id},
                set(),
                evaluate_milestone=False,
            )
            accepted = view.accepted_revision
            if accepted is None or accepted < floor:
                raise ProjectError(
                    "reconciliation_history",
                    "Retained exact acceptance history is required",
                )
            restorations = {
                result["revision"]
                for saved in db.execute(
                    "SELECT data FROM document_mutations WHERE project_id=?",
                    (project,),
                )
                if (entry := json.loads(saved[0])).get("kind") == "reconciliation"
                and (result := entry["result"])["state"] == "completed"
                and key in result["restored_milestones"]
            }
            closure: set[str] = set()
            pending = [key]
            while pending:
                item = pending.pop()
                if item in closure:
                    continue
                closure.add(item)
                pending.extend(
                    r[0]
                    for r in db.execute(
                        "SELECT target FROM refs WHERE project_id=? AND owner=?",
                        (project, item),
                    )
                )
                # Resource bindings and outgoing semantic dependencies of an entity.
                pending.extend(
                    r[0]
                    for r in db.execute(
                        "SELECT id FROM records WHERE project_id=? AND "
                        "((kind='binding' AND "
                        "json_extract(data,'$.entity_id')=?) OR "
                        "(kind='relationship' AND json_extract(data,'$.source_id')=?))",
                        (project, item, item),
                    )
                )
            for item in closure:
                row = db.execute(
                    "SELECT * FROM records WHERE project_id=? AND id=?", (project, item)
                ).fetchone()
                record = store._record(db, project, item)
                if isinstance(record, Document) and record.id != request.document_id:
                    raise ProjectError(
                        "reconciliation_scope",
                        "This recovery proof covers one document",
                    )
                if row["revision"] > accepted:
                    changes = db.execute(
                        "SELECT data FROM journal WHERE project_id=? "
                        "AND id=? AND revision>?",
                        (project, item, accepted),
                    ).fetchall()
                    if (
                        not isinstance(record, (Milestone, Validation))
                        or not changes
                        or any(
                            (change := json.loads(r[0]))["action"] != "staled"
                            and not (
                                change["action"] == "verified"
                                and change["revision"] in restorations
                            )
                            for r in changes
                        )
                    ):
                        raise ProjectError(
                            "reconciliation_claim_changed",
                            "An accepted semantic claim changed",
                            record_id=item,
                        )
                if isinstance(record, Validation) and record.status != "passed":
                    raise ProjectError(
                        "reconciliation_claim_changed",
                        "A prerequisite validation no longer passes",
                        record_id=item,
                    )
            protected.update(closure)
        if protected.intersection(target.entity_ids):
            raise ProjectError(
                "reconciliation_ownership", "Working ownership overlaps accepted claims"
            )
        bindings = [
            Binding.model_validate_json(r[0])
            for r in db.execute(
                "SELECT data FROM records WHERE project_id=? AND "
                "kind='binding' AND "
                "json_extract(data,'$.document_id')=?",
                (project, request.document_id),
            )
        ]
        return target, stages, protected, bindings

    @staticmethod
    def _resources(evidence: DocumentAttestation) -> dict[tuple[str, str], Any]:
        if evidence.status != "complete" or not evidence.resource_scope:
            raise ProjectError(
                "reconciliation_incomplete", "Complete scoped attestation is required"
            )
        values = {(r.resource_kind, r.resource_id): r for r in evidence.resources}
        if len(values) != len(evidence.resources):
            raise ProjectError(
                "reconciliation_incomplete", "Ambiguous resource observations"
            )
        return values

    @staticmethod
    def _same_document(
        evidence: DocumentAttestation, baseline: Baseline, *, live: bool
    ) -> None:
        if (
            evidence.project_id != baseline.application_project_id
            or evidence.format != baseline.format
            or (
                live
                and (
                    evidence.host_session_id != baseline.host_session_id
                    or evidence.document_session_id != baseline.document_session_id
                )
            )
        ):
            raise ProjectError(
                "reconciliation_lineage",
                "Document identity or attestation format differs",
            )

    async def _run(
        self, project: str, request: ReconcileInput, data: dict[str, Any]
    ) -> None:
        try:
            async with self.service.mutations.lock, asyncio.timeout(600):
                await self._prove(project, request, data)
        except BaseException as exc:
            if isinstance(exc, ProjectError):
                code, message = exc.code, str(exc)
            elif isinstance(exc, asyncio.CancelledError):
                code, message = (
                    "reconciliation_interrupted",
                    "Recovery stopped without committing a working head",
                )
            else:
                logger.exception("Reconciliation failed before commit")
                code, message = "reconciliation_failed", str(exc)[:512]
            data["result"] = ReconcileResult(
                reconciliation_id=request.reconciliation_id,
                state="failed",
                error_code=code,
                error_message=message,
            ).model_dump()
            self._write(request.reconciliation_id, data)

    async def _prove(
        self, project: str, request: ReconcileInput, data: dict[str, Any]
    ) -> None:
        service, store = self.service, self.service.store
        baseline = service.continuity.baseline(project, request.document_id)
        if baseline is None or baseline.digest != request.prior_digest:
            raise ProjectError(
                "reconciliation_prior_head",
                "Declared prior head must match the durable trusted head",
            )
        live, proof = (
            service.core.registry.get(a)
            for a in (request.adapter_id, request.proof_adapter_id)
        )
        with store.transaction() as db:
            store.expect(store.project(db, project), request.expected_revision)
            document = store._record(db, project, request.document_id)
            if not isinstance(document, Document):
                raise ProjectError("document_missing", "Choose a bound document")
            target, stages, protected, bindings = self._claims(
                db, project, request, baseline
            )
        if any(
            a.registration.application != document.application
            or a.registration.project_id != document.application_project_id
            for a in (live, proof)
        ):
            raise ProjectError(
                "reconciliation_lineage",
                "Proof and live adapters must represent the bound lineage",
            )
        contracts = {c.name: c for c in proof.registration.operations}
        for step in request.delta:
            c = contracts.get(step.operation)
            if (
                c is None
                or "recovery_replay" not in c.tags
                or c.effect != "mutating"
                or c.execution != "synchronous"
                or c.input_artifacts != "none"
                or c.output_artifacts != "none"
            ):
                raise ProjectError(
                    "reconciliation_replay_unsupported",
                    "Delta operation is not qualified for isolated recovery replay",
                    operation=step.operation,
                )
        current = await service.continuity.observe(live)
        self._same_document(current, baseline, live=True)
        if current.digest != request.expected_digest:
            raise ProjectError(
                "reconciliation_delta_mismatch",
                "Live content differs from the declared exact result",
            )
        previous = await service.continuity.observe(proof)
        self._same_document(previous, baseline, live=False)
        if (
            previous.host_session_id == current.host_session_id
            or previous.digest != baseline.digest
        ):
            raise ProjectError(
                "reconciliation_prior_head",
                "Independent proof host must contain exactly the trusted prior head",
            )
        protected_bindings = [
            b
            for b in bindings
            if b.entity_id not in target.entity_ids or b.id in protected
        ]
        prior_resources = self._resources(previous)
        current_resources = self._resources(current)
        if previous.resource_scope != current.resource_scope:
            raise ProjectError(
                "reconciliation_incomplete", "Resource scopes do not match"
            )
        for b in protected_bindings:
            resource_key = b.resource_kind, b.resource_id
            r = prior_resources.get(resource_key)
            if (
                r is None
                or r.state != "present"
                or not r.fingerprint
                or current_resources.get(resource_key) != r
            ):
                raise ProjectError(
                    "reconciliation_upstream_changed",
                    "Protected prerequisite resource differs",
                    binding_id=b.id,
                )
        data["prior_proof"] = previous.model_dump(mode="json")
        for step in request.delta:
            args = DocumentMutationRequest(
                mutation_id=uuid4().hex,
                operation=step.operation,
                arguments=step.arguments,
                host_session_id=previous.host_session_id,
                document_session_id=previous.document_session_id,
                project_id=baseline.application_project_id,
                format=baseline.format,
                digest=previous.digest,
                resources=[
                    ResourceReference(
                        resource_kind=b.resource_kind, resource_id=b.resource_id
                    )
                    for b in protected_bindings
                ],
            )
            receipt = await service.mutations.guarded(proof.registration, args)
            if (
                receipt.mutation_id != args.mutation_id
                or receipt.before.digest != previous.digest
                or any(
                    getattr(receipt.before, k) != getattr(previous, k)
                    for k in (
                        "host_session_id",
                        "document_session_id",
                        "project_id",
                        "format",
                        "resource_scope",
                    )
                )
            ):
                raise ProjectError(
                    "reconciliation_receipt",
                    "Replay receipt does not extend the proven prior content",
                )
            if any(
                evidence.status != "complete"
                or any(
                    getattr(evidence, field) != getattr(previous, field)
                    for field in (
                        "host_session_id",
                        "document_session_id",
                        "project_id",
                        "format",
                        "resource_scope",
                    )
                )
                for evidence in (receipt.before, receipt.after)
            ):
                raise ProjectError(
                    "reconciliation_receipt",
                    "Replay changed document identity or scope",
                )
            previous = receipt.after
            data["receipts"].append(receipt.model_dump(mode="json"))
            self._write(request.reconciliation_id, data)
        if previous.digest != request.expected_digest:
            raise ProjectError(
                "reconciliation_delta_mismatch",
                "Trusted prior head plus declared delta does not "
                "explain the complete live content",
            )
        final = await service.continuity.observe(live)
        self._same_document(final, baseline, live=True)
        if (
            final.digest != previous.digest
            or final.resource_scope != previous.resource_scope
            or self._resources(final) != self._resources(previous)
        ):
            raise ProjectError(
                "reconciliation_delta_mismatch",
                "Live document changed or resource identities differ "
                "from the proved result",
            )
        # Recheck protected resources against the replayed result.
        final_resources = self._resources(final)
        for b in protected_bindings:
            resource_key = b.resource_kind, b.resource_id
            if final_resources.get(resource_key) != prior_resources[resource_key]:
                raise ProjectError(
                    "reconciliation_upstream_changed",
                    "Declared delta changes protected resources",
                )
        with store.transaction(write=True) as db:
            state = store.project(db, project)
            store.expect(state, request.expected_revision)
            row = db.execute(
                "SELECT data FROM document_attestations WHERE "
                "project_id=? AND document_id=?",
                (project, document.id),
            ).fetchone()
            if row is None or Baseline.model_validate_json(row[0]) != baseline:
                raise ProjectError(
                    "reconciliation_conflict", "Trusted prior head changed during proof"
                )
            target, stages, protected, bindings = self._claims(
                db, project, request, baseline
            )
            revision = state.revision + 1
            connections = {document.id: baseline.context_id}
            for key in protected:
                record = store._record(db, project, key)
                if isinstance(record, Validation):
                    store._put(
                        db,
                        project,
                        record.model_copy(update={"freshness": "current"}),
                        revision,
                        "verified",
                    )
                    context = store._contexts(db, project, record, connections)
                    if any(not value for value in context.values()):
                        raise ProjectError(
                            "reconciliation_scope", "Unproved validation document"
                        )
                    db.execute(
                        "INSERT INTO validation_context VALUES (?,?,?) "
                        "ON CONFLICT(project_id,id) DO UPDATE SET "
                        "data=excluded.data",
                        (project, key, json.dumps(context)),
                    )
                elif isinstance(record, Binding):
                    row = db.execute(
                        "SELECT data FROM observations WHERE project_id=? AND id=?",
                        (project, key),
                    ).fetchone()
                    observation = (
                        BindingObservation.model_validate_json(row[0]) if row else None
                    )
                    if (
                        observation is None
                        or observation.state not in {"verified", "stale"}
                        or not observation.fingerprint
                        or observation.connection_id != baseline.context_id
                    ):
                        raise ProjectError(
                            "reconciliation_claim_changed",
                            "Prior verified binding evidence is missing",
                            binding_id=key,
                        )
                    db.execute(
                        "UPDATE observations SET data=? WHERE project_id=? AND id=?",
                        (
                            observation.model_copy(
                                update={
                                    "state": "verified",
                                    "verified_revision": revision,
                                }
                            ).model_dump_json(),
                            project,
                            key,
                        ),
                    )
            for key in stages:
                stage = store._record(db, project, key)
                store._put(
                    db,
                    project,
                    stage.model_copy(update={"status": "accepted"}),
                    revision,
                    "verified",
                )
            store._stale(db, project, set(target.entity_ids), revision, set())
            store._put(
                db,
                project,
                target.model_copy(update={"status": "in_progress"}),
                revision,
                "verified",
            )
            gates = store._gates(
                db, project, connections, set(service.core.artifacts.available_ids)
            )
            store._reject_gate(gates.activation[target.id])
            updated = baseline.model_copy(
                update={
                    "digest": final.digest,
                    "resources": [r.model_dump(mode="json") for r in final.resources],
                    "resource_scope": final.resource_scope,
                }
            )
            db.execute(
                "UPDATE document_attestations SET data=? WHERE "
                "project_id=? AND document_id=?",
                (updated.model_dump_json(), project, document.id),
            )
            state = state.model_copy(
                update={"revision": revision, "stage": target.id, "updated_at": now()}
            )
            db.execute(
                "UPDATE projects SET data=? WHERE id=?",
                (state.model_dump_json(), project),
            )
            store._change(
                db,
                project,
                Change(
                    revision=revision,
                    kind="document",
                    id=document.id,
                    action="verified",
                    label="Proven working-state reconciliation",
                    status=target.id,
                ),
            )
            data["current_proof"] = final.model_dump(mode="json")
            data["result"] = ReconcileResult(
                reconciliation_id=request.reconciliation_id,
                state="completed",
                revision=revision,
                digest=final.digest,
                restored_milestones=sorted(stages),
            ).model_dump()
            db.execute(
                "UPDATE document_mutations SET data=? WHERE id=?",
                (json.dumps(data), request.reconciliation_id),
            )
        service.attachments[project, document.id] = live.instance_id
