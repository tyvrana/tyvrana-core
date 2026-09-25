"""Restore a durable artifact without trusting the divergent content discarded."""

import asyncio
import hashlib
import json
import logging
import sqlite3
from typing import TYPE_CHECKING, Any

from tyvrana_protocol import (
    DocumentAttestation,
    DocumentRestoreJob,
    DocumentRestoreRequest,
    DocumentRestoreTarget,
    DocumentState,
    ResourceObservation,
)

from ..errors import AdapterDisconnected, AdapterNotFound
from .continuity import Baseline
from .models import (
    RECORD,
    Binding,
    BindingObservation,
    Change,
    Document,
    Milestone,
    Validation,
)
from .restore_models import RestoreInput, RestoreResult
from .store import ProjectError, now

if TYPE_CHECKING:
    from ..registry import AdapterInfo
    from .service import ProjectService

logger = logging.getLogger(__name__)


class TrustedRestore:
    def __init__(self, service: "ProjectService") -> None:
        self.service = service
        self.tasks: dict[str, asyncio.Task[None]] = {}

    async def shutdown(self) -> None:
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _native_status(
        self, adapter_id: str, operation: str, job_id: str
    ) -> DocumentRestoreJob:
        # Opening a file changes registration metadata and briefly reconnects.
        # Only resume observation of the retained job; never repeat its load.
        registry = self.service.core.registry
        try:
            async with asyncio.timeout(30):
                while True:
                    revision = registry.revision
                    try:
                        response = await self.service.core.dispatcher.execute(
                            adapter_id=adapter_id,
                            operation=operation,
                            arguments={"job_id": job_id},
                            _internal=True,
                        )
                        return DocumentRestoreJob.model_validate(response.result)
                    except (AdapterNotFound, AdapterDisconnected):
                        await registry.wait_for_change(revision, timeout=30)
        except TimeoutError as exc:
            raise ProjectError(
                "restore_disconnected",
                "Restore host did not reconnect; inspect current content",
            ) from exc

    def _read(self, project: str, key: str) -> dict[str, Any] | None:
        with self.service.store.transaction() as db:
            row = db.execute(
                "SELECT data FROM document_mutations WHERE project_id=? AND id=?",
                (project, key),
            ).fetchone()
            data = json.loads(row[0]) if row else None
            return data if data and data.get("kind") == "restore" else None

    def _write(self, key: str, data: dict[str, Any]) -> None:
        with self.service.store.transaction(write=True) as db:
            db.execute(
                "UPDATE document_mutations SET data=? WHERE id=?",
                (json.dumps(data), key),
            )

    def status(self, project: str, key: str) -> RestoreResult:
        data = self._read(project, key)
        if data is None:
            raise ProjectError("restore_missing", "Unknown restore")
        result = RestoreResult.model_validate(data["result"])
        if result.state == "running" and key not in self.tasks:
            result = result.model_copy(
                update=dict(
                    state="failed",
                    error_code="restore_interrupted",
                    error_message="Restore interrupted; inspect current content",
                )
            )
            data["result"] = result.model_dump()
            self._write(key, data)
        return result

    async def start(self, project: str, request: RestoreInput) -> RestoreResult:
        encoded = request.model_dump_json()
        if len(encoded.encode()) > 128 * 1024:
            raise ProjectError("restore_limit", "Restore request exceeds 128 KiB")
        signature = hashlib.sha256(encoded.encode()).hexdigest()
        prior = self._read(project, request.restore_id)
        if prior:
            if prior["signature"] != signature:
                raise ProjectError(
                    "restore_conflict", "Restore ID already names a different request"
                )
            return self.status(project, request.restore_id)
        with self.service.store.transaction(write=True) as db:
            self.service.store.expect(
                self.service.store.project(db, project), request.expected_revision
            )
            if db.execute(
                "SELECT 1 FROM document_mutations WHERE id=?", (request.restore_id,)
            ).fetchone():
                raise ProjectError(
                    "restore_conflict", "Lineage record ID is already used"
                )
            data = dict(
                kind="restore",
                signature=signature,
                request=request.model_dump(mode="json"),
                result=RestoreResult(
                    restore_id=request.restore_id, state="running"
                ).model_dump(),
            )
            db.execute(
                "INSERT INTO document_mutations VALUES (?,?,?,?)",
                (request.restore_id, project, request.document_id, json.dumps(data)),
            )
        task = asyncio.create_task(self._run(project, request, data))
        self.tasks[request.restore_id] = task
        task.add_done_callback(lambda _: self.tasks.pop(request.restore_id, None))
        try:
            async with asyncio.timeout(20):
                await asyncio.shield(task)
        except TimeoutError:
            pass  # Retained work; observe status instead of repeating the load.
        return self.status(project, request.restore_id)

    def _target(
        self, db: sqlite3.Connection, project: str, request: RestoreInput
    ) -> tuple[Baseline, dict[str, Any] | None, str]:
        store = self.service.store
        documents = store.documents(project)
        if len(documents) != 1 or documents[0].id != request.document_id:
            raise ProjectError(
                "restore_scope", "Restore currently requires one bound document"
            )
        stage = store.project(db, project).stage
        if request.checkpoint_id:
            row = db.execute(
                "SELECT data,revision FROM checkpoints WHERE project_id=? AND id=?",
                (project, request.checkpoint_id),
            ).fetchone()
            if row is None:
                raise ProjectError(
                    "restore_target_missing", "Checkpoint is unavailable"
                )
            marker = json.loads(row[0])
            if marker["revision"] != row[1] or marker["id"] != request.checkpoint_id:
                raise ProjectError(
                    "history_corrupt", "Checkpoint revision identity differs"
                )
            snapshot = store.materialize_revision(db, project, marker["revision"])
            stage = snapshot["project"]["stage"]
            value = snapshot["documents"].get(request.document_id)
            if value is None:
                raise ProjectError(
                    "restore_target_missing",
                    "Checkpoint lacks a strong document baseline",
                )
            target = Baseline.model_validate(value)
            physical = marker["document_states"].get(request.document_id)
            if physical is None or any(
                physical.get(field) != getattr(target, field)
                for field in (
                    "format",
                    "digest",
                    "context_id",
                    "artifact_sha256",
                    "artifact_locator",
                    "application_project_id",
                )
            ):
                raise ProjectError(
                    "restore_target_mismatch",
                    "Checkpoint document state differs from revision evidence",
                )
        else:
            snapshot = None
            value = self.service.continuity.baseline(project, request.document_id)
            if value is None:
                raise ProjectError(
                    "restore_target_missing", "No trusted document baseline"
                )
            target = value
        if not target.artifact_sha256:
            raise ProjectError(
                "restore_target_unsaved",
                "Target has no durable saved artifact identity",
            )
        if target.application_project_id != documents[0].application_project_id:
            raise ProjectError("restore_lineage", "Target belongs to another document")
        return target, snapshot, stage

    def _claims(
        self,
        db: sqlite3.Connection,
        project: str,
        request: RestoreInput,
        target: Baseline,
        snapshot: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], set[str]]:
        store = self.service.store
        if snapshot is not None:
            records = snapshot["records"]
            if request.mode == "checkout":
                return dict(records), {
                    k
                    for k, v in records.items()
                    if v["kind"] == "milestone" and v["status"] == "accepted"
                }
            for key, saved in records.items():
                current = store._record(db, project, key).model_dump(mode="json")
                ignored = (
                    {"freshness"}
                    if saved["kind"] == "validation"
                    else (
                        {"status"}
                        if saved["kind"] == "milestone"
                        else ({"adapter_id"} if saved["kind"] == "document" else set())
                    )
                )
                if {k: v for k, v in saved.items() if k not in ignored} != {
                    k: v for k, v in current.items() if k not in ignored
                }:
                    raise ProjectError(
                        "restore_claim_changed",
                        "Checkpoint semantic claims changed",
                        record_id=key,
                    )
            return dict(records), {
                k
                for k, v in records.items()
                if v["kind"] == "milestone" and v["status"] == "accepted"
            }
        stages = set()
        for row in db.execute(
            "SELECT * FROM records WHERE project_id=? AND kind='milestone'", (project,)
        ):
            record = Milestone.model_validate_json(row["data"])
            if request.document_id not in record.document_ids or record.status not in {
                "accepted",
                "invalidated",
            }:
                continue
            view = store._view(db, project, row, {}, set(), evaluate_milestone=False)
            if view.historical_status == "accepted":
                stages.add(record.id)
        protected = self.service.reconciliation.accepted_claims(
            db, project, stages, request.document_id, target
        )
        return {
            key: store._record(db, project, key).model_dump(mode="json")
            for key in protected
        }, stages

    @staticmethod
    def _state(value: DocumentAttestation) -> DocumentState:
        return DocumentState.model_validate(
            {k: getattr(value, k) for k in DocumentState.model_fields}
        )

    async def _run(
        self, project: str, request: RestoreInput, data: dict[str, Any]
    ) -> None:
        try:
            async with self.service.mutations.lock, asyncio.timeout(600):
                await self._restore(project, request, data)
        except BaseException as exc:
            if isinstance(exc, ProjectError):
                code, message = exc.code, str(exc)
            elif isinstance(exc, asyncio.CancelledError):
                code, message = (
                    "restore_interrupted",
                    "Restore interrupted; current content is unqualified",
                )
            else:
                logger.exception("Trusted restore failed")
                code = getattr(getattr(exc, "error", None), "code", "restore_failed")
                message = str(exc)[:512]
            data["result"] = RestoreResult(
                restore_id=request.restore_id,
                state="failed",
                error_code=code,
                error_message=message,
            ).model_dump()
            self._write(request.restore_id, data)

    async def _restore(
        self, project: str, request: RestoreInput, data: dict[str, Any]
    ) -> None:
        with self.service.store.transaction() as db:
            target, _, _ = self._target(db, project, request)
        parent = self.service.core.registry.get(request.adapter_id)
        if request.mode == "checkout":
            current = await self.service.continuity.observe(parent)
            if self._state(current) != request.expected_current:
                raise ProjectError(
                    "restore_current_changed",
                    "Discard authorization no longer matches live content",
                )
            if (
                current.project_id == target.application_project_id
                and current.digest == target.digest
                and current.format == target.format
                and current.file_sha256 == target.artifact_sha256
                and current.file_locator == target.artifact_locator
            ):
                await self._restore_proven(project, request, data, None, current)
                return
        artifact = self.service.proofs.artifact(
            locator=target.artifact_locator,
            sha256=target.artifact_sha256,
            project_id=target.application_project_id,
        )
        async with self.service.proofs.acquire(parent, artifact) as proof:
            await self._restore_proven(project, request, data, proof)

    async def _restore_proven(
        self,
        project: str,
        request: RestoreInput,
        data: dict[str, Any],
        proof_host: "AdapterInfo | None",
        observed: DocumentAttestation | None = None,
    ) -> None:
        service, store = self.service, self.service.store
        with store.transaction() as db:
            state = store.project(db, project)
            store.expect(state, request.expected_revision)
            target, snapshot, stage = self._target(db, project, request)
            records, accepted = self._claims(db, project, request, target, snapshot)
            document = store._record(db, project, request.document_id)
        assert isinstance(document, Document)
        live = service.core.registry.get(request.adapter_id)
        if (proof_host and live.instance_id == proof_host.instance_id) or any(
            a.registration.application != document.application
            for a in (live, proof_host)
            if a is not None
        ):
            raise ProjectError(
                "restore_lineage",
                "An independent proof host of the same application is required",
            )
        if live.registration.project_id not in {None, document.application_project_id}:
            raise ProjectError(
                "restore_lineage", "Current host belongs to another project"
            )
        current = observed or await service.continuity.observe(live)
        if self._state(current) != request.expected_current:
            raise ProjectError(
                "restore_current_changed",
                "Discard authorization no longer matches live content",
            )
        proof = await service.continuity.observe(proof_host) if proof_host else current
        if (
            proof_host is not None and proof.host_session_id == current.host_session_id
        ) or proof.project_id != document.application_project_id:
            raise ProjectError(
                "restore_lineage", "Independent proof belongs to another document"
            )
        if proof.file_sha256 != target.artifact_sha256:
            raise ProjectError(
                "restore_file_mismatch", "Proof file hash differs from durable evidence"
            )
        if proof.digest != target.digest or proof.format != target.format:
            raise ProjectError(
                "restore_target_mismatch",
                "Independent content differs from the trusted target",
            )
        assert target.artifact_sha256 is not None
        assert target.artifact_locator is not None
        resources = service.reconciliation._resources(proof)
        if request.mode == "checkout" and (
            proof.resource_scope != target.resource_scope
            or resources
            != service.reconciliation._resources(
                proof.model_copy(
                    update={
                        "resources": [
                            ResourceObservation.model_validate(v)
                            for v in target.resources
                        ]
                    }
                )
            )
        ):
            raise ProjectError(
                "restore_incomplete", "Checkpoint resource evidence differs"
            )
        for value in records.values():
            if value["kind"] == "binding" and request.mode != "checkout":
                resource = resources.get((value["resource_kind"], value["resource_id"]))
                if (
                    resource is None
                    or resource.state != "present"
                    or not resource.fingerprint
                ):
                    raise ProjectError(
                        "restore_incomplete",
                        "Checkpoint binding lacks complete target evidence",
                    )
        data["preflight"] = proof.model_dump(mode="json")
        self._write(request.restore_id, data)
        already = (
            current.digest == target.digest
            and current.format == target.format
            and current.project_id == target.application_project_id
            and current.file_sha256 == target.artifact_sha256
        )
        # Recheck semantic authorization after independent preflight, before discard.
        with store.transaction() as db:
            store.expect(store.project(db, project), request.expected_revision)
            checked_target, checked_snapshot, _ = self._target(db, project, request)
            if checked_target != target or checked_snapshot != snapshot:
                raise ProjectError(
                    "restore_conflict", "Target changed during preflight"
                )
            self._claims(db, project, request, target, snapshot)
        if not already:
            contracts = live.registration.operations

            def operation(tag: str) -> str:
                values = [c.name for c in contracts if tag in c.tags]
                if len(values) != 1:
                    raise ProjectError(
                        "restore_unsupported",
                        "Adapter lacks typed restore capability",
                        tag=tag,
                    )
                return values[0]

            args = DocumentRestoreRequest(
                mutation_id=request.restore_id,
                operation=operation("document_open"),
                locator=target.artifact_locator,
                discard_current=True,
                current=request.expected_current,
                target=DocumentRestoreTarget(
                    project_id=target.application_project_id,
                    format=target.format,
                    digest=target.digest,
                    file_sha256=target.artifact_sha256,
                ),
            )
            response = await service.core.dispatcher.execute(
                adapter_id=live.instance_id,
                operation=operation("document_restore"),
                arguments=args.model_dump(mode="json"),
                _internal=True,
            )
            job = DocumentRestoreJob.model_validate(response.result)
            while job.state in {"queued", "running"}:
                await asyncio.sleep(job.poll_after_seconds)
                job = await self._native_status(
                    live.instance_id, operation("document_restore_status"), job.job_id
                )
            if job.result is None:
                raise ProjectError(
                    job.error.code if job.error else "restore_incomplete",
                    job.error.message if job.error else "Native restore failed",
                )
            if (
                job.result.mutation_id != request.restore_id
                or self._state(job.result.before) != request.expected_current
            ):
                raise ProjectError(
                    "restore_receipt_invalid",
                    "Restore receipt does not match discard authorization",
                )
            data["receipt"] = job.result.model_dump(mode="json")
            self._write(request.restore_id, data)
        final_host = service.core.registry.get(live.instance_id)
        final = await service.continuity.observe(final_host)
        if (
            request.mode == "checkout"
            and already
            and self._state(final) != request.expected_current
        ):
            raise ProjectError(
                "restore_current_changed", "Live state changed before checkout commit"
            )
        if (
            final.host_session_id != current.host_session_id
            or final.project_id != target.application_project_id
            or final.digest != target.digest
            or final.format != target.format
            or final.file_sha256 != target.artifact_sha256
            or service.reconciliation._resources(final) != resources
        ):
            raise ProjectError(
                "restore_post_mismatch",
                "Post-load content is not the independently proved trusted artifact",
            )
        with store.transaction(write=True) as db:
            state = store.project(db, project)
            store.expect(state, request.expected_revision)
            checked, check_snapshot, check_stage = self._target(db, project, request)
            if checked != target or check_snapshot != snapshot or check_stage != stage:
                raise ProjectError(
                    "restore_conflict", "Trusted target changed before restore commit"
                )
            records, accepted = self._claims(db, project, request, target, snapshot)
            if request.mode == "checkout":
                from .checkout import commit_checkout

                assert snapshot is not None
                commit_checkout(
                    self,
                    db,
                    project,
                    request,
                    data,
                    target,
                    snapshot,
                    final,
                    final_host,
                )
                service.attachments[project, document.id] = live.instance_id
                return
            semantic_changes = not already or stage != state.stage
            for key, value in records.items():
                record = store._record(db, project, key)
                if isinstance(record, Validation):
                    desired = value["freshness"] if snapshot else "current"
                    semantic_changes |= record.freshness != desired
                elif isinstance(record, Milestone) and key in accepted:
                    semantic_changes |= record.status != "accepted"
            for row in db.execute(
                "SELECT data FROM records WHERE project_id=?", (project,)
            ):
                other = RECORD.validate_json(row[0])
                if other.id not in records:
                    semantic_changes |= (
                        isinstance(other, Validation) and other.freshness != "stale"
                    ) or (isinstance(other, Milestone) and other.status == "accepted")
            baseline = service.continuity.baseline(project, request.document_id)
            semantic_changes |= baseline is None or baseline.digest != target.digest
            revision = state.revision + int(semantic_changes)
            # Restore only recorded claims after native proof.
            for key, value in records.items():
                record = store._record(db, project, key)
                if isinstance(record, Validation):
                    desired = value["freshness"] if snapshot else "current"
                    if record.freshness != desired:
                        store._put(
                            db,
                            project,
                            record.model_copy(update={"freshness": desired}),
                            revision,
                            "verified",
                        )
                    context = store._contexts(
                        db, project, record, {document.id: target.context_id}
                    )
                    db.execute(
                        "INSERT OR REPLACE INTO validation_context VALUES (?,?,?)",
                        (project, key, json.dumps(context)),
                    )
                elif isinstance(record, Binding):
                    saved = (snapshot or {}).get("observations", {}).get(key)
                    if saved is None:
                        row = db.execute(
                            "SELECT data FROM observations WHERE project_id=? AND id=?",
                            (project, key),
                        ).fetchone()
                        saved = json.loads(row[0]) if row else None
                    if saved is None or not saved.get("fingerprint"):
                        raise ProjectError(
                            "restore_incomplete",
                            "Prior binding verification is missing",
                        )
                    observation = BindingObservation.model_validate(saved).model_copy(
                        update=dict(
                            state="verified",
                            connection_id=target.context_id,
                            verified_revision=revision,
                        )
                    )
                    db.execute(
                        "INSERT OR REPLACE INTO observations VALUES (?,?,?)",
                        (project, key, observation.model_dump_json()),
                    )
                elif isinstance(record, Milestone):
                    desired_status = "accepted" if key in accepted else value["status"]
                    if record.status != desired_status:
                        store._put(
                            db,
                            project,
                            record.model_copy(update={"status": desired_status}),
                            revision,
                            "verified",
                        )
            # Claims outside the snapshot cannot become current.
            for row in db.execute(
                "SELECT * FROM records WHERE project_id=?", (project,)
            ).fetchall():
                record = RECORD.validate_json(row["data"])
                if record.id in records:
                    continue
                if isinstance(record, Validation) and record.freshness != "stale":
                    store._put(
                        db,
                        project,
                        record.model_copy(update={"freshness": "stale"}),
                        revision,
                        "staled",
                    )
                elif isinstance(record, Milestone) and record.status == "accepted":
                    store._put(
                        db,
                        project,
                        record.model_copy(update={"status": "invalidated"}),
                        revision,
                        "staled",
                    )
                elif isinstance(record, Binding):
                    db.execute(
                        "DELETE FROM observations WHERE project_id=? AND id=?",
                        (project, record.id),
                    )
            updated = target.model_copy(
                update=dict(
                    host_session_id=final.host_session_id,
                    document_session_id=final.document_session_id,
                    resources=[r.model_dump(mode="json") for r in final.resources],
                    resource_scope=final.resource_scope,
                )
            )
            db.execute(
                "INSERT OR REPLACE INTO document_attestations VALUES (?,?,?)",
                (project, document.id, updated.model_dump_json()),
            )
            if semantic_changes:
                state = state.model_copy(
                    update=dict(revision=revision, stage=stage, updated_at=now())
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
                        label="Trusted artifact restored",
                        status=stage,
                    ),
                )
            data["result"] = RestoreResult(
                restore_id=request.restore_id,
                state="completed",
                revision=revision,
                digest=final.digest,
                restored_milestones=sorted(accepted),
                already_current=already,
            ).model_dump()
            data["postflight"] = final.model_dump(mode="json")
            db.execute(
                "UPDATE document_mutations SET data=? WHERE id=?",
                (json.dumps(data), request.restore_id),
            )
        service.attachments[project, document.id] = live.instance_id
