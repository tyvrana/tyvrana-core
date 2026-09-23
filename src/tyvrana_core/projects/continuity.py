"""Strong document evidence and runtime attachment, separate from semantic history."""

import asyncio
import json
import time
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from pydantic import Field
from tyvrana_protocol import (
    DocumentAttestation,
    DocumentAttestationJob,
    DocumentAttestationResponse,
)

from .models import Document, Key, Model, ProjectInput
from .store import ProjectError

if TYPE_CHECKING:
    from ..registry import AdapterInfo
    from .service import ProjectService


class AttestInput(ProjectInput):
    document_id: Key
    adapter_id: Key
    expected_revision: int = Field(ge=0)
    mode: Literal["capture", "reattach", "bootstrap"] = "capture"
    proof_adapter_id: Key | None = None
    trusted_artifact_sha256: str | None = Field(default=None, pattern="^[0-9a-f]{64}$")
    provenance: str = Field(default="", max_length=2048)


class AttestResult(Model):
    document_id: str
    revision: int
    digest: str
    context_id: str
    host_session_id: str
    document_session_id: str
    adapter_id: str
    semantic_revision_changed: bool = False


class Baseline(Model):
    application_project_id: str
    format: str
    digest: str
    context_id: str
    host_session_id: str
    document_session_id: str
    provenance: str
    artifact_sha256: str | None = None
    resources: list[dict[str, object]] = Field(default_factory=list)
    resource_scope: str | None = None


class Continuity:
    def __init__(self, service: "ProjectService") -> None:
        self.service = service

    def baseline(self, project: str, document: str) -> Baseline | None:
        with self.service.store.transaction() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS document_attestations (project_id"
                " TEXT NOT NULL, document_id TEXT NOT NULL, data TEXT NOT "
                "NULL, PRIMARY KEY(project_id,document_id), FOREIGN "
                "KEY(project_id,document_id) REFERENCES "
                "records(project_id,id) ON DELETE CASCADE)"
            )
            row = db.execute(
                (
                    "SELECT data FROM document_attestations WHERE project_id=? "
                    "AND document_id=?"
                ),
                (project, document),
            ).fetchone()
            return Baseline.model_validate_json(row[0]) if row else None

    async def observe(self, adapter: "AdapterInfo") -> DocumentAttestation:
        contracts = [
            c
            for c in adapter.registration.operations
            if "document_attestation" in c.tags
            and c.effect == "read_only"
            and c.input_artifacts == c.output_artifacts == "none"
        ]
        if len(contracts) != 1:
            raise ProjectError(
                "attestation_unsupported",
                "Adapter must advertise one read-only document_attestation contract",
            )
        response = await self.service.core.dispatcher.execute(
            adapter_id=adapter.instance_id, operation=contracts[0].name, arguments={}
        )
        observed = DocumentAttestationResponse.model_validate(response.result).root
        if isinstance(observed, DocumentAttestationJob):
            statuses = [
                c
                for c in adapter.registration.operations
                if "document_attestation_status" in c.tags
                and c.effect == "read_only"
                and c.execution == "job_status"
            ]
            if len(statuses) != 1:
                raise ProjectError(
                    "attestation_unsupported",
                    "Observable attestation requires one tagged status contract",
                )
            deadline = time.monotonic() + 20
            delay = observed.poll_after_seconds
            while observed.state in {"queued", "running"}:
                if time.monotonic() + delay >= deadline:
                    raise ProjectError(
                        "attestation_pending",
                        "Attestation continues as an observable application job; "
                        "inspect its status",
                        job_id=observed.job_id,
                        operation=statuses[0].name,
                    )
                await asyncio.sleep(delay)
                try:
                    async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
                        response = await self.service.core.dispatcher.execute(
                            adapter_id=adapter.instance_id,
                            operation=statuses[0].name,
                            arguments={"job_id": observed.job_id},
                        )
                except TimeoutError:
                    raise ProjectError(
                        "attestation_pending",
                        "Attestation continues as an observable application job; "
                        "inspect its status",
                        job_id=observed.job_id,
                        operation=statuses[0].name,
                    ) from None
                observed = DocumentAttestationJob.model_validate(response.result)
                delay = min(2.0, max(observed.poll_after_seconds, delay * 1.5))
            if observed.state != "completed" or observed.result is None:
                raise ProjectError(
                    "attestation_incomplete",
                    "Attestation job did not complete",
                    state=observed.state,
                    error=observed.error.model_dump() if observed.error else None,
                )
            evidence = observed.result
        else:
            evidence = observed
        current = self.service.core.registry.get(adapter.instance_id)
        if (
            current.connection_id != adapter.connection_id
            or current.registration.project_id != evidence.project_id
        ):
            raise ProjectError(
                "application_changed", "Connection/document changed during attestation"
            )
        if evidence.status != "complete":
            raise ProjectError(
                "attestation_incomplete",
                "Document content could not be completely attested",
                omissions=evidence.omissions,
            )
        return evidence

    async def resolve(
        self, project: str, document: Document
    ) -> tuple["AdapterInfo", str] | None:
        baseline = self.baseline(project, document.id)
        if baseline is None:
            return None
        if baseline.application_project_id != document.application_project_id:
            return None
        matches = []
        for adapter in self.service.core.registry.list():
            if (
                adapter.registration.application != document.application
                or adapter.registration.project_id != document.application_project_id
            ):
                continue
            try:
                evidence = await self.observe(adapter)
            except ProjectError:
                continue
            if (
                evidence.host_session_id == baseline.host_session_id
                and evidence.document_session_id == baseline.document_session_id
                and evidence.format == baseline.format
                and evidence.digest == baseline.digest
            ):
                matches.append(adapter)
        return (matches[0], baseline.context_id) if len(matches) == 1 else None

    async def attest(self, project: str, request: AttestInput) -> AttestResult:
        store = self.service.store
        documents = {d.id: d for d in store.documents(project)}
        document = documents.get(request.document_id)
        if document is None:
            raise ProjectError("document_missing", "Document must already be bound")
        adapter = self.service.core.registry.get(request.adapter_id)
        if (
            adapter.registration.application != document.application
            or adapter.registration.project_id != document.application_project_id
        ):
            raise ProjectError(
                "document_mismatch", "Adapter does not represent the bound document"
            )
        evidence = await self.observe(adapter)
        baseline = self.baseline(project, document.id)
        if request.mode == "capture":
            if baseline and (
                evidence.host_session_id != baseline.host_session_id
                or evidence.document_session_id != baseline.document_session_id
            ):
                raise ProjectError(
                    "reattachment_required",
                    "A replacement host/document requires explicit strong reattachment",
                )
            if adapter.instance_id != document.adapter_id and baseline is None:
                raise ProjectError(
                    "baseline_missing",
                    "A stale un-attested binding requires "
                    "independently proven bootstrap",
                )
            # A capture observes content; it cannot restore previously stale acceptance.
            connections = {
                document.id: baseline.context_id if baseline else adapter.connection_id
            }
            with store.transaction() as db:
                gates = store._gates(
                    db,
                    project,
                    connections,
                    set(self.service.core.artifacts.available_ids),
                )
                if any(
                    m.status == "accepted" and gates.acceptance[m.id]
                    for m in gates.milestones.values()
                ):
                    raise ProjectError(
                        "baseline_untrusted",
                        "Capture cannot refresh invalidated acceptance",
                    )
            if baseline and (
                evidence.digest != baseline.digest or evidence.format != baseline.format
            ):
                with store.transaction() as db:
                    accepted = db.execute(
                        (
                            "SELECT 1 FROM records WHERE project_id=? AND "
                            "kind='milestone' AND "
                            "json_extract(data,'$.status')='accepted'"
                        ),
                        (project,),
                    ).fetchone()
                if accepted:
                    raise ProjectError(
                        "content_changed",
                        (
                            "Reopen and validate changed content before replacing an "
                            "accepted baseline"
                        ),
                    )
        elif request.mode == "reattach":
            if (
                baseline is None
                or evidence.digest != baseline.digest
                or evidence.format != baseline.format
            ):
                raise ProjectError(
                    "content_mismatch",
                    "Reattachment requires matching durable strong evidence",
                )
            resolved = await self.resolve(project, document)
            if resolved and resolved[0].instance_id != adapter.instance_id:
                raise ProjectError(
                    "attachment_conflict",
                    "The attested document is still attached to another live host",
                )
        else:
            if (
                baseline is not None
                or request.proof_adapter_id is None
                or not request.trusted_artifact_sha256
                or not request.provenance.strip()
            ):
                raise ProjectError(
                    "bootstrap_proof_required",
                    (
                        "Bootstrap requires a missing baseline and independently "
                        "loaded trusted artifact evidence"
                    ),
                )
            proof_adapter = self.service.core.registry.get(request.proof_adapter_id)
            proof = await self.observe(proof_adapter)
            if (
                proof_adapter.registration.application != document.application
                or proof.host_session_id == evidence.host_session_id
                or proof.project_id != document.application_project_id
                or proof.file_sha256 != request.trusted_artifact_sha256
                or proof.digest != evidence.digest
                or proof.format != evidence.format
            ):
                raise ProjectError(
                    "bootstrap_mismatch",
                    (
                        "Independent accepted artifact and live content are not "
                        "proven equivalent"
                    ),
                )
        if request.mode == "bootstrap":
            confirmed = await self.observe(adapter)
            if any(
                getattr(confirmed, field) != getattr(evidence, field)
                for field in (
                    "digest",
                    "format",
                    "host_session_id",
                    "document_session_id",
                    "project_id",
                )
            ):
                raise ProjectError(
                    "application_changed",
                    "Live content changed during independent equivalence verification",
                )
        assert evidence.digest is not None
        result = Baseline(
            application_project_id=document.application_project_id,
            format=evidence.format,
            digest=evidence.digest,
            context_id=baseline.context_id
            if baseline
            and baseline.digest == evidence.digest
            and baseline.format == evidence.format
            else uuid4().hex,
            host_session_id=evidence.host_session_id,
            document_session_id=evidence.document_session_id,
            provenance=request.provenance
            or "Verified content at the bound application document",
            resources=[r.model_dump(mode="json") for r in evidence.resources],
            resource_scope=evidence.resource_scope,
            artifact_sha256=request.trusted_artifact_sha256
            if request.mode == "bootstrap"
            else (baseline.artifact_sha256 if baseline else evidence.file_sha256),
        )
        with store.transaction(write=True) as db:
            store.expect(store.project(db, project), request.expected_revision)
            if (
                self.service.core.registry.get(adapter.instance_id).connection_id
                != adapter.connection_id
            ):
                raise ProjectError(
                    "application_changed", "Adapter reconnected before baseline commit"
                )
            db.execute(
                "INSERT OR REPLACE INTO document_attestations VALUES (?,?,?)",
                (project, document.id, result.model_dump_json()),
            )
            # These are verification metadata, not new authored/accepted records.
            for row in db.execute(
                "SELECT id,data FROM validation_context WHERE project_id=?", (project,)
            ).fetchall():
                context = json.loads(row["data"])
                if document.id in context and (
                    (
                        baseline is None
                        and (
                            request.mode == "bootstrap"
                            or context[document.id] == adapter.connection_id
                        )
                    )
                    or (
                        baseline is not None
                        and baseline.context_id == result.context_id
                    )
                ):
                    context[document.id] = result.context_id
                    db.execute(
                        "UPDATE validation_context SET data=? "
                        "WHERE project_id=? AND id=?",
                        (json.dumps(context, sort_keys=True), project, row["id"]),
                    )
            for row in db.execute(
                (
                    "SELECT o.id,o.data FROM observations o JOIN records r ON "
                    "r.project_id=o.project_id AND r.id=o.id WHERE o.project_id=?"
                    " AND json_extract(r.data,'$.document_id')=?"
                ),
                (project, document.id),
            ).fetchall():
                observation = json.loads(row["data"])
                observation["connection_id"] = result.context_id
                db.execute(
                    "UPDATE observations SET data=? WHERE project_id=? AND id=?",
                    (json.dumps(observation, sort_keys=True), project, row["id"]),
                )
        return AttestResult(
            document_id=document.id,
            revision=request.expected_revision,
            digest=result.digest,
            context_id=result.context_id,
            host_session_id=result.host_session_id,
            document_session_id=result.document_session_id,
            adapter_id=adapter.instance_id,
        )
