"""Strong document evidence and runtime attachment, separate from semantic history."""

import asyncio
import json
import time
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from pydantic import Field, model_validator
from tyvrana_protocol import (
    DocumentAttestation,
    DocumentAttestationJob,
    DocumentAttestationResponse,
)

from ..errors import AdapterDisconnected, AdapterNotFound
from .attestation_migration import FormatMigration, claim_revisions
from .models import Binding, BindingObservation, Document, Key, Model, ProjectInput
from .store import ProjectError

if TYPE_CHECKING:
    from ..registry import AdapterInfo
    from .service import ProjectService


class AttestInput(ProjectInput):
    document_id: Key
    adapter_id: Key
    expected_revision: int = Field(ge=0)
    mode: Literal["capture", "reattach", "bootstrap", "migrate"] = "capture"
    proof_adapter_id: Key | None = None
    trusted_artifact_sha256: str | None = Field(default=None, pattern="^[0-9a-f]{64}$")
    provenance: str = Field(default="", max_length=2048)
    from_format: str | None = Field(default=None, min_length=1, max_length=256)
    to_format: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def migration_intent(self) -> "AttestInput":
        if self.mode == "migrate":
            if (
                not self.from_format
                or not self.to_format
                or self.from_format == self.to_format
                or not self.proof_adapter_id
                or not self.trusted_artifact_sha256
                or not self.provenance.strip()
            ):
                raise ValueError(
                    "Migration requires explicit distinct from_format/to_format, "
                    "independent proof adapter, trusted artifact SHA256 and provenance"
                )
        elif self.from_format is not None or self.to_format is not None:
            raise ValueError("Format migration intent requires mode='migrate'")
        return self


class AttestResult(Model):
    document_id: str
    revision: int
    digest: str
    context_id: str
    host_session_id: str
    document_session_id: str
    adapter_id: str
    semantic_revision_changed: bool = False
    baseline_migrated: bool = False
    already_migrated: bool = False


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
    format_migration: FormatMigration | None = None


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
        baseline = self.baseline(project, document.id)
        if request.mode == "migrate":
            async with self.service.mutations.lock:
                return await self._migrate(
                    project, request, document, adapter, baseline
                )
        evidence = await self.observe(adapter)
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
            format_migration=baseline.format_migration
            if baseline
            and baseline.digest == evidence.digest
            and baseline.format == evidence.format
            else None,
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

    async def _migrate(
        self,
        project: str,
        request: AttestInput,
        document: Document,
        adapter: "AdapterInfo",
        baseline: Baseline | None,
    ) -> AttestResult:
        store = self.service.store
        if baseline is None:
            raise ProjectError(
                "migration_baseline_missing",
                "Migration requires a baseline; use bootstrap",
            )
        if baseline.application_project_id != document.application_project_id:
            raise ProjectError(
                "migration_lineage", "Baseline belongs to another document"
            )
        if not baseline.artifact_sha256:
            raise ProjectError(
                "migration_artifact_missing",
                "Baseline lacks durable trusted file SHA256",
            )
        if request.trusted_artifact_sha256 != baseline.artifact_sha256:
            raise ProjectError(
                "migration_file_mismatch", "Trusted artifact SHA256 differs"
            )
        repeated = baseline.format == request.to_format
        if (not repeated and baseline.format != request.from_format) or (
            repeated
            and (
                baseline.format_migration is None
                or baseline.format_migration.from_format != request.from_format
                or baseline.format_migration.to_format != request.to_format
            )
        ):
            raise ProjectError(
                "migration_format", "Explicit format transition does not match"
            )
        with store.transaction() as db:
            store.expect(store.project(db, project), request.expected_revision)
        live = await self.observe(adapter)
        assert request.proof_adapter_id is not None
        proof_adapter = self.service.core.registry.get(request.proof_adapter_id)
        proof = await self.observe(proof_adapter)
        if (
            proof_adapter.registration.application != document.application
            or proof.project_id != document.application_project_id
            or live.project_id != document.application_project_id
            or proof.host_session_id == live.host_session_id
        ):
            raise ProjectError(
                "migration_lineage", "Independent document lineage required"
            )
        if (
            live.file_sha256 != baseline.artifact_sha256
            or proof.file_sha256 != baseline.artifact_sha256
        ):
            raise ProjectError(
                "migration_file_mismatch", "Current file differs from trusted artifact"
            )
        if (
            live.format != request.to_format
            or proof.format != request.to_format
            or live.digest != proof.digest
            or (repeated and live.digest != baseline.digest)
        ):
            raise ProjectError(
                "migration_digest_mismatch", "New-format content does not match"
            )
        resources = self.service.reconciliation._resources(live)
        if (
            live.resource_scope != proof.resource_scope
            or resources != self.service.reconciliation._resources(proof)
        ):
            raise ProjectError(
                "migration_resource_mismatch", "Resource evidence differs"
            )
        adapter = await self._reconfirm_migration_source(adapter, live)
        assert live.digest is not None
        with store.transaction(write=True) as db:
            store.expect(store.project(db, project), request.expected_revision)
            saved = db.execute(
                "SELECT data FROM document_attestations WHERE "
                "project_id=? AND document_id=?",
                (project, document.id),
            ).fetchone()
            if (
                saved is None
                or Baseline.model_validate_json(saved[0]) != baseline
                or store._record(db, project, document.id) != document
                or self.service.core.registry.get(adapter.instance_id).connection_id
                != adapter.connection_id
            ):
                raise ProjectError(
                    "migration_conflict", "Document or baseline changed before commit"
                )
            if repeated:
                result = baseline
            else:
                assert request.from_format is not None and request.to_format is not None
                migration = FormatMigration(
                    from_format=request.from_format,
                    to_format=request.to_format,
                    previous_digest=baseline.digest,
                    proof_host_session_id=proof.host_session_id,
                    proof_document_session_id=proof.document_session_id,
                    resource_scope=live.resource_scope or "",
                )
                for row in db.execute(
                    "SELECT * FROM records WHERE project_id=? AND kind='milestone'",
                    (project,),
                ).fetchall():
                    view = store._view(
                        db, project, row, {}, set(), evaluate_milestone=False
                    )
                    if view.historical_status != "accepted":
                        continue
                    try:
                        protected = self.service.reconciliation.accepted_claims(
                            db, project, {row["id"]}, document.id, baseline
                        )
                        observations: dict[str, BindingObservation] = {}
                        fingerprints: dict[str, str] = {}
                        for key in protected:
                            record = store._record(db, project, key)
                            if not isinstance(record, Binding):
                                continue
                            resource = resources.get(
                                (record.resource_kind, record.resource_id)
                            )
                            observed = db.execute(
                                "SELECT data FROM observations WHERE "
                                "project_id=? AND id=?",
                                (project, key),
                            ).fetchone()
                            if (
                                resource is None
                                or resource.state != "present"
                                or not resource.fingerprint
                                or observed is None
                            ):
                                raise ProjectError(
                                    "migration_claim_unverified",
                                    "Binding lacks evidence",
                                )
                            observation = BindingObservation.model_validate_json(
                                observed[0]
                            )
                            if (
                                observation.state != "verified"
                                or not observation.fingerprint
                            ):
                                raise ProjectError(
                                    "migration_claim_unverified",
                                    "Prior binding lacks evidence",
                                )
                            observations[key] = observation
                            fingerprints[key] = resource.fingerprint
                    except ProjectError as exc:
                        if not exc.code.startswith(
                            ("reconciliation_", "migration_claim_")
                        ):
                            raise
                        continue  # Migrate the format, but leave unproved claims stale.
                    migration.claims[row["id"]] = claim_revisions(
                        db, project, row["id"]
                    )
                    migration.observations.update(observations)
                    migration.resource_fingerprints.update(fingerprints)
                result = baseline.model_copy(
                    update=dict(
                        format=live.format,
                        digest=live.digest,
                        host_session_id=live.host_session_id,
                        document_session_id=live.document_session_id,
                        provenance=request.provenance,
                        resources=[r.model_dump(mode="json") for r in live.resources],
                        resource_scope=live.resource_scope,
                        format_migration=migration,
                    )
                )
                db.execute(
                    "UPDATE document_attestations SET data=? WHERE "
                    "project_id=? AND document_id=?",
                    (result.model_dump_json(), project, document.id),
                )
        return AttestResult(
            document_id=document.id,
            revision=request.expected_revision,
            digest=result.digest,
            context_id=result.context_id,
            host_session_id=result.host_session_id,
            document_session_id=result.document_session_id,
            adapter_id=adapter.instance_id,
            baseline_migrated=not repeated,
            already_migrated=repeated,
        )

    async def _reconfirm_migration_source(
        self, original: "AdapterInfo", live: DocumentAttestation
    ) -> "AdapterInfo":
        """Reconfirm the pinned live document, never the latest proof connection."""
        registry = self.service.core.registry
        try:
            candidates = [registry.get(original.instance_id)]
        except (AdapterDisconnected, AdapterNotFound):
            # Instance IDs route requests; the attested host/loaded-document pair
            # identifies the source across a transport or adapter reconnection.
            candidates = [
                candidate
                for candidate in registry.list()
                if candidate.registration.application
                == original.registration.application
                and candidate.registration.project_id == live.project_id
            ]
        matches = []
        for candidate in candidates:
            confirmed = await self.observe(candidate)
            if (
                confirmed.host_session_id == live.host_session_id
                and confirmed.document_session_id == live.document_session_id
                and confirmed.project_id == live.project_id
            ):
                matches.append((candidate, confirmed))
        if len(matches) != 1:
            raise ProjectError(
                "application_changed",
                "Pinned live host/document is missing or ambiguous "
                "during migration proof",
            )
        adapter, confirmed = matches[0]
        # Work is performance/progress telemetry, including nested elapsed times
        # and cost-based ordering. It is not document content or lineage. Complete
        # status, omissions, file identity and all authored evidence remain guarded.
        before = live.model_dump(exclude={"elapsed_ms", "work"})
        after = confirmed.model_dump(exclude={"elapsed_ms", "work"})
        changed = [key for key in before if before[key] != after[key]]
        if changed:
            raise ProjectError(
                "application_changed",
                "Live content changed during migration proof",
                changed_fields=changed,
            )
        return adapter
