"Bind core semantics to portable advertised application identity inspection."

import asyncio
import sqlite3
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError
from tyvrana_protocol import (
    AdapterRegistration,
    DocumentAttestationJob,
    JsonValue,
    ResourceInspectionRequest,
    ResourceInspectionResult,
    ResourceReference,
)

from .catalog import DECLARATIONS
from .continuity import OBSERVATION_OWNER, AttestInput, AttestStatusInput, Continuity
from .models import (
    ApplicationStatus,
    ApplyInput,
    AttestationHandle,
    BindingObservation,
    ContinueInput,
    CreateInput,
    DeltaInput,
    Document,
    MutationStatusInput,
    ProjectOperationStatusInput,
    ProjectPatch,
    RemoveInput,
    RemoveResult,
    SavedInspectionInput,
    SearchInput,
    SearchResult,
    VerifyInput,
)
from .mutations import WorkingMutations
from .operations import ProjectOperations
from .proofs import ProofHosts
from .reconcile import Reconciliation
from .reconcile_models import ReconcileInput, ReconcileStatusInput
from .restore import TrustedRestore
from .restore_models import RestoreInput, RestoreStatusInput
from .store import PACKET_BYTES, ProjectError, ProjectStore

if TYPE_CHECKING:
    from ..registry import AdapterInfo
    from ..server import AdapterServer


class ProjectService:
    def __init__(self, core: "AdapterServer", path: Path) -> None:
        self.core = core
        self.store = ProjectStore(path)
        self.proofs = ProofHosts(self)
        self.continuity = Continuity(self)
        self.operations = ProjectOperations(self)
        self.attachments: dict[tuple[str, str], str] = {}
        self.mutations = WorkingMutations(self)
        self.reconciliation = Reconciliation(self)
        self.restores = TrustedRestore(self)

    async def environment(
        self,
        project_id: str,
        *,
        progress: Callable[["AdapterInfo", DocumentAttestationJob, str], None]
        | None = None,
        diagnostics: bool = False,
    ) -> tuple[dict[str, str], list[ApplicationStatus]]:
        connections: dict[str, str] = {}
        statuses = []
        recovering = self.reconciliation.pending_documents(project_id)
        recovering.update(
            j.document_id
            for j in self.continuity.continuation(project_id)
            if j.state == "running" and j.document_id is not None
        )
        recovering.update(
            m.document_id
            for m in await self.mutations.continuation(project_id)
            if m.state == "pending"
            and m.document_id is not None
            and self.mutations.tasks.get(m.mutation_id, (None, None))[1]
            is not asyncio.current_task()
        )
        if any(
            o.state == "pending" and o.operation in {"project.apply", "project.verify"}
            for o in self.operations.continuation(project_id)
        ):
            recovering.update(d.id for d in self.store.documents(project_id))
        for document in self.store.documents(project_id):
            failure = None
            try:
                resolved = (
                    None
                    if document.id in recovering
                    else await self.continuity.resolve(
                        project_id, document, progress=progress
                    )
                )
            except ProjectError as exc:
                if not diagnostics:
                    raise
                resolved, failure = None, exc
            baseline = self.continuity.baseline(project_id, document.id)
            if baseline is not None:
                self.attachments.pop((project_id, document.id), None)
                if resolved:
                    adapter, context = resolved
                    self.attachments[project_id, document.id] = adapter.instance_id
                    connections[document.id] = context
                statuses.append(
                    ApplicationStatus(
                        document_id=document.id,
                        application=document.application,
                        application_project_id=document.application_project_id,
                        adapter_ids=[resolved[0].instance_id] if resolved else [],
                        state="connected" if resolved else "unavailable",
                        locator=document.locator,
                        trust="current"
                        if resolved
                        else "pending"
                        if document.id in recovering
                        else "reattachment_required"
                        if failure and failure.code == "reattachment_required"
                        else "diverged"
                        if failure and failure.code == "content_diverged"
                        else "unverified",
                        error_code=failure.code if failure else None,
                        error_details=failure.details if failure else None,
                        next_action="continue"
                        if resolved
                        else "observe_status"
                        if document.id in recovering
                        else "project.attest(mode=reattach)"
                        if failure and failure.code == "reattachment_required"
                        else str(failure.details["next_action"])
                        if failure
                        and isinstance(failure.details, dict)
                        and "next_action" in failure.details
                        else "inspect_document",
                        committed_digest=baseline.digest,
                        saved_artifact_sha256=baseline.artifact_sha256,
                    )
                )
                continue
            matches = [
                a
                for a in self.core.registry.list()
                if a.registration.application == document.application
                and a.registration.project_id == document.application_project_id
                and a.instance_id == document.adapter_id
            ]
            if len(matches) == 1:
                connections[document.id] = matches[0].connection_id
            statuses.append(
                ApplicationStatus(
                    document_id=document.id,
                    application=document.application,
                    application_project_id=document.application_project_id,
                    adapter_ids=[m.instance_id for m in matches[:8]],
                    state="connected" if matches else "unavailable",
                    locator=None,
                )
            )
        return connections, statuses

    async def before_mutation(self, registration: AdapterRegistration) -> None:
        connections: dict[str, str] = {}
        project = None
        if registration.project_id:
            project = self.store.document_project(
                registration.application, registration.project_id
            )
            if project:
                connections, _ = await self.environment(project.id)
        self.store.prepare_mutation(
            registration.application,
            registration.project_id,
            registration.instance_id,
            connections,
            set(self.core.artifacts.available_ids),
            attached_documents={
                doc: adapter
                for (key, doc), adapter in self.attachments.items()
                if project and key == project.id
            },
        )

    async def execute(self, operation: str, arguments: JsonValue) -> BaseModel:
        declaration = DECLARATIONS.get(operation)
        if declaration is None:
            raise ProjectError(
                "operation_unsupported",
                (
                    "Discover core operations with "
                    "tyvrana_list_operations(adapter_id='core')"
                ),
            )
        request = declaration[0].model_validate(arguments)
        try:
            if isinstance(request, CreateInput):
                return self.store.create(request)
            if isinstance(request, SearchInput) and request.kind == "project":
                projects = self.store.list_projects()
                if request.query:
                    terms = request.query.casefold().split()
                    projects = [
                        p
                        for p in projects
                        if all(t in (p.title + " " + p.goal).casefold() for t in terms)
                    ]
                if request.ids:
                    projects = [p for p in projects if p.id in request.ids]
                end = request.offset + request.limit
                return SearchResult(
                    project_id=None,
                    revision=None,
                    projects=projects[request.offset : end],
                    matched_count=len(projects),
                    next_offset=end if end < len(projects) else None,
                )
            assert isinstance(
                request,
                (
                    ApplyInput,
                    ContinueInput,
                    DeltaInput,
                    SearchInput,
                    VerifyInput,
                    MutationStatusInput,
                    RemoveInput,
                    AttestInput,
                    AttestStatusInput,
                    RestoreInput,
                    RestoreStatusInput,
                    ReconcileInput,
                    ReconcileStatusInput,
                    ProjectOperationStatusInput,
                    SavedInspectionInput,
                ),
            )
            connected = {
                (a.registration.application, a.registration.project_id or "")
                for a in self.core.registry.list()
            }
            project_id = self.store.select(request.project_id, connected)
            if isinstance(request, ProjectOperationStatusInput):
                return await self.operations.observe_status(
                    project_id, request.operation_id, request.wait_seconds
                )
            if (
                isinstance(
                    request,
                    (ApplyInput, ContinueInput, VerifyInput, SavedInspectionInput),
                )
                and OBSERVATION_OWNER.get() is None
            ):
                return await self.operations.start(project_id, operation, request)
            if isinstance(request, SavedInspectionInput):
                from .saved_inspection import inspect_saved

                return await inspect_saved(self, project_id, request)
            if isinstance(request, MutationStatusInput):
                return await self.mutations.status(project_id, request)
            if operation.endswith("_cancel"):
                owner: Continuity | TrustedRestore | Reconciliation
                if isinstance(request, AttestStatusInput):
                    owner, key = self.continuity, request.attestation_id
                elif isinstance(request, RestoreStatusInput):
                    owner, key = self.restores, request.restore_id
                else:
                    assert isinstance(request, ReconcileStatusInput)
                    owner, key = self.reconciliation, request.reconciliation_id
                owner.status(project_id, key)
                task = owner.tasks.get(key)
                if task:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                return owner.status(project_id, key)
            if isinstance(request, AttestStatusInput):
                return await self.continuity.observe_status(
                    project_id, request.attestation_id, request.wait_seconds
                )
            if isinstance(request, RestoreInput):
                return await self.restores.start(project_id, request)
            if isinstance(request, RestoreStatusInput):
                return self.restores.status(project_id, request.restore_id)
            if isinstance(request, ReconcileInput):
                return await self.reconciliation.start(project_id, request)
            if isinstance(request, ReconcileStatusInput):
                return await self.reconciliation.observe_status(
                    project_id, request.reconciliation_id, request.wait_seconds
                )
            if isinstance(request, AttestInput):
                return await self.continuity.start(project_id, request)
            connections, applications = await self.environment(
                project_id, diagnostics=True
            )
            if isinstance(request, RemoveInput):
                self.store.remove(
                    project_id, request.expected_revision, request.confirm_project_id
                )
                return RemoveResult(removed_project_id=project_id)
            if isinstance(request, VerifyInput):
                return await self._verify(project_id, request)
            if isinstance(request, ApplyInput):
                # Include documents being established in this same coherent batch.
                existing = {d.id: d for d in self.store.documents(project_id)}
                for record in request.upsert:
                    if isinstance(record, Document):
                        previous = existing.get(record.id)
                        if previous is not None and (
                            previous.application,
                            previous.application_project_id,
                            previous.adapter_id,
                        ) == (
                            record.application,
                            record.application_project_id,
                            record.adapter_id,
                        ):
                            # Metadata does not replace the freshly resolved strong
                            # context, including an unavailable document's absence.
                            continue
                        connections.pop(record.id, None)
                        if self.continuity.baseline(project_id, record.id) is not None:
                            resolved = await self.continuity.resolve(project_id, record)
                            if (
                                resolved
                                and resolved[0].instance_id == record.adapter_id
                            ):
                                connections[record.id] = resolved[1]
                            continue
                        matches = [
                            a
                            for a in self.core.registry.list()
                            if a.registration.application == record.application
                            and a.instance_id == record.adapter_id
                            and a.registration.project_id
                            == record.application_project_id
                        ]
                        if len(matches) == 1:
                            connections[record.id] = matches[0].connection_id
                if request.checkpoint or any(
                    getattr(r, "kind", None) == "milestone"
                    and getattr(r, "status", None) == "accepted"
                    for r in request.upsert
                ):
                    documents = {d.id: d for d in self.store.documents(project_id)}
                    documents.update(
                        {r.id: r for r in request.upsert if isinstance(r, Document)}
                    )
                    for document in documents.values():
                        candidates = [
                            a
                            for a in self.core.registry.list()
                            if a.instance_id
                            == self.attachments.get(
                                (project_id, document.id), document.adapter_id
                            )
                        ]
                        if candidates and any(
                            "document_attestation" in c.tags
                            for c in candidates[0].registration.operations
                        ):
                            if (
                                self.continuity.baseline(project_id, document.id)
                                is None
                            ):
                                await self.continuity.attest(
                                    project_id,
                                    AttestInput(
                                        project_id=project_id,
                                        document_id=document.id,
                                        adapter_id=candidates[0].instance_id,
                                        expected_revision=request.expected_revision,
                                    ),
                                )
                                connections, applications = await self.environment(
                                    project_id
                                )
                            if document.id not in connections:
                                status = next(
                                    (
                                        a
                                        for a in applications
                                        if a.document_id == document.id
                                    ),
                                    None,
                                )
                                raise ProjectError(
                                    status.error_code
                                    if status and status.error_code
                                    else "content_diverged",
                                    "Checkpoint requires current trust; complete "
                                    "reattachment for matching reopened content",
                                    document_id=document.id,
                                    next_action=status.next_action
                                    if status
                                    else "project.continue",
                                )

                return self.store.apply(
                    project_id,
                    request,
                    connections,
                    artifacts=set(self.core.artifacts.available_ids),
                )
            artifacts = set(self.core.artifacts.available_ids)
            if isinstance(request, SearchInput):
                return self.store.search(project_id, request, connections, artifacts)
            if isinstance(request, DeltaInput):
                return self.store.delta(project_id, request, connections, artifacts)
            packet = self.store.continuation(
                project_id, request.since_revision, connections, artifacts
            )
            notices = list(packet.notices)
            if len(applications) > 8:
                notices.append(
                    f"{len(applications) - 8} document statuses omitted; "
                    "search document records."
                )
            if any(a.state != "connected" for a in applications):
                notices.append(
                    "Unavailable bound documents require inspection before "
                    "relying on their bindings/validation."
                )
            reconciliations = self.reconciliation.continuation(project_id)
            if any(r.state in {"running", "pending"} for r in reconciliations):
                notices.append(
                    "Reconciliation is nonterminal. Do not replay native mutations or "
                    "save. Observe its status with the supplied identity and bounded "
                    "wait; Core owns attestation polling."
                )
            packet = packet.model_copy(
                update={
                    "applications": applications[:8],
                    "notices": notices,
                    "reconciliations": reconciliations,
                    "mutations": await self.mutations.continuation(project_id),
                    "attestations": [
                        AttestationHandle.model_validate(
                            j.model_dump(include=set(AttestationHandle.model_fields))
                        )
                        for j in self.continuity.continuation(project_id)
                    ],
                    "operations": self.operations.continuation(project_id),
                }
            )
            while True:
                selected: Counter[str] = Counter(r.record.kind for r in packet.records)
                packet = packet.model_copy(
                    update={
                        "omitted_counts": {
                            kind: count - selected[kind]
                            for kind, count in packet.counts.items()
                            if count > selected[kind]
                        }
                    }
                )
                if len(packet.model_dump_json().encode()) <= PACKET_BYTES:
                    return packet
                if packet.records:
                    packet.records.pop()
                elif packet.applications:
                    packet.applications.pop()
                elif packet.mutations:
                    packet.mutations.pop()
                elif packet.attestations:
                    packet.attestations.pop()
                elif packet.operations:
                    packet.operations.pop()
                elif packet.recent_delta is not None:
                    packet = packet.model_copy(update={"recent_delta": None})
                else:
                    raise ProjectError(
                        "packet_limit", "Retrieve details with bounded project.search"
                    )
        except (sqlite3.DatabaseError, ValidationError) as exc:
            raise ProjectError(
                "project_storage_error",
                (
                    "Local semantic storage failed; no partial semantic batch was "
                    "committed. Check storage integrity/permissions and retry."
                ),
            ) from exc

    async def _verify(self, project_id: str, request: VerifyInput) -> BaseModel:
        if len(set(request.binding_ids)) != len(request.binding_ids):
            raise ProjectError("duplicate_binding", "binding_ids must be unique")
        with self.store.transaction() as db:
            self.store.expect(
                self.store.project(db, project_id), request.expected_revision
            )
        bindings = self.store.bindings(project_id, request.binding_ids)
        documents = {d.id: d for d in self.store.documents(project_id)}
        groups = defaultdict(list)
        for binding in bindings:
            groups[binding.document_id].append(binding)
        observations: dict[str, BindingObservation] = {}
        checked: dict[str, str] = {}
        refreshed_documents: list[Document] = []
        for document_id, selected in groups.items():
            document = documents[document_id]
            matches = [
                a
                for a in self.core.registry.list()
                if a.registration.application == document.application
                and a.registration.project_id == document.application_project_id
                and a.instance_id
                == self.attachments.get((project_id, document.id), document.adapter_id)
                and (request.adapter_id is None or a.instance_id == request.adapter_id)
            ]
            if len(matches) != 1:
                raise ProjectError(
                    "application_unavailable",
                    (
                        "Connect the pinned adapter/document, or deliberately rebind "
                        "the document before verification"
                    ),
                    document_id=document_id,
                    adapter_ids=[a.instance_id for a in matches[:8]],
                )
            adapter = matches[0]
            operation = adapter.registration.resource_inspection
            if not operation:
                raise ProjectError(
                    "inspection_unsupported",
                    "Adapter does not advertise portable resource inspection",
                    adapter_id=adapter.instance_id,
                )
            refs = list(
                {
                    (b.resource_kind, b.resource_id): ResourceReference(
                        resource_kind=b.resource_kind, resource_id=b.resource_id
                    )
                    for b in selected
                }.values()
            )
            expected = ResourceInspectionRequest(
                project_id=document.application_project_id, resources=refs
            )
            response = await self.core.dispatcher.execute(
                adapter_id=adapter.instance_id,
                operation=operation,
                arguments=expected.model_dump(mode="json"),
            )
            try:
                observed = ResourceInspectionResult.model_validate(response.result)
            except ValidationError as exc:
                raise ProjectError(
                    "invalid_inspection",
                    "Adapter returned invalid resource observations",
                ) from exc
            by_key = {(r.resource_kind, r.resource_id): r for r in observed.resources}
            if (
                observed.project_id != document.application_project_id
                or set(by_key) != {(r.resource_kind, r.resource_id) for r in refs}
                or len(by_key) != len(observed.resources)
            ):
                raise ProjectError(
                    "invalid_inspection",
                    (
                        "Adapter inspection did not return exactly the requested "
                        "identities"
                    ),
                )
            current = self.core.registry.get(adapter.instance_id)
            if (
                current.connection_id != adapter.connection_id
                or current.registration.project_id != document.application_project_id
            ):
                raise ProjectError(
                    "application_changed",
                    (
                        "Application reconnected/switched during verification; inspect "
                        "and retry"
                    ),
                )
            checked[adapter.instance_id] = adapter.connection_id
            if document.locator != adapter.registration.project_path:
                refreshed_documents.append(
                    document.model_copy(
                        update={"locator": adapter.registration.project_path}
                    )
                )
            # Every resource in this document shares the same continuity context.
            # Refresh it once for the batch, not once per resource (which can
            # otherwise rehash the entire document for each binding).
            connections, _ = await self.environment(project_id)
            for binding in selected:
                obs = by_key[(binding.resource_kind, binding.resource_id)]
                observations[binding.id] = BindingObservation(
                    state="verified" if obs.state == "present" else obs.state,
                    name=obs.name,
                    fingerprint=obs.fingerprint,
                    fingerprint_scope=observed.fingerprint_scope,
                    connection_id=connections.get(document.id, adapter.connection_id),
                )
        for adapter_id, session in checked.items():
            if self.core.registry.get(adapter_id).connection_id != session:
                raise ProjectError(
                    "application_changed",
                    "Application reconnected during verification; retry",
                )
        # Empty field patch carries no semantic edit; observations commit one revision.
        return self.store.apply(
            project_id,
            ApplyInput(
                expected_revision=request.expected_revision,
                project=ProjectPatch(),
                upsert=list(refreshed_documents),
            ),
            connections=(await self.environment(project_id))[0],
            observations=observations,
            artifacts=set(self.core.artifacts.available_ids),
        )
