"Bind core semantics to portable advertised application identity inspection."

import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError
from tyvrana_protocol import (
    AdapterRegistration,
    JsonValue,
    ResourceInspectionRequest,
    ResourceInspectionResult,
    ResourceReference,
)

from .catalog import DECLARATIONS
from .models import (
    ApplicationStatus,
    ApplyInput,
    BindingObservation,
    ContinueInput,
    CreateInput,
    DeltaInput,
    Document,
    ProjectPatch,
    RemoveInput,
    RemoveResult,
    SearchInput,
    SearchResult,
    VerifyInput,
)
from .store import PACKET_BYTES, ProjectError, ProjectStore

if TYPE_CHECKING:
    from ..server import AdapterServer


class ProjectService:
    def __init__(self, core: "AdapterServer", path: Path) -> None:
        self.core = core
        self.store = ProjectStore(path)

    def environment(
        self, project_id: str
    ) -> tuple[dict[str, str], list[ApplicationStatus]]:
        connections: dict[str, str] = {}
        statuses = []
        for document in self.store.documents(project_id):
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

    def before_mutation(self, registration: AdapterRegistration) -> None:
        connections: dict[str, str] = {}
        if registration.project_id:
            project = self.store.document_project(
                registration.application, registration.project_id
            )
            if project:
                connections, _ = self.environment(project.id)
        self.store.prepare_mutation(
            registration.application,
            registration.project_id,
            registration.instance_id,
            connections,
            set(self.core.artifacts.available_ids),
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
                    RemoveInput,
                ),
            )
            connected = {
                (a.registration.application, a.registration.project_id or "")
                for a in self.core.registry.list()
            }
            project_id = self.store.select(request.project_id, connected)
            connections, applications = self.environment(project_id)
            if isinstance(request, RemoveInput):
                self.store.remove(
                    project_id, request.expected_revision, request.confirm_project_id
                )
                return RemoveResult(removed_project_id=project_id)
            if isinstance(request, VerifyInput):
                return await self._verify(project_id, request)
            if isinstance(request, ApplyInput):
                # Include documents being established in this same coherent batch.
                for record in request.upsert:
                    if isinstance(record, Document):
                        connections.pop(record.id, None)
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
            packet = packet.model_copy(
                update={"applications": applications[:8], "notices": notices}
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
                and a.instance_id == document.adapter_id
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
            for binding in selected:
                obs = by_key[(binding.resource_kind, binding.resource_id)]
                observations[binding.id] = BindingObservation(
                    state="verified" if obs.state == "present" else obs.state,
                    name=obs.name,
                    fingerprint=obs.fingerprint,
                    fingerprint_scope=observed.fingerprint_scope,
                    connection_id=adapter.connection_id,
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
            connections=self.environment(project_id)[0],
            observations=observations,
            artifacts=set(self.core.artifacts.available_ids),
        )
