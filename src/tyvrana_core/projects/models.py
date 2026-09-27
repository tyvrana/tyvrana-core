"""Bounded semantic records and lazy project operation contracts."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from tyvrana_protocol import JsonValue

type Key = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
]
type Label = Annotated[str, Field(min_length=1, max_length=160, pattern=r"\S")]
type Summary = Annotated[str, Field(max_length=800)]
type Stage = Annotated[
    str,
    Field(
        max_length=128, description="Active milestone ID; empty pauses managed work."
    ),
]
type Keys = Annotated[list[Key], Field(max_length=64)]
type RecordKind = Literal[
    "entity",
    "relationship",
    "milestone",
    "issue",
    "validation",
    "document",
    "binding",
    "evidence",
]
type Relation = Literal[
    "contains",
    "depends_on",
    "derived_from",
    "attached_to",
    "deformed_by",
    "references",
    "maps_to",
]
type Freshness = Literal["current", "stale", "unverified"]
type BindingState = Literal[
    "verified", "unverified", "stale", "missing", "ambiguous", "unsupported"
]


class Model(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, allow_inf_nan=False
    )


class Record(Model):
    id: Key
    label: Label
    summary: Summary = ""
    importance: int = Field(default=0, ge=0, le=5)
    tags: list[Annotated[str, Field(min_length=1, max_length=40)]] = Field(
        default_factory=list, max_length=8
    )


class Entity(Record):
    kind: Literal["entity"] = "entity"
    entity_type: Literal[
        "asset", "system", "component", "reference", "output", "runtime", "other"
    ] = "asset"


class Relationship(Record):
    kind: Literal["relationship"] = "relationship"
    source_id: Key
    target_id: Key
    relation: Relation


class Milestone(Record):
    kind: Literal["milestone"] = "milestone"
    status: Literal[
        "planned", "in_progress", "accepted", "failed", "deferred", "invalidated"
    ] = "planned"
    entity_ids: Keys = Field(
        default_factory=list, description="Affected outputs: the active mutation scope."
    )
    document_ids: Keys = Field(default_factory=list)
    prerequisite_ids: Keys = Field(default_factory=list)
    validation_ids: Keys = Field(
        default_factory=list, description="Required passed, current, evidenced checks."
    )
    acceptance: Summary = ""


class Issue(Record):
    kind: Literal["issue"] = "issue"
    status: Literal["open", "resolved", "deferred"] = "open"
    severity: Literal["critical", "major", "minor", "info"] = "major"
    entity_ids: Keys = Field(default_factory=list)
    next_action: Summary = ""


class Validation(Record):
    kind: Literal["validation"] = "validation"
    status: Literal["passed", "failed", "warning", "unknown"] = "unknown"
    validation_type: Label
    entity_ids: Keys = Field(default_factory=list)
    evidence_ids: Keys = Field(default_factory=list)
    freshness: Freshness = "unverified"


class Document(Record):
    kind: Literal["document"] = "document"
    application: Key
    application_project_id: Key
    adapter_id: Key = Field(
        description="Explicit intended adapter instance; rebind deliberately."
    )
    locator: Annotated[str, Field(max_length=4096)] | None = None


class Binding(Record):
    kind: Literal["binding"] = "binding"
    entity_id: Key
    document_id: Key
    resource_kind: Key
    resource_id: Key


class Evidence(Record):
    kind: Literal["evidence"] = "evidence"
    storage: Literal["ephemeral_artifact", "external", "application"]
    artifact_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")] | None = None
    uri: (
        Annotated[str, Field(max_length=2048, pattern=r"^[a-zA-Z][a-zA-Z0-9+.-]*:")]
        | None
    ) = None
    binding_id: Key | None = None
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None = None

    @model_validator(mode="after")
    def source(self) -> Self:
        fields = {
            "ephemeral_artifact": self.artifact_id,
            "external": self.uri,
            "application": self.binding_id,
        }
        if (
            fields[self.storage] is None
            or sum(v is not None for v in fields.values()) != 1
        ):
            raise ValueError("Provide exactly the reference corresponding to storage")
        return self


type SemanticRecord = Annotated[
    Entity
    | Relationship
    | Milestone
    | Issue
    | Validation
    | Document
    | Binding
    | Evidence,
    Field(discriminator="kind"),
]
RECORD: TypeAdapter[SemanticRecord] = TypeAdapter(SemanticRecord)


class Project(Model):
    id: Key
    title: Label
    goal: Summary
    stage: Stage = ""
    next_action: Summary = ""
    revision: int = Field(ge=0)
    created_at: str
    updated_at: str
    history_floor: int = 0
    working_base_checkpoint: Key | None = None
    working_base_revision: int | None = None
    checkout_revision: int | None = None


class ProjectPatch(Model):
    title: Label | None = None
    goal: Summary | None = None
    stage: Stage | None = None
    next_action: Summary | None = None


class CheckpointInput(Model):
    id: Key
    label: Label
    purpose: Summary = ""


class Checkpoint(CheckpointInput):
    scope: Literal["historical"] = Field(
        default="historical",
        description="Counts describe this checkpoint revision, not current live trust.",
    )
    document_states: dict[str, dict[str, str]] = Field(default_factory=dict)
    revision: int
    stage: Stage
    created_at: str
    accepted_milestones: list[Key]
    accepted_count: int
    documents: list[Key]
    document_count: int
    validation_counts: dict[str, int]


class CreateInput(Model):
    title: Label
    goal: Summary


class ProjectInput(Model):
    project_id: Key | None = Field(
        default=None,
        description="Omit only when one connected/stored project is unambiguous.",
    )


class MutationStatusInput(ProjectInput):
    mutation_id: Key
    wait_seconds: float = Field(default=0, ge=0, le=20, allow_inf_nan=False)


class AttestationObservation(Model):
    adapter_id: Key
    job_id: Key
    operation: str
    state: Literal["queued", "running", "completed", "failed", "cancelled"]

    digest: str | None = None


class MutationStatus(Model):
    project_id: Key
    mutation_id: Key
    state: Literal["pending", "completed", "uncommitted", "interrupted"]
    document_id: Key | None = None
    operation: str | None = None
    arguments: dict[str, JsonValue] | None = None
    revision: int | None = None
    error_code: str | None = None
    error_message: Annotated[str, Field(max_length=1024)] | None = None
    error_details: JsonValue = None
    native_execution: Literal["not_started", "started", "completed", "unknown"] = (
        "unknown"
    )
    before_digest: str | None = None
    after_digest: str | None = None
    stage_id: str | None = None
    reconciled_by: Key | None = None
    post_state_attested: bool = False
    persisted_artifact_sha256: str | None = None
    replay_safe: bool = False
    recovery_operation: Literal["project.reconcile"] | None = None
    recovery_proof: Literal["receipt", "inverse_delta"] | None = None
    admission_attestation: AttestationObservation | None = None
    status_operation: Literal["project.mutation_status"] = "project.mutation_status"
    wait_seconds: float = 20.0
    next_action: Literal["observe_status", "inspect_result", "inspect_failure"] = (
        "observe_status"
    )

    @model_validator(mode="after")
    def lifecycle(self) -> Self:
        object.__setattr__(
            self,
            "next_action",
            "observe_status"
            if self.state == "pending"
            else "inspect_result"
            if self.state == "completed"
            else "inspect_failure",
        )
        return self


class ApplyInput(ProjectInput):
    expected_revision: int = Field(ge=0)
    project: ProjectPatch | None = None
    upsert: list[SemanticRecord] = Field(default_factory=list, max_length=256)
    remove: Keys = Field(default_factory=list)
    checkpoint: CheckpointInput | None = None
    forget_checkpoints: Keys = Field(
        default_factory=list,
        description="Remove unused named markers; application files remain unchanged.",
    )

    @model_validator(mode="after")
    def coherent(self) -> Self:
        ids = [r.id for r in self.upsert] + self.remove
        if len(set(ids)) != len(ids):
            raise ValueError("Each record ID may occur only once in a batch")
        if len(set(self.forget_checkpoints)) != len(self.forget_checkpoints):
            raise ValueError("Checkpoint IDs must be unique")
        if self.checkpoint and self.checkpoint.id in self.forget_checkpoints:
            raise ValueError("Cannot replace a named checkpoint")
        if not (ids or self.project or self.checkpoint or self.forget_checkpoints):
            raise ValueError("An update needs records, project fields or a checkpoint")
        if len(self.model_dump_json().encode()) > 512 * 1024:
            raise ValueError("Semantic batch exceeds 512 KiB; split meaningful batches")
        return self


class ContinueInput(ProjectInput):
    since_revision: int | None = Field(default=None, ge=0)


class SearchInput(ProjectInput):
    kind: RecordKind | Literal["checkpoint", "project"] | None = None
    ids: Keys = Field(default_factory=list)
    query: Annotated[str, Field(min_length=1, max_length=160)] | None = None
    status: Annotated[str, Field(max_length=40)] | None = None
    entity_type: Annotated[str, Field(max_length=40)] | None = None
    relation: Relation | None = None
    related_to: Key | None = None
    application: Key | None = None
    binding_state: BindingState | None = None
    tag: Annotated[str, Field(max_length=40)] | None = None
    offset: int = Field(default=0, ge=0, le=100000)
    limit: int = Field(default=20, ge=1, le=50)
    at_revision: int | None = Field(
        default=None, ge=0, description="Pin pagination; retry on revision conflict."
    )


class DeltaInput(ProjectInput):
    since_revision: int | None = Field(default=None, ge=0)
    checkpoint_id: Key | None = None
    details: bool = False
    offset: int = Field(default=0, ge=0, le=100000)
    limit: int = Field(default=20, ge=1, le=50)
    at_revision: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def origin(self) -> Self:
        if (self.since_revision is None) == (self.checkpoint_id is None):
            raise ValueError("Supply exactly one of since_revision or checkpoint_id")
        return self


class VerifyInput(ProjectInput):
    expected_revision: int = Field(ge=0)
    binding_ids: list[Key] = Field(min_length=1, max_length=64)
    adapter_id: Key | None = Field(
        default=None,
        description="Assert pinned adapter; never override the document binding.",
    )


class RemoveInput(ProjectInput):
    project_id: Key
    expected_revision: int = Field(ge=0)
    confirm_project_id: Key


class BindingObservation(Model):
    state: BindingState
    verified_revision: int | None = None
    name: str | None = None
    fingerprint: str | None = None
    fingerprint_scope: str | None = None
    connection_id: str | None = None


class RecordView(Model):
    historical_status: Literal["accepted"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    accepted_revision: int | None = Field(default=None, exclude_if=lambda v: v is None)
    record: SemanticRecord
    changed_revision: int
    binding: BindingObservation | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    freshness: Freshness | None = Field(default=None, exclude_if=lambda v: v is None)
    freshness_reason: (
        Literal["not_verified", "dependencies_changed", "document_unverified"] | None
    ) = Field(
        default=None,
        exclude_if=lambda v: v is None,
        description="Why validation freshness or binding verification is not current.",
    )
    evidence_availability: Literal["available", "expired", "unverified"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


class GateBlocker(Model):
    milestone_id: str
    record_id: str
    reason: str


class StageState(Model):
    milestone_id: str | None
    blockers: list[GateBlocker]
    blocker_count: int


class Change(Model):
    revision: int
    kind: str
    id: str
    action: Literal["created", "updated", "removed", "staled", "verified"]
    label: str
    status: str = ""


class Delta(Model):
    project_id: str
    from_revision: int
    revision: int
    history_floor: int
    counts: dict[str, int]
    matched_count: int
    changes: list[Change]
    records: list[RecordView] = Field(default_factory=list)
    next_offset: int | None


class ApplyResult(Model):
    project: Project
    counts: dict[str, int]
    changed_ids: list[str]
    changed_count: int
    changed_ids_truncated: bool
    checkpoint: Checkpoint | None = None


class SearchResult(Model):
    project_id: str | None
    revision: int | None
    records: list[RecordView] = Field(default_factory=list)
    checkpoints: list[Checkpoint] = Field(default_factory=list)
    projects: list[Project] = Field(default_factory=list)
    matched_count: int
    next_offset: int | None


class ProjectOperationStatusInput(ProjectInput):
    operation_id: Key
    wait_seconds: float = Field(default=0, ge=0, le=20, allow_inf_nan=False)


class ProjectOperationHandle(Model):
    operation_id: Key
    operation: str
    state: Literal["pending", "completed", "failed"]
    attestation: AttestationObservation | None = None
    status_operation: Literal["project.operation_status"] = "project.operation_status"
    wait_seconds: float = 20.0
    next_action: Literal["observe_status", "inspect_result", "inspect_failure"] = (
        "observe_status"
    )
    error_code: str | None = None
    error_message: str | None = None
    error_details: JsonValue = None

    @model_validator(mode="after")
    def lifecycle(self) -> Self:
        object.__setattr__(
            self,
            "next_action",
            {"completed": "inspect_result", "failed": "inspect_failure"}.get(
                self.state, "observe_status"
            ),
        )
        return self


class AttestationHandle(Model):
    attestation_id: Key
    document_id: str | None = None
    state: Literal["running", "completed", "failed"]
    attestation: AttestationObservation | None = None
    status_operation: Literal["project.attest_status"] = "project.attest_status"
    wait_seconds: float = 20.0
    next_action: str
    error_code: str | None = None


class ApplicationStatus(Model):
    document_id: str
    application: str
    application_project_id: str
    adapter_ids: list[str]
    state: Literal["connected", "unavailable"]
    locator: str | None
    trust: Literal[
        "current", "unverified", "pending", "reattachment_required", "diverged"
    ] = "unverified"
    next_action: str = "inspect_document"
    error_code: str | None = None
    committed_digest: str | None = None
    saved_artifact_sha256: str | None = None


class ReconcileResult(Model):
    reconciliation_id: str
    state: Literal["running", "pending", "completed", "failed"]
    revision: int | None = None
    digest: str | None = None
    restored_milestones: list[str] = Field(default_factory=list)
    already_reconciled: bool = False
    poll_after_seconds: float = 2.0
    mutation_id: str | None = None
    native_execution: Literal[
        "not_started", "running", "completed", "failed", "unknown"
    ] = "unknown"
    replay_safe: Literal[False] = False
    publication: Literal["uncommitted", "committed"] = "uncommitted"
    attestation: AttestationObservation | None = None
    next_action: Literal["observe_status", "save", "inspect_failure"] = "observe_status"
    status_operation: Literal["project.reconcile_status"] = "project.reconcile_status"
    wait_seconds: float = 20.0
    error_code: str | None = None
    error_message: str | None = None
    error_details: JsonValue = None

    @model_validator(mode="after")
    def lifecycle(self) -> Self:
        # These are consequences of the state, never independent instructions.
        action = {"completed": "save", "failed": "inspect_failure"}.get(
            self.state, "observe_status"
        )
        object.__setattr__(self, "next_action", action)
        object.__setattr__(
            self,
            "publication",
            "committed" if self.state == "completed" else "uncommitted",
        )
        return self


class Continuation(Model):
    project: Project
    stage_state: StageState
    checkpoint: Checkpoint | None
    records: list[RecordView]
    counts: dict[str, int]
    omitted_counts: dict[str, int]
    applications: list[ApplicationStatus]
    recent_delta: Delta | None
    notices: list[str]
    reconciliations: list[ReconcileResult] = Field(default_factory=list, max_length=8)
    mutations: list[MutationStatus] = Field(default_factory=list, max_length=8)
    attestations: list[AttestationHandle] = Field(default_factory=list, max_length=8)
    operations: list[ProjectOperationHandle] = Field(default_factory=list, max_length=8)


class RemoveResult(Model):
    removed_project_id: str
    application_files_affected: Literal[False] = False


class ProjectRevision(Model):
    project_id: str
    revision: int


class ProjectOperation(ProjectOperationHandle):
    result: ApplyResult | Continuation | None = None
