"""Bounded semantic records and lazy project operation contracts."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

type Key = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
]
type Label = Annotated[str, Field(min_length=1, max_length=160, pattern=r"\S")]
type Summary = Annotated[str, Field(max_length=800)]
type Stage = Annotated[str, Field(max_length=120)]
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
    stage: Stage = ""
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
    status: Literal["planned", "in_progress", "accepted", "failed", "deferred"] = (
        "planned"
    )
    entity_ids: Keys = Field(default_factory=list)
    validation_ids: Keys = Field(default_factory=list)
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
    stage: Stage = ""


class ProjectInput(Model):
    project_id: Key | None = Field(
        default=None,
        description="Omit only when one connected/stored project is unambiguous.",
    )


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
    stage: Stage | None = None
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
    adapter_id: Key | None = None


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
    record: SemanticRecord
    changed_revision: int
    binding: BindingObservation | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    freshness: Freshness | None = Field(default=None, exclude_if=lambda v: v is None)
    evidence_availability: Literal["available", "expired", "unverified"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


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


class ApplicationStatus(Model):
    document_id: str
    application: str
    application_project_id: str
    adapter_ids: list[str]
    state: Literal["connected", "unavailable", "ambiguous"]
    locator: str | None


class Continuation(Model):
    project: Project
    checkpoint: Checkpoint | None
    records: list[RecordView]
    counts: dict[str, int]
    omitted_counts: dict[str, int]
    applications: list[ApplicationStatus]
    recent_delta: Delta | None
    notices: list[str]


class RemoveResult(Model):
    removed_project_id: str
    application_files_affected: Literal[False] = False


class ProjectRevision(Model):
    project_id: str
    revision: int
