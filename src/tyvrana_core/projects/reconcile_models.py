"""Explicit, bounded recovery of a known missing working mutation receipt."""

from typing import Annotated, Literal

from pydantic import Field, model_validator
from tyvrana_protocol import JsonValue, QualifiedName

from .models import Key, Model, ProjectInput


class ReplayStep(Model):
    operation: QualifiedName
    arguments: JsonValue
    owner_entity_id: Key


class ReconcileInput(ProjectInput):
    reconciliation_id: Key
    expected_revision: int = Field(ge=1)
    document_id: Key
    adapter_id: Key
    stage_id: Key
    prior_digest: Annotated[str, Field(pattern="^[0-9a-f]{64}$")]
    expected_digest: Annotated[str, Field(pattern="^[0-9a-f]{64}$")]
    provenance: str = Field(min_length=1, max_length=2048, pattern=r"\S")
    delta: list[ReplayStep] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def independent(self) -> "ReconcileInput":
        if self.prior_digest == self.expected_digest:
            raise ValueError("Recovery requires a declared content transition")
        return self


class ReconcileStatusInput(ProjectInput):
    reconciliation_id: Key


class ReconcileResult(Model):
    reconciliation_id: str
    state: Literal["running", "completed", "failed"]
    revision: int | None = None
    digest: str | None = None
    restored_milestones: list[str] = Field(default_factory=list)
    already_reconciled: bool = False
    poll_after_seconds: float = 2.0
    error_code: str | None = None
    error_message: str | None = None
