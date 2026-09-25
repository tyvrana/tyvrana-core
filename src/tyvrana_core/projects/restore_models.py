"""Explicit discard of an observed working state in favor of durable evidence."""

from typing import Literal

from pydantic import Field, field_validator, model_validator
from tyvrana_protocol import DocumentState

from .models import Key, Model, ProjectInput


class RestoreInput(ProjectInput):
    mode: Literal["content", "checkout"] = "content"
    restore_id: Key
    expected_revision: int = Field(ge=1)
    document_id: Key
    adapter_id: Key
    checkpoint_id: Key | None = None
    discard_current: Literal[True]
    expected_current: DocumentState
    provenance: str = Field(min_length=1, max_length=2048, pattern=r"\S")

    @model_validator(mode="after")
    def checkout_target(self) -> "RestoreInput":
        if self.mode == "checkout" and not self.checkpoint_id:
            raise ValueError("Checkout requires checkpoint_id")
        return self

    @field_validator("discard_current", mode="before")
    @classmethod
    def explicit_discard(cls, value: object) -> object:
        if value is not True:
            raise ValueError("Restore requires explicit discard_current: true")
        return value


class RestoreStatusInput(ProjectInput):
    restore_id: Key


class RestoreResult(Model):
    restore_id: str
    state: Literal["running", "completed", "failed"]
    revision: int | None = None
    digest: str | None = None
    restored_milestones: list[str] = Field(default_factory=list)
    already_current: bool = False
    base_revision: int | None = None
    abandoned_revision: int | None = None
    poll_after_seconds: float = 2.0
    error_code: str | None = None
    error_message: str | None = None
