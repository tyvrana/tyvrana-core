"Lazy semantic operation contracts, sharing normal discovery/execution tools."

import hashlib
import json
from typing import Literal

from pydantic import BaseModel
from tyvrana_protocol import OperationContract

from .models import (
    ApplyInput,
    ApplyResult,
    Continuation,
    ContinueInput,
    CreateInput,
    Delta,
    DeltaInput,
    Project,
    RemoveInput,
    RemoveResult,
    SearchInput,
    SearchResult,
    VerifyInput,
)

DECLARATIONS: dict[
    str, tuple[type[BaseModel], type[BaseModel], Literal["read_only", "mutating"], str]
] = {
    "project.create": (
        CreateInput,
        Project,
        "mutating",
        (
            "Create local durable semantic project meaning, with a generated "
            "stable UUID and revision 1. No application is created or "
            "changed. Persist concise goals, important "
            "entities/relationships, stages, milestones, issues, validations "
            "and downstream mappings; never conversations, reasoning, scene "
            "dumps or full tool results. Attach a saved application identity "
            "using a document record in project.apply. Stages are authored "
            "by the AI/user; core does not plan. Use meaningful "
            "milestone-level writes, not bookkeeping after every application "
            "command."
        ),
    ),
    "project.continue": (
        ContinueInput,
        Continuation,
        "read_only",
        (
            "Retrieve a compact continuation packet for fresh-session recovery. "
            "Omit project_id only for an "
            "unambiguous connected document association, or one stored "
            "project while no identified document is connected. Returns "
            "deterministic bounded current meaning, progress, issues, "
            "selected dependencies/bindings, validation freshness, latest "
            "checkpoint and small delta; omitted_counts identifies further "
            "detail. Maximum packet 32 KiB. Core never invents next_action. "
            "Inspect critical open/stale state and real application data "
            "before acting; retrieve targeted project.search details only as "
            "needed. Application geometry stays authoritative in its "
            "application."
        ),
    ),
    "project.search": (
        SearchInput,
        SearchResult,
        "read_only",
        (
            "Retrieve bounded project details. kind=project lists project "
            "identities without selection; kind=checkpoint lists named "
            "markers. Other filters intersect: query words in label/summary, "
            "kind, ids, status, stage, entity_type, relation, related_to "
            "(records referencing this entity/record), application, "
            "binding_state, tag. Default 20/max50 records; next_offset and "
            "matched_count signal pagination. Use returned revision as "
            "at_revision for consistent pages; changes cause "
            "revision_conflict. Resource verification is existence/identity "
            "only; evidence availability never implies validated content."
        ),
    ),
    "project.apply": (
        ApplyInput,
        ApplyResult,
        "mutating",
        (
            "Atomically update semantic meaning at expected_revision: at "
            "most256 typed complete-record upserts,64 removals, project "
            "fields and optional named checkpoint in one revision. Upsert "
            "replaces the complete record; include fields to preserve. IDs "
            "are project-local, stable and cannot change kind. References "
            "must resolve in final batch state; remove/update dependents "
            "together. Relations use canonical predicates; "
            "milestone.validation_ids associates validation records. "
            "Entity/binding changes conservatively stale related validations "
            "unless explicitly revalidated in this batch. Binding "
            "observations are core-owned; call project.verify. Validation "
            "freshness=current is an explicit authored assertion, not core "
            "evidence. Checkpoints are immutable revision markers, not "
            "application saves/undo or database snapshots. Create them at "
            "accepted stages, before major restructuring or handoff, not "
            "every operation. Max128/project. Journal retains at most256 "
            "revisions/20000 changes; older checkpoint markers survive but "
            "delta returns history_expired. No automatic deletion of current "
            "records or named markers. Preserve concise resolved "
            "issue/evidence summaries or remove unused records explicitly."
        ),
    ),
    "project.delta": (
        DeltaInput,
        Delta,
        "read_only",
        (
            "Compact changes since one semantic revision or named "
            "checkpoint. Counts cover all retained matching changes; change "
            "summaries paginate default20/max50. details=true includes "
            "current (not historical snapshot) changed records for this "
            "page. Use at_revision to pin pagination. Tombstones identify "
            "removed records. Checkpoints older than retained history return "
            "history_expired and remain inspectable markers. No "
            "conversation/history replay and no snapshot restoration."
        ),
    ),
    "project.verify": (
        VerifyInput,
        ApplyResult,
        "mutating",
        (
            "Verify up to64 unique semantic resource bindings through "
            "advertised read-only application inspection, then atomically "
            "record observations at expected_revision. Does not mutate "
            "applications. Matches saved document UUID and stable resource "
            "IDs, not names/paths. Renames update observed name. Present "
            "identities become verified; absent/duplicate/unsupported IDs "
            "are explicit. Disconnection/reconnection makes old observations "
            "unverified; observed application writes stale bindings and "
            "validations. Changed fingerprints or connection generations "
            "stale related validation. Fingerprints cover only the adapter's "
            "declared scope; verification is not geometric/visual/behavioral "
            "acceptance. Resolve ambiguous running copies with adapter_id. A "
            "revision or connection change during inspection rejects the "
            "whole write; retry after inspecting delta."
        ),
    ),
    "project.remove": (
        RemoveInput,
        RemoveResult,
        "mutating",
        (
            "Permanently remove only selected local Tyvrana semantic state, "
            "journal and checkpoints. Requires expected_revision and "
            "confirm_project_id equal to project_id. Never deletes or "
            "changes application files or external evidence. Core storage is "
            "local; no account, cloud sync or telemetry."
        ),
    ),
}

CONTRACTS = tuple(
    OperationContract(
        name=name,
        description=description,
        effect=effect,
        execution="synchronous",
        arguments_schema=arguments.model_json_schema(),
        result_schema=result.model_json_schema(mode="serialization"),
    )
    for name, (arguments, result, effect, description) in DECLARATIONS.items()
)
CATALOG_SHA256 = hashlib.sha256(
    json.dumps(
        [c.model_dump(mode="json") for c in CONTRACTS],
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()
