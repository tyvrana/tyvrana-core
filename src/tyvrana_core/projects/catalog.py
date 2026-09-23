"Lazy semantic operation contracts, sharing normal discovery/execution tools."

import hashlib
import json
from typing import Literal

from pydantic import BaseModel
from tyvrana_protocol import OperationContract

from .continuity import AttestInput, AttestResult
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
from .reconcile_models import ReconcileInput, ReconcileResult, ReconcileStatusInput
from .restore_models import RestoreInput, RestoreResult, RestoreStatusInput

DECLARATIONS: dict[
    str, tuple[type[BaseModel], type[BaseModel], Literal["read_only", "mutating"], str]
] = {
    "project.restore": (
        RestoreInput,
        RestoreResult,
        "mutating",
        "Explicitly discard the exact expected_current document and restore a trusted "
        "saved working checkpoint or the durable baseline (omit "
        "checkpoint_id). Requires "
        "discard_current=true, provenance, and an independent proof_adapter_id already "
        "loaded with the target artifact. Core verifies durable file/content/lineage "
        "evidence before invoking the advertised document_open with open_arguments. "
        "The adapter rechecks exact current state and file hash before load, then Core "
        "verifies strong content before restoring recorded freshness and working head. "
        "No force/trust-current option; no milestone reacceptance. New "
        "hosts may be empty "
        "or contain the same logical document. One document; unchanged semantic claims "
        "required. Same restore_id/request observes retained work; use restore_status "
        "when running. Already-current targets only reattach without revision churn.",
    ),
    "project.restore_status": (
        RestoreStatusInput,
        RestoreResult,
        "read_only",
        "Observe a retained trusted artifact restore without repeating "
        "destructive load.",
    ),
    "project.reconcile": (
        ReconcileInput,
        ReconcileResult,
        "mutating",
        "Guarded recovery of a known pre-receipt working delta, never "
        "trust-current adoption. "
        "Require the durable prior_digest, exact expected_digest, "
        "target unaccepted stage, "
        "provenance and at most 16 typed owner-scoped delta steps. "
        "proof_adapter_id must be a "
        "disposable independent host already loaded with exactly the "
        "trusted prior document. "
        "Core replays only adapter-qualified recovery_replay operations "
        "ON THE PROOF HOST; "
        "the live document is read-only. Complete final content and "
        "protected resource fingerprints "
        "must match; unknown changes, changed accepted claims and "
        "incomplete evidence reject. "
        "Success restores proved prerequisite freshness, records a "
        "recovery receipt and activates "
        "the unaccepted working stage. Same reconciliation_id and "
        "identical request are idempotent. "
        "Running work is observed with project.reconcile_status. No "
        "force flag or manual reacceptance.",
    ),
    "project.reconcile_status": (
        ReconcileStatusInput,
        ReconcileResult,
        "read_only",
        "Observe bounded guarded reconciliation by ID. Completed "
        "results identify the historical "
        "commit, not a new live attestation. Do not resubmit a "
        "different delta under the same ID.",
    ),
    "project.attest": (
        AttestInput,
        AttestResult,
        "mutating",
        (
            "Establish strong document verification metadata or reattach "
            "exact accepted content. Capture requires current binding; "
            "reattach requires matching durable digest; bootstrap "
            "requires an independently loaded trusted artifact SHA256 and"
            " provenance. Migrate requires explicit from_format/to_format, "
            "the existing baseline's durable file SHA256 and an independent "
            "matching new-format proof. It replaces only baseline metadata, "
            "derives freshness for unchanged historically accepted claims, "
            "and preserves acceptance history. No semantic revision churn."
        ),
    ),
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
            "using a document record with an explicit adapter_id in project.apply. "
            "Declare milestone prerequisite_ids and required validation_ids; stage "
            "selects an in_progress milestone ID. Stages are authored "
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
            "kind, ids, status, entity_type, relation, related_to "
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
            "Atomically apply complete typed records at expected_revision; max256 "
            "upserts/64 removals/512KiB. IDs are stable and cannot change kind. "
            "All references resolve in final batch state. Milestone prerequisite_ids "
            "form an acyclic graph; activation requires accepted prerequisites. "
            "Acceptance requires acceptance criteria, required validation_ids all "
            "passed/current with observation summaries and referenced evidence. "
            "Evidence needs observed findings, not names/existence. Core checks "
            "declared evidence structure, not its truth. Major/critical unresolved "
            "issues block acceptance; own corrective work remains allowed. "
            "Set project.stage to an in_progress milestone ID, or empty to pause. "
            "Its entity_ids declare mutation scope; document_ids declare targets. "
            "Bound application writes require this stage and its prerequisites. "
            "Changes stale affected validations and invalidate accepted dependents; "
            "revalidate and reaccept explicitly. Reopen upstream before editing it. "
            "No stage ritual for simple unbound edits. Verify resource identities "
            "with project.verify; that alone never accepts content. Checkpoints are "
            "immutable revision markers, not saves/snapshots; max128. Journal "
            "retains256 revisions/20000 changes; expired deltas require continuation. "
            "Persist meaningful batches, not every primitive; remove unused records "
            "and forget_checkpoints explicitly."
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
            "acceptance. Inspection uses the document's pinned adapter_id; "
            "rebind the document explicitly to change runtime. A "
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
        category="project",
        tags=("semantic", "continuity"),
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
