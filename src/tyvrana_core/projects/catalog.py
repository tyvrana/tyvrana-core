"Lazy semantic operation contracts, sharing normal discovery/execution tools."

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, RootModel
from tyvrana_protocol import OperationContract

from .continuity import AttestInput, AttestJob, AttestResult, AttestStatusInput
from .models import (
    ApplyInput,
    ApplyResult,
    Continuation,
    ContinueInput,
    CreateInput,
    Delta,
    DeltaInput,
    MutationStatus,
    MutationStatusInput,
    Project,
    RemoveInput,
    RemoveResult,
    SearchInput,
    SearchResult,
    VerifyInput,
)
from .reconcile_models import ReconcileInput, ReconcileResult, ReconcileStatusInput
from .restore_models import RestoreInput, RestoreResult, RestoreStatusInput


class AttestResponse(RootModel[AttestResult | AttestJob]):
    pass


DECLARATIONS: dict[
    str, tuple[type[BaseModel], type[BaseModel], Literal["read_only", "mutating"], str]
] = {
    "project.mutation_status": (
        MutationStatusInput,
        MutationStatus,
        "read_only",
        "Observe a mutation_pending receipt using its mutation_id and project_id. "
        "Optionally wait up to 20 seconds without polling or cancelling the work. "
        "Completed means Core committed the qualified receipt at revision, not "
        "artistic acceptance or fresh inspection. Uncommitted/interrupted means "
        "no qualified commit: inspect actual application state before recovery; "
        "native_execution distinguishes native work from semantic commit. "
        "A completed native result must not be replayed. recovery_operation and "
        "recovery_proof identify reconciliation using a retained receipt or isolated "
        "inverse-delta proof. before_digest, after_digest and stage_id identify "
        "the transition. No original operation output is exposed "
        "here; use its domain inspection for output details. No application calls.",
    ),
    "project.restore": (
        RestoreInput,
        RestoreResult,
        "mutating",
        "Explicitly discard the exact expected_current document and restore a trusted "
        "saved working checkpoint or the durable baseline (omit "
        "checkpoint_id). Requires "
        "discard_current=true and provenance. Core automatically proves the durable "
        "target locator/SHA in an owned disposable host before guarded document load. "
        "The adapter rechecks exact current state and file hash before load, then Core "
        "verifies strong content before restoring recorded freshness and working head. "
        "No force/trust-current option; no milestone reacceptance. New "
        "hosts may be empty "
        "or contain the same logical document. One document. Default mode=content "
        "requires unchanged semantic claims. To abandon an experimental branch, "
        "use mode=checkout with checkpoint_id: atomically replace the complete active "
        "semantic snapshot, preserving the abandoned head and history. Original stale "
        "claims remain stale; no reacceptance. Exact live digest/artifact identity "
        "avoids proof-host startup and document reload. Otherwise use the internal "
        "proof/restore path. Checkout creates one revision based on the checkpoint; "
        "an already-current checkout creates none. Same restore_id/request observes "
        "retained work; use restore_status "
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
        "Recover a known working transition without replay in the live application. "
        "Supply current expected_revision, document/adapter, unaccepted stage_id, "
        "prior_digest, expected_digest, provenance and reconciliation_id. For a "
        "retained failed mutation supply mutation_id: a qualified receipt permits "
        "reconciliation with empty delta. If post-attestation failed and no receipt "
        "exists, supply its exact delta plus inverse_delta: Core snapshots the live "
        "document into an owned proof host, applies the inverse there to reproduce "
        "the complete trusted prior digest/resources, then applies delta there and "
        "requires exact current digest/resources. Each step names owner_entity_id "
        "within the stage. Up to 16 steps in each direction. Without inverse_delta, "
        "replay needs a durable prior artifact. The live document is read-only; "
        "no force adoption, save or mutation replay. Missing/wrong/stale proof fails "
        "closed. On success one semantic revision is published; then normal typed "
        "save can persist the recovered state. Same ID/request is idempotent. "
        "Running/pending is nonterminal: Core retains and observes the same native "
        "attestation job, without replay. The result exposes its identity, publication "
        "state and next_action. Use project.reconcile_status with wait_seconds=20 "
        "until completed/failed; only completed publishes the head and permits save. "
        "A 600-second execution deadline or Core interruption fails closed with "
        "diagnostics; restarting Core does not resume a proof.",
    ),
    "project.reconcile_status": (
        ReconcileStatusInput,
        ReconcileResult,
        "read_only",
        "Observe retained reconciliation by ID; wait_seconds (0..20) waits without "
        "resubmitting or cancelling work. Pending is nonterminal attestation; "
        "follow next_action and never replay the native mutation. Completed "
        "results identify the historical "
        "commit, not a new live attestation. Do not resubmit a "
        "different delta under the same ID.",
    ),
    "project.attest": (
        AttestInput,
        AttestResponse,
        "mutating",
        (
            "Establish strong document verification metadata or reattach "
            "exact accepted content. Capture requires current binding; "
            "reattach requires matching durable digest; bootstrap "
            "automatically proves the trusted artifact locator/SHA256 with"
            " provenance. Migrate requires explicit from_format/to_format, "
            "the existing baseline's durable file SHA256 and an independent "
            "matching new-format proof. It replaces only baseline metadata, "
            "derives freshness for unchanged historically accepted claims, "
            "and preserves acceptance history. No semantic revision churn."
        ),
    ),
    "project.attest_status": (
        AttestStatusInput,
        AttestJob,
        "read_only",
        "Observe retained automatic attestation proof without lifecycle choreography.",
    ),
    "project.attest_cancel": (
        AttestStatusInput,
        AttestJob,
        "mutating",
        "Cancel retained attestation and release its owned proof host.",
    ),
    "project.restore_cancel": (
        RestoreStatusInput,
        RestoreResult,
        "mutating",
        "Cancel retained restore and release its owned proof host; inspect live state.",
    ),
    "project.reconcile_cancel": (
        ReconcileStatusInput,
        ReconcileResult,
        "mutating",
        "Cancel retained reconciliation and release its owned proof host.",
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
            "detail. Maximum packet 32 KiB. Core never invents project next_action. "
            "Retained reconciliations expose lifecycle next_action and status handles; "
            "pending is nonterminal and native work must not be replayed. "
            "Inspect critical open/stale state and real application data "
            "before acting; retrieve targeted project.search details only as "
            "needed. Application geometry stays authoritative in its "
            "application."
            " Checkpoint scope is historical; record freshness is live. "
            "freshness_reason distinguishes missing verification, document context "
            "loss and changed dependencies. Exact strong reattachment can restore "
            "document trust; identity verification alone cannot revalidate changed "
            "content. Historical acceptance is retained, not re-created."
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
            "are explicit. Unavailable document context makes old observations "
            "unverified; strong continuity preserves trust through transport "
            "reconnect. New hosts/loads require exact project.attest reattachment. "
            "Changed fingerprints or untrusted contexts stale related validation. "
            "Fingerprint coverage is limited to the adapter's "
            "declared scope; verification is not geometric/visual/behavioral "
            "acceptance. Inspection uses the verified attachment or pinned adapter; "
            "an adapter_id assertion cannot override it. A "
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
