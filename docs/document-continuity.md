# Document continuity and strong baselines

Adapter instance IDs select current RPC endpoints. Connection IDs identify a
particular transport lifetime. Neither proves durable content continuity.
Adapters may advertise exactly one read-only, artifact-free operation tagged
`document_attestation`, returning the shared `DocumentAttestation` contract.

`project.attest` establishes document verification metadata:

- `capture`: observe the explicitly bound current document before acceptance or a
  checkpoint. It cannot restore stale accepted state by assuming a current adapter
  is equivalent. Changed accepted content must be reopened and validated normally.
- `reattach`: choose a replacement loaded document/process by matching the durable
  document UUID, complete content digest and format. It does not inherit an old live
  session. A still-connected valid attachment to another host prevents takeover.
- `bootstrap`: for a binding predating strong evidence, require a supplied trusted
  historical artifact locator/SHA256/provenance. Core creates an independent host
  from that artifact and requires matching document UUIDs, matching complete material digests and a final
  unchanged live observation. A provided checksum is only trustworthy if the caller
  has established its provenance from accepted immutable evidence.

Strong baselines and their verification contexts are durable Core binding metadata,
separate from authored records/checkpoints. Capture/reattach/bootstrap do not rewrite
milestone status, create validation judgments or increment semantic revision.
Existing resource observation context fields carry the stable verification context
once attested; the baseline never depends on a new WebSocket connection ID.

For attestation-capable documents, `project.apply` requires matching strong evidence
before accepting milestones or creating a checkpoint. Bind and verify resources first; the acceptance/checkpoint flow obtains
initial strong evidence automatically when absent. Unsupported/incomplete content must be resolved, not accepted
through a weaker fingerprint fallback.

Live reads observe current adapter evidence and resolve exactly one matching
host-session/document-session/content combination. Reload/reconnect automatically
uses the new endpoint without revision churn. Different loaded documents or hosts
are unavailable until explicit strong reattachment. Changed content makes the
computed validation/milestone view stale or invalidated while preserving stored
historical acceptance. Reopening unchanged bytes in a new process uses `reattach`;
it does not require manually re-accepting milestones.

All comparisons require complete evidence in the same content format. Resource
structural fingerprints remain useful identity observations; they are not substitutes
for whole-document attestation. Baseline provenance does not make Core a visual critic.


Document observations may use the shared typed attestation job envelope. Core
discovers the read-only status contract by semantic tag, observes with bounded
backoff, validates final evidence and then applies the existing identity/content
policy. The global dispatcher timeout is unchanged. Proof-heavy project calls
return a retained operation job after 20 seconds, observed with the corresponding
project status operation. Incomplete evidence is never accepted as a strong baseline.

## Accepted checkpoints and current working content

Accepted milestones retain immutable semantic and document-evidence snapshots.
Record views report historical acceptance separately from current prerequisite
validity. The document baseline is the current trusted working head; it can advance
through authorized downstream work without replacing an accepted checkpoint.

For adapters advertising guarded mutation receipts, Core serializes mutation intents
and records the operation, active stage, revision and before digest durably. The
adapter checks that head, runs the requested advertised operation and returns complete
before/after evidence with matching identity and scope. Core commits the receipt,
working head, authored revision and dependency invalidation atomically. Failed or
rolled-back work does not advance the head. An uncertain changed outcome blocks.

Actual changed bound-resource fingerprints and the active stage's declared outputs
seed the existing semantic dependency graph. Unrelated additions preserve accepted
prerequisites; changes to accepted resources or their dependencies stale affected
bindings and validations, propagate to dependent milestones, and retain historical
acceptance. Unexplained external content changes never advance the working head.

Normal `project.apply` acceptance/checkpoint creation obtains initial strong evidence
when absent. Later checkpoints record the qualified current working digest and format
without a separate capture/bind/retry sequence. Saving during a downstream stage is
an ordinary guarded operation, not milestone acceptance. Working mutations and named
checkpoints advance semantic revision; reload/reconnect and exact reattachment do not.

Before risky experimental work, save and create a named working checkpoint. To
abandon the experiment, use `project.restore(mode="checkout")`, specifying the
checkpoint, expected revision/current document state and `discard_current=true`.
This replaces the active semantic snapshot as well as qualifying its document.
It preserves the abandoned head's full values in the restore ledger and leaves
the journal and acceptance history intact. `working_base_checkpoint`,
`working_base_revision` and `checkout_revision` identify the active branch origin.
Continuation selects the latest checkpoint on that working branch rather than a
later checkpoint on an abandoned branch. Failed/stale checkpoint validations stay
failed/stale. No milestone reacceptance or per-binding recovery sequence is needed.

Matching live content and durable file identity take the read-only document fast
path; differing content uses the existing independent proof and guarded load.
Both paths atomically establish the snapshot under one new semantic revision.
An identical already-current checkout does not churn revisions. Normal typed
authoring, automatic qualification, save and checkpoint then continue, including
after application restart. Default `mode="content"` retains current semantic
claims and requires them to match; it is not a branch checkout.

Core observes guarded work with bounded backoff. Work exceeding the 20-second caller
window returns `mutation_pending` and continues under the original intent; do not
blindly resubmit it. Shutdown or an unqualified result leaves the intent uncommitted.
The reply identifies `project.mutation_status`, the project and the mutation.
Observe that durable receipt with an optional bounded wait; a disconnected caller
does not cancel the retained task. Terminal `completed` means a qualified Core
commit, while `uncommitted` or `interrupted` requires inspecting native state before
recovery. These states do not supply an operation result or an acceptance judgment.
Adapters qualify supported nested contracts before invoking them. Blender supports
synchronous mutations, transferred input artifacts and atomic constructive-form jobs.
Cooperative jobs recheck the authorized content immediately before publication;
Core advances the working head once, only after a terminal job and strong receipt.
The original operation result schema is retained (a completed native job for forms).
Status and cancellation remain available during preparation. Cancellation cannot
undo already published work. Output-bearing mutations and unqualified job types
remain explicitly unsupported. Application failures preserve their code and JSON
diagnostics through the project wrapper; MCP adds operation context and bounds
failure details to 16 KiB (an explicit truncation marker replaces oversized data).
Invalid arguments, invalid geometry and unavailable capabilities remain distinct. This does not retroactively authorize
content created before mutation tracking or repair an already diverged document.

Historical acceptance comes from durable acceptance entries, journal acceptance events
and complete revision history. Checkpoint summary lists do not prove acceptance.
A checked-out branch inherits acceptance metadata from its exact revision, excluding
acceptance acquired only on the abandoned branch. Reading historical evidence never
restores current validity or changes semantic revision.

## Recovering a native result after failed publication

`project.mutation_status` separates `native_execution` from semantic `state`.
Native execution can be `not_started`, `started`, `completed`, or `unknown`.
Only semantic `completed` identifies a published revision. A failure response
includes the mutation/project IDs and status operation. Status reports exact
before/after digests when known, stage, whether post-state proof exists, and
whether replay is safe. Never replay completed or uncertain native work.

Core retains validated native receipts durably before semantic publication.
For a failed publication, `project.reconcile` accepts the original `mutation_id`
and an empty delta when that receipt exists. It reobserves live state and checks
exact document identity, trusted pre-state, resource scope, and post-state. A
receipt from another mutation, stale content, or changed protected resources
cannot authorize recovery.

If post-attestation failed, no complete receipt exists. Supply the original
`mutation_id`, exact `delta`, and `inverse_delta`. Core creates an owned disposable
snapshot host. The loaded copy must first equal live content and resource identity.
The inverse runs only there and must reproduce the entire trusted prior digest and
resource evidence. The forward delta then runs only there and must reproduce the
entire current live digest and resources. A final live observation must still agree.
The snapshot is untrusted input, never permission to adopt arbitrary divergence.
No save, inverse, or forward operation runs in the live document during recovery.

Each typed step names an `owner_entity_id` within the unaccepted target stage.
Only synchronous, artifact-free operations tagged `recovery_replay` qualify.
There are at most 16 steps per direction. Without inverse proof, a delta can still
be proven from the exact durable prior artifact; unsaved working heads generally
have no such artifact. Missing proof fails closed.

Recovery also checks unchanged semantic claims and prerequisites against retained
acceptance evidence. It publishes one revision, restores only independently proven
prior freshness, and leaves the target stage in progress. Historical acceptance
revisions are unchanged. The original mutation is marked reconciled with its proof
identity; its publication error remains retained in history. Normal typed save can
then persist the proven working head.

The request supplies current `expected_revision`, document/adapter, target stage,
prior and expected digests, provenance, and a unique reconciliation ID. An identical
request is idempotent. Observe running work with `project.reconcile_status`; a
completed response identifies historical reconciliation, not a new live attestation.
