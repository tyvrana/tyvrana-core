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
  historical artifact SHA256/provenance, a second independent host loaded from that
  artifact, matching document UUIDs, matching complete material digests and a final
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
policy. The global dispatcher timeout is unchanged. An observation pending after
20 seconds returns its job ID and status operation; no incomplete evidence is
accepted as a strong baseline.

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

Core observes guarded work with bounded backoff. Work exceeding the 20-second caller
window returns `mutation_pending` and continues under the original intent; do not
blindly resubmit it. Shutdown or an unqualified result leaves the intent uncommitted.
Synchronous artifact-free mutations are supported; adapters reject unsupported nested
job/artifact contracts before invoking them. This does not retroactively authorize
content created before mutation tracking or repair an already diverged document.

Historical views also use durable acceptance entries already present in the project
journal and named checkpoints. A checkpoint alone proves historical accepted status;
it does not invent an exact acceptance revision or an expanded resource snapshot.
Reading this evidence never restores current validity or changes semantic revision.

## Recovering a known transition without a receipt

`project.reconcile` is guarded recovery for work authored before mutation receipts
were available. It does not accept a milestone or adopt arbitrary current content.
Use normal authoring operations for new work.

Provide the durable prior working digest, expected current digest, an unaccepted
working stage, provenance, and a unique reconciliation ID. The explicit delta is
at most 16 advertised typed operations (128 KiB total request); each step declares
its owning entity in that stage. Raw geometry is not needed.

The caller must supply a disposable independent adapter loaded with the exact
trusted prior document. Core requires the same logical document and strong
attestation format, but a different host. Preparing that proof document is an
explicit lifecycle prerequisite; Core does not launch application processes.
Only synchronous, artifact-free operations carrying the adapter's
`recovery_replay` qualification may be replayed. Core uses existing guarded native
mutation receipts on this proof adapter. The live document is observed read-only.
A failed attempt can leave the disposable proof document changed; reset it to the
trusted prior content before a new attempt.

Core requires the complete replayed content digest and scoped resource observations
to equal the fresh live result exactly. Extra objects, unexplained material edits,
and mismatched operation parameters therefore reject recovery. Bound resources
outside the working stage are protected, including prerequisite dependency
closures. Retained acceptance history must show unchanged semantic claims; only
recorded freshness invalidation may be restored. This operation currently proves
one document, and rejects prerequisites needing evidence from another document.
Missing history, incomplete attestation, resource drift and wrong lineage fail
closed. There is no force or trust-current option.

Success atomically advances project revision once, stores the correlated replay
proof, restores only proved prerequisite freshness, activates the unaccepted
working stage, and establishes its new trusted working digest. Historical
acceptance revisions and snapshots remain unchanged. Ordinary mutation receipts,
saving and working checkpoints resume afterward.

The operation returns a completed/failed result or a retained running job after
20 seconds; observe it using `project.reconcile_status`. Repeating the identical
successful request with the same ID returns its original commit without revision
churn. Changing the payload under that ID rejects. Interrupted work never commits
a partial head and is reported failed after restart. A completed result describes
that historical reconciliation, not a new live attestation.
