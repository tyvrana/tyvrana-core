# Durable semantic project state

Tyvrana preserves project meaning across client sessions and core restarts. Core
builds a compact continuation packet from typed local records without a model
invocation. The external AI decides the workflow and next action; core stores,
validates, indexes and retrieves declared meaning.

Application data remains authoritative for geometry, shaders, animation and native
properties. Semantic state is authoritative for declared goals, relationships,
progress, issues and validation summaries. Disagreement requires inspection and
explicit reconciliation. A checkpoint is not an application save or undo state.

## Start and continue

Use the existing `tyvrana_list_operations` and `tyvrana_execute_operation` tools
with `adapter_id: "core"` (the default). Search summaries before loading selected
schemas. The seven core operations are:

| Operation | Purpose |
| --- | --- |
| `project.create` | Create a stable project UUID and revision 1 |
| `project.continue` | Recover compact current context, progress and freshness |
| `project.search` | Retrieve selected records, project identities or checkpoints |
| `project.apply` | Commit one coherent semantic batch and optional checkpoint |
| `project.delta` | Inspect compact changes since a revision or checkpoint |
| `project.verify` | Resolve resource bindings through an adapter, recording observations |
| `project.remove` | Delete only semantic state with explicit identity confirmation |

A fresh client should retrieve continuation, inspect critical open or stale state,
request only relevant details, and check actual application data before acting.
Persist important meaning at a meaningful boundary after the work is verified.
Application results include `project_revision` when their document is associated
with a semantic project. Use that current revision for the next semantic update;
observed application mutations may have invalidated previous evidence.

## Identity and selection

A core project UUID is independent of files, paths, application instances and
client conversations. A `document` record attaches an application name and saved
application-project identity. One native document identity belongs to at most one
semantic project; a semantic project may attach multiple documents/applications.
A `binding` links an entity to a document plus adapter-defined resource kind and
stable resource ID. Names and paths are human-readable locators, not keys.

Explicit `project_id` selects a project. When omitted, core chooses a unique
association with connected saved documents, or a sole stored project when no
application is connected. It never uses a global last-selected project shared
across clients. Ambiguous or unrelated connected documents produce a bounded
selection error; `project.search(kind="project")` lists candidates. An explicitly
selected disconnected project remains readable, with unavailable application state.

An adapter advertises portable resource inspection in its registration. Establish
saved IDs through the application's typed identity operation before attaching a
document. Unsaved IDs remain temporary until the application project is saved.
Save-as normally preserves document lineage; independent forks need a deliberate
new application identity. Simultaneous copies with the same UUID are ambiguous;
`project.verify(adapter_id=...)` chooses the inspected runtime without silently
selecting a permanent winner. Verification refreshes the recorded file locator.

## Typed meaning

Records have a stable project-local ID, short label/summary, importance 0–5,
optional free-form stage and up to eight short tags. There is no arbitrary metadata
bag or prescribed stage enum.

- **Entity:** asset, system, component, reference, output, runtime counterpart or
  other declared item. Only important project meaning belongs here.
- **Relationship:** canonical `contains`, `depends_on`, `derived_from`, `attached_to`,
  `deformed_by`, `references` or `maps_to` between entities. Duplicate/self edges
  are rejected; no automatic dependency planning is performed.
- **Milestone:** planned, in progress, accepted, failed or deferred; concise
  acceptance text and entity/validation references.
- **Issue:** open, resolved or deferred; severity, affected entities and an
  explicitly authored next action if useful.
- **Validation:** passed, failed, warning or unknown; a declared validation type,
  affected entities, evidence references and freshness.
- **Document / binding:** saved application identity and resource mapping.
- **Evidence:** an ephemeral artifact ID, external URI or application binding,
  optionally with an integrity hash. Never embedded image/file bytes.

Use existing tags and summaries to distinguish domain structure, control/proxy,
deformation helper and production surface where relevant. Record their actual
relationships. Accept a milestone from inspected representations and behavior
evidence; a proxy's existence or label does not establish its dependent structure.
Core stores these judgments; it does not infer physical correctness.

Persist established systems, meaningful dependencies, stage changes, milestone
acceptance/failure, significant issues, validation decisions and downstream
mappings. Do not store conversations, private reasoning, full operation responses,
scene copies, every object/property, troubleshooting transcripts or intermediate
calculations. Application-specific detail stays in its adapter or referenced output.

## Atomic updates and concurrency

`project.apply` requires `expected_revision`. It replaces complete typed records
by ID; include fields that should survive. Optional project fields patch only the
specified non-null fields; use an empty string to clear a stage/next-action note.
An upsert cannot change a record's kind. References must exist with the expected
kind in the final batch state. Remove/update dependent records together when
removing entities. A failed batch leaves all records, markers and revision unchanged.

One successful batch advances revision once, including a checkpoint in that batch.
Two clients writing against the same revision cannot silently overwrite each other.
A `revision_conflict` returns the current revision; inspect `project.delta`, reconcile
and retry. Search/delta pages may pin `at_revision` to reject shifting pagination.
SQLite immediate transactions protect writes across local core processes. Interrupted
writes roll back on recovery. No distributed consensus or cloud collaboration is implied.

## Freshness and evidence

Binding `verified` means that the saved identity uniquely exists in the checked
runtime. It is not geometry, appearance, animation or engineering acceptance.
Read-only inspection may also report missing, ambiguous or unsupported resources.
A new connection generation or unavailable runtime makes prior observations
unverified. Verification checks exactly the requested document/resource IDs.
Adapter fingerprints describe their scope; they are not whole-scene hashes.

Core conservatively stales bindings and related validations before advertised
mutating application operations. Repeated mutations while already stale coalesce
without another semantic write. Failures can leave the conservative invalidation
in place because native work may have partially occurred. Entity/dependency/binding
changes also stale affected validations, unless explicitly revalidated in that same
semantic batch. A connection change or fingerprint change during verification
stales related validation. Search and continuation report effective freshness.

`freshness: "current"` is an explicit AI/user assertion backed by the recorded
validation work. Core does not assess its truth. An accepted milestone is historical
progress, not proof that its supporting validation remains fresh. Inspect associated
validation before relying on it. External native edits outside the fingerprint scope
cannot all be detected; revalidate when evidence may no longer apply.

Ephemeral transport artifacts expire on release/shutdown, and outbound images are
normally released after the MCP response is constructed. Their semantic references
remain meaningful but report expired when bytes are unavailable. External or
application evidence reports unverified availability: core does not fetch or retain
it automatically. A hash identifies content; it is not a retrieval mechanism.

## Checkpoints, history and cleanup

Create named checkpoints at accepted stages, before major restructuring, at a
handoff or an important downstream boundary. They record semantic revision, stage,
bounded accepted milestone/document references, validation counts and timestamp.
They contain no duplicate database snapshot and cannot restore application state.
Checkpoint IDs are immutable. `forget_checkpoints` explicitly removes unused markers.

The current typed records are materialized separately from a compact change journal.
The journal retains at most 256 revisions and 20,000 changes, pruning whole revisions.
Named markers survive this pruning. A request older than `history_floor` returns
`history_expired` with the earliest usable boundary; it never invents an incomplete
delta. Read current continuation instead. Detailed delta records are current values,
not reconstructed historic versions. Deleted records have compact tombstone entries
while their changes remain in retained history.

Resolved issues and superseded validations do not auto-delete. Upsert a stable
validation ID to replace obsolete results; deliberately remove unused records and
markers when appropriate. SQLite reuses freed pages; file size may retain its prior
high-water allocation. `project.remove` requires the current revision and matching
`confirm_project_id`, and deletes only core semantic records/history. Native files,
external evidence and application state are never removed with it.

## Bounds and deterministic selection

| Resource | Limit |
| --- | --- |
| Local projects | 100 |
| Current records per project | 50,000 |
| Documents per project | 64 |
| Batch upserts / removals / serialized bytes | 256 / 64 / 512 KiB |
| Summary / label / stage | 800 / 160 / 120 characters |
| References per typed reference list | 64 |
| Named checkpoints per project | 128 |
| Search / delta page | default 20, maximum 50 |
| Binding verification batch | 64 |
| Default continuation | 32 KiB structured packet |

Continuation prioritizes open/failed/active concerns, critical issues, referenced
entities, current-stage records and explicit importance, with stable ID tie breaks.
It includes bounded categories of entities, dependencies, progress, validation and
bindings, plus counts/omissions and a small recent delta. This is deterministic
selection, not an opaque semantic search or AI summary. Keyword search matches
label/summary terms; explicit type/status/stage/tag/application filters narrow detail.
Record and relation expansion is always paginated. The packet is not proportional
to total project size, and `omitted_counts` makes omission visible.

## Local storage

The CLI accepts `--state-directory`. The default is `tyvrana` beneath
`XDG_DATA_HOME` or `~/.local/share` on Linux, `~/Library/Application Support/Tyvrana`
on macOS, and `Tyvrana` beneath `LOCALAPPDATA` on Windows. The SQLite file is
`projects.sqlite3`; its WAL/shared-memory sidecars are owned by SQLite. This is
persistent data, separate from temporary artifact storage. Use a separate directory
for disposable tests. Back up through SQLite's backup facilities or stop all writers
before copying the store. Do not copy a live main database while ignoring its WAL.

No client-vendor session identifier, transcript format, model, account, telemetry,
remote database or inference service is part of this contract. This foundation does
not promise arbitrary long-term/distributed scale or universal native edit detection.
