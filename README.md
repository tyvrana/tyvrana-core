# tyvrana-core

Local orchestration and adapter connections for Tyvrana. Shared application
contracts are defined in the independent
[tyvrana-protocol](https://github.com/tyvrana/tyvrana-protocol) package.

Core currently accepts local WebSocket adapter connections, registers running
application instances, routes operations, correlates responses, and distributes
adapter events internally. Its MCP interface lets local clients discover connected
adapters and execute their advertised operations. Application adapters are
separate repositories. Binary artifacts are verified and held in bounded temporary
storage; PNG and JPEG outputs can be returned as MCP image content. Local files
can be imported into core-owned storage and attached as binary operation inputs.

## Run the adapter server

After installing the development environment below:

```sh
uv run --locked tyvrana-core serve
uv run --locked tyvrana-core serve --host 127.0.0.1 --port 8765
uv run --locked tyvrana-core serve --help
```

The default listener is `ws://127.0.0.1:8765`. Use port `0` to select an available
port. The CLI logs the listening address and stops cleanly on Ctrl+C or SIGTERM
on systems supporting asyncio signal handlers.

## MCP stdio

Configure a local MCP client to launch this command with the repository as its
working directory:

```sh
uv run --locked tyvrana-core mcp
```

MCP mode runs one process: the official MCP Python SDK serves stdin/stdout while
the adapter WebSocket server listens concurrently at `ws://127.0.0.1:8765`.
All modes accept `--host` and `--port` for the adapter listener. Use
`uv run --locked tyvrana-core mcp --help` for MCP mode options. The CLI requires
an explicit `serve`, `mcp` or `mcp-http` subcommand.

Normal logs go to stderr. In MCP mode, stdout contains only MCP transport data.
The MCP lifespan starts the existing `AdapterServer`, which owns the registry,
dispatcher, event broker, and temporary artifact store. Session exit, stdin closure, Ctrl+C, or SIGTERM
shuts down the listener, closes adapter connections and event subscriptions,
fails pending operations, and joins outstanding work. Importing the MCP package
does not start services.

### Shared local MCP service

For independent clients or conversations connecting to the same running
application, start one core process independently of the clients:

```sh
uv run --locked tyvrana-core mcp-http
```

Connect an MCP Streamable HTTP client to `http://127.0.0.1:8766/mcp/`.
`--mcp-port` changes the HTTP port; `--port` still selects the adapter port.
The HTTP listener binds only to loopback and validates local Host/Origin headers.
It provides the same tools, instructions, artifacts and project state as stdio.
Each client has its own MCP session; disconnecting a client leaves core and its
application connections running. Ctrl+C or SIGTERM stops the shared service and
cleans up core-owned resources. Run only one core listener per adapter endpoint;
do not launch a competing stdio core on the same adapter port.

This is a local service for trusted clients on the same machine, without remote
authentication or a public-network deployment mode.

### Application-control policy

When an application has a connected Tyvrana adapter, all meaningful mutations of
its project/editor state must use the adapter's advertised typed operations.
Clients must discover connected adapters and their operations first. A missing
operation is a capability gap to report, not permission to bypass Tyvrana through
Computer Use, mouse/keyboard automation, menus, shortcuts, gizmos, editor consoles,
arbitrary scripts, direct application APIs, or another editor-control integration.
Necessary direct UI observation/window management is non-authoritative and must
not mutate project/editor state.

Normal source-code/file editing with software-development tools remains allowed;
ordinary programming does not belong in editor RPC. Application-owned scene,
asset and prefab mutations, editor/runtime actions, compilation inspection and
screenshots use Tyvrana. The source-editing exception is not an alternative path
for modifying application-owned scene or asset state.

Every MCP client receives this AI-facing policy automatically in the server's
initialization instructions, alongside tool descriptions and schemas. Users do
not need a Tyvrana-specific `AGENTS.md` or prompt pack. Instructions also cover
structured inspection, visual verification, reversible changes, surface targeting,
snapshot-local indices and recovery after possible partial mutation. Exact
presentation to the model depends on the MCP host; instruction delivery is tested
with the official Python client, including stdio initialization and discovery.
No user-invoked MCP prompt templates are currently advertised.

Before meaningful mutation, initialization guidance asks the client to infer a
professional workflow from the requested result, existing state, hidden structure,
behavior, references, materials, downstream constraints and acceptance needs.
Intended use determines appropriate complexity; structural and functional checks
precede expensive dependent detail. Motion requirements need range/transition
validation on simple geometry before final surface layers. Each dependent stage
has an observable acceptance gate, without imposing a domain recipe. Real-world
correctness requires authoritative research and actual visual reference inspection.
Capability gaps are reported; reusable tool development requires authorization.
Semantic operations, cached contracts and bounded inspections keep interaction
cost manageable. Workflow reasoning remains with the external AI client.

Substantial work persists a compact semantic contract before construction. Core
requires current accepted prerequisites to activate or accept dependent milestones,
and requires observed evidence for acceptance. Bound application mutations use the
active milestone's declared outputs and document/adapter target. Changes invalidate
related validation and dependent acceptance, while read-only inspection and upstream
repair remain possible. See [project-state contracts](docs/project-state.md).

This guidance does not technically prevent external clients from using other
capabilities they possess. Operation validation, limits and adapter safety checks
remain enforced in code. Attestation-capable adapters detect unexplained content divergence and qualify
authorized working mutations; see [document continuity](docs/document-continuity.md).

### Tools

Five tools are available. All publish Pydantic-generated input and output
JSON schemas and reject unexpected input fields. Successful responses provide
the object below as MCP `structuredContent` and as JSON in a text content block.

`tyvrana_list_adapters` accepts optional `application` and `adapter_id` filters,
`wait_seconds` (0–30, default 0), and `after_revision`. Without a revision it waits
for a matching adapter. With a revision it waits for any registry change, then
returns the filtered snapshot; an unrelated registration can also wake the wait.
An empty result after the deadline is valid. Waiting is cancellable and uses
registry notifications rather than periodic polling.

```json
{
  "adapters": [{
    "instance_id": "example-editor",
    "application": "Example Editor",
    "application_version": "2026.9",
    "project_path": "projects/example.project",
    "operation_count": 1,
    "catalog_sha256": "<SHA-256 of canonical operation contracts>"
  }],
  "revision": 1
}
```

Optional application metadata is omitted when absent. Adapters sort by instance
ID. Registry revisions are local to the running core; restart invalidates them.
Catalog hashes are independent of adapter identity and declaration order and
change when a contract changes. Cache contracts by hash across reconnections.

`tyvrana_list_operations` searches compact summaries from a connected adapter.
Use `query` keywords for the required capability, then retrieve selected contracts:

```json
{
  "adapter_id": "example-editor",
  "names": ["document.inspect"],
  "schemas": "arguments"
}
```

Optional `query` (1–256 characters with at least one letter or number) searches
case-insensitive name/description/category/tag text. Punctuation separates terms; any matching
term includes an operation. Name/category/tag matches outrank description mentions, with name
as a stable tie-breaker. This is text matching, not inferred synonyms; try alternate
terms before concluding that work is unsupported. Without a query, results sort
by name. Search, exact `names` (1–16 unique names), `prefix`, `category` and `tag`
filters intersect. Unknown
exact names are reported in `unavailable_names`; valid requested contracts are still returned. Results contain `adapter_id`, `catalog_sha256`,
`matched_count`, `next_offset` and `operations`. Each operation has its qualified name,
description, category, tags, effect, execution mode, interactive-context requirement and artifact
behavior. Use `schemas: "arguments"` for self-contained input contracts.
`schemas: "full"` additionally includes `result_schema`; the default `"none"` omits schemas. `offset` starts at
zero; `limit` defaults to 20 with a maximum of 50 summaries or eight detailed
contracts per page. Follow `next_offset` until null. Fetch only needed schemas;

Conditional discovery accepts up to 64 `known_contracts` name/fingerprint pairs.
Retain each returned `contract_sha256` with its contract, then pass it on later
searches, including overlapping queries. Matching entries return
`schema_status: "unchanged"` without schemas. Unknown or changed contracts return
`schema_status: "included"` and the requested schemas. Fingerprints cover the
entire contract and schema mode, so argument-only knowledge cannot suppress a
later full contract. Omit the mapping to refresh deliberately. The server retains
no client cache; unchanged contracts remain reusable across adapter reconnects.

requesting the complete detailed catalog is usually unnecessary.

The schema exposes structural types, enums, bounds and defaults. Preserve omitted
fields in partial updates; materializing every default can change operation intent.
Native state and cross-field constraints still require adapter validation.
Execution modes distinguish synchronous operations, job starts/status queries
and lifecycle transitions. Effects distinguish read-only, mutating, transient
state and lifecycle work. Metadata describes behavior; it is not authorization
or a guarantee of rollback. `requires_interactive` identifies operations that
always need an interactive host; individual options can impose further context
requirements described by their contract.

`tyvrana_execute_operation` requires these three fields:

```json
{
  "adapter_id": "example-editor",
  "operation": "document.inspect",
  "arguments": {"include": ["summary"]}
}
```

It selects the adapter by its instance ID and delegates to `OperationDispatcher`,
which verifies that the adapter exists and advertises the operation, then awaits
its real response. `document.inspect` here is an illustrative adapter operation.
The tool uses the configured core operation deadline (30 seconds by default);
there is no MCP timeout override.

Optional `artifact_ids` is an ordered array of zero through eight unique opaque
artifact IDs returned by import. Omission means no inputs; explicit null is
invalid. Core resolves complete descriptors and transfers every input before
sending the operation. The application's arguments may identify which attachment
to use, but contain no source path or embedded bytes:

```json
{
  "adapter_id": "example-editor",
  "operation": "asset.import",
  "arguments": {"artifact_id": "00112233445566778899aabbccddeeff"},
  "artifact_ids": ["00112233445566778899aabbccddeeff"]
}
```

`asset.import` is illustrative; each adapter defines its typed operation arguments.
Input and output attachments are independent; input descriptors are not echoed
as output artifacts.

`tyvrana_import_artifact` takes required `path`, optional `name`, and optional
`media_type`. It copies a readable local regular file into core-owned temporary
storage and returns an `ArtifactDescriptor` directly:

```text
{
  artifact_id: string, name: string, media_type: string,
  byte_size: integer, sha256: string
}
```

For example, `{"path": "textures/checker.png", "name": "Checker"}` imports a
file relative to core's working directory. Absolute local paths are also accepted.
This is exclusively a **local ingestion boundary**: the source path terminates
at core, is not retained in public artifact metadata, and never enters the
application protocol. Adapters receive descriptors and binary bytes, not the
source filename's location. The returned name defaults to the source basename;
overrides must be nonblank display names without separators or control characters.

Directories, devices, FIFOs, and final-component symlinks are rejected. Core checks
the opened file's identity/type and size, reserves quota before copying, streams
in bounded chunks, computes exact size/SHA-256, and rejects changes detected during
copying. Subsequent source modification or deletion does not affect the imported
copy. File errors are sanitized and return `artifact_import_failed` without paths.

PNG/JPEG signatures take precedence over filename inference. Otherwise MIME
inference uses the filename with `application/octet-stream` as the fallback,
including filenames that imply an additional compression encoding.
An explicit override must be a valid lowercase type/subtype without parameters;
PNG/JPEG declarations must match the signature. Core is generic storage, not a
full raster decoder: adapters must validate supported formats and decoded bounds.
Nulls and unknown input fields are rejected.

`tyvrana_release_artifact` takes `{"artifact_id": "..."}` and returns
`{"released": true}` on the first release, or `{"released": false}` if unavailable
or already released. Imports remain reusable across operations until release or
runtime shutdown. Release prevents new admissions immediately. Already admitted
transfers retain their readers and quota reservation until transfer finishes;
they do not depend on the original local file.

Operation execution without output artifacts returns `{"result": <JSON value>}`.
Both `arguments` and `result` accept the
protocol's `JsonValue`: objects, arrays, strings, finite numbers, booleans, or
null, including nested values. Strings such as `"null"` and `"[]"` remain strings.
A null result is explicitly returned as `{"result": null}`.

### Failures and cancellation

Tool failures set MCP `isError` to `true`. Their text content is a JSON object
with `code`, `message`, and optional `details`; they have no success
`structuredContent`:

```json
{"code": "document_not_found", "message": "Document does not exist.", "details": {"name": "Sample"}}
```

Remote failures preserve the adapter's protocol error code, message, and details.
Core failures use `adapter_not_found`, `operation_unsupported`,
`operation_timeout`, or `adapter_disconnected`. Invalid tool inputs use
`invalid_arguments` with field diagnostics. Expected failures do not log
tracebacks; unexpected failures are logged with diagnostic information and return
a sanitized `internal_error` to the client.

MCP client cancellation cancels dispatcher execution, clears pending state, and
sends best-effort `operation.cancel` with the original request ID. The MCP layer
joins the cancelled dispatcher task before finishing cleanup. Late adapter
responses are ignored. Cancellation remains cancellation, rather than a successful
tool response, and does not guarantee that the application stopped its work.

## Connections and registration

An adapter sends `AdapterRegistration` as its first WebSocket message within five
seconds. Core stores its instance ID, application metadata, optional project path,
and typed operation contracts. Core caches their canonical SHA-256 once at
registration; it does not interpret application schemas or execute application
logic. A duplicate active instance ID is rejected without
disturbing the original connection. IDs can be reused after disconnection.

Each WebSocket text message contains exactly one canonical protocol JSON control
object. Binary messages carry only artifact chunks using `tyvrana-protocol`
framing helpers. A chunk is at most 65,536 payload bytes plus its 24-byte header.
WebSocket fragments are reassembled into one bounded message before decoding.
All contracts, validation, and wire serialization use `tyvrana-protocol`.

After registration, adapters may send only `operation.success`,
`operation.failure`, `adapter.event`, `artifact.begin`, `artifact.complete`,
`artifact.ready`, `artifact.accepted`, `artifact.abort`, or binary artifact chunks.
Input acknowledgements are correlated to core-originated transfers. Malformed protocol data and invalid
directions close the connection with code `1008`; invalid WebSocket text encoding
uses `1007`, oversized messages use `1009`, and server shutdown uses `1001`.
Unknown, duplicate, or stale response IDs are logged and ignored. Responses are
matched within the originating connection, so another adapter cannot complete
its requests.

`CoreConfig` is a frozen typed configuration with local defaults:

| Setting | Default |
| --- | --- |
| `host` | `127.0.0.1` |
| `port` | `8765` |
| `registration_timeout` | 5 seconds |
| `operation_timeout` | 30 seconds |
| `send_timeout` | 5 seconds |
| `close_timeout` | 2 seconds |
| `max_message_size` | 4 MiB |
| `max_artifact_size` | 128 MiB per artifact |
| `max_artifact_storage` | 512 MiB reserved + completed across the runtime |
| `max_artifact_entries` | 128 reserved + completed across the runtime |
| `max_artifact_transfers` | 4 incoming transfers and 4 concurrent input deliveries per adapter; 4 local imports |
| `max_inline_image_bytes` | 4 MiB raw image bytes total per MCP result |

The WebSocket opening handshake also uses the registration timeout. The listener
accepts clients without an `Origin` header; browser-origin connections are
rejected. Message size, send time, and close time are bounded. A failed, stalled,
or interrupted send retires the connection and fails its pending operations.

## Programmatic use

`AdapterServer` owns a registry, dispatcher, event broker, and temporary artifact store. Use it on one
asyncio event loop, with explicit lifecycle calls or an async context manager:

```python
import asyncio

from tyvrana_core import AdapterServer, CoreConfig


async def run_server() -> None:
    async with AdapterServer(CoreConfig(port=0)) as server:
        print(server.uri)
        await asyncio.Event().wait()
```

Cancelling `run_server()` closes connections and subscriptions. `start()` and
`stop()` are idempotent, and a stopped server can be started again. Shutdown fails
pending operations before waiting for connection close handshakes.

The registry exposes `list()`, `get(instance_id)`, and `supporting(operation)`.
These return frozen `AdapterInfo` snapshots containing the protocol registration
and connection state; lists are tuples. No internal connection map is exposed.

For an adapter that has already registered an operation:

```python
from tyvrana_core import AdapterServer
from tyvrana_protocol import OperationSuccess


async def inspect_document(server: AdapterServer, adapter_id: str) -> OperationSuccess:
    return await server.dispatcher.execute(
        adapter_id=adapter_id,
        operation="document.inspect",
        arguments={},
        timeout=10.0,
    )
```

`document.inspect` is an illustrative operation name, not a core command.
Dispatch verifies the adapter and advertised operation, creates a UUID request
ID, registers pending state before sending, and returns `OperationSuccess` with
its `result` and typed `artifacts`.
`RemoteOperationError` preserves the complete `ProtocolError` in its `error`
attribute. Missing adapters, unsupported operations, disconnects, and deadlines
have distinct exceptions derived from `CoreError`.

The operation deadline covers input admission/transfer, sending, and awaiting the response. A deadline
clears pending state, attempts `operation.cancel` on a live connection, and raises
`OperationTimeout`. Cancellation delivery can take up to `send_timeout` beyond
the operation deadline.

To explicitly cancel an in-flight call, run `dispatcher.execute()` in an asyncio
task, call `task.cancel()`, and await that task while handling
`asyncio.CancelledError`. Core clears its pending state and attempts cancellation
with the original request ID. Cancellation is best effort; it does not guarantee
the application interrupted execution. If a response has already been received,
no cancellation frame is needed. Caller cancellation follows normal asyncio task
semantics; late responses are ignored without affecting other operations.

## Temporary artifacts and MCP images

Input delivery uses the same canonical controls and binary frames as output:

```text
complete core artifact → begin → ready → binary chunks → complete → accepted
all inputs accepted → operation.request → operation.success / operation.failure
```

Each use gets a fresh transfer ID and retains request correlation through operation
completion, including aborts received after acceptance. Concurrent uses have
independent readers and offsets. Core checks stored size/hash while streaming;
the adapter must verify its receipt and accept every descriptor before executing.
An operation referencing incomplete or changed inputs must never begin. The
request carries descriptors in its typed `artifacts` field, outside `arguments`.
Input transfer failures/cancellation send request cancellation, including before
the operation itself was delivered. A failed or disconnected use does not release
the reusable core source. Adapter request-scoped copies are the adapter's cleanup
responsibility. There is no second binary protocol or upload service.

Core reserves the entire declared size before acknowledging `artifact.begin` with
`artifact.ready`. Chunks are written incrementally to a private incomplete file.
Offsets must start at zero and advance exactly; size and SHA-256 are verified on
`artifact.complete`. Only then is the file atomically renamed to completed storage
and `artifact.accepted` sent. Success must reference exactly the descriptors
completed for that request, on that connection. No partial file is retrievable as
a completed artifact. The protocol permits up to eight artifacts per result.

Limit, offset, size, hash, and ownership failures become an
`artifact_transfer_failed` operation error and `artifact.abort`. Core cancels the
related operation and deletes its received output artifacts. Other operations and
reusable imported sources remain independent.
Unknown late chunks receive `artifact.abort`; invalid framing and transfer-ID
collisions close the offending adapter connection. No failed transfer can replace
another artifact. The operation deadline includes transfer and acknowledgements.

The runtime's `artifacts` (`ArtifactStore`) exposes asynchronous
`import_file(path, name=None, media_type=None)`, `metadata(artifact_id)`,
`open(artifact_id)` (a binary file object to close after use), and
`release(artifact_id)`. It exposes no storage paths in the wire protocol or MCP.
For programmatic dispatcher users, successful artifacts remain until explicit
release or runtime shutdown, subject to the entry/byte bounds. Full stores reject
new admission; there is no eviction or permanent storage. Cancelled or failed
requests lose their output artifacts, including verified files awaiting response
delivery. Adapter disconnect removes unfinished requests; already returned
successful artifacts remain available. Shutdown removes the entire store.

MCP results preserve `{"result": <JSON value>, "artifacts": [<descriptors>]}` in
`structuredContent` and a text block, then append an actual SDK `ImageContent`
block for each `image/png` or `image/jpeg` artifact in descriptor order. MCP's
standard `data` field contains its normal base64 wire encoding; Tyvrana JSON
payloads contain no encoded image data. Non-image or oversized outputs remain in
bounded Core storage, identified by `retained_artifact_ids`. Setting execution's
`artifact_delivery: "reference"` retains every output and embeds no image bytes.
Inline outputs release after response construction; unsuccessful output delivery
releases all outputs. Retained outputs and imports remain until export/release or
runtime shutdown. A full store rejects new admission; it does not silently evict.

The inline limit applies to the sum of raw PNG/JPEG bytes. Exceeding it returns
references instead of inline content. Use `tyvrana_export_artifact` with
`artifact_id` and an absolute local `path` to retrieve retained output. Export
verifies integrity and publishes atomically; `overwrite` defaults to false.
Successful export releases temporary bytes unless `release: false` is explicit.
Failures/cancellation leave the source retrievable and remove partial files.
Paths terminate at Core's local import/export boundary and are never adapter
transport. Use modest render dimensions for immediate visual feedback. Limits
are configured through `CoreConfig`; the CLI uses these defaults.
No additional services, database, or storage dependencies are required.

The 128 MiB input limit accommodates typical professional 2K/4K texture files;
4K RGBA8 pixels alone require 64 MiB before file overhead. Input bytes stream via
temporary files, rather than a full in-memory upload. Decoder pixel/memory limits
remain an adapter concern. Store limits count partial, completed, and released
but still leased files; they do not permit arbitrary multi-gigabyte admission.

## Events

```python
from tyvrana_core import AdapterServer


async def watch_events(server: AdapterServer) -> None:
    with server.events.subscribe(capacity=128) as events:
        async for event in events:
            print(event.adapter_id, event.message.event, event.message.payload)
```

Each subscription is a single-consumer async iterator with a bounded queue.
`SourcedEvent` pairs the adapter ID with its protocol `AdapterEvent`. Subscribers
receive independent payload containers. Publishing never waits for consumers.
If a queue fills, that subscription closes, discards its backlog, logs a warning,
and raises `EventSubscriptionOverflow` when consumed. Other subscriptions and
operation responses continue normally. Consumers can explicitly resubscribe.

The context manager or `close()` unsubscribes and wakes a waiting consumer.
Server shutdown closes all current subscriptions. Events are not retained when
there are no subscribers.

## Development

Requires Python 3.12+ and uv. Create this repository's environment and install
the package and development dependencies:

```sh
uv sync --locked --python 3.12
```

The protocol dependency uses a public HTTPS Git URL pinned to a commit. The pin
is included in package metadata and `uv.lock`, so installations do not require a
sibling checkout or a package registry release.

Runtime dependencies are `tyvrana-protocol` for shared contracts, `websockets`
for adapter transport, Pydantic for configuration and MCP tool models, the
official `mcp` SDK for the MCP interface, and AnyIO for cancellation shielding
at the SDK lifecycle boundary. Core services remain independent of MCP.
All packages are installed in this repository's `.venv`.

Run the checks and build both package formats:

```sh
uv run --locked pytest
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy
uv build
git diff --check
```

To format Python files, run `uv run --locked ruff format .`.

Tests use lightweight adapters over real loopback WebSockets, the official MCP
SDK's in-process client, and actual stdio subprocesses launched with the command
above. They cover JSON preservation, tool failures, cancellation, shutdown, and
stdout safety. Ephemeral ports, asyncio debug mode, and warnings as errors keep
transport and cleanup checks isolated. Teardown checks for leaked tasks and
unhandled asynchronous failures.

## Durable project continuation

Core stores typed local project meaning across clients and restarts: goals,
important entities and relationships, milestones, issues, validation, application
bindings, revisions and named checkpoints. A fresh client calls `project.continue`
through the normal execution tool with `adapter_id: "core"`; discover the remaining
project contracts lazily. The AI reasons about the workflow while core retrieves a
bounded deterministic packet. Coherent semantic updates use one atomic revision
checked batch. See [project state](docs/project-state.md) for identity, freshness,
persistence, limits and cleanup semantics.

### Client control and application identity

MCP initialization includes the canonical [client control guidance](src/tyvrana_core/mcp/client_control.md)
before tool discovery, on both HTTP and stdio transports. It requires typed adapter
control, an explicitly selected interactive host for real visual work, and visible
milestone checkpoints. Clients that load local project instructions can reference
this same file instead of maintaining a second copy. Instructions are guidance,
not a security boundary; client execution policies remain the client's responsibility.

Application operations target the explicit `adapter_id`; Core never falls back to
another instance. `core` is reserved for semantic project operations. Reconnection
or extension reload requires rediscovery and deliberate target selection; verify
stale semantic resource bindings before continuing.

## Migrating an attestation format

`project.attest(mode="migrate")` upgrades the existing trusted baseline after a
canonical attestation-format correction. Supply distinct `from_format` and
`to_format`, the expected project revision, `trusted_artifact_sha256`, provenance,
and the intended work adapter ID. Core leases an application-owned background
proof host and opens the trusted artifact automatically. Both complete attestations must use the requested
new format, agree on content and resource evidence, and report the file SHA256
already recorded in the baseline. Missing durable file identity, wrong document
lineage, changed files, incomplete evidence and digest disagreement fail closed.
There is no force flag, old hashing implementation, or implicit baseline adoption.

Only the existing baseline metadata changes. Its migration proof derives current
freshness for historically accepted claims whose retained semantic dependency
closure and binding evidence remain unchanged. Normal prerequisite, validation,
resource and issue gates still apply; unproved claims remain stale. The proof is
invalidated by subsequent changes to those claims, observations or resource
fingerprints. Stored semantic records, historical acceptance, checkpoints and the
project revision are untouched. No milestone reacceptance is required.

Repeating the same transition rechecks the proof and makes no metadata or revision
change. A missing baseline belongs to the existing bootstrap workflow. Migration
does not edit or reload an application document.

## Restoring a trusted artifact

`project.restore` explicitly discards an observed divergent working document in
favor of a durable saved baseline or named checkpoint. It requires
`discard_current: true`, the expected project revision, and `expected_current`
with the live host/document sessions, logical project identity, attestation format
and strong digest. Core automatically creates an independent proof host for the
durable target locator. It requires its file SHA256, strong digest and document lineage
to match durable evidence before dispatching the adapter's guarded typed open.
The native adapter rechecks the current state and target file bytes immediately
before loading. Normal mutations retain their existing divergence guard.

Supply `checkpoint_id` for a saved working checkpoint; omit it for the durable
trusted baseline. Core selects the durable artifact locator; the adapter owns
the application-specific guarded open.
Checkpoints capture immutable content and semantic freshness evidence; a target
without saved artifact identity, or with changed semantic claims, is rejected.
The current scope is a single bound document, without semantic graph rebasing.

Restore uses the existing mutation ledger and native job lifecycle. Requests have
a stable `restore_id`; repeat requests return the original result, and
`project.restore_status` observes retained work without repeating a destructive
load. File-open reconnects resume status observation only. A failed or interrupted
load never promotes its content to trusted state. Post-load strong attestation
and resource evidence must match the independent proof before Core restores the
working head, runtime attachment and checkpoint freshness. Historical milestone
acceptance and checkpoints remain immutable; later unproved claims stay stale.

An actual content/freshness restore advances the project revision once. An
already-current target or exact runtime reattachment does not advance it unless
semantic freshness must change. Fresh application processes are supported;
previous process, adapter and runtime session identifiers are not required.

## Managed independent proof hosts

Bootstrap, format migration, restore and reconciliation select only the work adapter.
Core issues an unguessable lease, verifies the registered application/build/document
identity, and releases the proof process after success, failure, cancellation or timeout.
Adapters implement typed `proof_host_start`, `proof_host_status` and `proof_host_stop`
contracts. They own process launch, artifact opening, bounded termination and orphan
protection; Core contains no application command lines. Managed proof adapters are
excluded from normal discovery and cannot receive client operations or implicit routes.

Saved baselines and checkpoints retain artifact locators alongside authoritative SHA256
and content evidence. A bootstrap from a separate historical artifact uses explicit
`trusted_artifact_locator`. Locators are reopening hints, never document identity.
Unavailable artifacts or capabilities fail closed. No client-created proof application
is required or supported.

Proof workflows return a completed result or retained project job after 20 seconds.
Observe `project.attest_status`, `project.restore_status` or `project.reconcile_status`;
the corresponding `_cancel` operation waits for cleanup. Proof jobs are bounded to
ten minutes, with one proof per work host and four simultaneous leases. Core shutdown cancels retained work; a restarted Core reports interrupted
work rather than adopting its result. A later workflow creates a new proof process.
