# tyvrana-core

Local orchestration and adapter connections for Tyvrana. Shared application
contracts are defined in the independent
[tyvrana-protocol](https://github.com/tyvrana/tyvrana-protocol) package.

Core currently accepts local WebSocket adapter connections, registers running
application instances, routes operations, correlates responses, and distributes
adapter events internally. Its MCP interface lets local clients discover connected
adapters and execute their advertised operations. Application adapters are
separate repositories. Binary artifacts are verified and held in bounded temporary
storage; PNG and JPEG artifacts can be returned as MCP image content.

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
Both modes accept `--host` and `--port` for the adapter listener. Use
`uv run --locked tyvrana-core mcp --help` for MCP mode options. The CLI requires
an explicit `serve` or `mcp` subcommand.

Normal logs go to stderr. In MCP mode, stdout contains only MCP transport data.
The MCP lifespan starts the existing `AdapterServer`, which owns the registry,
dispatcher, event broker, and temporary artifact store. Session exit, stdin closure, Ctrl+C, or SIGTERM
shuts down the listener, closes adapter connections and event subscriptions,
fails pending operations, and joins outstanding work. Importing the MCP package
does not start services.

### Tools

Exactly two tools are available. Both publish Pydantic-generated input and output
JSON schemas and reject unexpected input fields. Successful responses provide
the object below as MCP `structuredContent` and as JSON in a text content block.

`tyvrana_list_adapters` takes `{}` (arguments may also be omitted) and returns:

```json
{
  "adapters": [
    {
      "instance_id": "example-editor",
      "application": "Example Editor",
      "application_version": "2026.9",
      "project_path": "projects/example.project",
      "operations": ["document.inspect"]
    }
  ]
}
```

`application_version` and `project_path` are omitted when the adapter did not
supply them. Adapters are sorted by instance ID and operations by name. With no
connected adapters the result is `{"adapters": []}`. Results are independent
snapshots of connected adapter metadata.

`tyvrana_execute_operation` requires all three fields:

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

Without artifacts, the output is `{"result": <JSON value>}`. Both `arguments` and `result` accept the
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
and advertised operations. A duplicate active instance ID is rejected without
disturbing the original connection. IDs can be reused after disconnection.

Each WebSocket text message contains exactly one canonical protocol JSON control
object. Binary messages carry only artifact chunks using `tyvrana-protocol`
framing helpers. A chunk is at most 65,536 payload bytes plus its 24-byte header.
WebSocket fragments are reassembled into one bounded message before decoding.
All contracts, validation, and wire serialization use `tyvrana-protocol`.

After registration, adapters may send only `operation.success`,
`operation.failure`, `adapter.event`, `artifact.begin`, `artifact.complete`,
`artifact.abort`, or binary artifact chunks. Malformed protocol data and invalid
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
| `max_message_size` | 1 MiB |
| `max_artifact_size` | 16 MiB per artifact |
| `max_artifact_storage` | 64 MiB reserved + completed across the runtime |
| `max_artifact_entries` | 128 reserved + completed across the runtime |
| `max_artifact_transfers` | 4 active incoming transfers per adapter |
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

The operation deadline covers sending and awaiting the response. A deadline
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

Core reserves the entire declared size before acknowledging `artifact.begin` with
`artifact.ready`. Chunks are written incrementally to a private incomplete file.
Offsets must start at zero and advance exactly; size and SHA-256 are verified on
`artifact.complete`. Only then is the file atomically renamed to completed storage
and `artifact.accepted` sent. Success must reference exactly the descriptors
completed for that request, on that connection. No partial file is retrievable as
a completed artifact. The protocol permits up to eight artifacts per result.

Limit, offset, size, hash, and ownership failures become an
`artifact_transfer_failed` operation error and `artifact.abort`. Core cancels the
related operation and deletes all of its artifacts. Other operations keep running.
Unknown late chunks receive `artifact.abort`; invalid framing and transfer-ID
collisions close the offending adapter connection. No failed transfer can replace
another artifact. The operation deadline includes transfer and acknowledgements.

The runtime's `artifacts` (`ArtifactStore`) exposes `metadata(artifact_id)`,
`open(artifact_id)` (a binary file object to close after use), and
`release(artifact_id)`. It exposes no storage paths in the wire protocol or MCP.
For programmatic dispatcher users, successful artifacts remain until explicit
release or runtime shutdown, subject to the entry/byte bounds. Full stores reject
new admission; there is no eviction or permanent storage. Cancelled or failed
requests lose all their artifacts, including verified files awaiting response
delivery. Adapter disconnect removes unfinished requests; already returned
successful artifacts remain available. Shutdown removes the entire store.

MCP results preserve `{"result": <JSON value>, "artifacts": [<descriptors>]}` in
`structuredContent` and a text block, then append an actual SDK `ImageContent`
block for each `image/png` or `image/jpeg` artifact in descriptor order. MCP's
standard `data` field contains its normal base64 wire encoding; Tyvrana JSON
payloads contain no encoded image data. Other media types currently return
metadata only, without an invented resource link or byte retrieval capability.
MCP releases all associated files after constructing its response, also on error
or cancellation. Descriptors in MCP output describe delivered content; they are
not persistent retrieval handles.

The inline limit applies to the sum of raw PNG/JPEG bytes. Exceeding it returns
`image_too_large` and releases the files. Base64 and JSON serialization add memory
and wire overhead; large production images need future resource/file semantics,
not unlimited inline responses. Use modest render dimensions for immediate visual
feedback. Limits are configured through `CoreConfig`; the CLI uses these defaults.
No additional services, database, or storage dependencies are required.

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
