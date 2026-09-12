# tyvrana-core

Local orchestration and adapter connections for Tyvrana. Shared application
contracts are defined in the independent
[tyvrana-protocol](https://github.com/tyvrana/tyvrana-protocol) package.

Core currently accepts local WebSocket adapter connections, registers running
application instances, routes operations, correlates responses, and distributes
adapter events internally. Its MCP interface lets local clients discover connected
adapters and execute their advertised operations. Application adapters are
separate repositories; no application integration is included yet.

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
dispatcher, and event broker. Session exit, stdin closure, Ctrl+C, or SIGTERM
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

The output is `{"result": <JSON value>}`. Both `arguments` and `result` accept the
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

Each WebSocket message contains exactly one canonical protocol JSON object.
Core sends UTF-8 JSON in binary WebSocket messages and accepts either binary UTF-8
JSON or text JSON from adapters. All validation and serialization use
`tyvrana-protocol`.

After registration, adapters may send only `operation.success`,
`operation.failure`, or `adapter.event`. Malformed protocol data and invalid
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

The WebSocket opening handshake also uses the registration timeout. The listener
accepts clients without an `Origin` header; browser-origin connections are
rejected. Message size, send time, and close time are bounded. A failed, stalled,
or interrupted send retires the connection and fails its pending operations.

## Programmatic use

`AdapterServer` owns a registry, dispatcher, and event broker. Use it on one
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
from tyvrana_protocol import JsonValue


async def inspect_document(server: AdapterServer, adapter_id: str) -> JsonValue:
    return await server.dispatcher.execute(
        adapter_id=adapter_id,
        operation="document.inspect",
        arguments={},
        timeout=10.0,
    )
```

`document.inspect` is an illustrative operation name, not a core command.
Dispatch verifies the adapter and advertised operation, creates a UUID request
ID, registers pending state before sending, and returns the JSON result.
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
