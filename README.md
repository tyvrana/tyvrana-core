# tyvrana-core

Local orchestration and adapter connections for Tyvrana. Shared application
contracts are defined in the independent
[tyvrana-protocol](https://github.com/tyvrana/tyvrana-protocol) package.

Core currently accepts local WebSocket adapter connections, registers running
application instances, routes operations, correlates responses, and distributes
adapter events. Application adapters are separate repositories.

## Run the adapter server

After installing the development environment below:

```sh
uv run --locked tyvrana-core
uv run --locked tyvrana-core --host 127.0.0.1 --port 8765
uv run --locked tyvrana-core --help
```

The default listener is `ws://127.0.0.1:8765`. Use port `0` to select an available
port. The CLI logs the listening address and stops cleanly on Ctrl+C or SIGTERM
on systems supporting asyncio signal handlers.

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

Tests use lightweight adapters over real loopback WebSockets, ephemeral ports,
asyncio debug mode, and warnings as errors. Teardown checks for leaked tasks and
unhandled asynchronous failures.
