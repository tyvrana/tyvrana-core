import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from tyvrana_protocol import (
    AdapterEvent,
    AdapterRegistration,
    JsonValue,
    Message,
    OperationContract,
    OperationSuccess,
    decode_message,
    encode_message,
)
from websockets.asyncio.client import ClientConnection, connect

from tyvrana_core import AdapterServer


def contract(name: str) -> OperationContract:
    return OperationContract(
        name=name,
        description="Inspect example state.",
        arguments_schema={"type": "object", "additionalProperties": False},
        result_schema={"type": "object"},
        effect="read_only",
        execution="synchronous",
    )


async def eventually(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(2):
        while not predicate():  # noqa: ASYNC110 - Observe a snapshot after transport close.
            await asyncio.sleep(0)


class FakeAdapter:
    def __init__(self, websocket: ClientConnection) -> None:
        self.websocket = websocket

    async def send(self, message: Message) -> None:
        wire = encode_message(message)
        await self.websocket.send(wire.decode())

    async def receive(self) -> Message:
        async with asyncio.timeout(2):
            data = await self.websocket.recv()
        return decode_message(data.encode() if isinstance(data, str) else data)


@asynccontextmanager
async def adapter(
    server: AdapterServer,
    instance_id: str = "adapter-a",
    *,
    operations: tuple[str, ...] = ("document.inspect",),
) -> AsyncIterator[FakeAdapter]:
    async with connect(server.uri, proxy=None, close_timeout=0.2) as websocket:
        fake = FakeAdapter(websocket)
        with server.events.subscribe() as subscription:
            await fake.send(
                AdapterRegistration(
                    type="adapter.register",
                    instance_id=instance_id,
                    application="Example Editor",
                    application_version="2026.9",
                    project_path="projects/example.project",
                    operations=tuple(contract(name) for name in operations),
                ),
            )
            # An event after registration is a deterministic receive-loop barrier.
            await fake.send(
                AdapterEvent(type="adapter.event", event="test.ready", payload=None)
            )
            async with asyncio.timeout(2):
                while (await anext(subscription)).adapter_id != instance_id:
                    pass
        yield fake
    await eventually(
        lambda: all(info.instance_id != instance_id for info in server.registry.list())
    )


def execute(
    server: AdapterServer, *, adapter_id: str = "adapter-a", arguments: JsonValue = None
) -> asyncio.Task[OperationSuccess]:
    return asyncio.create_task(
        server.dispatcher.execute(
            adapter_id=adapter_id, operation="document.inspect", arguments=arguments
        )
    )
