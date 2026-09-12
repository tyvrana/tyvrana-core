import asyncio
import logging

import pytest
from tyvrana_protocol import (
    AdapterRegistration,
    CancelRequest,
    OperationRequest,
    encode_message,
)
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import InvalidStatus
from websockets.typing import Origin

from tyvrana_core import AdapterServer, CoreConfig

from .helpers import adapter, eventually


async def test_server_lifecycle_and_repeated_start_stop() -> None:
    server = AdapterServer(CoreConfig(port=0))
    with pytest.raises(RuntimeError):
        _ = server.uri
    for _ in range(3):
        await server.start()
        uri = server.uri
        await server.start()
        assert server.uri == uri
        async with adapter(server):
            assert len(server.registry.list()) == 1
        await server.stop()
        await server.stop()
        with pytest.raises(RuntimeError):
            _ = server.uri


@pytest.mark.parametrize("text", [False, True])
async def test_registration_metadata_and_disconnect(
    server: AdapterServer, text: bool
) -> None:
    async with adapter(server, text=text):
        info = server.registry.get("adapter-a")
        assert info.connected
        assert info.registration.application == "Example Editor"
        assert info.registration.application_version == "2026.9"
        assert info.registration.project_path == "projects/example.project"
        assert info.registration.operations == ("document.inspect",)
        assert server.registry.list() == (info,)
        assert server.registry.supporting("document.inspect") == (info,)
        assert server.registry.supporting("missing.operation") == ()
    assert server.registry.list() == ()


async def test_multiple_adapters_and_operation_lookup(server: AdapterServer) -> None:
    async with (
        adapter(server, "first"),
        adapter(server, "second", operations=("asset.describe",)),
    ):
        assert {info.instance_id for info in server.registry.list()} == {
            "first",
            "second",
        }
        assert [
            info.instance_id for info in server.registry.supporting("asset.describe")
        ] == ["second"]


async def test_duplicate_active_id_does_not_remove_original(
    server: AdapterServer,
) -> None:
    async with adapter(server) as original:
        async with connect(server.uri, proxy=None) as duplicate:
            await duplicate.send(
                encode_message(server.registry.get("adapter-a").registration)
            )
            async with asyncio.timeout(2):
                await duplicate.wait_closed()
            assert duplicate.close_code == 1008
        assert server.registry.get("adapter-a").connected
        await original.websocket.ping()


async def test_instance_id_can_be_reused_after_disconnect(
    server: AdapterServer,
) -> None:
    async with adapter(server):
        pass
    async with adapter(server):
        assert server.registry.get("adapter-a").connected


@pytest.mark.parametrize(
    "wire",
    [
        encode_message(CancelRequest(type="operation.cancel", request_id="r")),
        encode_message(
            OperationRequest(
                type="operation.request", request_id="r", operation="a.b", arguments={}
            )
        ),
        b'{"type":"adapter.register","instance_id":"a"}',
        b'{"type":"adapter.register","instance_id":"a","application":"App","operations":["a.b","a.b"]}',
        b'{"type":"adapter.register","instance_id":"a","application":"App","operations":[],"extra":1}',
        b"not json",
        b"\xff",
        b'{"type":"adapter.event","event":"a.b","payload":'
        + b"[" * 10000
        + b"0"
        + b"]" * 10000
        + b"}",
    ],
)
async def test_invalid_first_message_is_rejected(
    server: AdapterServer, wire: bytes
) -> None:
    async with connect(server.uri, proxy=None) as websocket:
        await websocket.send(wire)
        async with asyncio.timeout(2):
            await websocket.wait_closed()
        assert websocket.close_code == 1008
    assert server.registry.list() == ()


async def test_registration_deadline_disconnects_silent_client() -> None:
    async with AdapterServer(CoreConfig(port=0, registration_timeout=0.03)) as server:
        async with connect(server.uri, proxy=None) as websocket:
            async with asyncio.timeout(2):
                await websocket.wait_closed()
            assert websocket.close_code == 1008
        assert server.registry.list() == ()


async def test_empty_operation_registration(server: AdapterServer) -> None:
    async with adapter(server, operations=()):
        assert server.registry.get("adapter-a").registration.operations == ()


async def test_abrupt_disconnect_removes_adapter(server: AdapterServer) -> None:
    async with adapter(server) as fake:
        fake.websocket.transport.abort()
        await eventually(lambda: not server.registry.list())


async def test_shutdown_closes_registered_and_unregistered_connections(
    server: AdapterServer,
) -> None:
    async with adapter(server) as fake, connect(server.uri, proxy=None) as unregistered:
        await server.stop()
        assert fake.websocket.close_code == 1001
        await unregistered.wait_closed()
        assert unregistered.close_code == 1001


@pytest.mark.parametrize(
    "wire",
    [
        b"{",
        b"\xff",
        b'{"type":"adapter.event","event":"a.b","payload":{},"extra":true}',
    ],
)
async def test_malformed_post_registration_data(
    server: AdapterServer, wire: bytes
) -> None:
    async with adapter(server) as fake:
        await fake.websocket.send(wire)
        async with asyncio.timeout(2):
            await fake.websocket.wait_closed()
        assert fake.websocket.close_code == 1008


@pytest.mark.parametrize(
    "message",
    [
        AdapterRegistration(
            type="adapter.register",
            instance_id="again",
            application="App",
            operations=(),
        ),
        OperationRequest(
            type="operation.request", request_id="r", operation="a.b", arguments={}
        ),
        CancelRequest(type="operation.cancel", request_id="r"),
    ],
)
async def test_invalid_post_registration_direction(
    server: AdapterServer,
    message: AdapterRegistration | OperationRequest | CancelRequest,
) -> None:
    async with adapter(server) as fake:
        await fake.send(message)
        async with asyncio.timeout(2):
            await fake.websocket.wait_closed()
        assert fake.websocket.close_code == 1008


async def test_oversized_message_is_rejected() -> None:
    async with AdapterServer(CoreConfig(port=0, max_message_size=512)) as server:
        async with adapter(server) as fake:
            await fake.websocket.send(b"x" * 513)
            async with asyncio.timeout(2):
                await fake.websocket.wait_closed()
            assert fake.websocket.close_code == 1009


async def test_invalid_utf8_in_text_frame_is_rejected(server: AdapterServer) -> None:
    async with adapter(server) as fake:
        await fake.websocket.send(b"\xff", text=True)
        async with asyncio.timeout(2):
            await fake.websocket.wait_closed()
        assert fake.websocket.close_code == 1007


async def test_browser_origin_is_rejected(server: AdapterServer) -> None:
    with pytest.raises(InvalidStatus) as caught:
        async with connect(
            server.uri, proxy=None, origin=Origin("https://example.test")
        ):
            pytest.fail("Unexpected browser connection")
    assert caught.value.response.status_code == 403


async def test_registration_racing_shutdown_is_not_added(
    server: AdapterServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="tyvrana_core.server")
    received = asyncio.Event()
    release = asyncio.Event()
    original_recv = ServerConnection.recv

    async def delayed_recv(
        websocket: ServerConnection, decode: bool | None = None
    ) -> str | bytes:
        data = await original_recv(websocket, decode=decode)
        received.set()
        await release.wait()
        return data

    monkeypatch.setattr(ServerConnection, "recv", delayed_recv)
    async with connect(server.uri, proxy=None) as websocket:
        await websocket.send(
            encode_message(
                AdapterRegistration(
                    type="adapter.register",
                    instance_id="too-late",
                    application="App",
                    operations=(),
                )
            )
        )
        async with asyncio.timeout(2):
            await received.wait()
        stopping = asyncio.create_task(server.stop())
        await eventually(lambda: server._stopping)
        release.set()
        async with asyncio.timeout(2):
            await stopping
        assert server.registry.list() == ()
        assert "Adapter registered: too-late" not in caplog.text
        assert websocket.close_code == 1001


async def test_cancelled_stop_finishes_connection_cleanup(
    server: AdapterServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with adapter(server):
        connection = server.registry._get_connection("adapter-a")
        original_close = connection._websocket.close
        entered = asyncio.Event()
        release = asyncio.Event()

        async def delayed_close(code: int = 1000, reason: str = "") -> None:
            entered.set()
            await release.wait()
            await original_close(code, reason)

        monkeypatch.setattr(connection._websocket, "close", delayed_close)
        stopping = asyncio.create_task(server.stop())
        async with asyncio.timeout(2):
            await entered.wait()
        stopping.cancel()
        release.set()
        async with asyncio.timeout(2):
            with pytest.raises(asyncio.CancelledError):
                await stopping
        assert not connection.connected
        assert server.registry.list() == ()
        with pytest.raises(RuntimeError):
            _ = server.uri


async def test_failed_bind_can_be_retried() -> None:
    async with AdapterServer(CoreConfig(port=0)) as first:
        port = int(first.uri.rsplit(":", 1)[1])
        second = AdapterServer(CoreConfig(port=port))
        with pytest.raises(OSError):
            await second.start()
    async with second:
        async with adapter(second):
            assert second.registry.get("adapter-a").connected
