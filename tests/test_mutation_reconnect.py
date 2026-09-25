"""A retained mutation receipt remains observable through a transport transition."""

import asyncio
from pathlib import Path
from typing import Any

import pytest
from tyvrana_protocol import AdapterRegistration, JsonValue, OperationSuccess
from tyvrana_protocol.mutations import DocumentMutationRequest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.errors import AdapterDisconnected, AdapterNotFound

from .helpers import contract
from .test_continuity import evidence


@pytest.mark.parametrize("failure", [AdapterNotFound, AdapterDisconnected])
@pytest.mark.parametrize("cancel", [False, True])
async def test_retained_mutation_status_waits_for_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[AdapterNotFound] | type[AdapterDisconnected],
    cancel: bool,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        registration = AdapterRegistration(
            type="adapter.register",
            instance_id="editor",
            application="editor",
            application_version="1",
            project_id="saved",
            project_path="/old.file",
            operations=(
                contract("editor.mutate").model_copy(
                    update={"tags": ("document_mutation",)}
                ),
                contract("editor.status").model_copy(
                    update={"tags": ("document_mutation_status",)}
                ),
            ),
        )
        calls: list[str] = []
        waiting = asyncio.Event()
        reconnected = asyncio.Event()
        value = evidence()
        value["resource_scope"] = "closure"

        async def dispatch(**kwargs: Any) -> OperationSuccess:
            operation = kwargs["operation"]
            calls.append(operation)
            result: JsonValue
            if operation == "editor.mutate":
                result = dict(job_id="save", state="running", poll_after_seconds=0.1)
            elif not reconnected.is_set():
                raise failure("editor")
            else:
                result = dict(
                    job_id="save",
                    state="completed",
                    result=dict(
                        mutation_id="save",
                        result={"saved": True},
                        before=value,
                        after=value,
                    ),
                )
            return OperationSuccess(
                type="operation.success", request_id="wire", result=result
            )

        async def change(revision: int, timeout: float) -> None:
            waiting.set()
            await reconnected.wait()

        monkeypatch.setattr(core.dispatcher, "execute", dispatch)
        monkeypatch.setattr(core.registry, "wait_for_change", change)
        args = DocumentMutationRequest(
            mutation_id="save",
            operation="editor.save",
            arguments={},
            host_session_id="host",
            document_session_id="loaded",
            project_id="saved",
            format="fixture-canonical",
            digest="a" * 64,
        )
        work = asyncio.create_task(core.projects.mutations.guarded(registration, args))
        try:
            ready = asyncio.create_task(waiting.wait())
            try:
                async with asyncio.timeout(1):
                    await asyncio.wait(
                        {ready, work}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if work.done():
                        work.result()
            finally:
                ready.cancel()
                await asyncio.gather(ready, return_exceptions=True)
            assert calls == ["editor.mutate", "editor.status"]
            if cancel:
                work.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await work
                assert calls == ["editor.mutate", "editor.status"]
                return
            reconnected.set()
            result = await work
            assert result.result == {"saved": True}
            assert calls == ["editor.mutate", "editor.status", "editor.status"]
        finally:
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)


async def test_real_transport_metadata_reconnect_retains_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tyvrana_protocol import AdapterEvent, OperationRequest
    from websockets.asyncio.client import connect

    from .helpers import FakeAdapter

    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        registration = AdapterRegistration(
            type="adapter.register",
            instance_id="editor",
            application="editor",
            application_version="1",
            project_id="saved",
            project_path="/old.file",
            operations=(
                contract("editor.mutate").model_copy(
                    update={"tags": ("document_mutation",)}
                ),
                contract("editor.status").model_copy(
                    update={"tags": ("document_mutation_status",)}
                ),
            ),
        )
        waiting = asyncio.Event()
        original = core.registry.wait_for_change

        async def wait(revision: int, timeout: float) -> None:
            waiting.set()
            await original(revision, timeout)

        monkeypatch.setattr(core.registry, "wait_for_change", wait)
        args = DocumentMutationRequest(
            mutation_id="save",
            operation="editor.save",
            arguments={},
            host_session_id="host",
            document_session_id="loaded",
            project_id="saved",
            format="fixture-canonical",
            digest="a" * 64,
        )
        work = None
        try:
            async with connect(core.uri, proxy=None) as socket:
                fake = FakeAdapter(socket)
                with core.events.subscribe() as events:
                    await fake.send(registration)
                    await fake.send(
                        AdapterEvent(
                            type="adapter.event", event="test.ready", payload=None
                        )
                    )
                    await anext(events)
                work = asyncio.create_task(
                    core.projects.mutations.guarded(registration, args)
                )
                request = await fake.receive()
                assert isinstance(request, OperationRequest)
                assert request.operation == "editor.mutate"
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result={
                            "job_id": "save",
                            "state": "running",
                            "poll_after_seconds": 0.1,
                        },
                    )
                )
            # Hold the real reconnect until Core observes the routing gap. No sleep
            # hides the transition and no original mutation is replayed.
            async with asyncio.timeout(2):
                await waiting.wait()
            assert not work.done()
            async with connect(core.uri, proxy=None) as socket:
                fake = FakeAdapter(socket)
                await fake.send(
                    registration.model_copy(update={"project_path": "/new.file"})
                )
                request = await fake.receive()
                assert isinstance(request, OperationRequest)
                assert request.operation == "editor.status"
                assert request.arguments == {"job_id": "save"}
                value = evidence()
                value["resource_scope"] = "closure"
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result={
                            "job_id": "save",
                            "state": "completed",
                            "result": {
                                "mutation_id": "save",
                                "before": value,
                                "after": value,
                                "result": {"saved": True},
                            },
                        },
                    )
                )
                assert (await work).result == {"saved": True}
                assert core.registry.revision == 3  # register, remove, re-register
        finally:
            if work is not None:
                work.cancel()
                await asyncio.gather(work, return_exceptions=True)
