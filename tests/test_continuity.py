"""Persisted acceptance gates with real adapter transport and strong evidence."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import pytest
from tyvrana_protocol import (
    AdapterEvent,
    AdapterRegistration,
    DocumentAttestation,
    DocumentAttestationJob,
    DocumentAttestationResponse,
    OperationContract,
    OperationRequest,
    OperationSuccess,
)
from websockets.asyncio.client import connect

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.models import ApplyInput, BindingObservation
from tyvrana_core.projects.store import ProjectError

from .helpers import FakeAdapter, eventually


def evidence(
    host: str = "host", document: str = "loaded", digest: str = "a" * 64
) -> dict[str, Any]:
    return dict(
        host_session_id=host,
        document_session_id=document,
        project_id="saved",
        algorithm="sha256",
        format="fixture-canonical",
        digest=digest,
        status="complete",
        resource_count=3,
        omissions=[],
        elapsed_ms=1.0,
        file_sha256="b" * 64,
    )


@asynccontextmanager
async def connected(
    core: AdapterServer,
    instance: str,
    value: dict[str, Any],
    *,
    asynchronous: bool = False,
) -> AsyncIterator[None]:
    async with connect(core.uri, proxy=None) as socket:
        fake = FakeAdapter(socket)
        operation = OperationContract(
            name="editor.document.attest",
            description="Strong material evidence",
            tags=("document_attestation",),
            arguments_schema={"type": "object"},
            result_schema=DocumentAttestation.model_json_schema(),
            effect="read_only",
            execution="synchronous",
        )
        operations: tuple[OperationContract, ...] = (operation,)
        if asynchronous:
            response_schema = DocumentAttestationResponse.model_json_schema()
            operations = (
                operation.model_copy(
                    update={
                        "execution": "job_start",
                        "result_schema": response_schema,
                    }
                ),
                OperationContract(
                    name="editor.document.status",
                    description="Observe work",
                    tags=("document_attestation_status",),
                    arguments_schema={"type": "object"},
                    result_schema=DocumentAttestationJob.model_json_schema(),
                    effect="read_only",
                    execution="job_status",
                ),
            )
        with core.events.subscribe() as events:
            await fake.send(
                AdapterRegistration(
                    type="adapter.register",
                    instance_id=instance,
                    application="editor",
                    project_id="saved",
                    operations=operations,
                )
            )
            await fake.send(
                AdapterEvent(type="adapter.event", event="test.ready", payload=None)
            )
            await anext(events)

        async def respond() -> None:
            while True:
                request = await fake.receive()
                assert isinstance(request, OperationRequest)
                await fake.send(
                    OperationSuccess(
                        type="operation.success",
                        request_id=request.request_id,
                        result=(
                            dict(job_id="job", state="queued", poll_after_seconds=0.1)
                            if request.operation == "editor.document.attest"
                            else dict(
                                job_id="job",
                                state="completed",
                                revision=1,
                                result=value,
                            )
                        )
                        if asynchronous
                        else value,
                    )
                )

        task = asyncio.create_task(respond())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    await eventually(
        lambda: all(a.instance_id != instance for a in core.registry.list())
    )


async def establish(core: AdapterServer, *, capture: bool = True) -> tuple[str, int]:
    project = await core.projects.execute(
        "project.create", {"title": "Fixture", "goal": "Preserve verified content"}
    )
    key = project.model_dump()["id"]
    batch = dict(
        expected_revision=1,
        upsert=[
            dict(kind="entity", id="part", label="Part"),
            dict(
                kind="document",
                id="doc",
                label="Document",
                application="editor",
                application_project_id="saved",
                adapter_id="initial",
            ),
            dict(
                kind="binding",
                id="binding",
                label="Part binding",
                entity_id="part",
                document_id="doc",
                resource_kind="mesh",
                resource_id="mesh",
            ),
            dict(
                kind="evidence",
                id="proof",
                label="Inspected part",
                storage="application",
                binding_id="binding",
                summary="Measured dimensions meet the declared tolerance.",
            ),
            dict(
                kind="validation",
                id="check",
                label="Geometry checked",
                validation_type="inspection",
                entity_ids=["part"],
                evidence_ids=["proof"],
                summary="Measured geometry matches the reference.",
                status="passed",
                freshness="current",
            ),
            dict(
                kind="milestone",
                id="stage",
                label="Approved geometry",
                entity_ids=["part"],
                document_ids=["doc"],
                validation_ids=["check"],
                acceptance="Dimensions and shape verified",
                status="accepted",
            ),
        ],
    )
    connection = core.registry.get("initial").connection_id
    # The store's real acceptance gates are exercised; no mocked freshness flag.
    core.projects.store.apply(
        key,
        ApplyInput.model_validate(batch),
        {"doc": connection},
        observations={
            "binding": BindingObservation(
                state="verified", fingerprint="structural", connection_id=connection
            )
        },
    )
    if capture:
        await core.projects.execute(
            "project.attest",
            dict(
                project_id=key,
                document_id="doc",
                adapter_id="initial",
                expected_revision=2,
            ),
        )
    return key, 2


async def state(core: AdapterServer, key: str) -> tuple[str, int]:
    packet = (
        await core.projects.execute("project.continue", {"project_id": key})
    ).model_dump()
    record = next(
        r["record"] for r in packet["records"] if r["record"]["id"] == "stage"
    )
    return record["status"], packet["project"]["revision"]


async def test_reload_reconnect_content_document_host_and_restart(
    tmp_path: Path,
) -> None:
    config = CoreConfig(port=0, state_directory=str(tmp_path))
    async with AdapterServer(config) as core:
        async with connected(core, "initial", evidence()):
            key, revision = await establish(core)
            assert await state(core, key) == ("accepted", revision)
        for instance in ["reload-a", "reload-b", "reload-b"]:
            async with connected(core, instance, evidence()):
                assert await state(core, key) == ("accepted", revision)
        for changed in [
            evidence(document="other"),
            evidence(digest="c" * 64),
            evidence(host="another"),
        ]:
            async with connected(core, "replacement", changed):
                assert await state(core, key) == ("invalidated", revision)
        async with connected(
            core, "restart", evidence(host="new-process", document="new-load")
        ):
            assert await state(core, key) == ("invalidated", revision)
            await core.projects.execute(
                "project.attest",
                dict(
                    project_id=key,
                    document_id="doc",
                    adapter_id="restart",
                    expected_revision=revision,
                    mode="reattach",
                ),
            )
            assert await state(core, key) == ("accepted", revision)
        async with connected(
            core, "bad-restart", evidence(host="third", digest="d" * 64)
        ):
            with pytest.raises(ProjectError, match="matching durable"):
                await core.projects.execute(
                    "project.attest",
                    dict(
                        project_id=key,
                        document_id="doc",
                        adapter_id="bad-restart",
                        expected_revision=revision,
                        mode="reattach",
                    ),
                )
    # Core restart retains strong durable evidence, not a cached transport mapping.
    async with AdapterServer(config) as core:
        async with connected(
            core, "fresh-connection", evidence(host="new-process", document="new-load")
        ):
            assert await state(core, key) == ("accepted", revision)


async def test_bootstrap_requires_independent_exact_trusted_artifact(
    tmp_path: Path,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence()):
            key, revision = await establish(core, capture=False)
        async with connected(core, "live", evidence()):
            with pytest.raises(ProjectError, match="stale un-attested"):
                await core.projects.execute(
                    "project.attest",
                    dict(
                        project_id=key,
                        document_id="doc",
                        adapter_id="live",
                        expected_revision=revision,
                    ),
                )
            async with connected(core, "proof", evidence(host="independent")):
                arguments: dict[str, Any] = dict(
                    project_id=key,
                    document_id="doc",
                    adapter_id="live",
                    expected_revision=revision,
                    mode="bootstrap",
                    proof_adapter_id="proof",
                    trusted_artifact_sha256="c" * 64,
                    provenance="Previously preserved accepted artifact SHA256",
                )
                with pytest.raises(ProjectError, match="not proven equivalent"):
                    await core.projects.execute("project.attest", arguments)
                assert await state(core, key) == ("invalidated", revision)
                arguments["trusted_artifact_sha256"] = "b" * 64
                await core.projects.execute("project.attest", arguments)
                assert await state(core, key) == ("accepted", revision)
                with pytest.raises(ProjectError, match="another live host"):
                    await core.projects.execute(
                        "project.attest",
                        dict(
                            project_id=key,
                            document_id="doc",
                            adapter_id="proof",
                            expected_revision=revision,
                            mode="reattach",
                        ),
                    )

                with pytest.raises(ProjectError, match="explicit strong reattachment"):
                    await core.projects.execute(
                        "project.attest",
                        dict(
                            project_id=key,
                            document_id="doc",
                            adapter_id="proof",
                            expected_revision=revision,
                            mode="capture",
                        ),
                    )


async def test_async_attestation_preserves_acceptance(tmp_path: Path) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence(), asynchronous=True):
            key, revision = await establish(core)
            assert await state(core, key) == ("accepted", revision)
