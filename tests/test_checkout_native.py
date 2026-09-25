"""Optional packaged Blender/MCP checkpoint, abandonment and restart regression.

Set TYVRANA_BLENDER_PACKAGE to an already validated extension archive. No package
build, shared profile, visible host or existing project is used by this test.
"""

import asyncio
import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

pytestmark = pytest.mark.skipif(
    not os.environ.get("TYVRANA_BLENDER_PACKAGE"),
    reason="Requires a validated Blender extension and Blender executable",
)

HOST = """import importlib, os, time
import addon_utils
from pathlib import Path
import bpy
# Select/validate the endpoint before enabling. Installation never persists enablement.
port = int(os.environ["CHECKOUT_TEST_PORT"])
assert 1 <= port <= 65535
module = "bl_ext.user_default.tyvrana_blender"
addon_utils.enable(module, default_set=True)
lifecycle = importlib.import_module(module + ".lifecycle")
assert lifecycle._backend is not None
# Registration installs a deferred timer; no networking may precede configuration.
assert lifecycle._backend._runtime is None
bpy.context.preferences.addons[module].preferences.port = port
print("FIXTURE_ENDPOINT", port, "NETWORK_NOT_STARTED", flush=True)
control = Path(os.environ["CHECKOUT_TEST_CONTROL"])
deadline = time.monotonic() + 600
try:
    while time.monotonic() < deadline:
        lifecycle._backend.pump()
        if os.environ.get("CHECKOUT_TEST_FAIL_START") == "1":
            raise RuntimeError("Injected fixture startup failure")
        if bpy.app.timers.is_registered(lifecycle._poll):
            if lifecycle._poll() is None:
                bpy.app.timers.unregister(lifecycle._poll)
        if (control / "stop").exists():
            break
        time.sleep(0.02)
    else:
        raise RuntimeError("Native checkout fixture exceeded deadline")
finally:
    runtime = lifecycle._backend._runtime
    worker = runtime.worker if runtime is not None else None
    lifecycle.disable()
    assert lifecycle._backend is None
    assert worker is None or worker.process.returncode == 0
    print("FIXTURE_CLEANED", flush=True)
"""


@pytest.mark.parametrize("repetition", range(3))
async def test_packaged_checkpoint_checkout(tmp_path: Path, repetition: int) -> None:
    package = await asyncio.to_thread(
        Path(os.environ["TYVRANA_BLENDER_PACKAGE"]).resolve
    )
    env = {
        **os.environ,
        "BLENDER_USER_RESOURCES": str(tmp_path / "profile"),
        "XDG_CACHE_HOME": str(tmp_path / "cache"),
        "TMPDIR": str(tmp_path),
    }
    calls: list[dict[str, Any]] = []
    outcomes: dict[str, Any] = {}
    install = await asyncio.create_subprocess_exec(
        "blender",
        "--factory-startup",
        "--command",
        "extension",
        "install-file",
        "--repo",
        "user_default",
        str(package),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await asyncio.wait_for(install.communicate(), 45)
    assert install.returncode == 0, output.decode()
    host_script = tmp_path / "host.py"
    host_script.write_text(HOST)
    state_path = tmp_path / "state"
    with (tmp_path / "core.log").open("w") as log:
        params = StdioServerParameters(
            command=shutil.which("tyvrana-core")
            or str(Path(sys.executable).with_name("tyvrana-core")),
            args=["mcp", "--port", "0", "--state-directory", str(state_path)],
            env={"TMPDIR": str(tmp_path)},
        )
        async with Client(
            stdio_client(params, errlog=log), read_timeout_seconds=45
        ) as client:
            port = next(
                int(line.rsplit(":", 1)[1])
                for line in (tmp_path / "core.log").read_text().splitlines()
                if "Adapter server listening on" in line
            )

            async def tool(
                name: str, args: dict[str, Any], expected_error: str | None = None
            ) -> Any:
                started = time.monotonic()
                response = await client.call_tool(name, args)
                calls.append(
                    dict(
                        elapsed_seconds=time.monotonic() - started,
                        tool=name,
                        operation=args.get("operation"),
                        request_bytes=len(
                            json.dumps(args, separators=(",", ":")).encode()
                        ),
                        response_bytes=len(response.model_dump_json().encode()),
                        error=response.is_error,
                    )
                )
                (tmp_path / "calls.json").write_text(json.dumps(calls, indent=2))
                if expected_error:
                    assert response.is_error and expected_error in str(response.content)
                    return None
                assert not response.is_error, response.content
                return response.structured_content

            async def discover(previous: str | None = None) -> str:
                query: dict[str, Any] = dict(application="blender", wait_seconds=10)
                async with asyncio.timeout(40):
                    while True:
                        result = await tool("tyvrana_list_adapters", query)
                        found = result["adapters"]
                        if len(found) == 1 and found[0]["instance_id"] != previous:
                            return str(found[0]["instance_id"])
                        query["after_revision"] = result["revision"]

            async def execute(
                target: str, operation: str, **args: Any
            ) -> dict[str, Any]:
                result = (
                    await tool(
                        "tyvrana_execute_operation",
                        dict(adapter_id=target, operation=operation, arguments=args),
                    )
                )["result"]
                while result.get("state") in {"queued", "running"}:
                    await asyncio.sleep(result.get("poll_after_seconds", 0.2))
                    if "restore_id" in result:
                        status, kw = (
                            "project.restore_status",
                            dict(project_id=key, restore_id=result["restore_id"]),
                        )
                    else:
                        status, kw = (
                            "blender.document.attest_status",
                            dict(job_id=result["job_id"]),
                        )
                    result = (
                        await tool(
                            "tyvrana_execute_operation",
                            dict(adapter_id=target, operation=status, arguments=kw),
                        )
                    )["result"]
                assert result.get("state") != "failed", result
                if operation == "blender.document.attest":
                    result = result["result"]
                if operation in {
                    "blender.project.bind",
                    "blender.file.new",
                    "blender.file.open",
                    "blender.file.save",
                }:
                    query: dict[str, Any] = dict(adapter_id=target, wait_seconds=10)
                    async with asyncio.timeout(30):
                        while True:
                            registration = await tool("tyvrana_list_adapters", query)
                            found = registration["adapters"]
                            if (
                                found
                                and found[0].get("project_id") == result["project_id"]
                                and found[0].get("project_path") == result["filepath"]
                            ):
                                break
                            query["after_revision"] = registration["revision"]
                return dict(result)

            @asynccontextmanager
            async def host(
                name: str, *, fail_start: bool = False
            ) -> AsyncIterator[None]:
                control = tmp_path / name
                control.mkdir()
                with (control / "host.log").open("wb") as host_log:
                    process = await asyncio.create_subprocess_exec(
                        "blender",
                        "--background",
                        "--python-exit-code",
                        "1",
                        "--python",
                        str(host_script),
                        env={
                            **env,
                            "CHECKOUT_TEST_PORT": str(port),
                            "CHECKOUT_TEST_CONTROL": str(control),
                            "CHECKOUT_TEST_FAIL_START": str(int(fail_start)),
                        },
                        stdout=host_log,
                        stderr=host_log,
                    )
                    try:
                        if fail_start:
                            await asyncio.wait_for(process.wait(), 20)
                        yield
                    finally:
                        (control / "stop").touch()
                        try:
                            await asyncio.wait_for(process.wait(), 20)
                        except TimeoutError:
                            process.kill()
                            await process.wait()
                        host_output = (control / "host.log").read_text()
                        assert process.returncode == int(fail_start), host_output
                        assert "FIXTURE_CLEANED" in host_output
                        assert (
                            f"FIXTURE_ENDPOINT {port} NETWORK_NOT_STARTED"
                            in host_output
                        )

            async with host("startup-failure", fail_start=True):
                outcomes["startup_failure_cleanup"] = "PASS"
            async with host("first"):
                adapter = await discover()
                await execute(adapter, "blender.file.new", discard_current=True)
                await execute(
                    adapter,
                    "blender.object.create_primitive",
                    primitive="cube",
                    name="Base",
                )
                native = await execute(
                    adapter,
                    "blender.project.bind",
                    resources=[dict(resource_kind="object", name="Base")],
                )
                foundation_path = tmp_path / "foundation.blend"
                await execute(
                    adapter,
                    "blender.file.save",
                    filepath=str(foundation_path),
                    overwrite=True,
                )
                project = await execute(
                    "core",
                    "project.create",
                    title="Checkpoint fixture",
                    goal="Abandon experiments and continue durable work",
                )
                key = project["id"]
                revision = project["revision"]

                async def apply(**kw: Any) -> dict[str, Any]:
                    nonlocal revision
                    result = await execute(
                        "core",
                        "project.apply",
                        project_id=key,
                        expected_revision=revision,
                        **kw,
                    )
                    revision = result["project"]["revision"]
                    return result

                async def packet() -> dict[str, Any]:
                    nonlocal revision
                    result = await execute("core", "project.continue", project_id=key)
                    revision = result["project"]["revision"]
                    return result

                async def check() -> dict[str, Any]:
                    value = await packet()
                    records = {r["record"]["id"]: r for r in value["records"]}
                    assert records["M1"]["record"]["status"] == "accepted", value
                    assert records["M2"]["record"]["status"] == "accepted", value
                    assert records["M3"]["record"]["status"] == "in_progress", value
                    return value

                evidence = dict(
                    kind="evidence",
                    id="base_proof",
                    label="Inspected base",
                    storage="application",
                    binding_id="base_binding",
                    summary="Native cube was inspected",
                )
                validation = dict(
                    kind="validation",
                    id="base_check",
                    label="Base check",
                    validation_type="inspection",
                    entity_ids=["base"],
                    evidence_ids=["base_proof"],
                    summary="Declared base geometry is present",
                    status="passed",
                    freshness="current",
                )
                m1 = dict(
                    kind="milestone",
                    id="M1",
                    label="First milestone",
                    status="in_progress",
                    entity_ids=["base"],
                    document_ids=["doc"],
                    validation_ids=["base_check"],
                    acceptance="Base meets fixture geometry requirements",
                )
                m2 = dict(
                    kind="milestone",
                    id="M2",
                    label="Second milestone",
                    status="accepted",
                    entity_ids=["second"],
                    document_ids=["doc"],
                    prerequisite_ids=["M1"],
                    validation_ids=["second_check"],
                    acceptance="Second fixture claim was inspected",
                )
                m3 = dict(
                    kind="milestone",
                    id="M3",
                    label="Provisional mechanics",
                    status="in_progress",
                    entity_ids=["rig"],
                    document_ids=["doc"],
                    prerequisite_ids=["M2"],
                )
                await apply(
                    project=dict(stage="M1"),
                    upsert=[
                        dict(kind="entity", id="base", label="Base"),
                        dict(
                            kind="document",
                            id="doc",
                            label="Document",
                            application="blender",
                            application_project_id=native["project_id"],
                            adapter_id=adapter,
                        ),
                        dict(
                            kind="binding",
                            id="base_binding",
                            label="Base object",
                            entity_id="base",
                            document_id="doc",
                            resource_kind="object",
                            resource_id=native["resources"][0]["resource_id"],
                        ),
                        evidence,
                        validation,
                        m1,
                    ],
                )
                verified = await execute(
                    "core",
                    "project.verify",
                    project_id=key,
                    expected_revision=revision,
                    binding_ids=["base_binding"],
                )
                revision = verified["project"]["revision"]
                await apply(
                    project=dict(stage="M3"),
                    upsert=[
                        {**m1, "status": "accepted"},
                        dict(kind="entity", id="second", label="Second claimed output"),
                        dict(kind="entity", id="rig", label="Provisional armature"),
                        dict(
                            kind="evidence",
                            id="second_proof",
                            label="Second proof",
                            storage="external",
                            uri="urn:fixture:second",
                            summary="Second claim inspected",
                        ),
                        dict(
                            kind="validation",
                            id="second_check",
                            label="Second check",
                            validation_type="inspection",
                            entity_ids=["second"],
                            evidence_ids=["second_proof"],
                            summary="Second claim passes",
                            status="passed",
                            freshness="current",
                        ),
                        m2,
                        m3,
                    ],
                )
                await execute(
                    adapter,
                    "blender.armature.create",
                    name="Rig",
                    bones=[
                        dict(
                            name=f"Joint{i:03}",
                            head=[0, i, 0],
                            tail=[0, i + 1, 0],
                            **(
                                dict(parent=f"Joint{i - 1:03}", connected=True)
                                if i
                                else {}
                            ),
                        )
                        for i in range(64)
                    ],
                )
                structure = await execute(
                    adapter,
                    "blender.armature.inspect",
                    object_name="Rig",
                    sample_limit=0,
                )
                assert structure["bone_count"] == 64
                await execute(
                    adapter,
                    "blender.file.save",
                    filepath=str(foundation_path),
                    overwrite=True,
                )
                await check()
                foundation_validation = dict(
                    kind="validation",
                    id="foundation_check",
                    label="Foundation check",
                    validation_type="inspection",
                    entity_ids=["rig"],
                    evidence_ids=["foundation_proof"],
                    summary="64 native bones and saved qualified content inspected",
                    status="failed",
                    freshness="stale",
                )
                await apply(
                    upsert=[
                        dict(
                            kind="evidence",
                            id="foundation_proof",
                            label="Foundation evidence",
                            storage="external",
                            uri="urn:fixture:foundation",
                            summary="64 bone structure inspected",
                        ),
                        foundation_validation,
                    ],
                    checkpoint=dict(id="foundation", label="Good working foundation"),
                )
                base_revision = revision
                saved = await execute(adapter, "blender.document.attest")
                await check()
                outcomes["foundation_checkpoint"] = "PASS"
                # The checkpoint summary can be stale; its revision is authoritative.
                with sqlite3.connect(state_path / "projects.sqlite3") as db:
                    db.execute(
                        "UPDATE checkpoints SET "
                        "data=json_set(data,'$.accepted_milestones',json('[]'),"
                        "'$.accepted_count',0) WHERE project_id=? AND id='foundation'",
                        (key,),
                    )
                await apply(
                    upsert=[
                        {
                            **foundation_validation,
                            "status": "passed",
                            "freshness": "current",
                        }
                    ]
                )
                assert revision == base_revision + 1
                outcomes["revision_plus_one_closure"] = "PASS"
                await execute(
                    adapter,
                    "blender.object.set_transform",
                    name="Rig",
                    location=[0, 0, 1],
                )
                await packet()
                await apply(
                    project=dict(stage="M2"),
                    upsert=[
                        {
                            **m2,
                            "status": "in_progress",
                            "summary": "Reopened failed experiment",
                        },
                        dict(
                            kind="issue",
                            id="experiment_failure",
                            label="Failed experiment",
                            entity_ids=["rig"],
                            severity="major",
                            summary="Keep this branch as historical evidence",
                        ),
                        dict(
                            kind="evidence",
                            id="failed_evidence",
                            label="Failed evidence",
                            storage="external",
                            uri="urn:fixture:failed",
                            summary="Experimental state failed",
                        ),
                    ],
                )
                # Reproduce the failure input: native open succeeds, but ordinary
                # mutation qualification rejects the changed document session.
                # Checkout below must recover without trusting that failed receipt.
                await tool(
                    "tyvrana_execute_operation",
                    dict(
                        adapter_id=adapter,
                        operation="blender.file.open",
                        arguments=dict(
                            filepath=str(foundation_path), discard_current=True
                        ),
                    ),
                    expected_error="mutation_unqualified",
                )
                current = await execute(adapter, "blender.document.attest")
                assert current["digest"] == saved["digest"]
                contaminated = await packet()
                assert any(
                    r["record"].get("status") == "invalidated"
                    for r in contaminated["records"]
                )
                outcomes["failed_branch_and_exact_document_rollback"] = "PASS"
                checkout_start = len(calls)
                result = await execute(
                    "core",
                    "project.restore",
                    project_id=key,
                    mode="checkout",
                    restore_id="checkout",
                    expected_revision=revision,
                    document_id="doc",
                    adapter_id=adapter,
                    checkpoint_id="foundation",
                    discard_current=True,
                    expected_current={
                        k: current[k]
                        for k in [
                            "host_session_id",
                            "document_session_id",
                            "project_id",
                            "format",
                            "digest",
                        ]
                    },
                    provenance="Abandon failed experimental branch",
                )
                outcomes["checkout_calls"] = calls[checkout_start:]
                assert (
                    result["revision"] == revision + 1
                    and result["base_revision"] == base_revision
                ), result
                restored = await check()
                values = {r["record"]["id"]: r for r in restored["records"]}
                assert (
                    "failed_evidence" not in values
                    and "experiment_failure" not in values
                )
                assert values["foundation_check"]["freshness"] == "stale"
                assert values["foundation_check"]["record"]["status"] == "failed"
                outcomes["exact_revision_failed_stale"] = "PASS"
                closed = await apply(
                    upsert=[
                        {
                            **foundation_validation,
                            "status": "passed",
                            "freshness": "current",
                        }
                    ],
                    checkpoint=dict(id="closed", label="Qualified foundation closure"),
                )
                assert closed["checkpoint"]["accepted_milestones"] == ["M1", "M2"]
                assert closed["checkpoint"]["accepted_count"] == 2
                values = {r["record"]["id"]: r for r in (await check())["records"]}
                assert values["foundation_check"]["freshness"] == "current"
                assert values["foundation_check"]["record"]["status"] == "passed"
                outcomes["closure_new_checkpoint"] = "PASS"
                current = await execute(adapter, "blender.document.attest")
                assert current["digest"] == saved["digest"]
                with sqlite3.connect(state_path / "projects.sqlite3") as db:
                    record = json.loads(
                        db.execute(
                            "SELECT data FROM document_mutations WHERE id='checkout'"
                        ).fetchone()[0]
                    )
                    assert (
                        record["postflight"]["document_session_id"]
                        == record["request"]["expected_current"]["document_session_id"]
                    )
                    assert "failed_evidence" in record["abandoned_snapshot"]["records"]
                    assert "receipt" not in record
                outcomes["checkout"] = "PASS"
                await execute(
                    adapter,
                    "blender.object.set_transform",
                    name="Rig",
                    location=[0, 0, 0.1],
                )
                await check()
                for index in range(3):
                    await execute(
                        adapter,
                        "blender.file.save",
                        filepath=str(tmp_path / f"continued-{index}.blend"),
                        overwrite=True,
                    )
                    await check()
                outcomes["post_checkout_save_as_count"] = 3
                checkpoint = await apply(
                    checkpoint=dict(id="continued", label="Continued working state")
                )
                continued = await execute(adapter, "blender.document.attest")
                assert (
                    checkpoint["checkpoint"]["document_states"]["doc"]["digest"]
                    == continued["digest"]
                )
                outcomes["mutation_save_checkpoint"] = "PASS"
                installed = (
                    Path(env["BLENDER_USER_RESOURCES"])
                    / "extensions/user_default/tyvrana_blender"
                )
                spec = importlib.util.spec_from_file_location(
                    "checkout_deployment", installed / "deployment.py"
                )
                assert spec and spec.loader
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                staged = await asyncio.to_thread(module.stage, package, installed)
                previous = adapter
                await execute(
                    adapter, "blender.extension.reload", expected_build=staged["build"]
                )
                adapter = await discover(previous)
                reloaded = await execute(adapter, "blender.document.attest")
                assert reloaded["digest"] == continued["digest"]
                await check()
                outcomes["reload"] = "PASS"
                for _ in range(2):
                    await execute(adapter, "blender.file.save", overwrite=True)
                    await check()
                outcomes["post_reload_same_path_save_count"] = 2
            async with host("restart"):
                adapter = await discover()
                current = await execute(adapter, "blender.document.attest")
                result = await execute(
                    "core",
                    "project.restore",
                    project_id=key,
                    mode="checkout",
                    restore_id="restart",
                    expected_revision=revision,
                    document_id="doc",
                    adapter_id=adapter,
                    checkpoint_id="continued",
                    discard_current=True,
                    expected_current={
                        k: current[k]
                        for k in [
                            "host_session_id",
                            "document_session_id",
                            "project_id",
                            "format",
                            "digest",
                        ]
                    },
                    provenance="Recover saved working state after process restart",
                )
                assert result["state"] == "completed"
                await check()
                await execute(
                    adapter,
                    "blender.object.set_transform",
                    name="Rig",
                    location=[0, 0, 0.2],
                )
                await check()
                outcomes["restart_restore_continue"] = "PASS"
                structure = await execute(
                    adapter,
                    "blender.armature.inspect",
                    object_name="Rig",
                    sample_limit=0,
                )
                assert structure["bone_count"] == 64
                assert len((await tool("tyvrana_list_adapters", {}))["adapters"]) == 1
            outcomes.update(
                repetition=repetition,
                calls=len(calls),
                request_bytes=sum(c["request_bytes"] for c in calls),
                response_bytes=sum(c["response_bytes"] for c in calls),
                failures=sum(bool(c["error"]) for c in calls),
            )
            leftovers = await asyncio.to_thread(
                lambda: list(tmp_path.glob("tyvrana-proof-*"))
            )
            assert not leftovers
            with sqlite3.connect(state_path / "projects.sqlite3") as db:
                record = json.loads(
                    db.execute(
                        "SELECT data FROM document_mutations WHERE id='restart'"
                    ).fetchone()[0]
                )
                assert (
                    record["receipt"]
                    and record["preflight"]["host_session_id"]
                    != record["postflight"]["host_session_id"]
                )
                outcomes["proof_hosts_used"] = 1
            (tmp_path / "qualification.json").write_text(json.dumps(outcomes, indent=2))
            print("CHECKOUT_NATIVE_PASS", json.dumps(outcomes))
