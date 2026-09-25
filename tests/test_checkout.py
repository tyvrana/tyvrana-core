"""Checkpoint checkout replaces active meaning without rewriting failed history."""

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from tyvrana_protocol import DocumentState

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.store import ProjectError

from .proof_fixture import proof_artifact
from .test_attestation_migration import accepted_chain
from .test_continuity import establish
from .test_working_lineage import editor


@pytest.mark.parametrize(
    "case",
    [
        "equal",
        "restore",
        "foreign",
        "missing",
        "expired",
        "authorization",
        "changed",
        "proof",
        "stale_checkpoint",
        "atomic",
        "saved_record_revisions",
        "migrated_checkpoint",
        "migrated_stale_checkpoint",
    ],
)
async def test_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    loads: list[dict[str, Any]] = []
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core, restores=loads) as native:
            key, revision = await (
                accepted_chain(core) if case.startswith("migrated") else establish(core)
            )
            if case.startswith("migrated"):
                native.update(format="corrected-canonical", digest="e" * 64)
                async with proof_artifact(
                    core, {**copy.deepcopy(native), "host_session_id": "proof-host"}
                ):
                    migrated = await core.projects.execute(
                        "project.attest",
                        dict(
                            project_id=key,
                            document_id="doc",
                            adapter_id="initial",
                            expected_revision=revision,
                            mode="migrate",
                            from_format="fixture-canonical",
                            to_format="corrected-canonical",
                            trusted_artifact_sha256="b" * 64,
                            provenance="Qualified canonical correction",
                        ),
                    )
                    assert migrated.model_dump()["baseline_migrated"]

            async def apply(**kw: Any) -> dict[str, Any]:
                nonlocal revision
                result = (
                    await core.projects.execute(
                        "project.apply",
                        dict(
                            project_id=key,
                            expected_revision=revision,
                            **kw,
                        ),
                    )
                ).model_dump()
                revision = result["project"]["revision"]
                return result

            m2 = dict(
                kind="milestone",
                id="m2",
                label="Second accepted",
                status="accepted",
                entity_ids=["part2"],
                document_ids=["doc"],
                prerequisite_ids=["stage"],
                validation_ids=["check2"],
                acceptance="Second part measurements pass",
            )
            m3 = dict(
                kind="milestone",
                id="m3",
                label="Experimental stage",
                status="in_progress",
                entity_ids=["harness"],
                document_ids=["doc"],
                prerequisite_ids=["m2"],
            )
            await apply(
                upsert=[
                    dict(kind="entity", id="part2", label="Second part"),
                    dict(kind="entity", id="harness", label="Provisional harness"),
                    dict(
                        kind="evidence",
                        id="proof2",
                        label="Second proof",
                        storage="external",
                        uri="urn:fixture:second",
                        summary="Second part inspected successfully",
                    ),
                    dict(
                        kind="validation",
                        id="check2",
                        label="Second check",
                        status="passed",
                        freshness="current",
                        validation_type="inspection",
                        entity_ids=["part2"],
                        evidence_ids=["proof2"],
                        summary="Second part passes its declared checks",
                    ),
                    m2,
                    m3,
                ],
                project=dict(stage="m3", next_action="Continue the foundation"),
            )
            if case in {"stale_checkpoint", "migrated_stale_checkpoint"}:
                await apply(
                    upsert=[
                        dict(kind="entity", id="part", label="Changed upstream claim")
                    ]
                )
            await apply(checkpoint=dict(id="foundation", label="Durable foundation"))
            base = revision
            store = core.projects.store
            with store.transaction() as db:
                foundation = store.working_snapshot(db, store.project(db, key))
                journal = [
                    tuple(r)
                    for r in db.execute(
                        "SELECT * FROM journal WHERE project_id=?", (key,)
                    )
                ]
            # Reopening changes claim text; content-only restore rejects it.
            await apply(
                project=dict(stage=""),
                checkpoint=dict(id="failed-branch", label="Abandoned branch"),
                upsert=[
                    {
                        **m2,
                        "status": "invalidated"
                        if case in {"stale_checkpoint", "migrated_stale_checkpoint"}
                        else "in_progress",
                        "summary": "Failed experiment reopened this stage",
                    },
                    dict(
                        kind="issue",
                        id="failed",
                        label="Experimental failure",
                        severity="major",
                        entity_ids=["harness"],
                        status="open",
                        summary="Retain this failed evidence in history",
                    ),
                    dict(
                        kind="evidence",
                        id="failed-proof",
                        label="Failed branch evidence",
                        storage="external",
                        uri="urn:fixture:failed",
                    ),
                ],
            )
            if case in {"restore", "proof"}:
                native["digest"] = "d" * 64
            expected = {k: native[k] for k in DocumentState.model_fields}
            request: dict[str, Any] = dict(
                project_id=key,
                mode="checkout",
                restore_id="checkout",
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                checkpoint_id="foundation",
                discard_current=True,
                expected_current=expected,
                provenance="Abandon the experimental branch",
            )
            error = None
            if case == "foreign":
                foreign = (
                    await core.projects.execute(
                        "project.create", dict(title="Other", goal="Other")
                    )
                ).model_dump()["id"]
                request["project_id"] = foreign
                request["expected_revision"] = 1
                error = "restore_scope"
            elif case == "missing":
                request["checkpoint_id"] = "absent"
                error = "restore_target_missing"
            elif case == "expired":
                with store.transaction(write=True) as db:
                    state = store.project(db, key).model_copy(
                        update={"history_floor": base + 1}
                    )
                    db.execute(
                        "UPDATE projects SET data=? WHERE id=?",
                        (state.model_dump_json(), key),
                    )
                error = "history_expired"
            elif case == "authorization":
                request["discard_current"] = False
            elif case == "changed":
                original = core.projects.continuity.observe
                count = 0

                async def observe(adapter: Any) -> Any:
                    nonlocal count
                    count += 1
                    if count == 2:
                        native["digest"] = "e" * 64
                    return await original(adapter)

                monkeypatch.setattr(core.projects.continuity, "observe", observe)
                error = "restore_current_changed"
            elif case == "proof":
                error = "restore_file_mismatch"
            elif case == "atomic":

                def fail(*args: Any) -> None:
                    raise ProjectError(
                        "fixture_commit_failure", "Rollback the whole transaction"
                    )

                monkeypatch.setattr(store, "_integrity", fail)
                error = "fixture_commit_failure"
            elif case == "saved_record_revisions" or case.startswith("migrated"):
                with store.transaction(write=True) as db:
                    row = db.execute(
                        "SELECT data FROM checkpoint_contents WHERE project_id=? "
                        "AND id='foundation'",
                        (key,),
                    ).fetchone()
                    snapshot = json.loads(row[0])
                    snapshot.pop("record_revisions")
                    snapshot.pop("project")
                    db.execute(
                        "UPDATE checkpoint_contents SET data=? WHERE project_id=? "
                        "AND id='foundation'",
                        (json.dumps(snapshot), key),
                    )
            with store.transaction() as db:
                before = store.working_snapshot(db, store.project(db, key))
                journal = [
                    tuple(r)
                    for r in db.execute(
                        "SELECT * FROM journal WHERE project_id=?", (key,)
                    )
                ]
                acceptances = [
                    tuple(r)
                    for r in db.execute(
                        "SELECT * FROM milestone_acceptances WHERE project_id=?", (key,)
                    )
                ]
            artifact = copy.deepcopy(native)
            artifact.update(
                digest="a" * 64,
                host_session_id="proof-host",
                file_sha256=("c" if case == "proof" else "b") * 64,
            )
            async with proof_artifact(core, artifact):
                if case == "authorization":
                    with pytest.raises(ValidationError):
                        await core.projects.execute("project.restore", request)
                    return
                result = (
                    await core.projects.execute("project.restore", request)
                ).model_dump()
            if error:
                assert result["state"] == "failed" and result["error_code"] == error, (
                    result
                )
                with store.transaction() as db:
                    assert store.working_snapshot(db, store.project(db, key)) == before
                return
            assert result["state"] == "completed", result
            assert (
                result["revision"] == revision + 1 and result["base_revision"] == base
            )
            assert result["abandoned_revision"] == revision
            assert len(loads) == int(case == "restore")
            with store.transaction() as db:
                current = store.working_snapshot(db, store.project(db, key))
                assert current["records"] == foundation["records"]
                assert current["observations"] == foundation["observations"]
                assert current["validation_context"] == foundation["validation_context"]
                assert [
                    tuple(r)
                    for r in db.execute(
                        "SELECT * FROM milestone_acceptances WHERE project_id=?", (key,)
                    )
                ] == acceptances
                assert all(
                    tuple(row)
                    in [
                        tuple(r)
                        for r in db.execute(
                            "SELECT * FROM journal WHERE project_id=?", (key,)
                        )
                    ]
                    for row in journal
                )
                archived = json.loads(
                    db.execute(
                        "SELECT data FROM document_mutations WHERE id='checkout'"
                    ).fetchone()[0]
                )
                assert archived["abandoned_snapshot"] == before
                assert "failed-proof" in archived["abandoned_snapshot"]["records"]
            packet = (
                await core.projects.execute("project.continue", dict(project_id=key))
            ).model_dump()
            assert packet["checkpoint"]["id"] == "foundation"
            statuses = {
                v["record"]["id"]: v["record"].get("status") for v in packet["records"]
            }
            if case in {"stale_checkpoint", "migrated_stale_checkpoint"}:
                assert (
                    statuses["stage"] == "invalidated"
                    and statuses["m2"] == "invalidated"
                )
            else:
                assert statuses["stage"] == "accepted" and statuses["m2"] == "accepted"
            assert statuses["m3"] == "in_progress"
            repeated = (
                await core.projects.execute("project.restore", request)
            ).model_dump()
            assert repeated == result
            request.update(
                restore_id="again",
                expected_revision=result["revision"],
                expected_current={k: native[k] for k in DocumentState.model_fields},
            )
            again = (
                await core.projects.execute("project.restore", request)
            ).model_dump()
            assert (
                again["already_current"] and again["revision"] == result["revision"]
            ), again
            if case not in {"stale_checkpoint", "migrated_stale_checkpoint"}:
                await core.dispatcher.execute(
                    adapter_id="initial",
                    operation="editor.change",
                    arguments={"mode": "downstream"},
                )
                head = core.projects.continuity.baseline(key, "doc")
                assert head and head.digest == "d" * 64
