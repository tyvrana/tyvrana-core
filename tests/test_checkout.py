"""Checkpoint checkout replaces active meaning without rewriting failed history."""

import copy
import hashlib
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
        "summary_conflict",
        "incomplete_history",
        "corrupt_history",
        "foreign_history",
        "checkpoint_revision_conflict",
        "document_summary_conflict",
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
            foundation_validation = dict(
                kind="validation",
                id="foundation_check",
                label="Foundation",
                validation_type="inspection",
                entity_ids=["harness"],
                evidence_ids=["proof2"],
                summary="Pending final closure",
                status="failed",
                freshness="stale",
            )
            await apply(
                upsert=[foundation_validation],
                checkpoint=dict(id="foundation", label="Durable foundation"),
            )
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
            # R+1 is a legitimate closure, but must not leak into checkout of R.
            await apply(
                upsert=[
                    {
                        **foundation_validation,
                        "status": "passed",
                        "freshness": "current",
                    }
                ]
            )
            if case not in {"stale_checkpoint", "migrated_stale_checkpoint"}:
                await apply(
                    project=dict(stage=""),
                    upsert=[
                        {
                            **foundation_validation,
                            "status": "passed",
                            "freshness": "current",
                        },
                        {
                            **m3,
                            "status": "accepted",
                            "validation_ids": ["foundation_check"],
                            "acceptance": "Qualified experimental foundation",
                        },
                    ],
                )
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
            elif case in {"incomplete_history", "corrupt_history", "foreign_history"}:
                with store.transaction(write=True) as db:
                    if case == "incomplete_history":
                        db.execute(
                            "DELETE FROM revision_states WHERE project_id=? AND "
                            "revision=?",
                            (key, base),
                        )
                        error = "history_incomplete"
                    elif case == "corrupt_history":
                        db.execute(
                            "UPDATE revision_states SET data='{}' WHERE "
                            "project_id=? AND revision=?",
                            (key, base),
                        )
                        error = "history_corrupt"
                    else:
                        other = json.loads(
                            db.execute(
                                "SELECT data FROM revision_states WHERE "
                                "project_id=? AND revision=?",
                                (key, base),
                            ).fetchone()[0]
                        )
                        other["project"]["id"] = "foreign"
                        encoded = json.dumps(other)
                        db.execute(
                            "UPDATE revision_states SET data=?,sha256=? WHERE "
                            "project_id=? AND revision=?",
                            (
                                encoded,
                                hashlib.sha256(encoded.encode()).hexdigest(),
                                key,
                                base,
                            ),
                        )
                        error = "history_corrupt"
            elif case == "checkpoint_revision_conflict":
                with store.transaction(write=True) as db:
                    db.execute(
                        "UPDATE checkpoints SET revision=revision+1 "
                        "WHERE project_id=? AND id='foundation'",
                        (key,),
                    )
                error = "history_corrupt"
            elif case == "document_summary_conflict":
                with store.transaction(write=True) as db:
                    db.execute(
                        "UPDATE checkpoints SET "
                        "data=json_set(data,'$.document_states.doc.digest',?) "
                        "WHERE project_id=? AND id='foundation'",
                        ("f" * 64, key),
                    )
                error = "restore_target_mismatch"
            # Summary metadata never overrides revision state or format proofs.
            with store.transaction(write=True) as db:
                db.execute(
                    "UPDATE checkpoints SET "
                    "data=json_set(data,'$.accepted_milestones',json('[]'),"
                    "'$.accepted_count',0,'$.validation_counts',json('{}'),"
                    "'$.stage','wrong-summary') WHERE project_id=? AND id='foundation'",
                    (key,),
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
            m3_view = next(v for v in packet["records"] if v["record"]["id"] == "m3")
            assert "historical_status" not in m3_view
            assert "accepted_revision" not in m3_view
            delta = (
                await core.projects.execute(
                    "project.delta", dict(project_id=key, since_revision=base, limit=50)
                )
            ).model_dump()
            assert any(
                c["id"] == "failed" and c["action"] == "created"
                for c in delta["changes"]
            )
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
                values = {v["record"]["id"]: v for v in packet["records"]}
                assert values["foundation_check"]["record"]["status"] == "failed"
                assert values["foundation_check"]["freshness"] == "stale"
                assert packet["project"]["next_action"] == "Continue the foundation"
                revision = again["revision"]
                closure = await apply(
                    upsert=[
                        {
                            **foundation_validation,
                            "status": "passed",
                            "freshness": "current",
                        }
                    ],
                    checkpoint=dict(id="closed", label="Foundation closure"),
                )
                expected_accepted = (
                    {"stage", "m2", "second"}
                    if case.startswith("migrated")
                    else {"stage", "m2"}
                )
                assert (
                    set(closure["checkpoint"]["accepted_milestones"])
                    == expected_accepted
                )
                assert closure["checkpoint"]["accepted_count"] == len(expected_accepted)
                with store.transaction() as db:
                    at_r = store.materialize_revision(db, key, base)
                    at_r1 = store.materialize_revision(db, key, base + 1)
                    assert at_r["records"]["foundation_check"]["status"] == "failed"
                    assert at_r1["records"]["foundation_check"]["status"] == "passed"
                    assert (
                        store.materialize_revision(
                            db, key, result["abandoned_revision"]
                        )["records"]["failed"]["status"]
                        == "open"
                    )
                await core.dispatcher.execute(
                    adapter_id="initial",
                    operation="editor.change",
                    arguments={"mode": "downstream"},
                )
                head = core.projects.continuity.baseline(key, "doc")
                assert head and head.digest == "d" * 64
