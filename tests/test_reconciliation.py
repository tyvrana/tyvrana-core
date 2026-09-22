"""Known-delta recovery rejects drift and retains exact historical acceptance."""

import copy
from pathlib import Path
from typing import Any

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.models import ApplyInput
from tyvrana_core.projects.store import ProjectError

from .test_continuity import establish
from .test_working_lineage import editor


@pytest.mark.parametrize(
    "case, expected",
    [
        ("known", None),
        ("extra", "reconciliation_delta_mismatch"),
        ("upstream", "reconciliation_upstream_changed"),
        ("hash", "reconciliation_delta_mismatch"),
        ("document", "reconciliation_lineage"),
        ("proof_prior", "reconciliation_prior_head"),
        ("incomplete", "attestation_incomplete"),
        ("semantic", "reconciliation_claim_changed"),
        ("scope", "reconciliation_incomplete"),
        ("owner", "reconciliation_ownership"),
    ],
)
async def test_reconciliation_safety(
    tmp_path: Path, case: str, expected: str | None
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native, editor(core, "proof") as proof:
            key, revision = await establish(core)
            await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=revision,
                    upsert=[
                        dict(kind="entity", id="harness", label="Harness"),
                        dict(
                            kind="milestone",
                            id="m2",
                            label="Working stage",
                            status="planned",
                            entity_ids=["harness"],
                            document_ids=["doc"],
                            prerequisite_ids=["stage"],
                        ),
                    ],
                ),
            )
            # Simulate stored invalidation caused by a pre-receipt divergence.
            with core.projects.store.transaction(write=True) as db:
                state = core.projects.store.project(db, key)
                for rid, update in [
                    ("stage", {"status": "invalidated"}),
                    ("check", {"freshness": "stale"}),
                ]:
                    record = core.projects.store._record(db, key, rid)
                    core.projects.store._put(
                        db,
                        key,
                        record.model_copy(update=update),
                        state.revision,
                        "staled",
                    )
            head = core.projects.continuity.baseline(key, "doc")
            assert head is not None
            native["digest"] = "d" * 64
            if case == "extra":
                native["digest"] = "e" * 64
            elif case == "upstream":
                native["resources"][0]["fingerprint"] = "changed"
            elif case == "document":
                native["document_session_id"] = "other"
            elif case == "proof_prior":
                proof["digest"] = "b" * 64
            elif case == "incomplete":
                native.update(status="unsupported", digest=None, omissions=["Unknown"])
            elif case == "scope":
                native["resource_scope"] = None
            elif case == "semantic":
                core.projects.store.apply(
                    key,
                    ApplyInput.model_validate(
                        dict(
                            expected_revision=state.revision,
                            upsert=[dict(kind="entity", id="part", label="New claim")],
                        )
                    ),
                    {"doc": head.context_id},
                )
                revision = state.revision + 1
            revision = revision if case == "semantic" else state.revision
            request: dict[str, Any] = dict(
                project_id=key,
                reconciliation_id="recover",
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                proof_adapter_id="proof",
                stage_id="m2",
                prior_digest=head.digest,
                expected_digest="f" * 64
                if case == "hash"
                else native["digest"] or "d" * 64,
                provenance="Known authoring operation predates receipts",
                delta=[
                    dict(
                        operation="editor.change",
                        arguments={"mode": "downstream"},
                        owner_entity_id="part" if case == "owner" else "harness",
                    )
                ],
            )
            live_before = copy.deepcopy(native)
            result = (
                await core.projects.execute("project.reconcile", request)
            ).model_dump()
            assert native == live_before
            if expected:
                assert result["state"] == "failed", result
                assert result["error_code"] == expected, result
                assert core.projects.continuity.baseline(key, "doc") == head
                with core.projects.store.transaction() as db:
                    assert core.projects.store.project(db, key).revision == revision
                return
            assert result["state"] == "completed", result
            assert result["revision"] == revision + 1
            assert result["restored_milestones"] == ["stage"]
            again = (
                await core.projects.execute("project.reconcile", request)
            ).model_dump()
            assert again["already_reconciled"]
            assert again["revision"] == result["revision"]
            with pytest.raises(ProjectError, match="different request"):
                await core.projects.execute(
                    "project.reconcile", {**request, "provenance": "changed"}
                )
            packet = (
                await core.projects.execute("project.continue", {"project_id": key})
            ).model_dump()
            stages = {
                r["record"]["id"]: r
                for r in packet["records"]
                if r["record"]["kind"] == "milestone"
            }
            assert stages["stage"]["record"]["status"] == "accepted", packet
            assert stages["stage"]["accepted_revision"] == 2
            assert stages["m2"]["record"]["status"] == "in_progress"
            await core.dispatcher.execute(
                adapter_id="initial",
                operation="editor.change",
                arguments={"mode": "downstream"},
            )
            await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=packet["project"]["revision"] + 1,
                    checkpoint=dict(id="working", label="Recovered work"),
                ),
            )
