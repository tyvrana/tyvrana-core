"""Trusted replacement never promotes the divergent content being discarded."""

import copy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from tyvrana_protocol import DocumentState

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.store import ProjectError

from .proof_fixture import proof_artifact
from .test_continuity import establish, evidence
from .test_working_lineage import editor


@pytest.mark.parametrize(
    "case,code",
    [
        ("accepted", None),
        ("checkpoint", None),
        ("authorization", "invalid"),
        ("stale", "restore_current_changed"),
        ("file", "restore_file_mismatch"),
        ("content", "restore_target_mismatch"),
        ("lineage", "application_changed"),
        ("incomplete", "attestation_incomplete"),
        ("post", "restore_post_mismatch"),
        ("load", "file_open_failed"),
    ],
)
async def test_guarded_restore(tmp_path: Path, case: str, code: str | None) -> None:
    calls: list[dict[str, Any]] = []
    faults: dict[str, Any] = {}
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with (
            editor(core, restores=calls, faults=faults) as native,
            proof_artifact(
                core,
                {
                    **evidence(host="proof-host"),
                    "resource_scope": "strong-closure",
                    "resources": [
                        dict(
                            resource_kind="mesh",
                            resource_id="mesh",
                            state="present",
                            name="Base",
                            fingerprint="base-original",
                        )
                    ],
                },
            ) as proof,
        ):
            key, revision = await establish(core)
            applied = await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=revision,
                    project=dict(stage="m2"),
                    checkpoint=dict(id="saved", label="Saved base"),
                    upsert=[
                        dict(kind="entity", id="harness", label="Harness"),
                        dict(
                            kind="milestone",
                            id="m2",
                            label="Working",
                            status="in_progress",
                            entity_ids=["harness"],
                            document_ids=["doc"],
                            prerequisite_ids=["stage"],
                        ),
                    ],
                ),
            )
            revision = applied.model_dump()["project"]["revision"]
            head = core.projects.continuity.baseline(key, "doc")
            assert head
            native["digest"] = "d" * 64
            expected = {k: native[k] for k in DocumentState.model_fields}
            request = dict(
                project_id=key,
                restore_id="restore",
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                expected_current=expected,
                discard_current=True,
                provenance="Discard known unsaved fixture edits",
            )
            if case == "checkpoint":
                request["checkpoint_id"] = "saved"
            elif case == "authorization":
                request["discard_current"] = False
            elif case == "stale":
                native["digest"] = "e" * 64
            elif case == "file":
                proof["file_sha256"] = "c" * 64
            elif case == "content":
                proof["digest"] = "c" * 64
            elif case == "lineage":
                proof["project_id"] = "another"
            elif case == "incomplete":
                proof.update(status="unsupported", digest=None, omissions=["Unknown"])
            elif case == "post":
                faults["post_mismatch"] = True
            elif case == "load":
                faults["load_failure"] = True
            before = copy.deepcopy(native)
            if case == "authorization":
                with pytest.raises(ValidationError):
                    await core.projects.execute("project.restore", request)
                assert not calls and native == before
                return
            result = (
                await core.projects.execute("project.restore", request)
            ).model_dump()
            assert not core.projects.proofs.leases
            assert [a.instance_id for a in core.registry.list(include_proofs=True)] == [
                "initial"
            ]
            if code:
                assert result["state"] == "failed" and result["error_code"] == code, (
                    result
                )
                assert core.projects.continuity.baseline(key, "doc") == head
                if case not in {"load", "post"}:
                    assert not calls and native == before
                with core.projects.store.transaction() as db:
                    assert core.projects.store.project(db, key).revision == revision
                return
            assert result["state"] == "completed", result
            assert native["digest"] == head.digest
            packet = (
                await core.projects.execute("project.continue", dict(project_id=key))
            ).model_dump()
            stages = {
                r["record"]["id"]: r
                for r in packet["records"]
                if r["record"]["kind"] == "milestone"
            }
            assert stages["stage"]["record"]["status"] == "accepted"
            assert stages["stage"]["accepted_revision"] == 2
            assert stages["m2"]["record"]["status"] == "in_progress"
            assert result["revision"] == revision + 1
            repeat = (
                await core.projects.execute("project.restore", request)
            ).model_dump()
            assert repeat == result and len(calls) == 1
            already = {
                **request,
                "restore_id": "again",
                "expected_revision": result["revision"],
                "expected_current": {k: native[k] for k in DocumentState.model_fields},
            }
            same = (
                await core.projects.execute("project.restore", already)
            ).model_dump()
            assert same["already_current"] and same["revision"] == result["revision"]
            assert len(calls) == 1
            await core.dispatcher.execute(
                adapter_id="initial",
                operation="editor.change",
                arguments={"mode": "downstream"},
            )
            assert core.projects.continuity.baseline(key, "doc").digest == "d" * 64  # type: ignore[union-attr]
            with pytest.raises(ProjectError):
                await core.projects.execute(
                    "project.restore", {**request, "provenance": "Different"}
                )
