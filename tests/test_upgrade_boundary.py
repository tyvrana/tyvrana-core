"""An unsaved head is never adopted across an attestation format boundary."""

import copy
from pathlib import Path

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.store import ProjectError

from .test_attestation_migration import request
from .test_continuity import establish
from .test_working_lineage import editor


@pytest.mark.parametrize("same_digest", [True, False])
async def test_unsaved_upgrade_reports_preservation_boundary(
    tmp_path: Path, same_digest: bool
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            project, revision = await establish(core)
            await core.projects.execute(
                "project.apply",
                dict(
                    project_id=project,
                    expected_revision=revision,
                    project=dict(stage="working"),
                    upsert=[
                        dict(kind="entity", id="work", label="Working content"),
                        dict(
                            kind="milestone",
                            id="working",
                            label="Work",
                            status="in_progress",
                            entity_ids=["work"],
                            document_ids=["doc"],
                        ),
                    ],
                ),
            )
            await core.dispatcher.execute(
                adapter_id="initial",
                operation="editor.change",
                arguments={"mode": "downstream"},
            )
            baseline = core.projects.continuity.baseline(project, "doc")
            assert baseline is not None and baseline.artifact_sha256 is None
            native["format"] = "fixture-authored"
            if not same_digest:
                native["digest"] = "f" * 64
            expected_native = copy.deepcopy(native)
            packet = (
                await core.projects.execute(
                    "project.continue", dict(project_id=project)
                )
            ).model_dump()
            application = packet["applications"][0]
            assert application["error_code"] == "attestation_upgrade_unsupported"
            assert application["next_action"] == "keep_open_unsupported_upgrade"
            assert application["error_details"]["from_format"] == "fixture-canonical"
            assert application["error_details"]["to_format"] == "fixture-authored"
            assert application["saved_artifact_sha256"] is None
            assert application["trust"] != "current"
            for mode in ["capture", "reattach"]:
                with pytest.raises(ProjectError) as rejected:
                    await core.projects.execute(
                        "project.attest",
                        dict(
                            project_id=project,
                            document_id="doc",
                            adapter_id="initial",
                            expected_revision=packet["project"]["revision"],
                            mode=mode,
                        ),
                    )
                assert rejected.value.code == "attestation_upgrade_unsupported"
            with pytest.raises(ProjectError) as rejected:
                await core.projects.execute(
                    "project.attest", request(project, packet["project"]["revision"])
                )
            assert rejected.value.code == "migration_artifact_missing"
            assert (
                rejected.value.details["next_action"] == "keep_open_unsupported_upgrade"
            )
            with pytest.raises(ProjectError) as rejected:
                await core.dispatcher.execute(
                    adapter_id="initial", operation="editor.file.save", arguments={}
                )
            assert rejected.value.code == "attestation_upgrade_unsupported"
            assert native == expected_native
            assert core.projects.continuity.baseline(project, "doc") == baseline
            final = (
                await core.projects.execute(
                    "project.continue", dict(project_id=project)
                )
            ).model_dump()
            assert final["project"]["revision"] == packet["project"]["revision"]
