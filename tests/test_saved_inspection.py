"""Historical reads cannot bless or replace a divergent live head."""

import copy
from pathlib import Path

import pytest

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.errors import InvalidAdapterBehavior
from tyvrana_core.projects.store import ProjectError

from .proof_fixture import proof_artifact
from .test_continuity import establish
from .test_working_lineage import editor


@pytest.mark.parametrize(
    "case",
    [
        "valid",
        "digest",
        "resources",
        "file",
        "document",
        "format",
        "mutation",
        "missing",
        "wrong_revision",
    ],
)
async def test_saved_inspection_isolated_and_exact(tmp_path: Path, case: str) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            key, revision = await establish(core)
            checkpoint = await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=revision,
                    checkpoint=dict(id="saved", label="Saved baseline"),
                ),
            )
            revision = checkpoint.model_dump()["project"]["revision"]
            saved = copy.deepcopy(native)
            saved["host_session_id"] = "proof-host"
            # Live unsaved divergence remains exactly as it was. Historical reads
            # provide evidence, not an adoption or native mutation path.
            native["digest"] = "f" * 64
            before = copy.deepcopy(native)
            baseline = core.projects.continuity.baseline(key, "doc")
            args = dict(
                project_id=key,
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                checkpoint_id="saved",
                operation="editor.document.attest",
            )
            if case == "digest":
                saved["digest"] = "e" * 64
            elif case == "resources":
                saved["resources"][0]["fingerprint"] = "changed"
            elif case == "file":
                saved["file_sha256"] = "e" * 64
            elif case == "document":
                saved["project_id"] = "another"
            elif case == "format":
                saved["format"] = "another"
            elif case == "mutation":
                args["operation"] = "editor.change"
            elif case == "missing":
                args["checkpoint_id"] = "absent"
            elif case == "wrong_revision":
                args["expected_revision"] = revision - 1
            async with proof_artifact(core, saved):
                if case == "valid":
                    result = (
                        await core.projects.execute("project.inspect_saved", args)
                    ).model_dump()
                    assert result["scope"] == "historical_saved_document"
                    assert result["current_head_adopted"] is False
                    assert result["digest"] == baseline.digest
                    assert (
                        result["result"]["host_session_id"] != native["host_session_id"]
                    )
                else:
                    with pytest.raises((ProjectError, InvalidAdapterBehavior)):
                        await core.projects.execute("project.inspect_saved", args)
            assert native == before
            assert core.projects.continuity.baseline(key, "doc") == baseline
            assert not core.projects.proofs.leases
            assert len(core.registry.list(include_proofs=True)) == 1


async def test_saved_inspection_retains_one_job_and_status(tmp_path: Path) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with editor(core) as native:
            key, revision = await establish(core)
            value = await core.projects.execute(
                "project.apply",
                dict(
                    project_id=key,
                    expected_revision=revision,
                    checkpoint=dict(id="saved", label="Saved baseline"),
                ),
            )
            revision = value.model_dump()["project"]["revision"]
            saved = copy.deepcopy(native)
            saved["host_session_id"] = "proof-host"
            before = copy.deepcopy(native)
            core.projects.operations.caller_wait_seconds = 0
            args = dict(
                project_id=key,
                expected_revision=revision,
                document_id="doc",
                adapter_id="initial",
                checkpoint_id="saved",
                operation="editor.document.attest",
            )
            async with proof_artifact(core, saved):
                first = await core.projects.execute("project.inspect_saved", args)
                second = await core.projects.execute("project.inspect_saved", args)
                assert first.operation_id == second.operation_id
                status = await core.projects.execute(
                    "project.operation_status",
                    dict(
                        project_id=key, operation_id=first.operation_id, wait_seconds=20
                    ),
                )
                assert status.state == "completed"
                again = await core.projects.execute(
                    "project.operation_status",
                    dict(project_id=key, operation_id=first.operation_id),
                )
                assert again == status
            assert len(core.projects.proofs.metrics) == 1
            assert core.projects.proofs.metrics[0]["cleanup_ok"]
            assert not core.projects.proofs.leases
            assert native == before
