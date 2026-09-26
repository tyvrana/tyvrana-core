"""Coherent document metadata, validation contexts and checkpoint projections."""

from pathlib import Path

import pytest

from tyvrana_core import AdapterServer, CoreConfig

from .test_continuity import connected, establish, evidence


@pytest.mark.parametrize("status", ["passed", "failed", "warning", "unknown"])
async def test_document_metadata_and_validation_share_strong_context(
    tmp_path: Path,
    status: str,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence()):
            key, revision = await establish(core)
            before = await core.projects.execute(
                "project.continue", {"project_id": key}
            )
            packet = before.model_dump(mode="json")
            validation = next(
                r["record"] for r in packet["records"] if r["record"]["id"] == "check"
            )
            validation["status"] = status
            document = core.projects.store.documents(key)[0].model_copy(
                update={"summary": "Same saved document, reviewed metadata"}
            )
            result = await core.projects.execute(
                "project.apply",
                {
                    "project_id": key,
                    "expected_revision": revision,
                    "upsert": [document.model_dump(mode="json"), validation],
                    "checkpoint": {"id": "review", "label": "Reviewed state"},
                },
            )
            saved = result.model_dump(mode="json")
            assert saved["checkpoint"]["validation_counts"] == {f"{status}:current": 1}
            after = await core.projects.execute("project.continue", {"project_id": key})
            current = after.model_dump(mode="json")
            checked = next(
                r for r in current["records"] if r["record"]["id"] == "check"
            )
            assert checked["record"]["freshness"] == "current"
            assert checked["record"]["status"] == status
            assert current["checkpoint"]["scope"] == "historical"
            assert current["project"]["revision"] == revision + 1


async def test_unavailable_document_metadata_cannot_promote_validation(
    tmp_path: Path,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        live = evidence()
        async with connected(core, "initial", live):
            key, revision = await establish(core)
            checkpoint = await core.projects.execute(
                "project.apply",
                {
                    "project_id": key,
                    "expected_revision": revision,
                    "checkpoint": {"id": "accepted", "label": "Historical proof"},
                },
            )
            revision = checkpoint.model_dump()["project"]["revision"]
            live["digest"] = "f" * 64
            document = core.projects.store.documents(key)[0].model_copy(
                update={"summary": "Metadata is not proof of changed content"}
            )
            await core.projects.execute(
                "project.apply",
                {
                    "project_id": key,
                    "expected_revision": revision,
                    "upsert": [document.model_dump(mode="json")],
                },
            )
            packet = (
                await core.projects.execute("project.continue", {"project_id": key})
            ).model_dump()
            check = next(r for r in packet["records"] if r["record"]["id"] == "check")
            assert check["record"]["status"] == "passed"
            assert check["freshness"] == "unverified"
            assert check["freshness_reason"] == "document_unverified"
            assert packet["checkpoint"]["scope"] == "historical"
            assert packet["checkpoint"]["validation_counts"] == {"passed:current": 1}
            assert packet["project"]["revision"] == revision + 1


async def test_new_document_checkpoint_establishes_context(tmp_path: Path) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence()):
            key, revision = await establish(core, capture=False)
            result = await core.projects.execute(
                "project.apply",
                {
                    "project_id": key,
                    "expected_revision": revision,
                    "checkpoint": {"id": "first", "label": "Initial evidence"},
                },
            )
            assert result.model_dump()["checkpoint"]["validation_counts"] == {
                "passed:current": 1
            }
            current = (
                await core.projects.execute("project.continue", {"project_id": key})
            ).model_dump()
            check = next(r for r in current["records"] if r["record"]["id"] == "check")
            assert check["freshness"] == "current"
            assert check.get("freshness_reason") is None


async def test_explicit_rebind_does_not_use_transport_as_strong_evidence(
    tmp_path: Path,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with connected(core, "initial", evidence()):
            key, revision = await establish(core)
            async with connected(
                core, "other", evidence(host="other", digest="f" * 64)
            ):
                doc = core.projects.store.documents(key)[0].model_copy(
                    update={"adapter_id": "other"}
                )
                await core.projects.execute(
                    "project.apply",
                    {
                        "project_id": key,
                        "expected_revision": revision,
                        "upsert": [doc.model_dump(mode="json")],
                    },
                )
                current = (
                    await core.projects.execute("project.continue", {"project_id": key})
                ).model_dump()
                check = next(
                    r for r in current["records"] if r["record"]["id"] == "check"
                )
                assert check["record"]["status"] == "passed"
                assert check["freshness"] == "stale"
                assert check["freshness_reason"] == "dependencies_changed"
