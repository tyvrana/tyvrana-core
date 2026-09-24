"""A format change preserves trust only for the exact durable artifact."""

import copy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from tyvrana_core import AdapterServer, CoreConfig
from tyvrana_core.projects.continuity import AttestInput
from tyvrana_core.projects.store import ProjectError

from .proof_fixture import proof_artifact
from .test_continuity import establish, evidence, state
from .test_working_lineage import editor


async def accepted_chain(core: AdapterServer) -> tuple[str, int]:
    project, revision = await establish(core)
    await core.projects.execute(
        "project.apply",
        dict(
            project_id=project,
            expected_revision=revision,
            upsert=[
                dict(
                    kind="milestone",
                    id="second",
                    label="Second approval",
                    status="accepted",
                    entity_ids=["part"],
                    document_ids=["doc"],
                    validation_ids=["check"],
                    prerequisite_ids=["stage"],
                    acceptance="Previously checked geometry",
                )
            ],
        ),
    )
    await core.projects.execute(
        "project.apply",
        dict(
            project_id=project,
            expected_revision=3,
            project=dict(next_action="Verify continuity"),
        ),
    )
    with core.projects.store.transaction(write=True) as db:
        for key, changes in [
            ("stage", {"status": "invalidated"}),
            ("second", {"status": "invalidated"}),
            ("check", {"freshness": "stale"}),
        ]:
            record = core.projects.store._record(db, project, key)
            core.projects.store._put(
                db, project, record.model_copy(update=changes), 4, "staled"
            )
        # A baseline predating resource-closure metadata is still anchored by its SHA.
        baseline = core.projects.continuity.baseline(project, "doc")
        assert baseline is not None
        db.execute(
            "UPDATE document_attestations SET data=? WHERE project_id=?",
            (
                baseline.model_copy(
                    update=dict(resources=[], resource_scope=None)
                ).model_dump_json(),
                project,
            ),
        )
    return project, 4


def request(project: str, revision: int) -> dict[str, Any]:
    return dict(
        project_id=project,
        document_id="doc",
        adapter_id="initial",
        expected_revision=revision,
        mode="migrate",
        trusted_artifact_sha256="b" * 64,
        from_format="fixture-canonical",
        to_format="fixture-authored",
        provenance="Canonical runtime-context exclusion",
    )


def snapshot(core: AdapterServer) -> dict[str, list[tuple[Any, ...]]]:
    with core.projects.store.transaction() as db:
        return {
            table: [
                tuple(row)
                for row in db.execute(f"SELECT * FROM {table} ORDER BY rowid")
            ]
            for (table,) in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            if table not in {"document_attestations", "document_mutations"}
        }


def new_format(*values: dict[str, Any]) -> None:
    for value in values:
        value.update(format="fixture-authored", digest="c" * 64)


async def test_format_migration_preserves_history_and_is_idempotent(
    tmp_path: Path,
) -> None:
    config = CoreConfig(port=0, state_directory=str(tmp_path))
    async with AdapterServer(config) as core:
        async with (
            editor(core) as live,
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
            project, revision = await accepted_chain(core)
            before = snapshot(core)
            new_format(live, proof)
            native_before = copy.deepcopy(live)
            assert await state(core, project) == ("invalidated", revision)
            result = (
                await core.projects.execute(
                    "project.attest", request(project, revision)
                )
            ).model_dump()
            assert result["baseline_migrated"]
            assert not result["semantic_revision_changed"]
            assert snapshot(core) == before
            assert live == native_before
            baseline = core.projects.continuity.baseline(project, "doc")
            assert baseline is not None and baseline.format == "fixture-authored"
            assert baseline.digest == "c" * 64
            assert baseline.format_migration is not None
            assert set(baseline.format_migration.claims) == {"stage", "second"}
            assert await state(core, project) == ("accepted", revision)
            with core.projects.store.transaction() as db:
                packet = core.projects.store._gates(
                    db, project, {"doc": baseline.context_id}, set()
                )
                assert packet.acceptance["second"] == []
                assert packet.activation["second"] == []
                for key, accepted in [("stage", 2), ("second", 3)]:
                    row = db.execute(
                        "SELECT * FROM records WHERE project_id=? AND id=?",
                        (project, key),
                    ).fetchone()
                    view = core.projects.store._view(
                        db, project, row, {"doc": baseline.context_id}, set()
                    )
                    assert view.record.model_dump()["status"] == "accepted"
                    assert view.historical_status == "accepted"
                    assert view.accepted_revision == accepted
            repeated = (
                await core.projects.execute(
                    "project.attest", request(project, revision)
                )
            ).model_dump()
            assert repeated["already_migrated"] and not repeated["baseline_migrated"]
            assert core.projects.continuity.baseline(project, "doc") == baseline
            assert snapshot(core) == before
    # The migration is durable; reading it does not need the proof host again.
    async with AdapterServer(config) as core:
        async with editor(core) as live:
            new_format(live)
            assert await state(core, project) == ("accepted", revision)


@pytest.mark.parametrize(
    "case,code",
    [
        ("live_file", "migration_file_mismatch"),
        ("proof_file", "migration_file_mismatch"),
        ("requested_file", "migration_file_mismatch"),
        ("digest", "migration_digest_mismatch"),
        ("format", "migration_digest_mismatch"),
        ("lineage", "migration_lineage"),
        ("proof_document", "application_changed"),
        ("same_host", "migration_lineage"),
        ("missing", "migration_baseline_missing"),
        ("missing_sha", "migration_artifact_missing"),
        ("incomplete", "attestation_incomplete"),
    ],
)
async def test_migration_rejects_untrusted_evidence(
    tmp_path: Path, case: str, code: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with (
            editor(core) as live,
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
            project, revision = await accepted_chain(core)
            new_format(live, proof)
            args = request(project, revision)
            if case == "live_file":
                live["file_sha256"] = "d" * 64
            elif case == "proof_file":
                proof["file_sha256"] = "d" * 64
            elif case == "requested_file":
                args["trusted_artifact_sha256"] = "d" * 64
            elif case == "digest":
                proof["digest"] = "d" * 64
            elif case == "format":
                proof["format"] = "different-format"
            elif case == "proof_document":
                proof["project_id"] = "different-document"
            elif case == "same_host":
                proof["host_session_id"] = live["host_session_id"]
            elif case == "incomplete":
                proof.update(
                    status="unsupported", digest=None, omissions=["Uncovered content"]
                )
            else:
                with core.projects.store.transaction(write=True) as db:
                    if case == "missing":
                        db.execute(
                            "DELETE FROM document_attestations WHERE project_id=?",
                            (project,),
                        )
                    else:
                        baseline = core.projects.continuity.baseline(project, "doc")
                        assert baseline is not None
                        changes = (
                            dict(application_project_id="wrong")
                            if case == "lineage"
                            else dict(artifact_sha256=None)
                        )
                        db.execute(
                            "UPDATE document_attestations SET data=? "
                            "WHERE project_id=?",
                            (
                                baseline.model_copy(update=changes).model_dump_json(),
                                project,
                            ),
                        )
            before = snapshot(core)
            baseline_before = core.projects.continuity.baseline(project, "doc")
            with pytest.raises(ProjectError) as error:
                await core.projects.execute("project.attest", args)
            assert error.value.code == code
            assert not core.projects.proofs.leases
            assert [a.instance_id for a in core.registry.list(include_proofs=True)] == [
                "initial"
            ]
            assert snapshot(core) == before
            assert core.projects.continuity.baseline(project, "doc") == baseline_before


@pytest.mark.parametrize("when", ["before", "after"])
async def test_changed_claims_do_not_regain_freshness(
    tmp_path: Path, when: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with (
            editor(core) as live,
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
            project, revision = await accepted_chain(core)
            new_format(live, proof)
            if when == "after":
                await core.projects.execute(
                    "project.attest", request(project, revision)
                )
                assert await state(core, project) == ("accepted", revision)
            await core.projects.execute(
                "project.apply",
                dict(
                    project_id=project,
                    expected_revision=revision,
                    upsert=[dict(kind="entity", id="part", label="A revised claim")],
                ),
            )
            revision += 1
            if when == "before":
                await core.projects.execute(
                    "project.attest", request(project, revision)
                )
            assert await state(core, project) == ("invalidated", revision)


async def test_existing_gates_still_block_migrated_prerequisites(
    tmp_path: Path,
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with (
            editor(core) as live,
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
            project, revision = await accepted_chain(core)
            new_format(live, proof)
            await core.projects.execute("project.attest", request(project, revision))
            await core.projects.execute(
                "project.apply",
                dict(
                    project_id=project,
                    expected_revision=revision,
                    upsert=[
                        dict(
                            kind="issue",
                            id="blocker",
                            label="Observed defect",
                            entity_ids=["part"],
                            severity="major",
                            status="open",
                            summary="Requires further work",
                        )
                    ],
                ),
            )
            assert await state(core, project) == ("invalidated", revision + 1)


@pytest.mark.parametrize("dependency", ["resource", "observation", "new_binding"])
async def test_later_dependency_changes_invalidate_migration_proof(
    tmp_path: Path, dependency: str
) -> None:
    async with AdapterServer(CoreConfig(port=0, state_directory=str(tmp_path))) as core:
        async with (
            editor(core) as live,
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
            project, revision = await accepted_chain(core)
            new_format(live, proof)
            await core.projects.execute("project.attest", request(project, revision))
            if dependency == "new_binding":
                await core.projects.execute(
                    "project.apply",
                    dict(
                        project_id=project,
                        expected_revision=revision,
                        upsert=[
                            dict(
                                kind="binding",
                                id="additional",
                                label="Additional dependency",
                                entity_id="part",
                                document_id="doc",
                                resource_kind="mesh",
                                resource_id="additional",
                            )
                        ],
                    ),
                )
                revision += 1
            else:
                with core.projects.store.transaction(write=True) as db:
                    if dependency == "observation":
                        db.execute(
                            "UPDATE observations SET "
                            "data=json_set(data,'$.state','unverified') "
                            "WHERE project_id=?",
                            (project,),
                        )
                    else:
                        # A later proved working head must not reuse old resource trust.
                        live["resources"][0]["fingerprint"] = "changed-resource"
                        baseline = core.projects.continuity.baseline(project, "doc")
                        assert baseline is not None
                        db.execute(
                            "UPDATE document_attestations SET data=? "
                            "WHERE project_id=?",
                            (
                                baseline.model_copy(
                                    update=dict(resources=live["resources"])
                                ).model_dump_json(),
                                project,
                            ),
                        )
            assert await state(core, project) == ("invalidated", revision)


@pytest.mark.parametrize(
    "change",
    [
        {"from_format": None},
        {"to_format": "fixture-canonical"},
        {"mode": "capture"},
        {"force": True},
    ],
)
def test_explicit_migration_intent_has_no_force_flag(change: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        AttestInput.model_validate({**request("project", 1), **change})
