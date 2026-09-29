"""Read verified historical fields without replacing a live working document."""

import json
from typing import TYPE_CHECKING

from tyvrana_protocol import DocumentAttestation, JsonValue

from .continuity import Baseline
from .models import Document, SavedInspectionInput, SavedInspectionResult
from .store import ProjectError

if TYPE_CHECKING:
    from .service import ProjectService


async def inspect_saved(
    service: "ProjectService", project: str, request: SavedInspectionInput
) -> SavedInspectionResult:
    store = service.store
    with store.transaction() as db:
        store.expect(store.project(db, project), request.expected_revision)
        document = store._record(db, project, request.document_id)
        row = db.execute(
            "SELECT data,revision FROM checkpoints WHERE project_id=? AND id=?",
            (project, request.checkpoint_id),
        ).fetchone()
        if not isinstance(document, Document) or row is None:
            raise ProjectError(
                "saved_inspection_missing", "Select a document and retained checkpoint"
            )
        marker = json.loads(row[0])
        snapshot = store.materialize_revision(db, project, row[1])
        value = snapshot["documents"].get(request.document_id)
        if value is None:
            raise ProjectError(
                "saved_inspection_missing", "Checkpoint lacks document evidence"
            )
        target = Baseline.model_validate(value)
        physical = marker["document_states"].get(request.document_id)
        if (
            marker["revision"] != row[1]
            or marker["id"] != request.checkpoint_id
            or physical is None
            or any(
                physical.get(k) != getattr(target, k)
                for k in (
                    "format",
                    "digest",
                    "context_id",
                    "artifact_sha256",
                    "artifact_locator",
                    "application_project_id",
                )
            )
        ):
            raise ProjectError(
                "history_corrupt", "Checkpoint document evidence differs"
            )
        if target.application_project_id != document.application_project_id:
            raise ProjectError(
                "saved_inspection_lineage", "Checkpoint belongs to another document"
            )
    parent = service.core.registry.get(request.adapter_id)
    if (
        parent.registration.application != document.application
        or parent.registration.project_id != document.application_project_id
    ):
        raise ProjectError(
            "saved_inspection_lineage", "Work adapter belongs to another document"
        )
    contracts = {c.name: c for c in parent.registration.operations}
    contract = contracts.get(request.operation)
    if (
        contract is None
        or contract.effect != "read_only"
        or contract.execution != "synchronous"
        or contract.input_artifacts != "none"
        or contract.output_artifacts != "none"
    ):
        raise ProjectError(
            "saved_inspection_operation",
            "Only synchronous artifact-free read-only inspection is permitted",
        )
    artifact = service.proofs.artifact(
        locator=target.artifact_locator,
        sha256=target.artifact_sha256,
        project_id=target.application_project_id,
    )
    before = await service.continuity.observe(parent)
    if before.project_id != document.application_project_id:
        raise ProjectError("saved_inspection_lineage", "Live document identity changed")
    if before.format != target.format:
        raise ProjectError(
            "saved_inspection_format",
            "Activate a qualified verifier of the checkpoint's original format first",
            required_format=target.format,
        )
    async with service.proofs.acquire(parent, artifact) as proof:
        observed = await service.continuity.observe(proof)
        expected = {
            (str(r["resource_kind"]), str(r["resource_id"])): r
            for r in target.resources
        }

        def resources(
            value: DocumentAttestation,
        ) -> dict[tuple[str, str], dict[str, JsonValue]]:
            return {
                k: r.model_dump(mode="json")
                for k, r in service.reconciliation._resources(value).items()
            }

        if (
            observed.project_id != target.application_project_id
            or observed.format != target.format
            or observed.digest != target.digest
            or observed.file_sha256 != target.artifact_sha256
            or observed.resource_scope != target.resource_scope
            or resources(observed) != expected
            or observed.host_session_id == before.host_session_id
        ):
            raise ProjectError(
                "saved_inspection_mismatch",
                "Saved document does not match complete checkpoint evidence",
                expected=dict(
                    digest=target.digest,
                    format=target.format,
                    file_sha256=target.artifact_sha256,
                    resource_scope=target.resource_scope,
                    resource_count=len(expected),
                ),
                observed=dict(
                    digest=observed.digest,
                    format=observed.format,
                    file_sha256=observed.file_sha256,
                    resource_scope=observed.resource_scope,
                    resource_count=len(observed.resources),
                ),
                differing_resources=[
                    list(k)
                    for k in sorted(expected.keys() | resources(observed).keys())
                    if expected.get(k) != resources(observed).get(k)
                ][:32],
            )
        result = await service.core.dispatcher.execute(
            adapter_id=proof.instance_id,
            operation=request.operation,
            arguments=request.arguments,
            _internal=True,
        )
        after = await service.continuity.observe(proof)
        if after.model_dump(exclude={"elapsed_ms", "work"}) != observed.model_dump(
            exclude={"elapsed_ms", "work"}
        ):
            raise ProjectError(
                "saved_inspection_changed", "Inspection changed the proof document"
            )
        live = await service.continuity.observe(parent)
        if service.core.registry.get(
            parent.instance_id
        ).connection_id != parent.connection_id or live.model_dump(
            exclude={"elapsed_ms", "work"}
        ) != before.model_dump(exclude={"elapsed_ms", "work"}):
            raise ProjectError(
                "saved_inspection_changed",
                "Live document or connection changed during historical inspection",
            )
    with store.transaction() as db:
        store.expect(store.project(db, project), request.expected_revision)
    return SavedInspectionResult(
        checkpoint_id=request.checkpoint_id,
        document_id=request.document_id,
        artifact_sha256=artifact.sha256,
        format=target.format,
        digest=target.digest,
        operation=request.operation,
        result=result.result,
    )
