"""Atomic checkpoint checkout, preserving the abandoned semantic head as history."""

import json
import sqlite3
from typing import TYPE_CHECKING, Any

from tyvrana_protocol import DocumentAttestation

from .continuity import Baseline
from .models import RECORD, Change, Document, Milestone
from .restore_models import RestoreInput, RestoreResult
from .store import ProjectError, now

if TYPE_CHECKING:
    from ..registry import AdapterInfo
    from .restore import TrustedRestore


def commit_checkout(
    restore: "TrustedRestore",
    db: sqlite3.Connection,
    project: str,
    request: RestoreInput,
    data: dict[str, Any],
    target: Baseline,
    snapshot: dict[str, Any],
    final: DocumentAttestation,
    live: "AdapterInfo",
) -> None:
    """Caller holds the mutation lock and verified content immediately before commit."""
    service, store = restore.service, restore.service.store
    state = store.project(db, project)
    store.expect(state, request.expected_revision)
    if service.core.registry.get(live.instance_id).connection_id != live.connection_id:
        raise ProjectError("application_changed", "Adapter reconnected before checkout")
    marker = db.execute(
        "SELECT revision FROM checkpoints WHERE project_id=? AND id=?",
        (project, request.checkpoint_id),
    ).fetchone()
    base = int(marker[0])
    desired = {
        key: RECORD.validate_python(value) for key, value in snapshot["records"].items()
    }
    documents = [r for r in desired.values() if isinstance(r, Document)]
    if (
        len(documents) != 1
        or documents[0].id != request.document_id
        or (
            documents[0].application_project_id != final.project_id
            or documents[0].application != live.registration.application
        )
    ):
        raise ProjectError("restore_lineage", "Checkpoint document lineage differs")
    desired[request.document_id] = documents[0].model_copy(
        update={"adapter_id": live.instance_id}
    )
    before = store.working_snapshot(db, state)
    project_values = {key: snapshot["project"][key] for key in ("title", "goal")}
    project_values.update(
        stage=snapshot["project"]["stage"],
        next_action=snapshot["project"]["next_action"],
        working_base_checkpoint=request.checkpoint_id,
        working_base_revision=base,
    )
    unchanged = (
        all(getattr(state, key) == value for key, value in project_values.items())
        and before["records"]
        == {k: v.model_dump(mode="json") for k, v in desired.items()}
        and before["record_revisions"] == snapshot["record_revisions"]
        and before["historical_acceptances"] == snapshot["historical_acceptances"]
        and before["observations"] == snapshot["observations"]
        and before["validation_context"] == snapshot["validation_context"]
    )
    revision = state.revision + int(not unchanged)
    if not unchanged:
        # Full values are retained, not just change labels. Neither the old journal,
        # acceptance receipts, checkpoints nor native mutation receipts are removed.
        data["abandoned_snapshot"] = before
        revisions = snapshot["record_revisions"]
        db.execute("DELETE FROM refs WHERE project_id=?", (project,))
        db.execute("DELETE FROM records WHERE project_id=?", (project,))
        for key, record in desired.items():
            store._put(db, project, record, revision, "verified")
            db.execute(
                "UPDATE records SET revision=? WHERE project_id=? AND id=?",
                (revisions[key], project, key),
            )
        for key in before["records"].keys() - desired.keys():
            old = before["records"][key]
            store._change(
                db,
                project,
                Change(
                    revision=revision,
                    kind=old["kind"],
                    id=key,
                    action="removed",
                    label=old["label"],
                ),
            )
        for table in ("observations", "validation_context"):
            for key, value in snapshot[table].items():
                db.execute(
                    f"INSERT INTO {table} VALUES (?,?,?)",
                    (project, key, json.dumps(value)),
                )
        store._integrity(db, project)
        updated_project = state.model_copy(
            update={
                **project_values,
                "revision": revision,
                "checkout_revision": revision,
                "updated_at": now(),
            }
        )
        db.execute(
            "UPDATE projects SET data=? WHERE id=?",
            (updated_project.model_dump_json(), project),
        )
        store._change(
            db,
            project,
            Change(
                revision=revision,
                kind="project",
                id=project,
                action="verified",
                label=f"Checked out {request.checkpoint_id} at revision {base}",
                status=snapshot["project"]["stage"],
            ),
        )
    baseline = target.model_copy(
        update=dict(
            host_session_id=final.host_session_id,
            document_session_id=final.document_session_id,
            resources=[r.model_dump(mode="json") for r in final.resources],
            resource_scope=final.resource_scope,
        )
    )
    db.execute(
        "INSERT OR REPLACE INTO document_attestations VALUES (?,?,?)",
        (project, request.document_id, baseline.model_dump_json()),
    )
    # Report effective snapshot acceptance, including qualified format projections;
    # never turn stored stale/failed validations into new passing claims.
    accepted = []
    for row in db.execute(
        "SELECT * FROM records WHERE project_id=? AND kind='milestone'", (project,)
    ):
        view = store._view(
            db, project, row, {request.document_id: target.context_id}, set()
        )
        if isinstance(view.record, Milestone) and view.record.status == "accepted":
            accepted.append(view.record.id)
    data["result"] = RestoreResult(
        restore_id=request.restore_id,
        state="completed",
        revision=revision,
        digest=final.digest,
        restored_milestones=sorted(accepted),
        already_current=unchanged,
        base_revision=base,
        abandoned_revision=None if unchanged else state.revision,
    ).model_dump()
    data["postflight"] = final.model_dump(mode="json")
    db.execute(
        "UPDATE document_mutations SET data=? WHERE id=?",
        (json.dumps(data), request.restore_id),
    )
