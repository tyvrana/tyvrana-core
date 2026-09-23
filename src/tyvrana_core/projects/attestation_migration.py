"""Format-migration evidence, without rewriting semantic acceptance history."""

import json
import sqlite3

from pydantic import Field

from .models import BindingObservation, Model


class FormatMigration(Model):
    from_format: str
    to_format: str
    previous_digest: str
    proof_host_session_id: str
    proof_document_session_id: str
    claims: dict[str, dict[str, int]] = Field(default_factory=dict)
    observations: dict[str, BindingObservation] = Field(default_factory=dict)
    resource_scope: str
    resource_fingerprints: dict[str, str] = Field(default_factory=dict)


def claim_revisions(db: sqlite3.Connection, project: str, stage: str) -> dict[str, int]:
    """Include references and resource/outgoing relationship dependencies."""
    return dict(
        db.execute(
            "WITH RECURSIVE closure(id) AS (VALUES(?) UNION "
            "SELECT r.target FROM refs r JOIN closure c ON r.owner=c.id "
            "WHERE r.project_id=? UNION "
            "SELECT r.id FROM records r JOIN closure c ON "
            "(r.kind='binding' AND json_extract(r.data,'$.entity_id')=c.id) OR "
            "(r.kind='relationship' AND json_extract(r.data,'$.source_id')=c.id) "
            "WHERE r.project_id=?) "
            "SELECT id,revision FROM records WHERE project_id=? "
            "AND id IN (SELECT id FROM closure)",
            (stage, project, project, project),
        )
    )


def verified_projection(
    db: sqlite3.Connection,
    project: str,
    connections: dict[str, str],
    record_id: str,
) -> tuple[bool, BindingObservation | None]:
    """Derive freshness only while the exact proved claims remain unchanged."""
    if (
        not connections
        or not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='document_attestations'"
        ).fetchone()
    ):
        return False, None
    for row in db.execute(
        "SELECT document_id,data FROM document_attestations WHERE project_id=?",
        (project,),
    ):
        baseline = json.loads(row["data"])
        if (
            not baseline.get("format_migration")
            or connections.get(row["document_id"]) != baseline["context_id"]
        ):
            continue
        migration = FormatMigration.model_validate(baseline["format_migration"])
        resources = {
            (r["resource_kind"], r["resource_id"]): r
            for r in baseline.get("resources", [])
        }
        if baseline.get("resource_scope") != migration.resource_scope:
            continue
        for stage, claims in migration.claims.items():
            if record_id not in claims or claim_revisions(db, project, stage) != claims:
                continue
            # A later verification must not be overwritten by this earlier proof.
            for key in claims.keys() & migration.observations.keys():
                binding_row = db.execute(
                    "SELECT data FROM records WHERE project_id=? AND id=?",
                    (project, key),
                ).fetchone()
                binding = json.loads(binding_row[0])
                resource = resources.get(
                    (binding["resource_kind"], binding["resource_id"])
                )
                if (
                    resource is None
                    or resource["state"] != "present"
                    or resource["fingerprint"] != migration.resource_fingerprints[key]
                ):
                    break
                saved = db.execute(
                    "SELECT data FROM observations WHERE project_id=? AND id=?",
                    (project, key),
                ).fetchone()
                if (
                    saved is None
                    or BindingObservation.model_validate_json(saved[0])
                    != migration.observations[key]
                ):
                    break
            else:
                observation = migration.observations.get(record_id)
                if observation:
                    observation = observation.model_copy(
                        update=dict(
                            state="verified", connection_id=baseline["context_id"]
                        )
                    )
                return True, observation
    return False, None
