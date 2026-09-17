"Local materialized semantic state and bounded revision journal in SQLite."

import json
import sqlite3
import threading
from collections import Counter
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from .models import (
    RECORD,
    ApplyInput,
    ApplyResult,
    Binding,
    BindingObservation,
    Change,
    Checkpoint,
    Continuation,
    CreateInput,
    Delta,
    DeltaInput,
    Document,
    Evidence,
    Issue,
    Milestone,
    Project,
    RecordView,
    Relationship,
    SearchInput,
    SearchResult,
    SemanticRecord,
    Validation,
)

HISTORY_REVISIONS = 256
HISTORY_CHANGES = 20000
MAX_RECORDS = 50000
MAX_PROJECTS = 100
MAX_CHECKPOINTS = 128
PACKET_BYTES = 32768


class ProjectError(Exception):
    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


def now() -> str:
    return datetime.now(UTC).isoformat()


def references(record: SemanticRecord) -> list[tuple[str, str]]:
    if isinstance(record, Relationship):
        return [(record.source_id, "entity"), (record.target_id, "entity")]
    if isinstance(record, Binding):
        return [(record.entity_id, "entity"), (record.document_id, "document")]
    if isinstance(record, (Milestone, Issue, Validation)):
        refs = [(key, "entity") for key in record.entity_ids]
        if isinstance(record, Milestone):
            refs += [(key, "validation") for key in record.validation_ids]
        if isinstance(record, Validation):
            refs += [(key, "evidence") for key in record.evidence_ids]
        return refs
    if isinstance(record, Evidence) and record.binding_id:
        return [(record.binding_id, "binding")]
    return []


class ProjectStore:
    "Connections are transaction-scoped; multiple core processes share one store."

    def __init__(self, path: Path) -> None:
        self.path = path
        self._ready = False
        self._initialize_lock = threading.Lock()

    def _initialize(self) -> None:
        with self._initialize_lock:
            if self._ready:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with closing(sqlite3.connect(self.path)) as db:
                db.executescript("""
                    PRAGMA journal_mode=WAL;
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE IF NOT EXISTS projects (
                        id TEXT PRIMARY KEY, data TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS records (
                        project_id TEXT NOT NULL
                            REFERENCES projects(id) ON DELETE CASCADE,
                        id TEXT NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL,
                        revision INTEGER NOT NULL,
                        PRIMARY KEY(project_id,id)
                    );
                    CREATE INDEX IF NOT EXISTS records_kind ON records(project_id,kind);
                    CREATE TABLE IF NOT EXISTS refs (
                        project_id TEXT NOT NULL, owner TEXT NOT NULL,
                        target TEXT NOT NULL, target_kind TEXT NOT NULL,
                        PRIMARY KEY(project_id,owner,target),
                        FOREIGN KEY(project_id,owner) REFERENCES records(project_id,id)
                            ON DELETE CASCADE,
                        FOREIGN KEY(project_id,target) REFERENCES records(project_id,id)
                            DEFERRABLE INITIALLY DEFERRED
                    );
                    CREATE INDEX IF NOT EXISTS refs_target ON refs(project_id,target);
                    CREATE TABLE IF NOT EXISTS observations (
                        project_id TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL,
                        PRIMARY KEY(project_id,id),
                        FOREIGN KEY(project_id,id) REFERENCES records(project_id,id)
                            ON DELETE CASCADE
                    );
                    CREATE TABLE IF NOT EXISTS validation_context (
                        project_id TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL,
                        PRIMARY KEY(project_id,id),
                        FOREIGN KEY(project_id,id) REFERENCES records(project_id,id)
                            ON DELETE CASCADE
                    );
                    CREATE TABLE IF NOT EXISTS journal (
                        project_id TEXT NOT NULL
                            REFERENCES projects(id) ON DELETE CASCADE,
                        revision INTEGER NOT NULL, kind TEXT NOT NULL,
                        id TEXT NOT NULL, data TEXT NOT NULL,
                        PRIMARY KEY(project_id,revision,kind,id)
                    );
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        project_id TEXT NOT NULL
                            REFERENCES projects(id) ON DELETE CASCADE,
                        id TEXT NOT NULL, revision INTEGER NOT NULL, data TEXT NOT NULL,
                        PRIMARY KEY(project_id,id)
                    );
                """)
            self.path.chmod(0o600)
            self._ready = True

    @contextmanager
    def transaction(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        self._initialize()
        db = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.create_function(
            "casefold", 1, lambda value: str(value).casefold(), deterministic=True
        )
        try:
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def project(db: sqlite3.Connection, project_id: str) -> Project:
        row = db.execute(
            "SELECT data FROM projects WHERE id=?", (project_id,)
        ).fetchone()
        if row is None:
            raise ProjectError(
                "project_not_found", "Select an existing project", project_id=project_id
            )
        return Project.model_validate_json(row[0])

    @staticmethod
    def expect(project: Project, expected: int | None) -> None:
        if expected is not None and project.revision != expected:
            raise ProjectError(
                "revision_conflict",
                "Read project.delta, then retry with the current revision",
                expected_revision=expected,
                current_revision=project.revision,
                project_id=project.id,
            )

    @staticmethod
    def _save_project(db: sqlite3.Connection, project: Project) -> None:
        db.execute(
            "INSERT INTO projects VALUES (?,?)",
            (project.id, project.model_dump_json()),
        )

    @staticmethod
    def _change(db: sqlite3.Connection, project_id: str, change: Change) -> None:
        db.execute(
            "INSERT OR REPLACE INTO journal VALUES (?,?,?,?,?)",
            (
                project_id,
                change.revision,
                change.kind,
                change.id,
                change.model_dump_json(),
            ),
        )

    def list_projects(self) -> list[Project]:
        with self.transaction() as db:
            return [
                Project.model_validate_json(r[0])
                for r in db.execute("SELECT data FROM projects ORDER BY id")
            ]

    def document_project(
        self, application: str, application_project_id: str
    ) -> Project | None:
        if not self.path.exists():
            return None
        with self.transaction() as db:
            row = db.execute(
                "SELECT project_id FROM records WHERE kind='document' AND "
                "json_extract(data,'$.application')=? AND "
                "json_extract(data,'$.application_project_id')=?",
                (application, application_project_id),
            ).fetchone()
            return self.project(db, row[0]) if row else None

    def select(self, project_id: str | None, connected: set[tuple[str, str]]) -> str:
        with self.transaction() as db:
            if project_id is not None:
                return self.project(db, project_id).id
            projects = [
                Project.model_validate_json(r[0])
                for r in db.execute("SELECT data FROM projects ORDER BY id")
            ]
            matches = {
                r["project_id"]
                for r in db.execute(
                    "SELECT project_id,data FROM records WHERE kind='document'"
                )
                if (
                    json.loads(r["data"])["application"],
                    json.loads(r["data"])["application_project_id"],
                )
                in connected
            }
            if len(matches) == 1:
                return str(next(iter(matches)))
            if not connected and len(projects) == 1:
                return projects[0].id
            code = "project_not_selected" if not projects else "project_ambiguous"
            raise ProjectError(
                code,
                (
                    "Choose project_id using project.search(kind='project'), or "
                    "create/attach a project"
                ),
                candidates=[
                    {"id": p.id, "title": p.title, "revision": p.revision}
                    for p in projects[:10]
                ],
                project_count=len(projects),
            )

    def create(self, request: CreateInput) -> Project:
        with self.transaction(write=True) as db:
            if (
                db.execute("SELECT count(*) FROM projects").fetchone()[0]
                >= MAX_PROJECTS
            ):
                raise ProjectError(
                    "project_limit", "Remove unused semantic project state first"
                )
            stamp = now()
            project = Project(
                id=uuid4().hex,
                title=request.title,
                goal=request.goal,
                stage=request.stage,
                revision=1,
                created_at=stamp,
                updated_at=stamp,
            )
            self._save_project(db, project)
            self._change(
                db,
                project.id,
                Change(
                    revision=1,
                    kind="project",
                    id=project.id,
                    action="created",
                    label=project.title,
                ),
            )
            return project

    @staticmethod
    def _record(db: sqlite3.Connection, project_id: str, key: str) -> SemanticRecord:
        row = db.execute(
            "SELECT data FROM records WHERE project_id=? AND id=?", (project_id, key)
        ).fetchone()
        if row is None:
            raise ProjectError(
                "unknown_record", "Record does not exist in this project", id=key
            )
        return RECORD.validate_json(row[0])

    def _put(
        self,
        db: sqlite3.Connection,
        project_id: str,
        record: SemanticRecord,
        revision: int,
        action: str | None = None,
    ) -> None:
        old = db.execute(
            "SELECT kind FROM records WHERE project_id=? AND id=?",
            (project_id, record.id),
        ).fetchone()
        if old and old[0] != record.kind:
            raise ProjectError(
                "incompatible_id",
                "A record ID cannot change kind",
                id=record.id,
                existing_kind=old[0],
            )
        db.execute(
            (
                "INSERT INTO records VALUES (?,?,?,?,?) ON "
                "CONFLICT(project_id,id) DO UPDATE SET "
                "data=excluded.data,revision=excluded.revision"
            ),
            (project_id, record.id, record.kind, record.model_dump_json(), revision),
        )
        db.execute(
            "DELETE FROM refs WHERE project_id=? AND owner=?", (project_id, record.id)
        )
        for target, kind in set(references(record)):
            db.execute(
                "INSERT INTO refs VALUES (?,?,?,?)",
                (project_id, record.id, target, kind),
            )
        self._change(
            db,
            project_id,
            Change.model_validate(
                {
                    "revision": revision,
                    "kind": record.kind,
                    "id": record.id,
                    "action": action or ("updated" if old else "created"),
                    "label": record.label,
                    "status": getattr(record, "status", ""),
                }
            ),
        )

    @staticmethod
    def _integrity(db: sqlite3.Connection, project_id: str) -> None:
        broken = db.execute(
            (
                "SELECT refs.owner,refs.target,refs.target_kind FROM refs LEFT "
                "JOIN records ON records.project_id=refs.project_id AND "
                "records.id=refs.target WHERE refs.project_id=? AND (records.id "
                "IS NULL OR records.kind<>refs.target_kind) LIMIT 8"
            ),
            (project_id,),
        ).fetchall()
        if broken:
            raise ProjectError(
                "invalid_reference",
                (
                    "Update or remove dependents in the same batch; references must "
                    "exist with the required kind"
                ),
                references=[dict(r) for r in broken],
            )
        duplicate = db.execute(
            (
                "SELECT "
                "json_extract(data,'$.source_id'),json_extract(data,'$.target_id'"
                "),json_extract(data,'$.relation') FROM records WHERE project_id="
                "? AND kind='relationship' GROUP BY 1,2,3 HAVING count(*)>1 OR js"
                "on_extract(data,'$.source_id')=json_extract(data,'$.target_id') "
                "LIMIT 1"
            ),
            (project_id,),
        ).fetchone()
        if duplicate:
            raise ProjectError(
                "invalid_relationship",
                "Relationships must be unique and connect different entities",
            )
        documents = db.execute(
            "SELECT "
            "json_extract(data,'$.application'),json_extract(data,'$.applicat"
            "ion_project_id') FROM records WHERE kind='document' GROUP BY 1,2"
            " HAVING count(*)>1 LIMIT 1"
        ).fetchone()
        if documents:
            raise ProjectError(
                "document_already_attached",
                (
                    "A saved application document can belong to only one semantic "
                    "project; reuse its document record"
                ),
            )
        if (
            db.execute(
                "SELECT count(*) FROM records WHERE project_id=? AND kind='document'",
                (project_id,),
            ).fetchone()[0]
            > 64
        ):
            raise ProjectError(
                "document_limit", "At most64 application documents per project"
            )
        if (
            db.execute(
                "SELECT count(*) FROM records WHERE project_id=?", (project_id,)
            ).fetchone()[0]
            > MAX_RECORDS
        ):
            raise ProjectError(
                "record_limit", "Project record limit exceeded", maximum=MAX_RECORDS
            )

    def _stale(
        self,
        db: sqlite3.Connection,
        project_id: str,
        entities: set[str],
        revision: int,
        exclude: set[str],
    ) -> None:
        if not entities:
            return
        for row in db.execute(
            "SELECT data FROM records WHERE project_id=? AND kind='validation'",
            (project_id,),
        ).fetchall():
            record = RECORD.validate_json(row[0])
            assert isinstance(record, Validation)
            if (
                record.id not in exclude
                and record.freshness != "stale"
                and entities.intersection(record.entity_ids)
            ):
                self._put(
                    db,
                    project_id,
                    record.model_copy(update={"freshness": "stale"}),
                    revision,
                    "staled",
                )

    @staticmethod
    def _contexts(
        db: sqlite3.Connection,
        project_id: str,
        record: Validation,
        connections: dict[str, str],
    ) -> dict[str, str]:
        documents = set()
        for row in db.execute(
            "SELECT data FROM records WHERE project_id=? AND kind='binding'",
            (project_id,),
        ):
            binding = RECORD.validate_json(row[0])
            assert isinstance(binding, Binding)
            if binding.entity_id in record.entity_ids:
                documents.add(binding.document_id)
        return {key: connections.get(key, "") for key in sorted(documents)}

    def _checkpoint(
        self,
        db: sqlite3.Connection,
        project: Project,
        request: ApplyInput,
        connections: dict[str, str],
    ) -> Checkpoint | None:
        if request.checkpoint is None:
            return None
        marker = request.checkpoint
        if db.execute(
            "SELECT 1 FROM checkpoints WHERE project_id=? AND id=?",
            (project.id, marker.id),
        ).fetchone():
            raise ProjectError(
                "checkpoint_exists", "Named checkpoints are immutable; choose a new ID"
            )
        if (
            db.execute(
                "SELECT count(*) FROM checkpoints WHERE project_id=?", (project.id,)
            ).fetchone()[0]
            >= MAX_CHECKPOINTS
        ):
            raise ProjectError(
                "checkpoint_limit",
                "Named checkpoint limit reached",
                maximum=MAX_CHECKPOINTS,
            )
        accepted = [
            r[0]
            for r in db.execute(
                (
                    "SELECT id FROM records WHERE project_id=? AND kind='milestone' "
                    "AND json_extract(data,'$.status')='accepted' ORDER BY id"
                ),
                (project.id,),
            )
        ]
        documents = [
            r[0]
            for r in db.execute(
                (
                    "SELECT id FROM records WHERE project_id=? AND kind='document' "
                    "ORDER BY id"
                ),
                (project.id,),
            )
        ]
        validation_rows = db.execute(
            "SELECT * FROM records WHERE project_id=? AND kind='validation'",
            (project.id,),
        ).fetchall()
        views = [
            self._view(db, project.id, row, connections, set())
            for row in validation_rows
        ]
        counts = Counter(
            f"{view.record.status}:{view.freshness}"
            for view in views
            if isinstance(view.record, Validation)
        )
        checkpoint = Checkpoint(
            **marker.model_dump(),
            revision=project.revision,
            stage=project.stage,
            created_at=project.updated_at,
            accepted_milestones=accepted[:16],
            accepted_count=len(accepted),
            documents=documents[:16],
            document_count=len(documents),
            validation_counts=dict(counts),
        )
        db.execute(
            "INSERT INTO checkpoints VALUES (?,?,?,?)",
            (project.id, marker.id, project.revision, checkpoint.model_dump_json()),
        )
        self._change(
            db,
            project.id,
            Change(
                revision=project.revision,
                kind="checkpoint",
                id=f"checkpoint:{marker.id}",
                action="created",
                label=marker.label,
            ),
        )
        return checkpoint

    @staticmethod
    def _compact(db: sqlite3.Connection, project: Project) -> Project:
        floor = max(project.history_floor, project.revision - HISTORY_REVISIONS)
        row = db.execute(
            (
                "SELECT revision FROM journal WHERE project_id=? ORDER BY "
                "revision DESC LIMIT 1 OFFSET ?"
            ),
            (project.id, HISTORY_CHANGES),
        ).fetchone()
        if row:
            floor = max(floor, row[0])
        db.execute(
            "DELETE FROM journal WHERE project_id=? AND revision<=?",
            (project.id, floor),
        )
        return project.model_copy(update={"history_floor": floor})

    def apply(
        self,
        project_id: str,
        request: ApplyInput,
        connections: dict[str, str] | None = None,
        observations: dict[str, BindingObservation] | None = None,
    ) -> ApplyResult:
        with self.transaction(write=True) as db:
            project = self.project(db, project_id)
            self.expect(project, request.expected_revision)
            revision = project.revision + 1
            changed_entities: set[str] = set()
            for record in request.upsert:
                old = db.execute(
                    "SELECT data FROM records WHERE project_id=? AND id=?",
                    (project_id, record.id),
                ).fetchone()
                if (
                    record.kind == "entity"
                    and old
                    and old[0] != record.model_dump_json()
                ):
                    changed_entities.add(record.id)
                if isinstance(record, Relationship) and (
                    not old or old[0] != record.model_dump_json()
                ):
                    changed_entities.update((record.source_id, record.target_id))
                if isinstance(record, Binding) and not old:
                    changed_entities.add(record.entity_id)
                if (
                    isinstance(record, Binding)
                    and old
                    and old[0] != record.model_dump_json()
                ):
                    previous = RECORD.validate_json(old[0])
                    assert isinstance(previous, Binding)
                    changed_entities.update((record.entity_id, previous.entity_id))
                    db.execute(
                        "DELETE FROM observations WHERE project_id=? AND id=?",
                        (project_id, record.id),
                    )
                if isinstance(record, Document) and old:
                    previous = RECORD.validate_json(old[0])
                    assert isinstance(previous, Document)
                    if (previous.application, previous.application_project_id) != (
                        record.application,
                        record.application_project_id,
                    ):
                        raise ProjectError(
                            "document_identity_immutable",
                            (
                                "Create a different document record and explicitl"
                                "y rebind "
                                "resources"
                            ),
                        )
                self._put(db, project_id, record, revision)
            for key in request.remove:
                record = self._record(db, project_id, key)
                if isinstance(record, Binding):
                    changed_entities.add(record.entity_id)
                elif isinstance(record, Relationship):
                    changed_entities.update((record.source_id, record.target_id))
                db.execute(
                    "DELETE FROM records WHERE project_id=? AND id=?", (project_id, key)
                )
                self._change(
                    db,
                    project_id,
                    Change(
                        revision=revision,
                        kind=record.kind,
                        id=key,
                        action="removed",
                        label=record.label,
                    ),
                )
            if request.upsert or request.remove:
                self._integrity(db, project_id)
            self._stale(
                db,
                project_id,
                changed_entities,
                revision,
                {r.id for r in request.upsert if isinstance(r, Validation)},
            )
            for record in request.upsert:
                if isinstance(record, Validation):
                    context = self._contexts(db, project_id, record, connections or {})
                    db.execute(
                        "INSERT OR REPLACE INTO validation_context VALUES (?,?,?)",
                        (project_id, record.id, json.dumps(context, sort_keys=True)),
                    )
            for key, observation in (observations or {}).items():
                record = self._record(db, project_id, key)
                if not isinstance(record, Binding):
                    raise ProjectError(
                        "invalid_binding",
                        "Only resource bindings can be verified",
                        id=key,
                    )
                previous = db.execute(
                    "SELECT data FROM observations WHERE project_id=? AND id=?",
                    (project_id, key),
                ).fetchone()
                old_observation = (
                    BindingObservation.model_validate_json(previous[0])
                    if previous
                    else None
                )
                if observation.state != "verified" or (
                    old_observation
                    and (
                        old_observation.fingerprint != observation.fingerprint
                        or old_observation.connection_id != observation.connection_id
                    )
                ):
                    self._stale(db, project_id, {record.entity_id}, revision, set())
                db.execute(
                    "INSERT OR REPLACE INTO observations VALUES (?,?,?)",
                    (
                        project_id,
                        key,
                        observation.model_copy(
                            update={"verified_revision": revision}
                        ).model_dump_json(),
                    ),
                )
                self._change(
                    db,
                    project_id,
                    Change(
                        revision=revision,
                        kind="binding",
                        id=key,
                        action="verified",
                        label=record.label,
                        status=observation.state,
                    ),
                )
            updates = (
                request.project.model_dump(exclude_none=True) if request.project else {}
            )
            project = project.model_copy(
                update={**updates, "revision": revision, "updated_at": now()}
            )
            if updates:
                self._change(
                    db,
                    project_id,
                    Change(
                        revision=revision,
                        kind="project",
                        id=project_id,
                        action="updated",
                        label=project.title,
                        status=project.stage,
                    ),
                )
            for key in request.forget_checkpoints:
                marker = db.execute(
                    "SELECT data FROM checkpoints WHERE project_id=? AND id=?",
                    (project_id, key),
                ).fetchone()
                if marker is None:
                    raise ProjectError(
                        "checkpoint_not_found",
                        "Cannot forget an unknown checkpoint",
                        id=key,
                    )
                db.execute(
                    "DELETE FROM checkpoints WHERE project_id=? AND id=?",
                    (project_id, key),
                )
                self._change(
                    db,
                    project_id,
                    Change(
                        revision=revision,
                        kind="checkpoint",
                        id=f"checkpoint:{key}",
                        action="removed",
                        label=Checkpoint.model_validate_json(marker[0]).label,
                    ),
                )
            checkpoint = self._checkpoint(db, project, request, connections or {})
            project = self._compact(db, project)
            # UPDATE avoids REPLACE's delete/cascade semantics for existing projects.
            db.execute(
                "UPDATE projects SET data=? WHERE id=?",
                (project.model_dump_json(), project_id),
            )
            changes = [
                Change.model_validate_json(r[0])
                for r in db.execute(
                    (
                        "SELECT data FROM journal WHERE project_id=? AND revision=? "
                        "ORDER BY id"
                    ),
                    (project_id, revision),
                )
            ]
            return ApplyResult(
                project=project,
                counts=dict(Counter(f"{c.kind}:{c.action}" for c in changes)),
                changed_ids=[c.id for c in changes[:32]],
                changed_count=len(changes),
                changed_ids_truncated=len(changes) > 32,
                checkpoint=checkpoint,
            )

    def invalidate(self, application: str, application_project_id: str) -> None:
        "Coalesce observed application writes; no semantic interpretation or plan."
        with self.transaction(write=True) as db:
            documents = db.execute(
                (
                    "SELECT project_id,id FROM records WHERE kind='document' AND "
                    "json_extract(data,'$.application')=? AND "
                    "json_extract(data,'$.application_project_id')=?"
                ),
                (application, application_project_id),
            ).fetchall()
            for document in documents:
                project_id = document["project_id"]
                project = self.project(db, project_id)
                revision = project.revision + 1
                entities = set()
                changed = False
                for row in db.execute(
                    (
                        "SELECT r.data,o.data AS observation FROM records r LEFT JOIN "
                        "observations o ON r.project_id=o.project_id AND r.id=o.i"
                        "d WHERE "
                        "r.project_id=? AND r.kind='binding' AND "
                        "json_extract(r.data,'$.document_id')=?"
                    ),
                    (project_id, document["id"]),
                ).fetchall():
                    binding = RECORD.validate_json(row["data"])
                    assert isinstance(binding, Binding)
                    entities.add(binding.entity_id)
                    if row["observation"]:
                        observation = BindingObservation.model_validate_json(
                            row["observation"]
                        )
                        if observation.state == "verified":
                            db.execute(
                                "UPDATE observations SET data=? WHERE project_id="
                                "? AND id=?",
                                (
                                    observation.model_copy(
                                        update={"state": "stale"}
                                    ).model_dump_json(),
                                    project_id,
                                    binding.id,
                                ),
                            )
                            self._change(
                                db,
                                project_id,
                                Change(
                                    revision=revision,
                                    kind="binding",
                                    id=binding.id,
                                    action="staled",
                                    label=binding.label,
                                    status="stale",
                                ),
                            )
                            changed = True
                self._stale(db, project_id, entities, revision, set())
                changed = changed or bool(
                    db.execute(
                        "SELECT 1 FROM journal WHERE project_id=? AND revision=?",
                        (project_id, revision),
                    ).fetchone()
                )
                if changed:
                    project = self._compact(
                        db,
                        project.model_copy(
                            update={"revision": revision, "updated_at": now()}
                        ),
                    )
                    db.execute(
                        "UPDATE projects SET data=? WHERE id=?",
                        (project.model_dump_json(), project_id),
                    )

    def _view(
        self,
        db: sqlite3.Connection,
        project_id: str,
        row: sqlite3.Row,
        connections: dict[str, str],
        available_artifacts: set[str],
    ) -> RecordView:
        record = RECORD.validate_json(row["data"])
        binding = None
        freshness = None
        availability: Literal["available", "expired", "unverified"] | None = None
        if isinstance(record, Binding):
            observed = db.execute(
                "SELECT data FROM observations WHERE project_id=? AND id=?",
                (project_id, record.id),
            ).fetchone()
            binding = (
                BindingObservation.model_validate_json(observed[0])
                if observed
                else BindingObservation(state="unverified")
            )
            if (
                binding.connection_id != connections.get(record.document_id)
                or not binding.connection_id
            ):
                binding = binding.model_copy(update={"state": "unverified"})
        if isinstance(record, Validation):
            freshness = record.freshness
            context_row = db.execute(
                "SELECT data FROM validation_context WHERE project_id=? AND id=?",
                (project_id, record.id),
            ).fetchone()
            context = json.loads(context_row[0]) if context_row else {}
            if freshness == "current" and any(
                not session or connections.get(key) != session
                for key, session in context.items()
            ):
                freshness = "unverified"
            record = record.model_copy(update={"freshness": freshness})
        if isinstance(record, Evidence):
            availability = "unverified"
            if record.storage == "ephemeral_artifact":
                availability = (
                    "available"
                    if record.artifact_id in available_artifacts
                    else "expired"
                )
        return RecordView(
            record=record,
            changed_revision=row["revision"],
            binding=binding,
            freshness=freshness,
            evidence_availability=availability,
        )

    def documents(self, project_id: str) -> list[Document]:
        with self.transaction() as db:
            self.project(db, project_id)
            return [
                Document.model_validate_json(r[0])
                for r in db.execute(
                    (
                        "SELECT data FROM records WHERE project_id=? AND kind='do"
                        "cument' "
                        "ORDER BY id"
                    ),
                    (project_id,),
                )
            ]

    def bindings(self, project_id: str, ids: list[str]) -> list[Binding]:
        with self.transaction() as db:
            result = []
            for key in ids:
                record = self._record(db, project_id, key)
                if not isinstance(record, Binding):
                    raise ProjectError(
                        "invalid_binding", "Requested record is not a binding", id=key
                    )
                result.append(record)
            return result

    def _search(
        self,
        db: sqlite3.Connection,
        project_id: str,
        request: SearchInput,
        connections: dict[str, str],
        artifacts: set[str],
    ) -> SearchResult:
        project = self.project(db, project_id)
        self.expect(project, request.at_revision)
        if request.kind == "checkpoint":
            rows = db.execute(
                (
                    "SELECT data FROM checkpoints WHERE project_id=? ORDER BY "
                    "revision DESC,id"
                ),
                (project_id,),
            ).fetchall()
            checkpoints = [Checkpoint.model_validate_json(r[0]) for r in rows]
            checkpoints = [
                c
                for c in checkpoints
                if (not request.ids or c.id in request.ids)
                and (
                    not request.query
                    or request.query.casefold()
                    in (c.label + " " + c.purpose).casefold()
                )
            ]
            end = request.offset + request.limit
            return SearchResult(
                project_id=project_id,
                revision=project.revision,
                checkpoints=checkpoints[request.offset : end],
                matched_count=len(checkpoints),
                next_offset=end if end < len(checkpoints) else None,
            )
        clauses = ["r.project_id=?"]
        values: list[Any] = [project_id]
        for field, value in [
            ("kind", request.kind),
            ("status", request.status),
            ("stage", request.stage),
            ("entity_type", request.entity_type),
            ("relation", request.relation),
        ]:
            if value is not None:
                clauses.append(f"json_extract(r.data,'$.{field}')=?")
                values.append(value)
        if request.ids:
            clauses.append("r.id IN (" + ",".join("?" for _ in request.ids) + ")")
            values.extend(request.ids)
        if request.query:
            for term in request.query.casefold().split()[:16]:
                clauses.append(
                    "instr(casefold(json_extract(r.data,'$.label') || ' ' || "
                    "json_extract(r.data,'$.summary')),?)>0"
                )
                values.append(term)
        if request.related_to:
            clauses.append(
                "EXISTS (SELECT 1 FROM refs WHERE refs.project_id=r.project_id "
                "AND refs.owner=r.id AND refs.target=?)"
            )
            values.append(request.related_to)
        if request.tag:
            clauses.append(
                "EXISTS (SELECT 1 FROM json_each(r.data,'$.tags') WHERE value=?)"
            )
            values.append(request.tag)
        if request.application:
            clauses.append(
                "(json_extract(r.data,'$.application')=? OR "
                "json_extract(r.data,'$.document_id') IN (SELECT id FROM records "
                "WHERE project_id=r.project_id AND kind='document' AND "
                "json_extract(data,'$.application')=?))"
            )
            values.extend([request.application, request.application])
        where = " AND ".join(clauses)
        order = " ORDER BY json_extract(r.data,'$.importance') DESC,r.id"
        if request.binding_state:
            # Only the binding subset needs connection-sensitive filtering.
            clauses.append("r.kind='binding'")
            where = " AND ".join(clauses)
            candidates = [
                self._view(db, project_id, r, connections, artifacts)
                for r in db.execute(
                    "SELECT r.* FROM records r WHERE " + where + order, values
                )
            ]
            matches = [
                r
                for r in candidates
                if r.binding and r.binding.state == request.binding_state
            ]
            count = len(matches)
            records = matches[request.offset : request.offset + request.limit]
        else:
            count = db.execute(
                "SELECT count(*) FROM records r WHERE " + where, values
            ).fetchone()[0]
            rows = db.execute(
                "SELECT r.* FROM records r WHERE "
                + where
                + order
                + " LIMIT ? OFFSET ?",
                [*values, request.limit, request.offset],
            ).fetchall()
            records = [
                self._view(db, project_id, r, connections, artifacts) for r in rows
            ]
        end = request.offset + request.limit
        return SearchResult(
            project_id=project_id,
            revision=project.revision,
            records=records,
            matched_count=count,
            next_offset=end if end < count else None,
        )

    def search(
        self,
        project_id: str,
        request: SearchInput,
        connections: dict[str, str] | None = None,
        artifacts: set[str] | None = None,
    ) -> SearchResult:
        with self.transaction() as db:
            return self._search(
                db, project_id, request, connections or {}, artifacts or set()
            )

    def _delta(
        self,
        db: sqlite3.Connection,
        project_id: str,
        request: DeltaInput,
        connections: dict[str, str],
        artifacts: set[str],
    ) -> Delta:
        project = self.project(db, project_id)
        self.expect(project, request.at_revision)
        start = request.since_revision
        if request.checkpoint_id:
            marker = db.execute(
                "SELECT revision FROM checkpoints WHERE project_id=? AND id=?",
                (project_id, request.checkpoint_id),
            ).fetchone()
            if not marker:
                raise ProjectError(
                    "checkpoint_not_found",
                    "Choose a checkpoint from project.search(kind='checkpoint')",
                )
            start = marker[0]
        assert start is not None
        if start < project.history_floor:
            raise ProjectError(
                "history_expired",
                (
                    "Checkpoint remains a marker; older changes were compacted. "
                    "Retrieve current continuation."
                ),
                requested_revision=start,
                history_floor=project.history_floor,
                current_revision=project.revision,
            )
        if start > project.revision:
            raise ProjectError(
                "invalid_revision",
                "Revision is ahead of this project",
                current_revision=project.revision,
            )
        rows = db.execute(
            (
                "SELECT data FROM journal WHERE project_id=? AND revision>? "
                "ORDER BY revision,id"
            ),
            (project_id, start),
        ).fetchall()
        changes = [Change.model_validate_json(r[0]) for r in rows]
        selected = changes[request.offset : request.offset + request.limit]
        records = []
        if request.details:
            for key in dict.fromkeys(c.id for c in selected):
                row = db.execute(
                    "SELECT * FROM records WHERE project_id=? AND id=?",
                    (project_id, key),
                ).fetchone()
                if row:
                    records.append(
                        self._view(db, project_id, row, connections, artifacts)
                    )
        end = request.offset + request.limit
        return Delta(
            project_id=project_id,
            from_revision=start,
            revision=project.revision,
            history_floor=project.history_floor,
            counts=dict(Counter(f"{c.kind}:{c.action}" for c in changes)),
            matched_count=len(changes),
            changes=selected,
            records=records,
            next_offset=end if end < len(changes) else None,
        )

    def delta(
        self,
        project_id: str,
        request: DeltaInput,
        connections: dict[str, str] | None = None,
        artifacts: set[str] | None = None,
    ) -> Delta:
        with self.transaction() as db:
            return self._delta(
                db, project_id, request, connections or {}, artifacts or set()
            )

    def continuation(
        self,
        project_id: str,
        since_revision: int | None,
        connections: dict[str, str],
        artifacts: set[str],
    ) -> Continuation:
        with self.transaction() as db:
            project = self.project(db, project_id)
            marker = db.execute(
                (
                    "SELECT data FROM checkpoints WHERE project_id=? ORDER BY "
                    "revision DESC LIMIT 1"
                ),
                (project_id,),
            ).fetchone()
            checkpoint = Checkpoint.model_validate_json(marker[0]) if marker else None
            counts = {
                r[0]: r[1]
                for r in db.execute(
                    (
                        "SELECT kind,count(*) FROM records WHERE project_id=? GROUP BY "
                        "kind"
                    ),
                    (project_id,),
                )
            }
            records: list[RecordView] = []
            # Explicit importance, open concerns and active stage determine selection.
            limits = {
                "issue": 4,
                "milestone": 4,
                "validation": 4,
                "entity": 6,
                "binding": 6,
                "relationship": 4,
                "document": 3,
                "evidence": 1,
            }
            relevant: set[str] = set()
            for kind, limit in limits.items():
                keys = sorted(relevant)[:128]
                placeholders = ",".join("?" for _ in keys) or "NULL"
                relevance = (
                    f"id IN ({placeholders}) OR "
                    f"json_extract(data,'$.entity_id') IN ({placeholders}) OR "
                    f"json_extract(data,'$.source_id') IN ({placeholders}) OR "
                    f"json_extract(data,'$.target_id') IN ({placeholders})"
                )
                rows = db.execute(
                    "SELECT * FROM records WHERE project_id=? AND kind=? ORDER BY "
                    "CASE WHEN json_extract(data,'$.status') IN "
                    "('open','failed','in_progress') THEN 0 ELSE 1 END, "
                    "CASE WHEN json_extract(data,'$.severity')='critical' "
                    "THEN 0 ELSE 1 END, "
                    f"CASE WHEN {relevance} THEN 0 ELSE 1 END, "
                    "CASE WHEN json_extract(data,'$.stage')=? AND ?<>'' "
                    "THEN 0 ELSE 1 END, "
                    "json_extract(data,'$.importance') DESC,id LIMIT ?",
                    (
                        project_id,
                        kind,
                        *keys,
                        *keys,
                        *keys,
                        *keys,
                        project.stage,
                        project.stage,
                        limit,
                    ),
                ).fetchall()
                selected_views = [
                    self._view(db, project_id, row, connections, artifacts)
                    for row in rows
                ]
                records.extend(selected_views)
                for view in selected_views:
                    if isinstance(view.record, (Issue, Milestone, Validation)):
                        relevant.update(view.record.entity_ids)
            notices = []
            start = (
                since_revision
                if since_revision is not None
                else (checkpoint.revision if checkpoint else None)
            )
            delta = None
            if start is not None:
                try:
                    delta = self._delta(
                        db,
                        project_id,
                        DeltaInput(since_revision=start, limit=5),
                        connections,
                        artifacts,
                    )
                except ProjectError as exc:
                    if exc.code != "history_expired":
                        raise
                    notices.append(
                        f"Delta unavailable before revision {project.history_floor}; "
                        "checkpoint marker survives."
                    )
            packet = Continuation(
                project=project,
                checkpoint=checkpoint,
                records=records,
                counts=counts,
                omitted_counts={},
                applications=[],
                recent_delta=delta,
                notices=notices,
            )
            while (
                len(packet.model_dump_json().encode()) > PACKET_BYTES - 4096
                and packet.records
            ):
                packet.records.pop()
            selected = Counter(r.record.kind for r in packet.records)
            omitted = {
                kind: count - selected[kind]
                for kind, count in counts.items()
                if count > selected[kind]
            }
            return packet.model_copy(update={"omitted_counts": omitted})

    def remove(
        self, project_id: str, expected_revision: int, confirmation: str
    ) -> None:
        if project_id != confirmation:
            raise ProjectError(
                "confirmation_mismatch",
                "confirm_project_id must equal the selected project_id",
            )
        with self.transaction(write=True) as db:
            self.expect(self.project(db, project_id), expected_revision)
            # Delete reference edges before project cascade to satisfy deferred targets.
            db.execute("DELETE FROM refs WHERE project_id=?", (project_id,))
            db.execute("DELETE FROM projects WHERE id=?", (project_id,))
