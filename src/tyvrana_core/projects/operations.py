"""Retain project publication and continuation while current evidence is pending."""

import asyncio
import json
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from tyvrana_protocol import DocumentAttestationJob

from ..errors import bounded_error, core_failure
from .continuity import OBSERVATION_OWNER
from .models import (
    ApplyInput,
    ApplyResult,
    AttestationObservation,
    Continuation,
    ContinueInput,
    ProjectOperation,
    ProjectOperationHandle,
    SavedInspectionInput,
    SavedInspectionResult,
    VerifyInput,
)
from .store import ProjectError

if TYPE_CHECKING:
    from ..registry import AdapterInfo
    from .service import ProjectService


class ProjectOperations:
    caller_wait_seconds = 20.0
    execution_seconds = 600.0

    def __init__(self, service: "ProjectService") -> None:
        self.service = service
        self.tasks: dict[
            str, asyncio.Task[ApplyResult | Continuation | SavedInspectionResult]
        ] = {}

    async def shutdown(self) -> None:
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def status(self, project: str, key: str) -> ProjectOperation:
        with self.service.store.transaction() as db:
            row = db.execute(
                "SELECT data FROM document_mutations WHERE project_id=? AND id=?",
                (project, key),
            ).fetchone()
        data = json.loads(row[0]) if row else None
        if not data or data.get("kind") != "project_operation":
            raise ProjectError(
                "operation_missing", "Unknown retained project operation"
            )
        result = ProjectOperation.model_validate(data["result"])
        if result.state == "pending" and key not in self.tasks:
            result = ProjectOperation.model_validate(
                {
                    **result.model_dump(),
                    "state": "failed",
                    "error_code": "operation_interrupted",
                    "error_message": "Core interrupted; no batch will be replayed",
                }
            )
        return result

    async def observe_status(
        self, project: str, key: str, wait_seconds: float
    ) -> ProjectOperation:
        self.status(project, key)
        task = self.tasks.get(key)
        if task and wait_seconds:
            await asyncio.wait({task}, timeout=wait_seconds)
        return self.status(project, key)

    def continuation(self, project: str) -> list[ProjectOperationHandle]:
        with self.service.store.transaction() as db:
            keys = [
                r[0]
                for r in db.execute(
                    "SELECT id FROM document_mutations WHERE project_id=? AND "
                    "json_extract(data,'$.kind')='project_operation' AND "
                    "json_extract(data,'$.result.state')='pending' "
                    "ORDER BY rowid DESC LIMIT 8",
                    (project,),
                )
            ]
        return [
            ProjectOperationHandle.model_validate(
                self.status(project, k).model_dump(exclude={"result"})
            )
            for k in keys
            if self.tasks.get(k) is not asyncio.current_task()
        ]

    async def start(
        self,
        project: str,
        operation: str,
        request: ApplyInput | ContinueInput | VerifyInput | SavedInspectionInput,
    ) -> ApplyResult | Continuation | SavedInspectionResult | ProjectOperation:
        args = request.model_dump(mode="json")
        duplicate = None
        with self.service.store.transaction() as db:
            for row in db.execute(
                "SELECT id,data FROM document_mutations WHERE project_id=?", (project,)
            ):
                stored = json.loads(row[1])
                if (
                    row[0] in self.tasks
                    and stored.get("kind") == "project_operation"
                    and stored.get("request") == args
                    and stored["result"]["operation"] == operation
                ):
                    duplicate = row[0]
                    break
        if duplicate:
            return await self.observe_status(
                project, duplicate, self.caller_wait_seconds
            )
        key = uuid4().hex
        data: dict[str, Any] = dict(
            kind="project_operation",
            request=args,
            result=ProjectOperation(
                operation_id=key, operation=operation, state="pending"
            ).model_dump(),
        )

        def write() -> None:
            with self.service.store.transaction(write=True) as db:
                db.execute(
                    "UPDATE document_mutations SET data=? WHERE id=?",
                    (json.dumps(data), key),
                )

        def progress(
            adapter: "AdapterInfo", job: DocumentAttestationJob, name: str
        ) -> None:
            data["result"]["attestation"] = AttestationObservation(
                adapter_id=adapter.instance_id,
                job_id=job.job_id,
                operation=name,
                state=job.state,
                digest=job.result.digest if job.result else None,
            ).model_dump()
            write()

        with self.service.store.transaction(write=True) as db:
            db.execute(
                "INSERT INTO document_mutations VALUES (?,?,?,?)",
                (key, project, "", json.dumps(data)),
            )

        async def execute() -> ApplyResult | Continuation | SavedInspectionResult:
            result = await self.service.execute(operation, args)
            assert isinstance(
                result, (ApplyResult, Continuation, SavedInspectionResult)
            )
            return result

        async def run() -> ApplyResult | Continuation | SavedInspectionResult:
            token = OBSERVATION_OWNER.set(progress)
            try:
                async with asyncio.timeout(self.execution_seconds):
                    if isinstance(request, (ApplyInput, VerifyInput)):
                        async with self.service.mutations.lock:
                            result = await execute()
                    else:
                        result = await execute()
                data["result"].update(
                    state="completed",
                    next_action="inspect_result",
                    result=result.model_dump(mode="json"),
                )
                return result
            except BaseException as exc:
                error = (
                    bounded_error(exc.code, str(exc), exc.details)
                    if isinstance(exc, ProjectError)
                    else bounded_error(
                        "verification_timeout"
                        if isinstance(exc, TimeoutError)
                        else "operation_interrupted",
                        "Project operation stopped without publication",
                    )
                    if isinstance(exc, (TimeoutError, asyncio.CancelledError))
                    else core_failure(exc)
                )
                data["result"].update(
                    state="failed",
                    next_action="inspect_failure",
                    error_code=error.code,
                    error_message=error.message,
                    error_details=error.details,
                )
                raise
            finally:
                OBSERVATION_OWNER.reset(token)
                write()

        task = asyncio.create_task(run())
        self.tasks[key] = task

        def finished(
            done: asyncio.Task[ApplyResult | Continuation | SavedInspectionResult],
        ) -> None:
            self.tasks.pop(key, None)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finished)
        try:
            async with asyncio.timeout(self.caller_wait_seconds):
                return await asyncio.shield(task)
        except TimeoutError:
            return self.status(project, key)
