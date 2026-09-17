"""Typed discovery and execution tools for the official MCP server."""

import asyncio
import base64
import json
import logging
import re
from typing import Literal

import anyio
from mcp.server import ServerRequestContext
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    ImageContent,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
    ToolAnnotations,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from tyvrana_protocol import (
    ArtifactDescriptor,
    ArtifactId,
    Identifier,
    JsonValue,
    OperationSuccess,
    QualifiedName,
)

from ..artifacts import ArtifactError
from ..errors import (
    AdapterDisconnected,
    AdapterNotFound,
    OperationTimeout,
    RemoteOperationError,
    UnsupportedOperation,
)
from ..server import AdapterServer

logger = logging.getLogger(__name__)


class _ToolModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ListAdaptersInput(_ToolModel):
    """Filter discovery and optionally wait for registration or a revision change."""

    application: str | None = Field(default=None, min_length=1, max_length=256)
    adapter_id: Identifier | None = None
    wait_seconds: float = Field(default=0.0, ge=0, le=30, allow_inf_nan=False)
    after_revision: int | None = Field(default=None, ge=0)


class AdapterSummary(_ToolModel):
    instance_id: Identifier
    application: str
    application_version: str | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    project_path: str | None = Field(default=None, exclude_if=lambda v: v is None)
    operation_count: int
    catalog_sha256: str


class ListAdaptersOutput(_ToolModel):
    adapters: tuple[AdapterSummary, ...]
    revision: int


class ListOperationsInput(_ToolModel):
    adapter_id: Identifier
    names: list[QualifiedName] | None = Field(default=None, min_length=1, max_length=16)
    prefix: str = Field(default="", max_length=128)
    query: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        description="Search name/description terms; any match, most terms first.",
    )
    offset: int = Field(default=0, ge=0, le=512)
    limit: int = Field(default=20, ge=1, le=50)
    include_schemas: bool = False

    @field_validator("query")
    @classmethod
    def searchable_query(cls, query: str | None) -> str | None:
        if query is not None and not any(character.isalnum() for character in query):
            raise ValueError("Search query must contain letters or numbers")
        return query

    @field_validator("names")
    @classmethod
    def unique_names(cls, names: list[str] | None) -> list[str] | None:
        if names is not None and len(set(names)) != len(names):
            raise ValueError("Operation names must be unique")
        return names


class DiscoveredOperation(_ToolModel):
    name: QualifiedName
    description: str
    effect: Literal["read_only", "mutating", "transient", "lifecycle"]
    execution: Literal["synchronous", "job_start", "job_status", "lifecycle"]
    requires_interactive: bool
    input_artifacts: Literal["none", "required"]
    output_artifacts: Literal["none", "optional", "required"]
    arguments_schema: dict[str, JsonValue] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    result_schema: dict[str, JsonValue] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


class ListOperationsOutput(_ToolModel):
    adapter_id: Identifier
    catalog_sha256: str
    matched_count: int
    next_offset: int | None
    operations: tuple[DiscoveredOperation, ...]


class ExecuteOperationInput(_ToolModel):
    adapter_id: Identifier
    operation: QualifiedName
    arguments: JsonValue
    artifact_ids: tuple[ArtifactId, ...] = Field(
        default=(), max_length=8, json_schema_extra={"uniqueItems": True}
    )

    @field_validator("artifact_ids", mode="before")
    @classmethod
    def accept_array(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("artifact_ids")
    @classmethod
    def unique_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("Attached artifact IDs must be unique")
        return value


class ImportArtifactInput(_ToolModel):
    path: str = Field(min_length=1)
    name: str | None = Field(default=None, min_length=1, max_length=255)
    media_type: str | None = Field(default=None, min_length=3, max_length=127)

    @field_validator("*", mode="before")
    @classmethod
    def reject_null(cls, value: object) -> object:
        if value is None:
            raise ValueError("Omit optional properties; null is invalid")
        return value


class ReleaseArtifactInput(_ToolModel):
    artifact_id: ArtifactId


class ReleaseArtifactOutput(_ToolModel):
    released: bool


class ExecuteOperationOutput(_ToolModel):
    result: JsonValue
    artifacts: tuple[ArtifactDescriptor, ...] = Field(
        default=(), exclude_if=lambda v: not v
    )


async def list_tools(
    ctx: ServerRequestContext[AdapterServer], params: PaginatedRequestParams | None
) -> ListToolsResult:
    return ListToolsResult(
        tools=[
            Tool(
                name="tyvrana_list_adapters",
                description=(
                    "Discover currently connected applications before choosing an "
                    "adapter. Returns adapter instance IDs, application metadata, "
                    "and a cacheable operation catalog hash/count. Filter by "
                    "application "
                    "or adapter_id. wait_seconds waits for a match, or with "
                    "after_revision "
                    "for a registry change, without client polling. An empty list "
                    "means no application adapter is connected. Application mutations "
                    "must use advertised typed operations. Report missing "
                    "capabilities; do not bypass a connected adapter."
                ),
                input_schema=ListAdaptersInput.model_json_schema(),
                output_schema=ListAdaptersOutput.model_json_schema(
                    mode="serialization"
                ),
                annotations=ToolAnnotations(read_only_hint=True),
            ),
            Tool(
                name="tyvrana_list_operations",
                description=(
                    "Search operation names/descriptions with query keywords "
                    "(any term matches; more matching terms rank first, then name). "
                    "Intersect with exact names or prefix; results are paginated. "
                    "Start with summaries: limit defaults to 20, maximum 50. "
                    "Descriptions include effect, execution/context and artifact "
                    "behavior. "
                    "Set include_schemas for self-contained argument/result "
                    "JSON Schemas "
                    "(at most four contracts per page). Cache by catalog_sha256; "
                    "request "
                    "schemas before constructing arguments, preserve omitted fields, "
                    "and do not infer native state constraints from schema alone."
                ),
                input_schema=ListOperationsInput.model_json_schema(),
                output_schema=ListOperationsOutput.model_json_schema(
                    mode="serialization"
                ),
                annotations=ToolAnnotations(read_only_hint=True),
            ),
            Tool(
                name="tyvrana_execute_operation",
                description=(
                    "Execute one typed, advertised operation on one connected adapter "
                    "using the core deadline. The arguments contract is specific to "
                    "the selected application operation. Optional artifact_ids attach "
                    "complete core-owned inputs, transferred before execution. "
                    "Results may include artifacts and images; operation failures "
                    "are returned as MCP tool errors. Discover capabilities first with "
                    "tyvrana_list_adapters, then tyvrana_list_operations for schemas. "
                    "Application mutations must use these typed "
                    "operations. Report missing capabilities; do not bypass a "
                    "connected adapter."
                ),
                input_schema=ExecuteOperationInput.model_json_schema(),
                output_schema=ExecuteOperationOutput.model_json_schema(
                    mode="serialization"
                ),
            ),
            Tool(
                name="tyvrana_import_artifact",
                description=(
                    "Copy a local regular file into core-owned temporary storage. "
                    "The path is local to core and never sent to adapters. "
                    "Adapters receive artifact bytes, not the source path. Returns "
                    "a reusable artifact descriptor whose ID can be attached to "
                    "operations; release it when finished."
                ),
                input_schema=ImportArtifactInput.model_json_schema(),
                output_schema=ArtifactDescriptor.model_json_schema(
                    mode="serialization"
                ),
            ),
            Tool(
                name="tyvrana_release_artifact",
                description=(
                    "Release a core-owned temporary artifact by ID when it is no "
                    "longer needed. Idempotent; new uses are prevented, while "
                    "already admitted transfers retain their bytes until finished."
                ),
                input_schema=ReleaseArtifactInput.model_json_schema(),
                output_schema=ReleaseArtifactOutput.model_json_schema(),
                annotations=ToolAnnotations(idempotent_hint=True),
            ),
        ]
    )


def _success(output: BaseModel) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=output.model_dump_json())],
        structured_content=output.model_dump(mode="json"),
    )


def _failure(
    code: str, message: str, details: JsonValue = None, operation: str | None = None
) -> CallToolResult:
    error: dict[str, JsonValue] = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    if operation is not None:
        error["operation"] = operation
    return CallToolResult(
        is_error=True,
        content=[
            TextContent(
                type="text",
                text=json.dumps(
                    error, ensure_ascii=False, allow_nan=False, sort_keys=True
                ),
            )
        ],
    )


def _core_failure(
    error: AdapterNotFound
    | UnsupportedOperation
    | RemoteOperationError
    | OperationTimeout
    | AdapterDisconnected,
    operation: str | None = None,
) -> CallToolResult:
    if isinstance(error, RemoteOperationError):
        return _failure(
            error.error.code, error.error.message, error.error.details, operation
        )
    if isinstance(error, AdapterNotFound):
        code = "adapter_not_found"
    elif isinstance(error, UnsupportedOperation):
        code = "operation_unsupported"
    elif isinstance(error, OperationTimeout):
        code = "operation_timeout"
    else:
        code = "adapter_disconnected"
    return _failure(code, str(error), operation=operation)


async def _execute(
    core: AdapterServer, request: ExecuteOperationInput
) -> OperationSuccess:
    # Keep core's edge-triggered asyncio cancellation independent of the SDK's
    # level-triggered AnyIO scopes. Own and join the worker on every exit path.
    work = asyncio.create_task(
        core.dispatcher.execute(
            adapter_id=request.adapter_id,
            operation=request.operation,
            arguments=request.arguments,
            artifact_ids=request.artifact_ids,
        )
    )
    try:
        return await asyncio.shield(work)
    except asyncio.CancelledError:
        work.cancel()
        with anyio.CancelScope(shield=True):
            await asyncio.gather(work, return_exceptions=True)
        if not work.cancelled() and work.exception() is None:
            for descriptor in work.result().artifacts:
                core.artifacts.release(descriptor.artifact_id)
        raise


def _operation_success(
    core: AdapterServer, response: OperationSuccess
) -> CallToolResult:
    try:
        images = [
            descriptor
            for descriptor in response.artifacts
            if descriptor.media_type in ("image/png", "image/jpeg")
        ]
        if (
            sum(descriptor.byte_size for descriptor in images)
            > core.config.max_inline_image_bytes
        ):
            return _failure(
                "image_too_large",
                "Images exceed the MCP inline byte limit; use a smaller output",
            )
        output = _success(
            ExecuteOperationOutput(result=response.result, artifacts=response.artifacts)
        )
        for descriptor in images:
            with core.artifacts.open(descriptor.artifact_id) as stream:
                data = stream.read(core.config.max_inline_image_bytes + 1)
            if len(data) != descriptor.byte_size:
                raise ArtifactError("Completed image size changed")
            output.content.append(
                ImageContent(
                    type="image",
                    mime_type=descriptor.media_type,
                    data=base64.b64encode(data).decode("ascii"),
                )
            )
        return output
    finally:
        for descriptor in response.artifacts:
            core.artifacts.release(descriptor.artifact_id)


async def _adapters(
    core: AdapterServer, request: ListAdaptersInput
) -> ListAdaptersOutput:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + request.wait_seconds
    while True:
        revision = core.registry.revision
        infos = [
            info
            for info in core.registry.list()
            if (
                request.application is None
                or info.registration.application == request.application
            )
            and (request.adapter_id is None or info.instance_id == request.adapter_id)
        ]
        ready = (
            (revision != request.after_revision)
            if request.after_revision is not None
            else bool(infos)
        )
        remaining = deadline - loop.time()
        if ready or remaining <= 0:
            return ListAdaptersOutput(
                adapters=tuple(
                    AdapterSummary(
                        instance_id=info.instance_id,
                        application=info.registration.application,
                        application_version=info.registration.application_version,
                        project_path=info.registration.project_path,
                        operation_count=len(info.registration.operations),
                        catalog_sha256=info.catalog_sha256,
                    )
                    for info in sorted(infos, key=lambda item: item.instance_id)
                ),
                revision=revision,
            )
        await core.registry.wait_for_change(revision, remaining)


def _operations(
    core: AdapterServer, request: ListOperationsInput
) -> ListOperationsOutput:
    info = core.registry.get(request.adapter_id)
    available = {item.name: item for item in info.registration.operations}
    if request.names is not None:
        missing = sorted(set(request.names) - available.keys())
        if missing:
            raise UnsupportedOperation(request.adapter_id, missing[0])
    terms = set(re.findall(r"[^\W_]+", (request.query or "").casefold()))
    scores = {
        item.name: sum(
            term in f"{item.name} {item.description}".casefold() for term in terms
        )
        for item in available.values()
    }
    selected = sorted(
        (
            item
            for item in available.values()
            if (request.names is None or item.name in request.names)
            and item.name.startswith(request.prefix)
            and (not terms or scores[item.name] > 0)
        ),
        key=lambda item: (-scores[item.name], item.name),
    )
    limit = min(request.limit, 4) if request.include_schemas else request.limit
    end = request.offset + limit
    excluded = (
        set() if request.include_schemas else {"arguments_schema", "result_schema"}
    )
    return ListOperationsOutput(
        adapter_id=request.adapter_id,
        catalog_sha256=info.catalog_sha256,
        matched_count=len(selected),
        next_offset=end if end < len(selected) else None,
        operations=tuple(
            DiscoveredOperation.model_validate(item.model_dump(exclude=excluded))
            for item in selected[request.offset : end]
        ),
    )


async def call_tool(
    ctx: ServerRequestContext[AdapterServer], params: CallToolRequestParams
) -> CallToolResult:
    core = ctx.lifespan_context
    arguments = params.arguments if params.arguments is not None else {}
    try:
        request: (
            ListAdaptersInput
            | ExecuteOperationInput
            | ListOperationsInput
            | ImportArtifactInput
            | ReleaseArtifactInput
        )
        if params.name == "tyvrana_list_adapters":
            request = ListAdaptersInput.model_validate(arguments)
        elif params.name == "tyvrana_list_operations":
            request = ListOperationsInput.model_validate(arguments)
        elif params.name == "tyvrana_execute_operation":
            request = ExecuteOperationInput.model_validate(arguments)
        elif params.name == "tyvrana_import_artifact":
            request = ImportArtifactInput.model_validate(arguments)
        elif params.name == "tyvrana_release_artifact":
            request = ReleaseArtifactInput.model_validate(arguments)
        else:
            return _failure("unknown_tool", f"Unknown tool: {params.name}")
    except ValidationError as exc:
        details: list[JsonValue] = [
            {
                "field": ".".join(str(part) for part in error["loc"])[:500],
                "message": str(error["msg"])[:500],
                "reason": str(error["type"]),
            }
            for error in exc.errors(
                include_input=False, include_context=False, include_url=False
            )[:8]
        ]
        return _failure("invalid_arguments", "Invalid tool arguments", details)
    try:
        if isinstance(request, ImportArtifactInput):
            try:
                descriptor = await core.artifacts.import_file(
                    request.path, name=request.name, media_type=request.media_type
                )
            except ArtifactError as exc:
                return _failure("artifact_import_failed", str(exc))
            return _success(descriptor)
        if isinstance(request, ReleaseArtifactInput):
            return _success(
                ReleaseArtifactOutput(
                    released=core.artifacts.release(request.artifact_id)
                )
            )
        if isinstance(request, ListAdaptersInput):
            return _success(await _adapters(core, request))
        if isinstance(request, ListOperationsInput):
            return _success(_operations(core, request))
        return _operation_success(core, await _execute(core, request))
    except ArtifactError as exc:
        return _failure("artifact_transfer_failed", str(exc))
    except (
        AdapterNotFound,
        UnsupportedOperation,
        RemoteOperationError,
        OperationTimeout,
        AdapterDisconnected,
    ) as exc:
        return _core_failure(
            exc,
            request.operation if isinstance(request, ExecuteOperationInput) else None,
        )
    except Exception:
        logger.exception("Unexpected failure in MCP tool %s", params.name)
        return _failure("internal_error", "Tool execution failed; see the server logs.")
