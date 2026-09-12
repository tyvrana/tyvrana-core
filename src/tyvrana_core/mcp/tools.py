"""Typed discovery and execution tools for the official MCP server."""

import asyncio
import base64
import json
import logging

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
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from tyvrana_protocol import (
    ArtifactDescriptor,
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
    """Discovery takes no arguments."""


class AdapterSummary(_ToolModel):
    instance_id: Identifier
    application: str
    application_version: str | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    project_path: str | None = Field(default=None, exclude_if=lambda v: v is None)
    operations: tuple[QualifiedName, ...]


class ListAdaptersOutput(_ToolModel):
    adapters: tuple[AdapterSummary, ...]


class ExecuteOperationInput(_ToolModel):
    adapter_id: Identifier
    operation: QualifiedName
    arguments: JsonValue


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
                    "List connected application adapters and advertised operations."
                ),
                input_schema=ListAdaptersInput.model_json_schema(),
                output_schema=ListAdaptersOutput.model_json_schema(
                    mode="serialization"
                ),
                annotations=ToolAnnotations(read_only_hint=True),
            ),
            Tool(
                name="tyvrana_execute_operation",
                description=(
                    "Execute one advertised operation on a selected connected adapter "
                    "using the core deadline."
                ),
                input_schema=ExecuteOperationInput.model_json_schema(),
                output_schema=ExecuteOperationOutput.model_json_schema(
                    mode="serialization"
                ),
            ),
        ]
    )


def _success(output: ListAdaptersOutput | ExecuteOperationOutput) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=output.model_dump_json())],
        structured_content=output.model_dump(mode="json"),
    )


def _failure(code: str, message: str, details: JsonValue = None) -> CallToolResult:
    error: dict[str, JsonValue] = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
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
) -> CallToolResult:
    if isinstance(error, RemoteOperationError):
        return _failure(error.error.code, error.error.message, error.error.details)
    if isinstance(error, AdapterNotFound):
        code = "adapter_not_found"
    elif isinstance(error, UnsupportedOperation):
        code = "operation_unsupported"
    elif isinstance(error, OperationTimeout):
        code = "operation_timeout"
    else:
        code = "adapter_disconnected"
    return _failure(code, str(error))


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


async def call_tool(
    ctx: ServerRequestContext[AdapterServer], params: CallToolRequestParams
) -> CallToolResult:
    core = ctx.lifespan_context
    arguments = params.arguments if params.arguments is not None else {}
    try:
        request: ListAdaptersInput | ExecuteOperationInput
        if params.name == "tyvrana_list_adapters":
            request = ListAdaptersInput.model_validate(arguments)
        elif params.name == "tyvrana_execute_operation":
            request = ExecuteOperationInput.model_validate(arguments)
        else:
            return _failure("unknown_tool", f"Unknown tool: {params.name}")
    except ValidationError as exc:
        details: list[JsonValue] = [
            {
                "field": ".".join(str(part) for part in error["loc"]),
                "message": str(error["msg"]),
            }
            for error in exc.errors(
                include_input=False, include_context=False, include_url=False
            )
        ]
        return _failure("invalid_arguments", "Invalid tool arguments", details)
    try:
        if isinstance(request, ListAdaptersInput):
            adapters = tuple(
                AdapterSummary(
                    instance_id=info.instance_id,
                    application=info.registration.application,
                    application_version=info.registration.application_version,
                    project_path=info.registration.project_path,
                    operations=tuple(sorted(info.registration.operations)),
                )
                for info in sorted(
                    core.registry.list(), key=lambda info: info.instance_id
                )
            )
            return _success(ListAdaptersOutput(adapters=adapters))
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
        return _core_failure(exc)
    except Exception:
        logger.exception("Unexpected failure in MCP tool %s", params.name)
        return _failure("internal_error", "Tool execution failed; see the server logs.")
