"""Compose MCP transports with the existing adapter server lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.resources import files

import anyio
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..server import AdapterServer
from .tools import call_tool, list_tools

CLIENT_CONTROL = files(__package__).joinpath("client_control.md").read_text()

INSTRUCTIONS = (
    CLIENT_CONTROL
    + "\n"
    + """Before meaningful mutation, infer workflow dependencies from the requested
result,
intended use and current state: hidden/internal structure, motion/deformation/assembly,
references/measurements, materials and downstream export/runtime. Add only complexity
required by that use. This is not a fixed pipeline; the external AI reasons about it.
Keep plans concise and separate assumptions from evidence.
Distinguish required domain structure from control/deformation/procedural proxies.
Rigs, guides, cages and constraints do not establish physical structure. Build an
inspectable representation at the fidelity downstream correctness needs; validate
its relationships and required ranges and transitions before dependent detail.
For realistic deformable biology, explicitly represent relevant anatomical skeletal
geometry separately from the armature; accept that anatomical foundation and joint
mechanics before dependent systems. Construct muscle/soft-tissue approximations
that influence or validate the bare-body prototype; validate its deformation before
finalizing production topology or dependent exterior systems. Joint articulation alone
is not body-deformation acceptance. Static likeness does not require hidden anatomy.
Use simple geometry where sufficient. Record representation roles and dependencies
in project state. Acceptance gates require observed structures and behavior evidence,
not proxy existence or labels. Discover needed typed Tyvrana capabilities before
executing.

List adapters; search tyvrana_list_operations with query terms.
Use compact summaries. Once a semantic match is found, batch selected names with
schemas=arguments and use it. Discover later-stage tools when needed;
result schemas are opt-in. Cache by catalog hash; avoid repeat searches.
Try alternate terms before declaring gaps. Use only advertised operations;
use discovery waits during connection/reload.

Inspect structured state before significant changes. Operation arguments are
application-specific; follow their contracts and validation errors.
Prefer semantic/batched operations, filtered compact inspection, delta/comparison
tools, application-side calculation, bounded outputs and minimal meaningful renders.
Use automatically framed multiview inspection when advertised. Retrieve each
completed image once; inline image artifacts are released automatically.
Avoid unchanged queries/renders, per-element edits for one intent, repeated
discovery and trial-and-error argument guessing. Respect the user's selected model
and reasoning configuration; do not change it as a cost optimization.
After topology changes, inspect/query again: indices belong to the current snapshot.
Use advertised raycasting before targeted sculpt work. Attach imported artifacts
by ID; paths terminate at core's local import/export boundary. For large outputs use
artifact_delivery=reference, then tyvrana_export_artifact. Export releases by default;
release other retained_artifact_ids and imports when finished.
"""
)


def create_mcp_server(core: AdapterServer) -> Server[AdapterServer]:
    """Create an unstarted MCP server that owns core for its lifespan.

    The low-level SDK API preserves JsonValue strings such as "null" and "[]";
    the high-level function helper pre-parses them into other JSON types.
    """

    @asynccontextmanager
    async def lifespan(server: Server[AdapterServer]) -> AsyncIterator[AdapterServer]:
        await core.start()
        try:
            yield core
        finally:
            with anyio.CancelScope(shield=True):
                await core.stop()

    return Server(
        "Tyvrana",
        instructions=INSTRUCTIONS,
        lifespan=lifespan,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


async def run_stdio(core: AdapterServer) -> None:
    """Serve one local MCP session; stdin/stdout belong only to MCP."""
    server = create_mcp_server(core)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, write_stream, server.create_initialization_options()
        )
