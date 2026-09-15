"""Compose MCP stdio with the existing adapter server lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..server import AdapterServer
from .tools import call_tool, list_tools

INSTRUCTIONS = """Tyvrana controls connected professional applications through adapters.
Before meaningful project mutation, analyze the requested result and current state;
infer the professional workflow and dependencies. Consider hidden/internal
structure, motion/deformation/assembly, references and measurements, materials,
surfaces/simulation, downstream export/runtime constraints, and acceptance tests.
Discover the typed Tyvrana capabilities needed for those stages before executing.
These are decision considerations, not a fixed pipeline; add only complexity
required by the result. The external AI reasons about the workflow.

Examples, when relevant:
- Biology: anatomical skeleton, joint centers/axes/ranges, muscle/tendon
  attachments, muscles, connective tissue/fascia, fat/soft tissue, skin,
  deformation topology, hair/fur/feathers/scales, materials, control rig,
  deformation acceptance and movement QA. Birds also need shoulder girdle/wing
  biomechanics, folded/partial/extended states, feather tracts/types, attachment,
  layering/direction and deformation/dynamics. Arthropods instead need exoskeletal
  segments, articulated appendages/limits, wings, antennae/mouthparts and surfaces.
- Mechanical assemblies: dimensions, datums, mating surfaces, tolerances,
  clearance/interference, axes/pivots, constraints, assembly/disassembly and motion.
- Vehicles: chassis, mechanisms, steering/suspension/drivetrain, body/interior,
  moving panels and runtime/performance needs.
- Buildings/environments: dimensions, structure, openings/circulation, modular
  systems/repeated assets, materials, lighting and runtime/render performance.
- Static props may need only references, dimensions, modeling, topology,
  UV/material and QA.

When real-world correctness matters, research current authoritative references
and actually inspect relevant images, diagrams, scans or video frames when visual
evidence affects construction. Collecting URLs or text alone is insufficient for
anatomy, mechanisms, architecture, materials or proportion matching.

List connected adapters, then retrieve needed contracts with tyvrana_list_operations.
Use only advertised operations. Cache contracts by catalog hash.
Use discovery waits during connection/reload.
Do not assume an application or unsupported operation is available.

When a Tyvrana adapter is connected for an application, all meaningful mutations
of that application's project/editor state must use Tyvrana's advertised typed
operations. Do not bypass the adapter through Computer Use, mouse or keyboard
automation, editor menus, shortcuts, gizmos, editor console commands, arbitrary
scripts, direct application APIs, or another editor-control/MCP integration.
If a capability is missing or one semantic intent needs excessive low-level calls,
identify a TYVRANA CAPABILITY GAP: in an authorized development environment improve
the reusable tool; otherwise report the capability gap as unsupported work. Do not
assume source-development permission or blindly repeat hundreds of calls.
Direct UI interaction is limited to necessary non-authoritative
observation/window management and must not mutate project/editor state.

Normal source-code/file editing with software-development tools remains allowed;
do not force ordinary programming through editor RPC. This exception does not
allow direct scene/asset/prefab state edits. Use Tyvrana for editor/runtime
operations, compilation inspection, and screenshots.

Inspect structured state before significant changes. Operation arguments are
application-specific; follow their contracts and validation errors.
Prefer semantic/batched operations, filtered compact inspection, delta/comparison
tools, application-side calculation, bounded outputs and minimal meaningful renders.
Avoid unchanged queries/renders, per-element edits for one intent, repeated
discovery and trial-and-error argument guessing. Respect the user's selected model
and reasoning configuration; do not change it as a cost optimization.

For visual work, render or capture a baseline, inspect it, make a controlled
change, render/capture again, inspect, correct, and repeat until verified. API
success is not visual verification. Use advertised render-to-surface raycasting
before targeted sculpt work instead of guessing 3D coordinates. After topology
changes, inspect/query again: mesh element indices belong to the current snapshot.

Prefer reversible, non-destructive workflows where appropriate and preserve
unrelated user state. If an operation reports possible partial mutation,
reinspect state and rerender before retrying; do not assume rollback. Runtime
validation and safety limits remain authoritative. Attach imported artifacts by
ID; source paths belong only to core's local ingestion boundary. Release imported
artifacts when finished.
"""


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
