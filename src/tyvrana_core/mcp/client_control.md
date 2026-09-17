# Tyvrana client control

When a Tyvrana adapter is connected, all meaningful application/project/editor
mutations and rendering must use its advertised typed operations. Do not bypass
it through shell/application CLI launches, direct host APIs, generated or arbitrary
scripts, console commands, UI automation, or another editor-control integration.
Launching another application process, including a background/headless instance,
is not an alternative real-work path. Ordinary source-code/file editing remains
allowed; it does not authorize direct scene/asset/prefab edits. Use Tyvrana for
editor/runtime control, compilation inspection and screenshots.

For real visual work, identify the intended interactive application instance
before mutation: inspect adapter ID, host/process mode and project identity.
Keep that explicit adapter target until deliberately rebinding; never silently
switch to another instance. Recheck identity after reconnect/reload. Keep the
evolving project in the user's intended visible instance. Background/headless
hosts are for isolated development, tests and performance fixtures only, unless
the user explicitly requests a headless task. At meaningful checkpoints, verify
the target and use typed selection/viewport framing to leave the current result
inspectable in that window. Renders supplement, not replace, visible editor
progress. Window presence proves neither monitor visibility nor user attention.
Non-authoritative observation/window management must not mutate project/editor
state. Do not render after every operation.

Missing capability or excessive low-level calls for one intent means
TYVRANA TOOLING GAP. Discover alternate terms first; in authorized development,
improve the reusable tool; otherwise report unsupported work. Do not assume
source-development permission or bypass the adapter to finish.

API success is not task success. Inspect a baseline; change -> inspect/run/render
-> detect issues -> fix -> verify. For real-world correctness, research authoritative
references and actually inspect relevant images, diagrams or video; URLs/text
alone are not visual evidence. Preserve unrelated state and prefer reversible
changes. After possible partial mutation, reinspect before retrying; never assume
rollback.

For substantial multi-stage work, persist a compact contract with project.create/apply
before construction: goal, systems, milestone prerequisites, required validations
and evidence criteria. Bind the intended document/adapter; set project.stage to the
active in_progress milestone ID. Its entity_ids declare affected outputs, document_ids
its authoring targets. Exploratory work stays provisional within that stage; accept
only after observing required evidence, then advance. Repair stale prerequisites or
blocking issues before dependent work; reopen the upstream stage before changing it.
Simple unbound one-step edits need no contract. For continued work retrieve
project.continue first, verify stale bindings, and use bounded search/delta details.
Keep milestone-level batches; the conversation is not project memory.
