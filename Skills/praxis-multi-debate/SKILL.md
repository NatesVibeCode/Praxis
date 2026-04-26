---
name: praxis-multi-debate
description: "Praxis durable debate coordination skill. Use when several debates must be packaged, tracked, or sequenced instead of answered once inline."
---

# Praxis Multi-Debate

## Current Surface Docs

- MCP/catalog reference: `docs/MCP.md`
- CLI reference: `docs/CLI.md`
- API route reference: `docs/API.md`
- Regenerate all three with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python Code&DBs/Workflow/scripts/generate_mcp_docs.py`
- If generated docs disagree with runtime output, trust `praxis workflow tools describe ...` and `praxis workflow routes --json`

## Application Metadata

| Application | Name | Description |
| --- | --- | --- |
| Claude | `Praxis Multi-Debate` | Package, track, and sequence durable debates as bounded workflow runs. |
| Gemini | `Praxis Multi-Debate` | Package, track, and sequence durable debates as bounded workflow runs. |
| Codex/OpenAI | `Praxis Multi-Debate` | Package, track, and sequence durable debates as bounded workflow runs. |

Legacy internal skill id: `praxis-multi-debate`

Primary references:

- `Skills/praxis-debate/SKILL.md`
- `Skills/praxis-workflow/SKILL.md`
- `Skills/praxis-multi-workflow/SKILL.md`

Use when debate work needs durable execution instead of a single chat reply. If one inline answer is enough, use `praxis-debate` instead.

## Mission

Keep each debate explicit, inspectable, and bounded.

## Workflow

1. Sharpen each topic:
   - `praxis workflow discover "<topic>"`
   - `praxis workflow recall "<topic>"`
   - `praxis workflow debate "<topic>"` when a quick council baseline helps
2. Design the debate (see below).
3. Package each debate as its own bounded run or explicit job set.
4. Use `praxis-workflow` for run mechanics. Use `praxis-multi-workflow` only when wave sequencing matters.
5. After the run, read every submission yourself. Do **not** add a synthesis job — the orchestrator does the synthesis.

## Designing the Debate

The prompts are the debate. Thin prompts produce thin results.

- **Positions:** find real fault lines, not pro/con. Most topics have 2–4 honest positions. Run as many as actually exist.
- **Attacks (sprint 2, optional):** add when positions talk past each other. Each attacker reads prior positions and targets the weakest points.
- **Review lenses (optional):** specialized cuts across all positions — blast radius, alternatives nobody proposed, platform fit, cost/timeline realism. Only add when the topic warrants it.

Every position prompt needs:

1. Shared context block — same factual grounding, copied verbatim into every position.
2. Assigned stance — specific, not "argue for option A".
3. Specific questions — open-ended, identical across positions.
4. Word budget — 800–1200 for positions, 600–900 for reviews.

## Rules

- one topic per debate packet
- all debate jobs use `"agent": "auto/debate"` — never hardcode model names
- jobs submit output via `praxis_submit_research_result` — no filesystem writes
- every summary must include `Must Do`, `Should Do`, and `Remove / Simplify / Change`
- no synthesis job — the orchestrator reads the raw material and extracts actionable items

## Persist, Don't Narrate

Debate output is not authority. `Must Do` / `Should Do` / `Remove / Simplify / Change` items are chat residue until they land in a durable surface. Route each synthesized item:

- defect surfaced by the debate -> `praxis workflow bugs file --title "<title>" --severity <P0|P1|P2|P3> --category <category> --description "<description>" --filed-by "<actor>" --source-kind <source_kind>`
- enhancement, refactor, or new capability -> `praxis workflow roadmap write <preview|validate|commit> --title <title> --intent-brief <brief>`
- architecture policy the debate settled -> `praxis workflow tools call praxis_operator_architecture_policy --input-json '{...}' --yes`
- already-tracked item the debate confirms complete -> `praxis workflow roadmap closeout <preview|commit> [--bug-id <id>]... [--roadmap-item-id <id>]...`

Verify by reading back through the same authority (`praxis workflow roadmap view`, `praxis workflow bugs search`, `praxis workflow query`). Call out any item that lacked a safe durable home.

## Output Contract

Return:

1. `Debate Set` — the topics and why each is its own packet
2. `Per-Topic Scope` — positions, attack structure, review lenses
3. `Execution Shape` — single run vs wave sequence, spec path(s)
4. `Evidence Path` — how to retrieve submissions (`praxis workflow run-status`, `praxis workflow tools call praxis_get_submission --workflow-token <token> --input-json '{"job_label":"<label>"}'`)
5. `Synthesis Gate` — what the orchestrator must extract before closing: `Must Do`, `Should Do`, `Remove / Simplify / Change`
6. `Writes Performed` — ids and authority of every roadmap row, bug, or policy persisted from the synthesis
7. `Verification Readback` — the read-lane call that confirmed each write
8. `Unpersisted Residue` — synthesized items that lacked a safe durable home, and why
