---
name: praxis-debate
description: "Praxis repo-local adversarial strategy skill. Use when the current state needs a hard source-backed challenge before packaging or building."
---

# Praxis Debate

## Current Surface Docs

- MCP/catalog reference: `docs/MCP.md`
- CLI reference: `docs/CLI.md`
- API route reference: `docs/API.md`
- Regenerate all three with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`
- If generated docs disagree with runtime output, trust `praxis workflow tools describe ...` and `praxis workflow routes --json`

Use this skill when the direction is non-trivial and the current state needs adversarial pressure before work is packaged.

## Execution Authority

This is an inline Codex skill. Run the debate yourself in the current conversation.

Do not call `praxis workflow debate`, do not launch a workflow, and do not shell out to another model unless the user explicitly asks for durable workflow execution. The only allowed Praxis CLI calls for this skill are ground-truth reads such as `discover`, `recall`, `query`, and catalog inspection.

## Mission

Start from reality, not from a forced A/B frame.

The debate should answer:

Given the current state, what must change, what should change, and what should be removed or simplified?

## Ground Truth First

Before debating:

- read the user source doc or current code
- run `praxis workflow discover "<behavior or subsystem>"`
- run `praxis workflow recall "<decision or constraint>"`
- run `praxis workflow query "<plain-English question>"` when the right surface is unclear

## Debate Rules

- current-state first
- source-backed claims only
- no fake precision
- perspectives are reasoning lenses inside this answer, not separate provider/model calls
- no ranked option theater
- every perspective must state:
  - `Must Do`
  - `Should Do`
  - `Remove / Simplify / Change`

## Escalation Path

- if the result becomes a bounded build packet, hand it to `praxis-lunchbox`
- if the result needs durable tracked execution, hand it to `praxis-multi-debate` or `praxis-workflow`

## Output Contract

Return:

1. `Current State`
2. `Must Do`
3. `Should Do`
4. `Remove / Simplify / Change`
5. `Why These Survived`
