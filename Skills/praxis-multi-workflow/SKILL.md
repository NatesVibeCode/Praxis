---
name: praxis-multi-workflow
description: "Praxis batch workflow coordination skill. Use when multiple workflow runs need shared tracking, explicit sequencing, or Solution-based execution."
---

# Praxis Multi-Workflow

## Current Surface Docs

- MCP/catalog reference: `docs/MCP.md`
- CLI reference: `docs/CLI.md`
- API route reference: `docs/API.md`
- Regenerate all three with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`
- If generated docs disagree with runtime output, trust `praxis workflow tools describe ...` and `praxis workflow routes --json`

Use this skill when one run is not enough and the work needs batch coordination.

## Mission

Coordinate many runs without inventing a second orchestration system.

## Default Shape

- use one spec when the jobs belong to one lifecycle
- use many specs when retries and cancellation should stay independent
- use a Solution only when later workflow phases must wait on earlier outcomes

## Surfaces

Per-run execution stays on `praxis-workflow`.

For templated iteration across a list of items, use the loop surface:

```text
praxis workflow loop --items "a,b,c" --prompt "Analyze: {{item}}" [--tier mid] [--max-parallel 4]
```

Solution control lives on the catalog-backed Solution tool:

```text
praxis workflow tools describe praxis_solution
praxis workflow tools call praxis_solution --input-json '{"action":"list"}'
```

Verified `praxis_solution` actions:

- `start`
- `submit`
- `status`
- `show`
- `list`
- `observe`

Example:

```text
praxis workflow tools call praxis_solution --input-json '{"action":"status","solution_id":"<solution_id>"}'
```

## Rules

- do not create a Solution when plain parallel launch is enough
- keep an explicit map of `spec -> run_id`
- run `praxis workflow firecheck --json` before launching a batch
- prove one representative job can fire before expanding to the fleet
- never mass-retry; each retry needs the failed label, previous failure, and retry delta
- use `praxis workflow active` and `praxis workflow run-status <run_id>` for live health
- use `praxis workflow tools call praxis_workflow --input-json '{"action":"list"}'` when you need a catalog-backed batch view

## Output Contract

Return:

1. `Batch Shape`
2. `Run Set`
3. `Solution Plan`
4. `Tracking Surface`
5. `Failure Containment`
