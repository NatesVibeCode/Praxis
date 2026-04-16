---
name: praxis-multi-workflow
description: "Praxis batch workflow coordination skill. Use when multiple workflow runs need shared tracking, explicit sequencing, or wave-based execution."
---

# Praxis Multi-Workflow

Use this skill when one run is not enough and the work needs batch coordination.

## Mission

Coordinate many runs without inventing a second orchestration system.

## Default Shape

- use one spec when the jobs belong to one lifecycle
- use many specs when retries and cancellation should stay independent
- use waves only when later work must wait on earlier outcomes

## Surfaces

Per-run execution stays on `praxis-workflow`.

Wave control lives on the catalog-backed wave tool:

```text
praxis workflow tools describe praxis_wave
praxis workflow tools call praxis_wave --input-json '{"action":"observe"}'
```

Verified `praxis_wave` actions:

- `observe`
- `start`
- `next`
- `record`

Example:

```text
praxis workflow tools call praxis_wave --input-json '{"action":"record","wave_id":"<wave_id>","jobs":"job_a:pass,job_b:fail"}'
```

## Rules

- do not use wave state when plain parallel launch is enough
- keep an explicit map of `spec -> run_id`
- use `praxis workflow active` and `praxis workflow run-status <run_id>` for live health
- use `praxis workflow tools call praxis_workflow --input-json '{"action":"list"}'` when you need a catalog-backed batch view

## Output Contract

Return:

1. `Batch Shape`
2. `Run Set`
3. `Wave Plan`
4. `Tracking Surface`
5. `Failure Containment`
