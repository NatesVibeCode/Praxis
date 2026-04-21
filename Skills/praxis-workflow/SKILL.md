---
name: praxis-workflow
description: "Praxis workflow execution skill. Use to author, validate, run, inspect, retry, or cancel workflow specs through the canonical Praxis CLI and catalog-backed workflow tool surface."
---

# Praxis Workflow

Use this skill when the task is to author, validate, launch, inspect, or repair workflow execution in Praxis.

## Authority

The front door is `praxis workflow`.

If the exact JSON shape matters, inspect the tool before touching state:

```text
praxis workflow tools describe praxis_workflow
```

## Core Loop

1. Validate first:

```text
praxis workflow validate <spec.json>
```

2. Launch asynchronously:

```text
praxis workflow run <spec.json>
```

3. Capture the returned `run_id`.

4. Check health with:

```text
praxis workflow run-status <run_id>
praxis workflow active
praxis workflow inspect <run_id>
```

5. Cancel or retry explicitly when needed:

```text
praxis workflow cancel <run_id>
praxis workflow retry <run_id> <label>
```

## Rules

- `run` is kickoff, not wait semantics
- `run_id` is the durable tracking handle
- use `--kill-if-idle` only after `run-status` shows an unhealthy idle run
- when no direct alias fits, use `praxis workflow tools call praxis_workflow --input-json '{...}' --yes` for launch/write actions
- if you do not know the schema, query the catalog instead of guessing

## Output Contract

Return:

1. `Spec Authority`
2. `Launch or Inspection Action`
3. `Run Tracking`
4. `Validation Path`
5. `Failure Gate`
