---
name: praxis-lunchbox
description: "Praxis repo-local work-packet packaging skill. Use to turn a decision or discovery into one bounded, testable build unit with explicit scope, outcomes, authority, and verification."
---

# Praxis Lunchbox

## Application Metadata

| Application | Name | Description |
| --- | --- | --- |
| Claude | `Praxis Lunchbox` | Package one bounded repo-local work unit with explicit scope, authority, and proof of done. |
| Gemini | `Praxis Lunchbox` | Package one bounded repo-local work unit with explicit scope, authority, and proof of done. |
| Codex/OpenAI | `Praxis Lunchbox` | Package one bounded repo-local work unit with explicit scope, authority, and proof of done. |

Legacy internal skill id: `praxis-lunchbox`

Primary references:

- `AGENTS.md`
- `README.md`
- `Code&DBs/Workflow/PUBLIC_NAMING.md`
- the contract, spec, queue file, or implementation slice that defines the targeted work unit

Use this skill to package one bounded work unit for the Praxis repo.

## Mission

Turn the next piece of work into a packet that a builder can execute without ambiguity or scope drift.

## Rules

1. One lunchbox = one product.
2. Done criteria must be outcome-based, not implementation-based.
3. Files to modify must be minimal and explicit.
4. Boundaries must say what not to touch.
5. Verification commands must be concrete and runnable.

## Workflow Contract

If the packet uses repo workflow surfaces, write it so the builder:

- validates workflow specs before launch
- treats `praxis_workflow(action='run', ...)` as async enqueue only
- records `run_id` as the tracking handle
- expects live status from a separate stream or `action='status'`, not from the launch call
- checks `health.likely_failed`, `health.signals`, and `health.resource_telemetry` before deciding a run is stuck
- references `workflow tools describe <tool>` when a packet depends on an exact tool schema
- uses the curated aliases (`workflow query`, `workflow discover`, `workflow recall`, `workflow health`, `workflow bugs`) when they reduce ambiguity for an operator

## Required Fields

Every lunchbox should include:

- `goal`
- `contracts_involved`
- `authority_owner`
- `done_criteria`
- `files_to_read`
- `files_to_modify`
- `boundaries`
- `edge_cases`
- `verification_commands`
- `stop_boundary`

## Packaging Checklist

1. Restate the goal in one sentence.
2. Write 3-6 pass/fail done criteria.
3. Include only the files the builder truly needs.
4. Add edge cases and failure expectations.
5. Add at least one regression or contract check.
6. If validation runs through `./scripts/test.sh`, make the exact command explicit and expect the canonical JSON envelope.

## Output Contract

Return exactly:

1. `Lunchbox Goal`
2. `Contracts Involved`
3. `Authority Owner`
4. `Done Criteria`
5. `Files To Read`
6. `Files To Modify`
7. `Boundaries`
8. `Edge Cases`
9. `Verification Commands`
10. `Stop Boundary`

## Reject These

- “Refactor broadly”
- “Clean up architecture”
- no verification commands
- vague done criteria
- multiple unrelated deliverables in one lunchbox
