---
name: praxis-phase
description: "Praxis repo-local delivery method skill. Use for planning and executing one bounded unit through discovery, debate, packaging, build, review, test, promotion, and recording without leaning on dead paths or legacy system assumptions."
---

# Praxis Phase

## Application Metadata

| Application | Name | Description |
| --- | --- | --- |
| Claude | `Praxis Phase` | Repo-local delivery loop for one bounded unit from discovery through proof and record. |
| Gemini | `Praxis Phase` | Repo-local delivery loop for one bounded unit from discovery through proof and record. |
| Codex/OpenAI | `Praxis Phase` | Repo-local delivery loop for one bounded unit from discovery through proof and record. |

Legacy internal skill id: `praxis-phase`

Primary references:

- `AGENTS.md`
- `README.md`
- `Code&DBs/README.md`
- `Code&DBs/Workflow/PUBLIC_NAMING.md`
- the closest repo-local spec, doc, or implementation authority under `config/cascade/specs/`, `Code&DBs/Workflow/docs/`, `planning/`, or the touched runtime surface

Use this skill when working on feature delivery, refactors, architecture slices, or execution planning in this repo.

## Mission

Build the Praxis repo through a disciplined repo-local loop:

1. Discovery
2. Debate
3. Package work
4. Build one bounded unit
5. Review the result
6. Test in isolation
7. Promote if gates pass
8. Record what was built and learned

This is a method skill, not an infrastructure skill.

## Core Laws

1. One work unit = one thing.
2. Define done before building.
3. Builder does not self-certify the final result.
4. Every non-obvious choice gets documented.
5. Every completed unit leaves behind code, tests, docs, and a record.

## Workflow Contract

When this repo's workflow surfaces are involved:

- validate changed workflow specs before running them
- use `workflow run <spec.json>` as async kickoff only
- capture `run_id` and treat streaming/status as separate follow-up channels
- use `workflow run-status <run_id>` for health, failure signals, and idle detection
- use `--kill-if-idle` only when the run is clearly unhealthy and idle
- do not rely on legacy wait-style behavior
- when the exact tool shape is unclear, use `workflow tools describe <tool>` instead of guessing from memory
- prefer `workflow query`, `workflow discover`, `workflow recall`, and `workflow health` as the operator-facing aliases for read-mostly work

## Required Inputs

Before starting work, read:

- `AGENTS.md`
- `README.md`
- `Code&DBs/README.md`
- `Code&DBs/Workflow/PUBLIC_NAMING.md`
- the closest contract/spec/docs that define the touched surface

Then identify:

- the contract being changed
- the authority that owns it
- the acceptance gate that proves it
- the non-goal that keeps scope bounded

## Execution Loop

### 1. Discovery

Clarify:

- what problem is being solved
- what success looks like
- what contracts or boundaries are involved
- what the failure modes are

Discovery is design work, not file inventory.

### 2. Debate

Before packaging work, pressure-test the current direction:

- what should we build
- what should we simplify
- what should we defer

Use `praxis-debate` (`Praxis Debate`) when the direction is non-trivial.

### 3. Package

Break the next unit of work into one bounded packet with:

- goal
- done criteria
- files to read
- files to modify
- boundaries
- verification commands

Use `praxis-lunchbox` (`Praxis Lunchbox`).

### 4. Build

Build exactly one bounded unit.

Expected outputs:

- code
- tests
- local docs or decision notes where needed

### 5. Review

Run a post-build reflection immediately after the unit is working.

Use `praxis-review` (`Praxis Review`).

### 6. Test

Run the narrowest meaningful test surface:

- unit if local logic changed
- contract if interfaces changed
- integration if behavior spans modules

### 7. Promote

If the work passes its gates, wire it into the repo’s intended structure and prepare the next bounded unit.

### 8. Record

Log:

- what was built
- why it was built that way
- what changed
- what remains

## Output Contract

When using this skill, return:

1. `Current Unit`
2. `Done Criteria`
3. `Build Boundary`
4. `Verification Plan`
5. `Next Decision`

## Anti-Patterns

Reject:

- broad “clean up everything” work
- mixing discovery and implementation without a boundary
- building multiple products in one unit
- advancing to the next major step without recording the result
- importing legacy system phase numbering or workflow-runtime ceremony into this repo
