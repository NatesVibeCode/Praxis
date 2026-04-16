---
name: praxis-multi-debate
description: "Praxis durable debate coordination skill. Use when several debates must be packaged, tracked, or sequenced instead of answered once inline."
---

# Praxis Multi-Debate

Use this skill when debate work needs durable execution instead of a single chat reply.

## Mission

Keep each debate explicit, inspectable, and bounded.

## Workflow

1. Sharpen each topic with source material plus:
   - `praxis workflow discover "<topic>"`
   - `praxis workflow recall "<topic>"`
   - `praxis workflow debate "<topic>"` when a quick council baseline helps
2. Package each debate as its own bounded run or explicit job set.
3. Use `praxis-workflow` for run mechanics.
4. Use `praxis-multi-workflow` only when wave sequencing matters.

## Rules

- one topic per debate packet
- every debate must emit an explicit summary artifact
- every summary must include `Must Do`, `Should Do`, and `Remove / Simplify / Change`
- if one inline answer is enough, use `praxis-debate` instead

## Output Contract

Return:

1. `Debate Set`
2. `Per-Topic Scope`
3. `Execution Shape`
4. `Evidence Path`
5. `Synthesis Gate`
