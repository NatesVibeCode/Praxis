---
name: praxis-debate
description: "Praxis repo-local adversarial strategy skill. Use when the current state needs a hard source-backed challenge before packaging or building."
---

# Praxis Debate

Use this skill when the direction is non-trivial and the current state needs adversarial pressure before work is packaged.

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

If a fast council baseline helps, use:

```text
praxis workflow debate "<topic>"
```

The verified CLI default personas are `Pragmatist`, `Skeptic`, `Innovator`, and `Operator`.

## Debate Rules

- current-state first
- source-backed claims only
- no fake precision
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
