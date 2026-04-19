---
name: praxis-review
description: "Praxis repo-local review skill. Use to pressure-test a proposed or shipped change for duplicated authority, hidden state, weak contracts, simpler durable replacements, and math/data-model alternatives."
---

# Praxis Review

## Application Metadata

| Application | Name | Description |
| --- | --- | --- |
| Claude | `Praxis Review` | Pressure-test a proposed or shipped change for the simplest durable shape. |
| Gemini | `Praxis Review` | Pressure-test a proposed or shipped change for the simplest durable shape. |
| Codex/OpenAI | `Praxis Review` | Pressure-test a proposed or shipped change for the simplest durable shape. |

Legacy internal skill id: `praxis-review`

Primary references:

- `docs/ARCHITECTURE.md`
- `Skills/praxis-conversation-closeout/SKILL.md`
- `Skills/praxis-roadmap/SKILL.md`
- `Skills/praxis-bug-logging/SKILL.md`

Use when a packet is proposed, a slice is built, or a closeout claims success.

## Mission

Answer one question:

Is this the simplest durable shape with one authority, one reason, and one proof path?

## Required Prompt

Ask verbatim:

`Review what you built could you have built anything differently mathematically or component wise?`

Separate candidates into two buckets only:

- Mathematical / data-model alternatives
- Component / architecture alternatives

Then ask verbatim:

`which ones are genuine improvements?`

Keep only improvements that are measurable, non-trivial, and reduce ambiguity, operational burden, or failure surface.

## First Moves

Read the thing being reviewed:

- plan, manifest, spec, or changed code
- validation output, tests, or receipts
- the owning contract or architecture note for the touched surface

If authority is unclear:

- `praxis workflow query "what owns <area>?"`
- `praxis workflow discover "<behavior>"`
- `praxis workflow recall "<decision or constraint>"`

## Review Lens

Look for:

1. duplicated authority
2. hidden or implicit state
3. scripts doing work that belongs in the runtime, registry, or DB model
4. observability gaps that make outcomes hard to verify
5. avoidable blast radius
6. complexity added to preserve a weak pattern
7. mathematical or data-model alternatives before settling for component churn

Reject:

- style-only churn
- naming-only cleanup with no operational gain
- extra abstraction that hides the real authority

## Persist, Don't Narrate

Review output is not authority. A verdict that stays in chat is residue. For every genuine improvement, route it to the narrowest durable surface:

- defect discovered by the review -> `praxis workflow bugs file`, attach evidence, resolve only after validation
- enhancement, hardening, refactor, or new capability -> `praxis workflow roadmap write preview|validate|commit`
- architecture policy established by the review -> `praxis workflow tools call praxis_operator_architecture_policy --input-json '{...}' --yes`
- already-tracked item the review confirms complete -> `praxis workflow roadmap closeout preview|commit`

If authority is unclear, ask the router: `praxis workflow query "what record should capture this review outcome?"` — then drop to the specific surface once clear.

Verify every write by reading back through the same authority:

- `praxis workflow roadmap view [--root <id>]`
- `praxis workflow bugs search ...`
- `praxis workflow query "<what changed?>"`

If a write cannot be verified, say so plainly.

## Output Contract

Return exactly:

1. `Verdict`
2. `Why`
3. `Genuine Improvements`
4. `Top 3 Next Moves`
5. `Validation Path` — the specific command, receipt, or spec that would prove the improvement landed (e.g. `praxis workflow run-status <run_id>`, a failing test that now passes, a receipt id)
6. `Writes Performed` — ids, titles, and authority of every roadmap row, bug, policy, or closeout the review persisted
7. `Verification Readback` — the read-lane call that confirmed each write
8. `Unpersisted Residue` — improvements that lacked a safe durable home, and why
9. `Risks`
