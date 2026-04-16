---
name: praxis-review
description: "Praxis repo-local review skill. Use to pressure-test a proposed or shipped change for duplicated authority, hidden state, weak contracts, simpler durable replacements, and math/data-model alternatives."
---

# Praxis Review

Use this skill when a packet is proposed, a slice is built, or a closeout claims success.

## Mission

Answer one question:

Is this the simplest durable shape with one authority, one reason, and one proof path?

## Required Prompt

When the review is retrospective or post-close, ask this question verbatim:

`Review what you built could you have built anything differently mathematically or component wise?`

Then separate candidate alternatives into two buckets only:

- Mathematical/data-model alternatives
- Component/architecture alternatives

Then ask this question verbatim:

`which ones are genuine improvements?`

Only keep improvements that are measurable, non-trivial, and reduce ambiguity, operational burden, or failure surface.

## First Moves

Read the thing being reviewed:

- plan, manifest, spec, or changed code
- validation output, tests, or receipts
- the owning contract or architecture note for the touched surface

If authority is unclear, orient with:

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

## Output Contract

Return exactly:

1. `Verdict`
2. `Why`
3. `Genuine Improvements`
4. `Validation Path`
5. `Risks`

Only keep an improvement if it reduces ambiguity, operational burden, or failure surface.
