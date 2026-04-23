---
name: praxis-job-finding-two-pass
description: "Praxis-adjacent recruiter discovery skill. Use for ATS-backed job search work through the recruiter-runtime surfaces, while using Praxis CLI tools for discovery, decisions, and connector context."
---

# Praxis Job Finding Two Pass

Use this skill for recruiter discovery work.

## Mission

Find real ATS-backed opportunities without pretending weak search is complete.

## Execution Surfaces

Actual job discovery and review:

- `recruiter-operator`
- `recruiter-runtime`
- `recruiter-tracker`

Resolve these through `RECRUITER_RUNTIME_BIN` when the runtime exports it;
otherwise use the binaries on `PATH`. Do not bake an operator-local checkout
path into prompts, workflow specs, or docs.

Praxis support surfaces for architecture, integrations, and memory:

- `praxis workflow discover "<ATS adapter or recruiter behavior>"`
- `praxis workflow recall "<recruiter decision or pattern>"`
- `praxis workflow tools describe praxis_connector`

## Core Rule

Never treat search as single-pass exact matching.

Always use:

1. broad recall
2. narrow fallback recovery
3. merge plus dedupe before ranking

## Workflow

1. Start from the guided recruiter surface when possible.
2. Prefer known ATS sources first: Greenhouse, Lever, Ashby, SmartRecruiters, Workable, Workday, Jobvite.
3. Run broad recall first.
4. If recall is weak, run the second pass instead of declaring failure.
5. Filter by company, role family, location, freshness, and confidence.
6. Persist accepted opportunities through the recruiter runtime.
7. Use tracker or today views for action priority.

## Rules

- this is not a generic web scraping skill
- do not skip dedupe
- do not blur supported-source results with fallback recoveries
- if connector or integration behavior is unclear, inspect the Praxis connector surface before inventing a new path

## Output Contract

Return:

1. `Target Set`
2. `Broad Pass`
3. `Recovery Pass`
4. `Deduped Opportunities`
5. `Action Queue`
