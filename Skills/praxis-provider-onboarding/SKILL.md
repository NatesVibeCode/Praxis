---
name: praxis-provider-onboarding
description: "Praxis provider onboarding skill. Use when probing or onboarding a new CLI/API provider or model route through the native provider authority."
---

# Praxis Provider Onboarding

Use this skill when adding a provider or model route to Praxis.

## Mission

Probe first. Write second.

## Ground Truth

Before mutating anything:

- read the provider's official docs
- run `praxis workflow recall "provider routing" --type decision`
- inspect the live schema with:

```text
praxis workflow tools describe praxis_provider_onboard
```

## Verified Surface

`praxis_provider_onboard` supports:

- `action: probe`
- `action: onboard`

Required field:

- `provider_slug`

Optional verified fields:

- `transport` -> `cli` or `api`
- `models`
- `api_key_env_var`

Example probe:

```text
praxis workflow tools call praxis_provider_onboard --input-json '{"action":"probe","provider_slug":"openrouter","transport":"api"}'
```

Only after a clean probe should you run `action: onboard`.

## Rules

- do not guess request or auth shape
- do not onboard a provider you have not probed
- do not invent model slugs
- only set `api_key_env_var` from docs or explicit user instruction

## Output Contract

Return:

1. `Docs Checked`
2. `Probe Result`
3. `Onboard Decision`
4. `Routes or Models Added`
5. `Verification Path`
