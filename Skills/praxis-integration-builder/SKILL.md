---
name: praxis-integration-builder
description: "Build a manifest-declared Praxis integration for a third-party API (HubSpot, Stripe, Slack, etc.) end-to-end: discover, research, scope, write, patch, register, verify."
---

# Praxis Integration Builder

## Current Surface Docs

- MCP/catalog reference: `docs/MCP.md`
- CLI reference: `docs/CLI.md`
- API route reference: `docs/API.md`
- Regenerate all three with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`
- If generated docs disagree with runtime output, trust `praxis workflow tools describe ...` and `praxis workflow routes --json`

Use this skill when adding a third-party API to Praxis as a declarative manifest integration. The default shape is a zero-Python TOML manifest dropped into `Code&DBs/Integrations/manifests/`; only escape to a custom adapter if the discovery phase proves the loader cannot express what's needed.

## Mission

Search first. Manifest before code. Keychain for secrets.

## Phases

Six phases, authored as a single workflow spec:

1. **discover_existing** — confirm no prior manifest/adapter for the target; pick the closest analogue to copy from.
2. **research_api** — resolve auth mode, base URL, scopes, and ≥ 8 candidate endpoints.
3. **scope_mvp** — trim to ≤ 12 capabilities; decide per-endpoint whether a `body_template` is needed; explicitly list out-of-scope follow-ups.
4. **write_manifest** — emit `Code&DBs/Integrations/manifests/<slug>.toml` and verify it parses.
5. **patch_infra_gaps** — fix small loader/credential gaps in-tree, or file bugs for larger ones. Runs in parallel with `write_manifest`.
6. **register_and_verify** — sync the registry, prove `praxis workflow discover "<slug>"` hits, smoke-test one `list_*` capability.

## How to call

1. Copy the template spec:

   ```bash
   cp config/cascade/specs/W_integration_builder_template.queue.json \
      config/cascade/specs/W_integration_builder_<slug>_20260418.queue.json
   ```

2. Substitute the five placeholders in the new file:

   - `<<INTEGRATION_SLUG>>` — e.g. `hubspot`, `stripe`, `notion`. Must match `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,126}[a-zA-Z0-9]$`.
   - `<<INTEGRATION_NAME>>` — display name, e.g. `HubSpot CRM`.
   - `<<API_BASE_URL>>` — e.g. `https://api.hubapi.com`.
   - `<<AUTH_DOCS_URL>>` — vendor doc URL for auth setup.
   - `<<SECRET_ENV_VAR>>` — UPPER_SNAKE env var the manifest auth block will reference, e.g. `HUBSPOT_ACCESS_TOKEN`.

3. Launch the run:

   ```bash
   praxis workflow run config/cascade/specs/W_integration_builder_<slug>_20260418.queue.json
   ```

4. Track with `praxis workflow run-status <run_id>`. Artifacts land under `artifacts/integration_builder_<slug>/`:

   - `00_discovery.md` — prior art + analogue pick
   - `01_api_contract.md` — full endpoint inventory
   - `02_mvp_scope.md` — trimmed MVP + out-of-scope list
   - `03_infra_gaps.md` — what was patched and what was filed as a bug
   - `04_verify.md` — registry + discover + smoke-test status

5. Store the secret in the macOS Keychain (the operator does this once, by hand):

   ```bash
   security add-generic-password -U -a praxis -s <<SECRET_ENV_VAR>> -w <token>
   ```

## Ground truth

Before writing a manifest, re-read the loader and confirm the schema hasn't drifted:

- `Code&DBs/Workflow/runtime/integration_manifest.py` — `AuthShape`, `ActionSpec`, `IntegrationManifest`, `_parse_manifest`, `resolve_token`.
- `Code&DBs/Integrations/manifests/webhook-example.toml` — canonical 15-line example.
- `Code&DBs/Integrations/manifests/hubspot.toml` — reference output of this flow.

## What the manifest can express

- Auth kinds: `env_var`, `api_key`, `oauth2`.
- Bearer token auth: `auth.kind = "api_key"` + `auth.env_var = "FOO_TOKEN"`. Resolved via `.env → Keychain(service=praxis) → os.environ`.
- OAuth credential refs: `auth.credential_ref = "secret.<path>.<provider>"` — resolves via `adapters.credentials.resolve_credential`, including live OAuth token refresh when `conn` + `integration_id` are wired.
- Capabilities: full-URL path, any HTTP method, optional `body_template` with `{{arg}}` interpolation, optional `response_extract` to unwrap a top-level response key.

## What the manifest cannot express (flag as gaps)

- URL path parameters (`/contacts/{id}`) — `_validate_url_scheme` requires the path to already be a full https URL. Path-param endpoints need a loader patch or a custom adapter.
- Non-JSON bodies (multipart, form-encoded).
- Custom signing (HMAC request signatures, AWS SigV4).
- Pagination helpers / cursor unrolling — callers drive pagination themselves.

When a target needs any of these, the `patch_infra_gaps` job is where it gets fixed or filed.

## Done criteria

- `Code&DBs/Integrations/manifests/<slug>.toml` exists and `load_manifests()` returns a matching `IntegrationManifest`.
- `integration_registry` has a row for `<slug>` after a reload.
- `praxis workflow discover "<slug>"` returns the manifest.
- At least one `list_*` capability either returns HTTP 200 or cleanly reports `credential.env_var_missing` (documented in `04_verify.md`).
