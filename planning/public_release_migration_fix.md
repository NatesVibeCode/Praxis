# Public Release Migration Fix — Handoff Plan

## Context (read this first)

> **DB authority (2026+):** Load `WORKFLOW_DATABASE_URL` from the repo resolver (`source scripts/_workflow_env.sh && workflow_load_repo_env` at the Praxis root). Do not copy legacy loopback Postgres DSN examples from this document—they described a prior **local** Postgres layout and are not portable.

Praxis is about to go public on GitHub. The migration tree at `Code&DBs/Databases/migrations/workflow/` (135 files) has never been tested end-to-end against a truly empty Postgres cluster. Several migrations reference tables that no migration creates, because those tables were originally created ad-hoc (inline Python, legacy squashed migrations, or manual psql) in existing developer DBs. A fresh clone will fail to bootstrap.

A previous Claude session already drafted fixes to five migrations — those edits are currently **uncommitted** in the working tree and should be treated as a starting point, not the final answer. Validate each before keeping.

## Repo layout

- Working dir: e.g. `…/Praxis` (use your checkout path, not a hardcoded home directory)
- Migrations: `Code&DBs/Databases/migrations/workflow/` (note the `&` in the path — quote it in shell)
- Active DB: whatever cluster `WORKFLOW_DATABASE_URL` names after you load env (see **DB authority** note above)
- Env var the code uses: `WORKFLOW_DATABASE_URL`
- Operator CLI: `workflow` (e.g. `workflow query "status"`)
- Runtime code that lazily creates tables: `Code&DBs/Workflow/runtime/observability.py:215`
- Python tests: `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q <test_file>`

## Known gaps (verified against the running DB)

| Table | In running DB? | Created by migration? | Referenced by |
|---|---|---|---|
| `provider_cli_profiles` | yes | **no** (first reference is 076 ALTER) | 076, 077, 078, 091, 093, 094 |
| `platform_config` | yes | **no** | 100 and runtime config registry |
| `platform_events` | **no** | **no** | only 081 ALTER+CREATE INDEX, one test |
| `workflow_metrics` | **no** | **no** — lazily created at `runtime/observability.py:215` | 081 ALTER+CREATE INDEX |
| `provider_model_candidates` | yes | yes (006) | 046 UPDATEs (noops on fresh) |

## Uncommitted draft fixes to review

`git status` will show 15 modified files. For this task, only these 5 migrations are in scope:

- `046_provider_model_candidate_profiles.sql` — draft converts UPDATE to INSERT...ON CONFLICT so fresh DBs get seed rows.
- `076_provider_cli_profile_transport_metadata.sql` — draft prepends a `CREATE TABLE IF NOT EXISTS provider_cli_profiles` + seed rows.
- `081_observability_lineage_and_metrics.sql` — draft wraps index creation in `DO $$ IF EXISTS` blocks.
- `100_adapter_config_authority.sql` — draft prepends `CREATE TABLE IF NOT EXISTS platform_config` + default rows.
- `106_acceptance_status_index.sql` — draft removes `CONCURRENTLY` (cannot run inside a transaction).

The other 10 modified files (Moon UI, build_authority, canonical_workflows, capability_catalog wiring, tests) are **out of scope** for this task — leave them alone.

## Goals

1. A fresh Postgres database with no prior state must bootstrap cleanly by running the migrations in order.
2. Existing developer DBs (with applied migration versions 001–135) must continue to work without re-running old migrations.
3. `workflow query "status"` must succeed against the fresh DB.
4. No runtime-created tables — schema authority lives in migrations only.
5. Public release quality: readable, idempotent, no "patch the patch" layering.

## Required work

### 1. Validate or rewrite the 5 draft migration edits

For each of 046 / 076 / 081 / 100 / 106:

- Review the uncommitted diff (`git diff <path>`).
- Confirm the fix is correct and idempotent.
- If the fix is correct but ugly (e.g. 076 now has CREATE TABLE + ALTER for the same columns in one file), consider folding: either leave as-is for minimal history churn, or rewrite the migration so the CREATE block defines all current columns and the later ALTER is removed — but **only** if you also audit every subsequent migration that touches `provider_cli_profiles` (077, 078, 091, 093, 094) for interactions.
- Preserve migration version numbers and filenames. Do not renumber.

### 2. Create missing `platform_events` migration

Decision first: does Praxis actually need this table? Search usage:

```
rg -n 'platform_events' 'Code&DBs/Workflow'
```

If it's only referenced by 081 and the migration contract test — it's dead weight. Two options:

- **Option A (recommended if unused in runtime code):** remove the `platform_events` references from `081_observability_lineage_and_metrics.sql` entirely, and remove the test assertion in `Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py` that expects it.
- **Option B (if runtime code needs it):** add a new migration `136_platform_events.sql` that creates the table with the columns 081 expects (`parent_run_id`, `failure_category`, `attempts`, `latency_ms`, `created_at`, plus whatever base columns 081 assumes). Then adjust 081 so it doesn't depend on file-ordering assumptions — ideally the new 136 carries the full schema and 081's ALTER statements become unnecessary.

Pick one. Do not leave both states half-done.

### 3. Fix `workflow_metrics` runtime creation

Currently `Code&DBs/Workflow/runtime/observability.py:215` does `CREATE TABLE IF NOT EXISTS workflow_metrics (...)` inline. This violates "schema authority lives in migrations only."

- Move the CREATE TABLE definition from `runtime/observability.py` into a new migration (e.g. `137_workflow_metrics.sql`), or into `081` if you're reshaping 081 anyway.
- Remove the inline CREATE from `observability.py` — the runtime should assume the table exists.
- Ensure 081's ALTERs on `workflow_metrics` still work in order relative to the new creator migration.

### 4. End-to-end fresh-bootstrap test

Spin up a throwaway database and run every migration in order:

```bash
. ./scripts/_workflow_env.sh && workflow_load_repo_env
createdb praxis_fresh
# Same authority host as WORKFLOW_DATABASE_URL, new database name (adjust if your site uses a different create-db flow):
FRESH_URL="${WORKFLOW_DATABASE_URL%/*}/praxis_fresh"
WORKFLOW_DATABASE_URL="$FRESH_URL" \
  <whatever the migrator entry point is — find it; likely a script under Code&DBs/Workflow or invoked via `workflow`>
```

Then:

```bash
WORKFLOW_DATABASE_URL="$FRESH_URL" workflow query "status"
```

Both must succeed. If a migration fails, fix the root cause — do not add a try/except wrapper. When done:

```bash
dropdb praxis_fresh
```

### 5. Re-run affected unit tests

```bash
PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q \
  Code\&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py \
  Code\&DBs/Workflow/tests/unit/test_startup_wiring.py \
  Code\&DBs/Workflow/tests/unit/test_workflow_query_handlers.py
```

These are already modified in the working tree — make sure they still pass after your migration changes.

## Constraints

- **Do not point migration experiments at your live production database name** on the shared authority host. Use a throwaway database name (e.g. `praxis_fresh`) and verify `WORKFLOW_DATABASE_URL` before running DDL.
- **Do not rename or renumber existing migrations.** Applied versions on live developer DBs must keep matching their file.
- **Idempotent only.** Every migration must use `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `INSERT ... ON CONFLICT`, etc. Rerunning against a partially-migrated DB must not fail.
- **No `CREATE INDEX CONCURRENTLY`** if migrations run inside a wrapping transaction (they do — see 106).
- **No shims, no feature flags, no "legacy mode" branches.** Fix root causes.
- **Do not revert the other 10 modified files** in `git status` — those are in-progress feature work and are out of scope.
- Do not commit anything. Leave the result as working-tree edits for the user to review and commit.

## Deliverable

A clean diff that:

1. Makes 046/076/081/100/106 work on a fresh DB (keeping, rewriting, or folding the draft fixes).
2. Resolves the `platform_events` ghost (create it properly OR remove references).
3. Moves `workflow_metrics` creation out of `runtime/observability.py` into a migration.
4. Passes the fresh-bootstrap test in step 4.
5. Passes the unit tests in step 5.

Write a short `CHANGELOG.md`-style summary at the bottom of this plan file describing what you did and why, including which option you picked for `platform_events` and where `workflow_metrics` now lives.
