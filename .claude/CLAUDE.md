# Praxis

Praxis is an autonomous engineering control plane. The execution runtime is **Praxis Engine**, backed by **Praxis.db** (Postgres).

The canonical operator CLI frontdoor is `workflow`.

Orient with:

- `workflow query "status"`
- `POST /orient` when only HTTP is available

## Database

Single Postgres database: `postgresql://localhost:5432/praxis`

- Auto-starts on login via launchd (`com.praxis.postgres`)
- Data dir: `Code&DBs/Databases/postgres-dev/data`
- Env var: `WORKFLOW_DATABASE_URL`
- All subsystems (bugs, receipts, constraints, friction, artifacts, memory graph, authority) use Postgres
- Migrations live under `Code&DBs/Databases/migrations/workflow/`
- pgvector extension is enabled for vector similarity search

## Tool Authority

Praxis currently exposes **42 catalog-backed tools**.

Do not memorize a static surface table. Ask the catalog:

- `workflow tools list`
- `workflow tools search <text>`
- `workflow tools describe <tool>`
- `workflow tools call <tool> --input-json '{...}'`

Curated high-frequency aliases stay flat:

- `workflow query`
- `workflow bugs`
- `workflow recall`
- `workflow discover`
- `workflow artifacts`
- `workflow health`

The registry/integration projection is a read model. Tool truth lives in `Code&DBs/Workflow/surfaces/mcp/catalog.py`.

## Search Before Building

Before writing new infrastructure, search first:

- `workflow discover "<behavior>"`
- `workflow recall "<topic>"` for architecture/context

Uses vector embeddings over AST-extracted behavioral fingerprints. After code changes, refresh the index with:

- `workflow discover reindex --yes`

## Workflow Contract

Use the platform surfaces, not ad hoc bash wrappers, to launch workflows.

```text
workflow run <spec.json>
```

Treat launch as kickoff-first:

- `run_id` is the tracking handle
- use `workflow run-status <run_id>` for health, failure signals, and idle detection
- use stream/status URLs as the live observation channels

Use `--kill-if-idle` only when a run is clearly unhealthy and idle.

## Naming Convention

Per `PUBLIC_NAMING.md`:

- **Praxis** — the product/suite brand
- **Praxis Engine** — the workflow execution runtime
- **Praxis.db** — the Postgres-backed data authority
- **workflow** — the domain noun (workflow specs, workflow runs, workflow jobs)
- MCP tools use the `praxis_` prefix

## Tests

`PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q <test_file>`

## Repo Skills

Shared repo-local skills live under `Skills/*/SKILL.md`.

- Use `Skills/praxis-bug-logging/SKILL.md` when filing, deduplicating, evidence-linking, or resolving bugs in Praxis.db.
- Inspect with `workflow bugs ...` and mutate with `workflow tools call praxis_bugs --input-json '{...}' --yes`.
- Use `Skills/praxis-discover/SKILL.md` before adding new code or helpers.
- Use `Skills/praxis-phase/SKILL.md` and `Skills/praxis-lunchbox/SKILL.md` for bounded delivery and packaging.
