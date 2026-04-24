# Praxis

Praxis is an autonomous engineering control plane. The execution runtime is **Praxis Engine**, backed by **Praxis.db** (Postgres).

The canonical operator CLI frontdoor is `praxis workflow`.

Orient with:

- `praxis workflow query "status"`
- `POST /orient` when only HTTP is available

## Vision — One graph, many lenses

Praxis is an **agent substrate**, not a workflow builder. One graph lives in
Praxis.db (nodes, edges, gates, typed by data-dictionary `consumes` / `produces`).
Every surface is a lens on that same graph:

- **Moon canvas** — live render of graph rows
- **Executor** — interpreter walking the rows
- **CLI / MCP / HTTP** — alternate lenses on the same rows
- **NL authoring** — grammar that mutates rows
- **"Spec"** — export format, not source of truth
- **"Run"** — same graph with status + cursor overlay

Composition is **prescriptive**: at any state, the graph narrows to the 3–5
tools whose `consumes` type matches what the accumulator has. LLM planning
collapses from 100-tool search to a graph walk. Moon steering shows only
legal next steps.

The user (Nate) does not code. **Agents build and mutate the graph; the user
steers** — approves gates, edits a misbehaving node — they do not assemble
nodes from a palette. Moon is the control room, not an IDE.

When "edit in Moon" and "next run's behavior" are the same DB write, the shim
between UI and runtime is dead. Any feature that reintroduces a translation
layer between a surface and the graph is a regression.

Standing-order row: `platform_architecture / one-graph-many-lenses`
(`operator_decisions`). Surface it via `praxis_orient`.

## Database

Single Postgres database: resolve it through workflow authority, never a baked localhost DSN.

- Env var: `WORKFLOW_DATABASE_URL`
- Active authority is the network/registry-resolved Praxis.db, not the retired
  localhost development database
- If `WORKFLOW_DATABASE_URL` is absent, discover the current DSN through the
  workflow registry/runtime authority or ask for it; do not start local Postgres
- All subsystems (bugs, receipts, constraints, friction, artifacts, memory graph, authority) use Postgres
- Migrations live under `Code&DBs/Databases/migrations/workflow/`
- pgvector extension is enabled for vector similarity search

## Fresh Clone Bootstrap

If Praxis.db is missing, empty, or cannot answer the standing-order query, do
not infer private operator policy from this file. Run:

```bash
./scripts/bootstrap
```

Then orient again through `praxis workflow query "status"` or `POST /orient`.
Until Praxis.db answers, stay inside the public fresh-clone scope from
`README.md`, `SETUP.md`, and `config/runtime_profiles.json`.

## Tool Authority

Praxis exposes a live surface of catalog-backed tools.

Do not memorize a static surface table. Ask the catalog:

- `praxis workflow tools list`
- `praxis workflow tools search <text>`
- `praxis workflow tools describe <tool|alias>`
- `praxis workflow tools call <tool|alias> --input-json '{...}'`

Curated high-frequency aliases stay flat:

- `praxis workflow query`
- `praxis workflow bugs`
- `praxis workflow recall`
- `praxis workflow discover`
- `praxis workflow artifacts`
- `praxis workflow health`

The registry/integration projection is a read model. Tool truth lives in `Code&DBs/Workflow/surfaces/mcp/catalog.py`.

## Search Before Building

Before writing new infrastructure, search first:

- `praxis workflow discover "<behavior>"`
- `praxis workflow recall "<topic>"` for architecture/context

Uses vector embeddings over AST-extracted behavioral fingerprints. After code changes, refresh the index with:

- `praxis workflow discover reindex --yes`

## Workflow Contract

Use the platform surfaces, not ad hoc bash wrappers, to launch praxis workflows.

```text
praxis workflow run <spec.json>
```

Treat launch as kickoff-first:

- `run_id` is the tracking handle
- use `praxis workflow run-status <run_id>` for health, failure signals, and idle detection
- use stream/status URLs as the live observation channels

Use `--kill-if-idle` only when a run is clearly unhealthy and idle.

## Naming Convention

Live in Praxis.db under
`architecture-policy::public-naming::workflow-vocabulary-convention` and
`architecture-policy::public-naming::dispatch-historical-exceptions`
(`operator_decisions`). Snapshot:

- **Praxis** — the product/suite brand
- **Praxis Engine** — the workflow execution runtime
- **Praxis.db** — the Postgres-backed data authority
- **workflow** — the domain noun (workflow specs, workflow runs, workflow jobs)
- **launch** — the verb for starting a workflow run (never "dispatch")
- MCP tools use the `praxis_` prefix

Inspect exceptions with `praxis workflow query "public naming exceptions"`.

## Tests

`PYTHONPATH='Code&DBs/Workflow' python3 -m pytest --noconftest -q <test_file>`

## Repo Skills

Shared repo-local skills live under `Skills/*/SKILL.md`.

- Use `Skills/praxis-bug-logging/SKILL.md` when filing, deduplicating, evidence-linking, or resolving bugs in Praxis.db.
- Inspect with `praxis workflow bugs ...` and mutate with `praxis workflow tools call praxis_bugs --input-json '{...}' --yes`.
- Use `Skills/praxis-discover/SKILL.md` before adding new code or helpers.
- Use `Skills/praxis-phase/SKILL.md` and `Skills/praxis-lunchbox/SKILL.md` for bounded delivery and packaging.
