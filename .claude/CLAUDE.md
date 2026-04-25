# Praxis

Praxis is an autonomous engineering control plane. The execution runtime is **Praxis Engine**, backed by **Praxis.db** (Postgres).

The canonical operator CLI frontdoor is `praxis workflow`.

Orient with:

- `praxis workflow query "status"`
- `POST /orient` when only HTTP is available

## Product thesis — LLM-first infrastructure powered by a trust compiler

Praxis is **LLM-first infrastructure**. LLMs are smart; they mostly make
mistakes because the environment gives them incomplete context, too many
ambiguous choices, hidden state, and weak proof. The **trust compiler** is the
engine: it compiles the right context, legal actions, typed state, tools,
data pills, gates, receipts, and recovery paths **at the moment of action**,
so the right choice is the easiest choice.

Not "a better workflow builder." Not only "trust compiler for messy work."
The LLM is the operator; humans supervise via Moon / chat / roadmap / bugs
surfaces that are lenses over the same DB-backed graph.

Standing-order row: `product_architecture / llm-first-infrastructure-trust-compiler-engine`.

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

## Active program — Public Beta Ramp

Master plan filed as `decision.2026-04-24.public-beta-ramp-master-plan`
(`decision_kind=delivery_plan`). Five phases over ~10 weeks.

**Phase 1 (foundations) — substantially shipped 2026-04-24.** 12 commits,
280+ tests passing. Summary by sub-phase:

- **1.1** LLM-first Launch Compiler ✅ `source_refs: []` plural + polymorphic
  resolver; `UnresolvedSourceRefError` / `UnresolvedStageError` /
  `UnresolvedWriteScopeError` with atomic pre-validation; enriched
  `packet_map` (inferred_stage, resolved_agent, capabilities,
  write_envelope, expected_gates, verification_gaps, data_pills);
  `plan.launched` event; `bind_data_pills` wired into compile path.
  Roadmap item `phase.1.1.launch.compiler` closed out (lifecycle=completed).
- **1.2** Built-not-wired sweep ✅ type-flow validation wired into
  `_handle_workflows_post` (Moon commit backend) and `compose_plan_from_intent`;
  `plan.launched` event contract registered via migration 224.
- **1.3** Data dictionary as universal clamp ✅ 14 bug-lifecycle type slugs
  registered as `data_dictionary_objects` rows (migration 225);
  catalog-level validator `validate_type_contract_slugs_against_data_dictionary`
  surfaces unresolved slugs as structured findings.
- **1.4** DataPill primitive family ✅ (via policy) Primitives map to
  existing `data_dictionary_entries` + `data_dictionary_lineage` authority;
  new tables (PillConflict, PillRedactionPolicy) deferred until concrete
  consumer forces them.
- **1.5** Fail-closed verifier path ✅ (via 1.1.c surfacing)
  `_compute_verification_gaps` emits structured rows for files without
  admitted verifiers; deeper catalog-backed dispatch deferred until
  concrete need.
- **1.6** `typed_gap.created` event ✅ Contract registered via migration
  226; `emit_typed_gap` / `emit_typed_gaps_for_findings` /
  `emit_typed_gaps_for_compile_errors` / `emit_typed_gaps_for_verification_gaps`
  helpers; `compile_plan` wires emission before raising Unresolved*
  errors (opt-in via `conn`).
- **1.7** Roadmap authoring template fix ✅ (Codex delivery 2026-04-24)
  `delivery_plan` scope policy registered, `praxis_operator_write` gains
  `roadmap_item_id` + `phase_order` + `lifecycle=retired` via migration
  223. Remaining: preview-first re-parent validation + `praxis_operator_closeout`
  branch for authoring cleanup vs proof-backed closeout.

**CQRS follow-ups (substantially landed 2026-04-24):** Migrations 234 + 235
register `launch_plan` + `compose_plan` as command operations in
`operation_catalog_registry` (with `event_required=TRUE`, `event_type=plan.launched`
/ `plan.composed`), add the `plan.composed` event contract, and re-point
the handler_ref at the new gateway-friendly wrappers in
`runtime/operations/commands/plan_orchestration.py` (`LaunchPlanCommand` +
`handle_launch_plan`, `ComposePlanCommand` + `handle_compose_plan`). MCP
tools `tool_praxis_launch_plan` and `tool_praxis_compose_plan` now dispatch
through `execute_operation_from_subsystems` instead of calling the
underlying functions directly — the gateway auto-generates `event_ids` on
completed receipts and inserts `plan.launched` / `plan.composed` rows into
`authority_events` with `receipt_id` linkage. End-to-end smoke verified:
gateway dispatch produces `authority_events.event_type=plan.composed`
rows with proper `operation_ref` + `receipt_id`. **Remaining:** the
underlying `runtime.spec_compiler.launch_plan` and
`runtime.intent_composition.compose_plan_from_intent` still call
`emit_system_event` as a sidecar — dual-write during transition. Removing
the sidecar requires migrating any `system_events` consumers (Moon
observability, replay tooling) to read from `authority_events` first.

**Parallel work (other model):** Phase 2 Moon Composer — query-side
projections (`legal_modules_for(pills)`, `surface_for(workflow_run)`),
modules subscribing to `graph.mutated` instead of polling REST.
Supporting policy filed: `platform_architecture / legal-equals-computable-to-non-gap-output`.

Before resuming any Praxis work, check:
- `praxis workflow query "public beta ramp master plan"`
- `praxis_operator_decisions(action="list", decision_kind="architecture_policy")` (14 policies now bind this program)
- `praxis_orient` for standing orders

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
