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

Praxis exposes a live surface of catalog-backed tools through three
sibling surfaces over the same `operation_catalog_gateway` engine bus.
Pick by audience — they all return the same gateway result, the
difference is the front door:

### Claude Code agent (this assistant) → `bin/praxis-agent`

The bash CLI is denied in `.claude/settings.json` (`Bash(praxis:*)`)
because the host shell carries `CLAUDECODE=1` and trips the Claude CLI
nested-session guard, AND Claude Desktop's stdio MCP server triggers
the post-2026-04-24 disclaimer wrapper on every spawn of unsigned
homebrew Python. Both lanes are broken for Claude Code.

`bin/praxis-agent` is the right lane: a thin bash wrapper that does
`docker exec praxis-api-server-1 python3 → surfaces.mcp.invocation.invoke_tool`
so dispatch happens inside the container with a clean env. No host
shell pollution, no disclaimer wrapper, same gateway result as the
MCP tool would return.

Usage:

```bash
bin/praxis-agent --list                                    # browse catalog
bin/praxis-agent --describe praxis_compose_and_launch       # tool schema
bin/praxis-agent praxis_compose_and_launch \
  --input-json '{"approved_by":"nate@praxis","plan_name":"audit","intent":"1. ...\n2. ..."}'
bin/praxis-agent praxis_search --input-file query.json
```

### Operator at a terminal / scripts / launchd → `praxis workflow ...`

The full bash CLI is fine for human operators and shell automation —
no nested-session guard from a real terminal, no Claude.app disclaimer.
Same catalog, same gateway.

```text
praxis workflow tools list
praxis workflow tools search <text>
praxis workflow tools describe <tool|alias>
praxis workflow tools call <tool|alias> --input-json '{...}'
```

Curated high-frequency aliases stay flat:

- `praxis workflow query`
- `praxis workflow bugs`
- `praxis workflow recall`
- `praxis workflow discover`
- `praxis workflow artifacts`
- `praxis workflow health`

### Codex / Gemini agents → `mcp__praxis__*`

Their MCP transport is unaffected by the Claude.app disclaimer wrapper
because their host sessions don't carry `CLAUDECODE` and their parent
app doesn't disclaimer-wrap stdio servers. They use the MCP tools
directly through their respective harnesses.

### Auth refresh

When the worker reports `auth_state: timeout` for any provider (or
after Docker Desktop / OrbStack / macOS restarts), run
`scripts/praxis-up`. It does the keychain export and compose recreate
in one command and verifies via `praxis_cli_auth_doctor` inside the
worker. See `architecture-policy::deployment::docker-restart-caches-env`
and `architecture-policy::auth::via-docker-creds-not-shell`.

Tools self-declare a `kind` field in their MCP metadata so the choice space narrows fast:

- `kind=search` — read-many across sources (`praxis_search`, `praxis_query`, `praxis_recall`, `praxis_discover`, `praxis_research`)
- `kind=write` — mutates state (default for unlabeled tools — err on the safe side)
- `kind=walk` — graph traversal / run-scoped views (`praxis_run`)
- `kind=analytics` — aggregations / cost rollups
- `kind=alias` — deprecated wrapper kept for one window before deletion

`praxis_orient` and `praxis workflow tools list` surface `kind` per tool. The registry/integration projection is a read model. Tool truth lives in `Code&DBs/Workflow/surfaces/mcp/catalog.py`.

## CQRS gateway dispatch is the engine bus

Every Praxis Engine operation runs through `runtime.operation_catalog_gateway.execute_operation_from_subsystems(subsystems, operation_name=..., payload=...)`. The gateway:

- validates the payload against the operation's Pydantic `input_model_ref`,
- writes a row to `authority_operation_receipts` (input hash, idempotency key, timestamps, `execution_status` ∈ {`completed`, `replayed`, `failed`}),
- replays cached results when `idempotency_policy='read_only'` or `'idempotent'` and the same payload reappears,
- emits `authority_events` rows for command operations with `event_required=TRUE`.

Standing-order row: `architecture_policy / cqrs_gateway_robust_determinism` — robust determinism for the hundreds-of-agents future. No MCP-tool-tier shims dispatching to subsystems directly. Every new MCP tool is a thin wrapper that calls `execute_operation_from_subsystems` and returns the gateway's result. Migrations registering new operations populate all three CQRS tables consistently:

- `operation_catalog_registry` (authoritative — `operation_kind`, `idempotency_policy`, `receipt_required`, `event_required`, `event_type`, `handler_ref`, `input_model_ref`),
- `authority_object_registry` (`object_kind` matches `operation_kind` — `'query'` for query ops, `'command'` for command ops; constraint widened in migration 279),
- `data_dictionary_objects` (`category` matches `operation_kind`).

Read receipts are not optional — read ops record their own ledger row so an agent's "what did the LLM ask?" trail is reproducible.

## Provider Execution Boundary

Operator standing order:

- **CLI is the provider execution lane for every use case, always.** Use CLI/provider execution unless the operator explicitly says otherwise.
- **API is opt-in only.** Do not use API routes merely because a UI/app surface could use them, or because CLI seems inconvenient.
- **Always label provider-routing discussion as CLI or API.** Do not describe a route change, provider choice, or model swap without naming which lane it affects.
- **Do not convert general work to API routing by implication.** A UI compile/chat exception is not permission to make API the default elsewhere.
- **Direct DeepSeek API remains research-only** unless the operator explicitly changes that policy. DeepSeek through OpenRouter is still API and must be explicitly requested.

Standing-order row: `architecture-policy::provider-routing::cli-default-api-exception`.

## Search Before Building

Lead with the canonical federated entry point, **`praxis_search`**. It dispatches through the CQRS gateway (`search.federated`), records a read receipt, and returns a topic-anchor cluster shape that bundles code, decisions, knowledge, bugs, receipts, git history, files, and allowlisted DB rows in one call.

```bash
# MCP / CLI
praxis workflow tools call praxis_search --input-json '{
  "query": "provider routing CLI default API exception",
  "sources": ["code", "decisions", "knowledge", "bugs"],
  "limit": 8
}'

# Variants
praxis_search(query="subprocess.", mode="exact",
  scope={"paths":["Code&DBs/Workflow/runtime/**/*.py"]},
  shape="context", context_lines=3)
praxis_search(query="/class .*Authority/", mode="regex", ...)
```

**Modes:** `auto` (default — `/regex/` → regex, `'quoted'` → exact, prose → semantic), `semantic`, `exact`, `regex`.

**Shapes:** `match` (single line), `context` (±N lines, default), `full` (whole file/record).

**Scope filters** narrow before ranking: `paths` / `exclude_paths` (glob), `since_iso` / `until_iso` (mtime or commit time), `type_slug`, `entity_kind` (`module|class|function|subsystem` for code), `exclude_terms`, source-specific knobs in `extras` (e.g. `extras.table` + `extras.where` for db, `extras.action` ∈ `log|diff|blame` for git).

**Response shape** (cluster mode, federated):

```jsonc
{
  "ok": true,
  "anchor_count": 5,
  "clusters": [
    {
      "anchor": "provider routing CLI/API",
      "primary": { source: "decisions", name: "...", score: 1.00, ... },
      "related": {
        "code":     { items: [...], count: 7, fetch_hint: {...} },
        "knowledge":{ items: [...], count: 4 },
        "bugs":     { items: [...], count: 3 }
      },
      "score": 1.00
    }, ...
  ],
  "also": {
    "receipts": { count: 12, preview: "...", fetch: {...} }
  },
  "source_empty_states": {
    "receipts": { reason_code: "retrieval.no_match", message: "..." }
  },
  "_meta": { dispatch_path: "gateway", index_freshness_per_source: {...} },
  "operation_receipt": { receipt_id: "...", execution_status: "completed" }
}
```

`related` is capped at 3 items per source with `count` showing how many more exist. `also` carries count + one-line preview + the exact follow-up `praxis_search` payload. Empty-state blocks emit when the whole query or a targeted source returns nothing — they include `reason_code=retrieval.no_match`, suggestions, and a `typed_gap_emitted` id when the standing "retrieval is the filter" decision applies.

**Per-source ops** are also gateway-dispatched (one receipt per call) when you only need one source: `search.code`, `search.knowledge`, `search.decisions`, `search.research`, `search.bugs`, `search.receipts`, `search.git_history`, `search.files`, `search.db`.

**Auto-reindex on stale code search** is on by default (`auto_reindex_if_stale=true`) — the indexer refreshes lazily when on-disk drift exceeds `stale_threshold` (default 5 files). Manual reindex remains as escape hatch:

```bash
praxis workflow discover reindex --yes
```

**Specialty search tools still exist** for niche cases — `praxis_discover` (hybrid code retrieval, AST fingerprints + FTS, returns reindex/stats actions), `praxis_recall` (knowledge-graph entity lookup), `praxis_query` (NL router) — but new code should reach for `praxis_search` first. They surface in the catalog with `kind=search`.

**Knowledge-graph noise filter** is on by default — machine-generated event-log entities (`hard_failure:*`, `verification:*`, `workflow_<hex>`, `receipt:*`) are filtered out of `knowledge`/`decisions`/`research` results. Opt back in with `scope.extras.include_event_log_facts=true` if you genuinely want event-log search.

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

## Run-scoped views — `praxis_run` (consolidated)

Four legacy tools `praxis_run_status`, `praxis_run_scoreboard`, `praxis_run_graph`, `praxis_run_lineage` collapsed into one tool with an `action=` enum. Old names remain as `kind=alias` for one window then get deleted.

```bash
praxis workflow tools call praxis_run --input-json '{
  "run_id": "run_123",
  "action": "graph"          # status | scoreboard | graph | lineage
}'
```

## Bug filing & verifier-backed resolution

Use `praxis_bugs` for filing, dedup, evidence, replay, and resolution. Bug search now applies a `min_similarity=0.3` floor on the vector branch (BUG-9475EEB0 fix) so unmatched queries return zero hits instead of dumping the open-bug list.

Resolving a bug to `FIXED` requires a **verifier run** — you can't claim "fixed", you have to point at a registered verifier authority that the tracker actually executes:

```bash
praxis workflow tools call praxis_bugs --input-json '{
  "action": "resolve",
  "bug_id": "BUG-XXXXXXXX",
  "status": "FIXED",
  "verifier_ref": "verifier.job.python.pytest_file",
  "inputs": {"path": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_X.py"},
  "target_kind": "path",
  "target_ref": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_X.py"
}'
```

The tracker writes a `verification_runs` row with stdout/stderr/exit_code/latency, links it as `evidence_role=validates_fix` on the bug via `bug_evidence_links`, and only then flips status. **Caveat:** `verifier.job.python.pytest_file` runs the executor from `/`, so paths must be **absolute** — relative paths fail with "file or directory not found." Use `FIX_PENDING_VERIFICATION` if you can't run the verifier yet; never claim FIXED without proof.

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
