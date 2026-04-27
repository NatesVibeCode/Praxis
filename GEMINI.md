# Praxis — Orient on Session Start

You are operating inside the Praxis repository. Before doing any work, fetch standing orders from Praxis.db.

## First action every session

Run one of these to retrieve current standing orders:

```
POST /orient                                           # HTTP when the Praxis API is reachable
praxis workflow tools call praxis_operator_decisions \ # MCP / CLI path
    --input-json '{"action":"list","decision_kind":"architecture_policy","active_only":true}'
```

If neither is reachable, fall back to the registry/runtime-provided network
Postgres DSN. Do not start, query, or infer authority from a localhost
Postgres instance; localhost Postgres is retired for operator state.

```
psql "$WORKFLOW_DATABASE_URL" -c "
SELECT decision_scope_ref, decision_key, title, rationale
FROM operator_decisions
WHERE decision_kind='architecture_policy'
  AND effective_from <= now()
  AND (effective_to IS NULL OR effective_to > now())
ORDER BY decided_at DESC;
"
```

## Fresh clone / empty DB fallback

If the database is not created yet, has no `operator_decisions` rows, or cannot
answer the standing-order query, do not invent operator-local policy from
sidecar docs. Run:

```
./scripts/bootstrap
```

Then repeat the orientation query. Until the DB answers, stay inside the public
fresh-clone scope from `README.md`, `SETUP.md`, and `config/runtime_profiles.json`.
Treat private operator decisions, local provider assumptions, and workspace
exceptions as unavailable unless they are present in Praxis.db.

## Why this matters

Praxis.db is the cross-harness source of truth. Codex, Claude, Gemini, and Cursor all read the same `operator_decisions` table. Anything that lives only in `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, or `.cursorrules` is proprietary sidecar drift. Operator authority lives in Postgres.

The active Praxis.db is the network/registry-resolved Postgres authority, not
the old localhost development database. If `WORKFLOW_DATABASE_URL` is absent,
discover it through the workflow registry/runtime authority or ask for the
current network DSN; do not resurrect local Postgres to make a command pass.

## Binding behavior

Each row returned is a **standing order**. Treat each `title` + `rationale` pair as a durable directive that governs this session until explicitly retired (`effective_to` set).

Do not argue with them. File new decisions via `praxis_operator_architecture_policy` if the ruleset needs to change.

## Filing decisions inline

When a durable decision is made in conversation, persist it immediately — do not wait for closeout. Use:

```
praxis workflow tools call praxis_operator_architecture_policy --input-json '{
  "authority_domain": "<domain>",
  "policy_slug": "<slug>",
  "title": "<one-line title>",
  "rationale": "<durable reason>",
  "decided_by": "<principal>",
  "decision_source": "conversation"
}' --yes
```

## Orientation packet

Beyond standing orders, `POST /orient` returns:

- `instruction_authority` — canonical directive for this lane
- `endpoints` — live HTTP surface directory
- `capabilities` — what this runtime provides
- `health`, `recent_activity`, `engineering_observability` — current platform state
- `search_surfaces` — federated retrieval (lead with `praxis_search`)
- `cli_surface` — curated CLI aliases

Read `standing_orders` first, then `instruction_authority.packet_read_order` for the rest.

## Tool surface — narrowing the choice space

Tools self-declare a `kind` field in their MCP catalog metadata. Use it to skip ahead:

- `kind=search` — read across sources (`praxis_search`, `praxis_query`, `praxis_recall`, `praxis_discover`, `praxis_research`)
- `kind=write` — mutates state (default; treat unlabeled as write)
- `kind=walk` — graph / run-scoped views (`praxis_run`)
- `kind=analytics` — aggregations
- `kind=alias` — deprecated wrapper, prefer the canonical name

Don't memorize the full surface — ask the catalog:

```
praxis workflow tools list
praxis workflow tools search <text>
praxis workflow tools describe <tool>
```

## Federated search — lead with `praxis_search`

`praxis_search` is the canonical entry point. Gateway-dispatched (`search.federated`), receipt-backed, returns a topic-anchor cluster shape that bundles code + decisions + knowledge + bugs + receipts + git_history + files + db in one call.

```
praxis workflow tools call praxis_search --input-json '{
  "query": "<plain English or /regex/ or \"exact\">",
  "sources": ["code","decisions","knowledge","bugs"],
  "limit": 8
}'
```

- **Modes:** `auto` (default — `/regex/` → regex, `"quoted"` → exact, prose → semantic), `semantic`, `exact`, `regex`.
- **Shapes:** `match` (line), `context` (±N lines, default), `full` (whole file/record).
- **Scope filters:** `paths` / `exclude_paths` (glob), `since_iso` / `until_iso`, `entity_kind` (`module|class|function|subsystem`), `type_slug`, `exclude_terms`, `extras` (source-specific knobs).
- **Auto-reindex:** on by default — lazy refresh when on-disk drift exceeds `stale_threshold`. Manual escape: `praxis workflow discover reindex --yes`.

Response carries `clusters[]` with `primary` + `related` (capped 3 per source with `count` + `fetch_hint`) + `also` (count + preview + fetch payload for residual hits) + `empty_state` / `source_empty_states` when nothing matches. The legacy flat `results[]` list still ships for backward compat.

Per-source ops are also gateway-dispatched when you only need one source: `search.code`, `search.knowledge`, `search.decisions`, `search.research`, `search.bugs`, `search.receipts`, `search.git_history`, `search.files`, `search.db`.

Knowledge-graph noise (`hard_failure:*`, `verification:*`, `workflow_<hex>`, `receipt:*`) is filtered by default. Opt-in via `scope.extras.include_event_log_facts=true`.

## CQRS gateway dispatch is the engine bus

Every Engine operation runs through `runtime.operation_catalog_gateway.execute_operation_from_subsystems`. The gateway validates the payload via the operation's Pydantic `input_model_ref`, persists a row in `authority_operation_receipts` (input hash, idempotency key, timestamps, `execution_status`), replays cached results when `idempotency_policy ∈ {idempotent, read_only}`, and emits `authority_events` for command operations with `event_required=TRUE`.

Standing-order: `architecture_policy / cqrs_gateway_robust_determinism`. **No MCP-tool-tier shims** dispatching to subsystems directly — every new MCP tool is a thin wrapper that calls the gateway and returns its result. New operations populate all three CQRS tables consistently:

- `operation_catalog_registry` — authoritative (`operation_kind`, `idempotency_policy`, `receipt_required`, `event_required`, `event_type`, `handler_ref`, `input_model_ref`).
- `authority_object_registry` — `object_kind` matches `operation_kind` (`'query'` or `'command'`; constraint widened in migration 279).
- `data_dictionary_objects` — `category` matches `operation_kind`.

## Run-scoped views — `praxis_run` (consolidated)

Four old tools (`praxis_run_status` / `_scoreboard` / `_graph` / `_lineage`) collapsed into one with `action=` enum. Old names remain as `kind=alias` for one window.

```
praxis_run(run_id="...", action="status" | "scoreboard" | "graph" | "lineage")
```

## Bug filing & verifier-backed resolution

`praxis_bugs` for filing, dedup, evidence, replay, resolution. Resolving to `FIXED` requires a registered verifier — the tracker actually executes it before flipping status. The verifier writes a `verification_runs` row, the tracker links it as `evidence_role=validates_fix` on the bug.

```
praxis workflow tools call praxis_bugs --input-json '{
  "action": "resolve", "bug_id": "BUG-XXXXXXXX", "status": "FIXED",
  "verifier_ref": "verifier.job.python.pytest_file",
  "inputs": {"path": "<ABSOLUTE path>"},
  "target_kind": "path", "target_ref": "<ABSOLUTE path>"
}'
```

`verifier.job.python.pytest_file` runs from `/` — paths MUST be absolute. Use `FIX_PENDING_VERIFICATION` if you can't run the verifier yet; never claim FIXED without proof.
