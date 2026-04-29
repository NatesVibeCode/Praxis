# Architecture

Internal architecture of Praxis Engine.

## System Overview

```
                  Human · MCP Client · HTTP Client · CLI
                                |
        +-----------------------v-----------------------+
        |                   Surfaces                     |
        |   Moon (Overview · Build · Atlas · Manifests   |
        |   · Console)   ·   CLI   ·   MCP   ·   REST    |
        +-----------------------+-----------------------+
                                |
        +-----------------------v-----------------------+
        |           CQRS Gateway (engine bus)            |
        |  every operation · receipts · idempotency      |
        |  policy · authority events · replay path       |
        +--+-------------------+--------------------+---+
           |                   |                    |
   +-------v-------+   +-------v--------+   +-------v--------+
   |   Compose     |   |   Workflow     |   |   Authority    |
   |   Pipeline    |   |   Runtime      |   |   Layer        |
   | synthesis     |   | DAG · lease ·  |   | operator_      |
   | + 20 forks    |   | execute · seal |   | decisions ·    |
   | + pill triage |   |                |   | primitive_     |
   |               |   |                |   | catalog ·      |
   |               |   |                |   | receipts       |
   +-------+-------+   +-------+--------+   +-------+--------+
           |                   |                    |
           +---------+---------+--------------------+
                     |
        +------------v------------+   +------------------+
        |    Provider Adapters    |   | Postgres + pgv   |
        | Anthropic · OpenAI ·    |   | (durable state,  |
        | Google · DeepSeek       |   |  knowledge graph,|
        | task-type routed        |   |  embeddings)     |
        +-------------------------+   +------------------+
```

## CQRS Gateway

The gateway (`runtime.operation_catalog_gateway.execute_operation_from_subsystems`) is the bus every Praxis Engine operation runs through. It:

- Validates the payload against the operation's Pydantic `input_model_ref`.
- Writes a row to `authority_operation_receipts` (input hash, idempotency key, execution status, replay path).
- Replays cached results when the operation's `idempotency_policy` is `read_only` or `idempotent` and the same payload reappears.
- Emits `authority_events` rows for command operations with `event_required=TRUE`.

Three CQRS tables stay consistent for every operation:

- `operation_catalog_registry` — authoritative. `operation_kind`, `idempotency_policy`, `receipt_required`, `event_required`, `event_type`, `handler_ref`, `input_model_ref`.
- `authority_object_registry` — `object_kind` matches `operation_kind` (`'query'` for query ops, `'command'` for command ops).
- `data_dictionary_objects` — `category` matches `operation_kind`.

Read receipts are not optional. Even read operations record their own ledger row so an agent's "what did the LLM ask?" trail is reproducible.

Standing-order row: `architecture_policy / cqrs_gateway_robust_determinism`. New MCP tools are thin wrappers that call the gateway; no tier-tier shims dispatching to subsystems directly.

## Compose Pipeline

The LLM-first launch compiler turns plain-English intent into a workflow plan with explicit gates the operator sees:

1. **Synthesis** — a frontier model decomposes intent into ~20 packet seeds (≈30s).
2. **Fork-out** — 20 parallel author calls expand the seeds, prefix-cached (≈2-3 min).
3. **Pill triage + validation** — typed gaps, source-ref resolution, write-scope contracts, verifier admission.

The Build screen surfaces these gates live during compose ("Composing... synthesis + 20 parallel forks") and the Release tray refuses dispatch until pre-flight checks pass — `0 jobs · triggered by no trigger · 1/4 checks` blocks by design.

`runtime.spec_compiler.launch_plan` and `runtime.intent_composition.compose_plan_from_intent` are the dispatched command operations. Both register `event_type=plan.launched` / `plan.composed`, so completed receipts emit `authority_events` rows tying the launched plan to its receipt.

`pattern_materialize_candidates` (`runtime/operations/commands/platform_patterns.py`) surfaces reusable shapes from completed runs. Successful patterns can be promoted into `primitive_catalog` after consistency check.

## Authority Layer

Praxis treats durable authority as a first-class layer, not a side-effect of execution:

- **`operator_decisions`** — standing orders, architecture policies, supersession history. Scoped by `decision_kind`. The decision table below is a projection of architecture-policy rows; the canonical authority is the table.
- **`primitive_catalog`** — declared platform primitives (authorities, engines, gateway wrappers, repositories) consistency-checked against code. Drift between blueprint and implementation surfaces as a structured finding.
- **`authority_operation_receipts`** — one ledger row per gateway dispatch. Input/output hashes, idempotency key, execution status, cause receipt, correlation id.
- **`authority_events`** — durable event stream emitted by command operations. Subscribers (Moon modules, replay tooling) read from here instead of polling REST.
- **Verifier-backed bug resolution** — bugs cannot be claimed `FIXED` without pointing at a registered verifier authority that the tracker actually executes. The verifier run lands in `verification_runs` and links via `bug_evidence_links` with `evidence_role=validates_fix` before status flips.

## Surfaces

Moon is the canonical operator UI. One canvas, four tabs, one chat panel:

- **Overview** — `WORKFLOW_CONTRACT` panel (TASK / READ / WRITE / LOCKED / TOOLS / APPROVAL / VERIFIER / RETRY / MATERIALIZED), receipts panel, sandbox terminal.
- **New workflow (Build)** — describe-the-outcome compose entry, hollow-node graph editor, Inspector/Authority tabs, Release tray with pre-flight checks.
- **Graph Diagram (Atlas)** — over-time accumulation across tracked architecture areas, weights, write-rate sparklines, hot/dormant signals.
- **Manifests** — manifest catalog search.
- **Strategy Console (Chat)** — slide-over partner panel with starter prompts (`What changed since my last session?`, `Help me plan the next build step.`, `Find the relevant context for this screen.`).

CLI and MCP are sibling surfaces over the same gateway. `praxis workflow ...` (operator terminal), `mcp__praxis__*` (Claude Code / Codex / any MCP host), and `bin/praxis-agent` (Claude Code's stdio-disclaimer-immune lane). Same catalog, same gateway, same receipts.

## Decision Table

Durable architecture policy lives in `operator_decisions` under the typed
`architecture_policy` decision kind. Keep this table as a projection of the
canonical rows, not a parallel decision store.

| Decision key | Scope | Decision | Effect |
| --- | --- | --- | --- |
| `architecture-policy::decision-tables::db-native-authority` | `decision_tables` | Decision tables are DB-native authority. | Durable control stays queryable and inspectable in Postgres. |
| `architecture-policy::decision-tables::scripts-support-only` | `decision_tables` | Scripts support decision tables; they do not replace them. | Shell glue stays tooling, not architecture. |
| `architecture-policy::sandbox-execution::docker-only-authority` | `sandbox_execution` | Workflow sandbox execution is Docker-only. | Local host-executed routes are not architectural authority. Use `docker_local` locally or admitted `cloudflare_remote`; fail closed if Docker is unavailable. |
| `architecture-policy::embedding-runtime::service-boundary` | `embedding_runtime` | Keep semantic capability at the surface contract while moving heavy local inference out of default control-plane images. | API and worker stay lean; the semantic backend owns local `torch`. |
| `architecture-policy::embedding-runtime::replacement-contract` | `embedding_runtime` | Do not remove semantic behavior without a validated replacement. | Lean backends must prove the same contract before becoming default. |
| `architecture-policy::compile-authority::db-backed-enrichment` | `compile_authority` | Compile truth is DB-backed; embeddings are enrichment. | Structural compile stays authoritative even when semantic mode degrades. |

Architecture-policy rows are durable operator authority. Workflow job execution now resolves scoped architecture-policy rows into execution bundles and workspace decision packs so agents see them before work begins; outside that path they remain available through `praxis_recall` and chat knowledge search rather than ambient prompt stuffing.

## DAG Execution

### Spec Compilation

1. **Parse** -- Load `.queue.json` file, validate required fields.
2. **Normalize** -- Auto-generate labels, translate `id` references to labels, expand `replicate` fan-outs and `replicate_with` loops, resolve sprint ordering to `depends_on` edges.
3. **Route resolution** -- Resolve `auto/` agent routes to concrete provider/model pairs via the provider registry.
4. **Graph lowering** -- Compile to `WorkflowRequest` containing `WorkflowNodeContract` nodes and `WorkflowEdgeContract` edges. Validate the graph is acyclic.

The compiler supports multiple adapter types:
- `llm_task` -- Standard LLM completion
- `api_task` -- HTTP API calls
- `cli_llm` -- CLI-based LLM execution
- `mcp_task` -- MCP tool invocation
- `deterministic_task` -- Non-LLM computation
- `context_compiler` -- Context assembly
- `output_parser` -- Output post-processing
- `file_writer` -- File output
- `verifier` -- Verification steps

### Execution Lifecycle

1. **Submit** -- Workflow request is persisted to Postgres with status `pending`.
2. **Claim** -- The engine claims the run via execution lease (prevents double-execution).
3. **Schedule** -- Topological sort determines execution order. Jobs with no unmet dependencies are eligible.
4. **Dispatch** -- Eligible jobs are dispatched to provider adapters. Multiple independent jobs run concurrently.
5. **Complete** -- As jobs finish, downstream dependents become eligible. Results are stored.
6. **Verify** -- If `verify_refs` are specified, verification scripts run after all jobs succeed.
7. **Seal** -- The run is sealed with final status (`completed`, `failed`, `partial`).

### Execution Leases

Jobs are claimed via database-level leases to prevent duplicate execution in multi-worker deployments. A lease includes:
- Holder identity
- Expiration timestamp
- Heartbeat tracking

If a worker crashes, the lease expires and the job becomes re-claimable.

## Provider Adapters

### Registry

The provider registry is the authority for provider configuration. It stores:
- Provider profiles (API endpoints, protocol family, supported models)
- Adapter contracts (how to call each provider's API)
- Task-type routing scores (which model is best for each task type)
- Economic data (cost per token)
- Health/budget state

The registry supports hot-reload from the database.

### Adapter Contract

All providers implement `ProviderAdapterContract`:

```python
class ProviderAdapterContract:
    provider_slug: str          # e.g. "anthropic"
    adapter_type: str           # e.g. "llm_task"
    api_protocol_family: str    # e.g. "anthropic_messages"
    
    async def execute(self, request) -> result
    async def health_check() -> status
```

### Routing Algorithm

For `auto/` routes:
1. Filter providers to those with valid API keys and healthy status.
2. Filter models to those allowed by the runtime profile.
3. Score each eligible model against the task type.
4. Select the highest-scoring model, with fallback to next-best on failure.

## Storage

### Postgres Schema

All state is in a single Postgres database. Key table groups:

**Workflow state:**
- Workflow runs, jobs, execution history
- Execution leases and claims
- Dispatch and outbox tables

**Provider registry:**
- Provider profiles and routing rules
- Task-type scoring and eligibility
- Health and budget tracking

**Knowledge graph:**
- Memory nodes and edges
- Vector embeddings (pgvector)
- Full-text search indexes

**Operator control plane:**
- Operator state and configuration
- Bug tracking and evidence links
- Roadmap and phase management

**Integration registry:**
- Third-party connector definitions
- Connector execution history

### Migrations

SQL migrations live in `Code&DBs/Databases/migrations/workflow/`, but the numeric file tree is not the only authority. The canonical manifest, policy buckets, and expected-object contracts live in `Code&DBs/Workflow/system_authority/workflow_migration_authority.json` and are generated into `Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py`. Startup and health must fail closed when the numbered SQL tree and generated authority drift.

### pgvector

The `vector` extension enables:
- Embedding storage for code discovery (`praxis_discover`)
- Knowledge graph similarity search (`praxis_recall`)
- Semantic intent matching (`praxis_intent_match`)

Embeddings are stored as `vector(384)` or `vector(768)` columns, but heavy local inference is isolated behind the semantic backend boundary. `runtime/embedding_service.py` prefers the HTTP semantic service and only uses local loading as a compatibility path. Control-plane images should not need `torch`.

## MCP Server

The MCP server (`surfaces/mcp/server.py`) exposes engine capabilities as MCP tools. It runs as a subprocess spawned by the MCP client (e.g., Claude Code).

### Tool Registration

Tools are organized by surface module:
- `surfaces/mcp/tools/workflow.py` -- Workflow tools
- `surfaces/mcp/tools/discover.py` -- Discovery tools
- `surfaces/mcp/tools/integration.py` -- Integration tools
- `surfaces/mcp/tools/health.py` -- Health tools

Each tool function is decorated with MCP metadata (name, description, input schema) and registered at server startup.

### Connection Flow

```
Claude Code  --stdio-->  MCP Server  --asyncpg-->  Postgres
                              |
                         Provider Adapters  --HTTPS-->  LLM APIs
```

## HTTP API

The HTTP API (`surfaces/api/native_operator_surface.py`) is a FastAPI application exposing the same capabilities as MCP tools via REST endpoints.

Key endpoints:
- `POST /orient` -- Full runtime status and orientation
- `GET /health` -- Health check
- `POST /workflow/run` -- Submit and execute a workflow
- `GET /workflow/{run_id}` -- Get workflow run status

## Moon Dashboard

A React/Vite single-page application (`surfaces/app/`) — the canonical operator UI. One canvas, four tabs, one chat panel.

- **Overview** (`dashboard/Dashboard.tsx`) — `WORKFLOW_CONTRACT` panel, receipts panel, sandbox terminal pane. The trust compiler made visible.
- **New workflow / Build** (`moon/MoonBuildPage.tsx`) — describe-the-outcome compose entry, hollow-node graph editor, Inspector/Authority tabs (`MoonNodeDetail.tsx`), Release tray with pre-flight checks (`MoonReleaseTray.tsx`).
- **Atlas** (`atlas/AtlasPage.tsx`) — over-time accumulation across tracked architecture areas. *Confidence infrastructure, materialized.*
- **Manifests** (`praxis/ManifestCatalogPage.tsx`) — durable manifest catalog search by family, type, status, free text.
- **Strategy Console** (`dashboard/StrategyConsole.tsx`) — slide-over partner panel with starter prompts that orient backward / forward / sideways through the work.

The build canvas is event-driven: `useLiveRunSnapshot.ts` consumes the stream from `/api/shell/state/stream`, `useBuildEvents.ts` subscribes to `/api/workflows/{id}/build/stream`. Polling is a fallback, not the default.

Built with `npm run build` and served by the API server.
