# Architecture

Internal architecture of Praxis Engine.

## System Overview

```
  User / MCP Client / HTTP Client
              |
     +--------v--------+
     |    Surfaces      |
     | (MCP, HTTP, CLI) |
     +--------+---------+
              |
     +--------v--------+
     |  Spec Compiler   |
     +--------+---------+
              |
     +--------v--------+
     | Workflow Engine  |
     +--+-----+-----+--+
        |     |     |
   +----v+ +--v--+ +v----+
   |Anthr| |Open | |Goog |   Provider Adapters
   +----++ +--+--+ ++----+
        |     |     |
   Claude   GPT   Gemini     LLM APIs
        |     |     |
     +--v-----v-----v--+
     |    Postgres      |
     |  (state, graph,  |
     |   embeddings)    |
     +------------------+
```

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
2. **Normalize** -- Auto-generate labels, translate `id` references to labels, expand `replicate` fan-outs, resolve sprint ordering to `depends_on` edges.
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

## Dashboard UI

A React/Vite single-page application (`surfaces/app/`) providing:
- Workflow run visualization
- Job status and output viewing
- Chat panel for interactive operation
- Module palette for workflow composition

Built with `npm run build` and served by the API server.
