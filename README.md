# Praxis Engine

Self-hostable AI workflow engine with multi-provider agent routing and MCP integration.

## What is it?

Praxis Engine is an autonomous workflow runner that executes multi-job DAG workflows across LLM providers (OpenAI, Anthropic, Google, DeepSeek). Jobs are routed semantically via task-type prefixes (`auto/build`, `auto/review`, `auto/architecture`, etc.) to the best-fit model for each task. The engine compiles workflow specs into dependency graphs, manages execution leases, and exposes catalog-backed MCP tools for integration with Claude Code and other MCP clients. All state is stored in Postgres with pgvector for embedding-backed knowledge graph and code discovery.

## Quickstart

```bash
# 1. Clone
git clone https://github.com/your-org/praxis.git
cd praxis

# 2. Start Postgres with pgvector
docker compose up -d

# 3. Configure environment
cp .env.example .env
# Edit .env — add at least one LLM provider API key

# 4. Install Python dependencies and run migrations
pip install -r Code\&DBs/Workflow/requirements.runtime.txt
WORKFLOW_DATABASE_URL=postgresql://localhost:5432/praxis \
  python Code\&DBs/Workflow/storage/postgres/migrate.py

# 5. Launch the API server
WORKFLOW_DATABASE_URL=postgresql://localhost:5432/praxis \
  python -m uvicorn surfaces.api.native_operator_surface:app \
    --host 0.0.0.0 --port 8420
```

The server is ready when you see `Uvicorn running on http://0.0.0.0:8420`. Hit `POST /orient` for full runtime status.

## Example Workflow Spec

Workflow specs are JSON files (`.queue.json`) that define a DAG of jobs:

```json
{
  "name": "feature-build-and-review",
  "workflow_id": "feature-build-and-review",
  "phase": "execute",
  "outcome_goal": "Build a feature and review it",
  "anti_requirements": ["Do not modify unrelated files"],
  "jobs": [
    {
      "label": "implement",
      "agent": "auto/build",
      "prompt": "Implement the user authentication module"
    },
    {
      "label": "test",
      "agent": "auto/test",
      "prompt": "Write tests for the authentication module",
      "depends_on": ["implement"]
    },
    {
      "label": "review",
      "agent": "auto/review",
      "prompt": "Review the implementation and tests for correctness",
      "depends_on": ["implement", "test"]
    }
  ]
}
```

Run it:
```
praxis_workflow(action="run", spec_path="specs/my_feature.queue.json")
```

See [docs/WORKFLOW_SPEC.md](docs/WORKFLOW_SPEC.md) for the full spec reference and [examples/](examples/) for more patterns.

## Agent Routing

The `agent` field uses `auto/` prefixes to route jobs to the best model for each task type:

| Route | Task Type | Selects |
|-------|-----------|---------|
| `auto/build` | Code generation | Best coder |
| `auto/review` | Code review | Best reviewer |
| `auto/architecture` | System design | Best reasoner |
| `auto/test` | Testing | Best terminal operator |
| `auto/refactor` | Refactoring | Best refactorer |
| `auto/wiring` | Glue/config tasks | Cheapest fast model |
| `auto/debate` | Adversarial analysis | Best reasoner |
| `auto/research` | Deep research | Research-specialized model |

The router resolves these to concrete provider/model pairs based on the provider registry and runtime profiles. You can also specify a provider directly: `"agent": "anthropic/claude-sonnet-4-6"`.

## MCP Integration

Praxis exposes catalog-backed tools via the Model Context Protocol. Add to your `.mcp.json`:

```json
{
  "mcpServers": {
    "praxis": {
      "command": "python",
      "args": ["-m", "surfaces.mcp.server"],
      "cwd": "Code&DBs/Workflow",
      "env": {
        "WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis"
      }
    }
  }
}
```

Tool surfaces: Workflow, Operator, Knowledge, Evidence, Bugs, Discovery, Query, Health, Session, Intent, Submission, Governance, Artifacts, Wave, Context, Connector.

See [docs/MCP.md](docs/MCP.md) for the full tool reference.

The same catalog now powers terminal discovery:

```bash
praxis workflow tools list
praxis workflow tools describe praxis_query
praxis workflow tools call praxis_health --input-json '{}'
praxis workflow api routes
```

The public HTTP contract lives under `/v1`. The internal route catalog remains available at `GET /api/routes`, which defaults to the public `/v1` slice and supports `visibility=all` when operators need the full internal surface.

## Architecture

```
                        .queue.json specs
                              |
                     +--------v--------+
                     |  Spec Compiler  |
                     |  (DAG builder)  |
                     +--------+--------+
                              |
                     +--------v--------+
                     | Workflow Engine  |
                     | (lease, execute) |
                     +--------+--------+
                              |
              +---------------+---------------+
              |               |               |
     +--------v--+   +-------v---+   +-------v----+
     |  Anthropic |   |  OpenAI   |   |  Google    |
     |  Adapter   |   |  Adapter  |   |  Adapter   |
     +--------+--+   +-------+---+   +-------+----+
              |               |               |
         Claude API      GPT API       Gemini API


    +------------------+     +------------------+
    |  Postgres + pgv  |     |   MCP Server     |
    |  (state, graph,  |     | (catalog-backed) |
    |   embeddings)    |     +------------------+
    +------------------+
```

**Key components:**

- **Spec Compiler** -- Parses `.queue.json` files, validates DAG structure, resolves agent routes, expands fan-out primitives, and lowers to a `WorkflowRequest` graph.
- **Workflow Engine** -- Manages execution lifecycle: claiming jobs via leases, dispatching to provider adapters, tracking status, handling retries and timeouts.
- **Provider Adapters** -- Unified interface (`ProviderAdapterContract`) for each LLM provider. Handles authentication, API protocols, token budgets, and response parsing.
- **Storage** -- Postgres with pgvector. Stores workflow state, execution history, knowledge graph, embeddings, operator control plane, and bug tracking.
- **MCP Server** -- Exposes engine capabilities as MCP tools for integration with Claude Code and other clients.

## Project Layout

```
Code&DBs/
  Workflow/                 # Python runtime
    adapters/               # Provider adapter contracts
    registry/               # Provider routing and catalog
    runtime/                # Engine core: execution, leases, specs
      workflow/             # DAG execution, claiming, status
      integrations/         # Connector system
    storage/                # Postgres repositories
    surfaces/
      api/                  # HTTP API (FastAPI)
      mcp/                  # MCP tool server
      app/                  # Dashboard UI (React/Vite)
  Databases/
    migrations/workflow/    # SQL migrations (001-028)
config/
  runtime_profiles.json     # Provider/model routing config
```

## Configuration

See [SETUP.md](SETUP.md) for detailed installation and configuration instructions, including:
- Docker and native setup paths
- API key configuration for all supported providers
- Runtime profile customization
- MCP server setup
- Troubleshooting

## License

[AGPL-3.0](LICENSE)
