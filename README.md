# Praxis Engine

Self-hostable AI workflow engine with multi-provider agent routing and MCP integration.

## What is it?

Praxis Engine is an autonomous workflow runner that executes multi-job DAG workflows across LLM providers (OpenAI, Anthropic, Google, DeepSeek). Jobs are routed semantically via task-type prefixes (`auto/build`, `auto/review`, `auto/architecture`, etc.) to the best-fit model for each task. The engine compiles workflow specs into dependency graphs, manages execution leases, and exposes catalog-backed MCP tools for integration with Claude Code and other MCP clients. All state is stored in Postgres with pgvector for embedding-backed knowledge graph and code discovery.

## Quickstart

Praxis is one repo package: API, MCP, CLI, website, runtime, migrations, and setup authority live together in this checkout. The active runtime target and Praxis.db can be local, remote, Docker-backed, managed, or moved later; they are selected through setup authority, not hardcoded by OS or host path.

One command for a fresh local target. Requires Python 3.14 on `PATH`, Postgres 16+ with pgvector reachable through `WORKFLOW_DATABASE_URL` or the repo `.env`, and provider credentials for provider-backed demos.

```bash
git clone https://github.com/your-org/praxis.git
cd praxis
./scripts/bootstrap
```

`scripts/bootstrap` is idempotent. It:

1. Resolves setup/registry DB authority and creates `.env` only from the selected target authority.
2. Creates the selected target Postgres database only when using the fresh local bootstrap path and enables `pgvector`.
3. Creates `.venv/` and installs `Code&DBs/Workflow/requirements.runtime.txt`.
4. Symlinks `scripts/praxis` into `~/.local/bin/praxis` (add that to `PATH` if you don't already).
5. Runs `scripts/native-bootstrap.sh` — applies all migrations and the DB-backed fresh-install authority seed.
6. Runs `scripts/native-smoke.sh` — verifies the native operator flow end to end.
7. Starts the REST API on `PRAXIS_API_PORT` (default `8420`).
8. Validates, submits, and streams `examples/bootstrap_smoke.queue.json`, a deterministic worker smoke that does not depend on an LLM provider.

### First run

Once bootstrap reports success:

```bash
# Check the package, runtime target, DB authority, and empty thin sandbox contract
praxis setup doctor --json

# Orient on current state (standing orders, status, open bugs)
praxis workflow query "status"

# Run the deterministic bootstrap smoke again if you want a fresh run
praxis workflow run examples/bootstrap_smoke.queue.json

# Run the hello-world provider demo once provider credentials are ready
praxis workflow run examples/hello_world.queue.json
```

### Launch the API server manually

`scripts/bootstrap` starts the API for you. To run it manually:

```bash
source .venv/bin/activate
PYTHONPATH="Code&DBs/Workflow" \
  python -m surfaces.api.server --host 0.0.0.0 --port 8420
```

The server is ready when `GET /api/health` succeeds. `POST /orient` returns full runtime status.

> **Note:** `PYTHONPATH="Code&DBs/Workflow"` is required because the API module is rooted there. Leaving it out gives you `ModuleNotFoundError: No module named 'surfaces'`.

### Moon UI (dashboard)

Moon is the canonical dashboard — one canvas, every click persists. With the API server running:

```bash
cd "Code&DBs/Workflow/surfaces/app"
npm install
npm run dev
```

Vite serves the UI and proxies API calls to the port in `PRAXIS_API_PORT` (default `8420`). Open the URL Vite prints — Moon will connect to the API automatically.

### Docker path (alternative)

If you prefer containers, `docker compose up -d` brings up the cockpit services: semantic backend, API server, and scheduler. The stack does **not** start its own database container; it uses `WORKFLOW_DATABASE_URL` from `.env` or the shell. That URL may point at host-local Postgres, another LAN machine, or any reachable Postgres 16+ instance with `pgvector`. When native host tools use a local-only DSN like `127.0.0.1`, set `PRAXIS_DOCKER_WORKFLOW_DATABASE_URL` to the container-reachable equivalent such as `postgresql://...@host.docker.internal:5432/praxis`.

The worker also mounts CLI auth files from `PRAXIS_CLI_AUTH_HOME` when set, otherwise from `$HOME`. Local worker slots are derived from the CPU and RAM visible to the worker; set `PRAXIS_WORKER_MAX_PARALLEL` only when you need an explicit cap.

Execution workers are opt-in. Start a worker node explicitly on the machine that should do the work:

```bash
docker compose --profile worker up -d --build workflow-worker
```

The launcher alias is equivalent for day-to-day control:

```bash
./scripts/praxis start worker
```

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
```bash
praxis workflow run specs/my_feature.queue.json
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
        "WORKFLOW_DATABASE_URL": "<selected Praxis.db URL>"
      }
    }
  }
}
```

Tool surfaces: Workflow, Operator, Knowledge, Evidence, Bugs, Discovery, Query, Health, Session, Intent, Submission, Governance, Artifacts, Wave, Context, Connector.

Generated surface references:

- [docs/MCP.md](docs/MCP.md) for MCP/catalog tools.
- [docs/CLI.md](docs/CLI.md) for terminal entrypoints and aliases.
- [docs/API.md](docs/API.md) for public and internal HTTP routes.

The same catalog now powers terminal discovery:

```bash
praxis workflow tools list
praxis workflow tools search query
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
- **Workflow Engine** -- Manages execution lifecycle: claiming jobs via leases, routing work to provider adapters, tracking status, handling retries and timeouts.
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
    migrations/workflow/    # SQL migrations (001-193+; generated authority controls order)
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
