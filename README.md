# Praxis Engine

Self-hostable AI platform where every action is receipt-backed and every successful pattern compounds into durable authority. Postgres + pgvector + CQRS gateway. Multi-provider agent routing, catalog-backed MCP, autonomous workflow execution.

## What is it?

Praxis closes the loop most AI tools leave open.

The standard model: you ask an AI to do something, it produces output, you move on. Next time you start over. The tool didn't get smarter. Your work didn't accumulate into anything reusable.

Praxis treats every action as a durable event:

- **Operations produce receipts.** Every dispatched operation goes through the CQRS gateway, which records input hash, output hash, idempotency key, execution status, and replay path in `authority_operation_receipts`. Replays are deterministic when the policy allows.
- **Decisions become queryable authority.** Operator decisions, standing orders, and supersession history live in `operator_decisions` rows you can query, scope, and inherit. The system remembers what it decided and why.
- **Successful patterns can be promoted.** `pattern_materialize_candidates` surfaces reusable shapes from completed work; the `primitive_catalog` is consistency-checked against code so promoted patterns stay grounded.
- **Resolutions require evidence.** Bugs can't be claimed "fixed" — they must point at a verifier run with stdout/stderr/exit_code linked as evidence. Trust but verify, enforced.

The runtime underneath: a DAG workflow engine with multi-provider agent routing (OpenAI, Anthropic, Google, DeepSeek), one CQRS gateway dispatching every operation, and a catalog-backed MCP surface usable from Claude Code, Codex, or any MCP client. All state in Postgres + pgvector.

**The thesis:** every successful work cycle should make the next easier. Breadth becomes an output of how the platform compounds, not a feature checklist you maintain by hand.

## What you'll see

Moon is one canvas, four surfaces:

- **Overview** — the live workspace. The `WORKFLOW_CONTRACT` panel renders TASK / READ SCOPE / WRITE SCOPE / **LOCKED** authority / TOOLS / APPROVAL / VERIFIER / RETRY / MATERIALIZED side-by-side with the receipts panel and a sandbox terminal. The trust compiler made visible — what's allowed, what's locked, what proof is required to dispatch.
- **New workflow** — the materialization entry. Describe an outcome in plain English; Praxis runs `synthesis (a frontier model decomposes intent into ~20 packet seeds, ≈30s) → fork-out (20 parallel prefix-cached author calls, ≈2-3 min) → pill triage + validation`. Nothing renders until all gates pass. The Release tray refuses to dispatch incomplete intent — `0 jobs · triggered by no trigger · 1/4 checks` blocks by design.
- **Graph Diagram (Atlas)** — *Confidence infrastructure, materialized.* The over-time accumulation view: 20 tracked architecture areas, weights, write-rate sparklines, hot/dormant signals. Shows what's compounding.
- **Manifests** — the catalog. Search durable manifests by family, type, status, free text.
- **Strategy Console (Chat)** — the partner panel that slides over any surface. Starter prompts: *What changed since my last session? · Help me plan the next build step. · Find the relevant context for this screen.*

CLI and MCP are sibling surfaces over the same gateway. `praxis workflow ...` is the operator front door from a terminal; `mcp__praxis__*` is the MCP-client front door from Claude Code, Codex, or any MCP host.

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

### First 5 minutes with Praxis

Once `./scripts/bootstrap` reports success, this is the demo loop.

**1. Confirm the platform is alive (10 seconds).**

```bash
praxis workflow query "status"
```

You'll see standing orders, open bugs, recent runs. Every line is a queryable durable row, not a log message — Praxis is showing you what it currently believes is true.

**2. Open Moon, the canvas (1 minute).**

In a second terminal:

```bash
cd "Code&DBs/Workflow/surfaces/app"
npm install        # first time only
npm run dev
```

Vite prints a URL (typically `http://localhost:5173`). Open it; Moon auto-connects to the API on `8420`.

You're now looking at the live workspace. Four surfaces:
- **Overview** — the trust compiler made visible: TASK / READ / WRITE / TOOLS / VERIFIER side-by-side with receipts and a sandbox terminal.
- **New workflow** — the materialization entry. Type intent in plain English; watch synthesis → 20-fork author → pill triage → render.
- **Atlas** — confidence infrastructure: 20 tracked architecture areas with weights and write-rate sparklines. Shows what's compounding.
- **Manifests** — the catalog. Search durable manifests by family, type, free text.

**3. Run a deterministic smoke (no LLM cost, 30 seconds).**

```bash
praxis workflow run examples/bootstrap_smoke.queue.json
```

Provider-independent. Exercises the worker graph runtime without spending on any LLM. If it passes, your installation is structurally sound.

**4. Run the LLM hello-world (needs a provider key, 30 seconds).**

```bash
praxis workflow run examples/hello_world.queue.json
```

Routes through `auto/chat`, hits whichever provider you have configured, returns a sentence. The receipt shows up in your runs list within seconds — visible in Moon's Overview panel.

**5. Use the host CLI dispatcher (recommended for agents).**

```bash
scripts/praxis-up agent-broker        # start the broker once
bin/praxis-agent --list               # browse the full catalog
bin/praxis-agent praxis_search --input-json '{"query":"provider routing"}'
```

The front door for Claude Code, Codex, Gemini, and any other Mac-host agent. Credentials stay inside the container — your shell session can't leak them into a tool call.

**Where to look next:** recent receipts via `praxis workflow query "recent receipts"`, the full tool catalog in Moon's Manifests tab, or open a `.queue.json` in `examples/` and modify the prompts to see how the workflow runtime handles your edits.

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

If you prefer containers, the canonical entry points are `make refresh` (full stack) and `make worker` (workflow worker only). Both wrap `scripts/praxis-up`, which exports macOS Keychain provider auth into the shell, then `docker compose up -d --force-recreate` so the containers receive the fresh tokens at recreate time.

```bash
make refresh         # api-server + workflow-worker + scheduler
make worker          # workflow-worker only (cheaper if auth went stale)
```

The stack does **not** start its own database container; it uses `WORKFLOW_DATABASE_URL` from `.env` or the shell. That URL may point at host-local Postgres, another LAN machine, or any reachable Postgres 16+ instance with `pgvector`. When native host tools use a local-only DSN like `127.0.0.1`, set `PRAXIS_DOCKER_WORKFLOW_DATABASE_URL` to the container-reachable equivalent such as `postgresql://...@host.docker.internal:5432/praxis`.

The worker mounts CLI auth files from `PRAXIS_CLI_AUTH_HOME` when set, otherwise from `$HOME`. Local worker slots are derived from the CPU and RAM visible to the worker; set `PRAXIS_WORKER_MAX_PARALLEL` only when you need an explicit cap.

**Do not** invoke `docker compose up -d --force-recreate workflow-worker` directly. That path skips the keychain re-export and leaves the worker with an empty `CLAUDE_CODE_OAUTH_TOKEN`, which causes every Anthropic call to 401 silently. Use `make worker` instead — same recreate, with the auth hydration step included.

### Host CLI dispatcher (`bin/praxis-agent`)

Every Praxis tool call from the host shell goes through `bin/praxis-agent`, a thin client that POSTs to a long-lived broker (`praxis-agentd`) running inside the Docker stack. The broker owns provider credentials (Anthropic / OpenAI / OpenRouter / etc.); the host shell never reads them, so a stray export in your terminal session can't leak into a tool call. This is the canonical front door for any Mac-host agent — Claude Code, Codex, Gemini, your own scripts.

```bash
scripts/praxis-up agent-broker                       # start (or recreate) the broker once
bin/praxis-agent --list                              # browse the catalog
bin/praxis-agent --describe praxis_search            # inspect a tool's schema
bin/praxis-agent praxis_search --input-json '{"query":"provider routing"}'
```

The broker listens on `127.0.0.1:8422`. `bin/praxis-agent` reads its bearer token from `artifacts/agent-broker/token` (auto-generated by the broker at boot, ignored by git). If `bin/praxis-agent` says `broker token file not found`, run `scripts/praxis-up agent-broker` and try again.

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

**Key components:**

- **Surfaces** — Moon (React/Vite), CLI (`praxis workflow ...`), MCP (`mcp__praxis__*`), REST (`/v1`). All sibling lenses on the same gateway.
- **CQRS Gateway** (`runtime.operation_catalog_gateway`) — the engine bus. Every operation goes through it: validates the payload against a Pydantic input model, writes a row to `authority_operation_receipts`, replays cached results when the idempotency policy says so, emits `authority_events` rows for command operations.
- **Compose Pipeline** — the LLM-first launch compiler. A frontier model decomposes intent into ~20 packet seeds; 20 parallel prefix-cached author calls expand them; pill triage and validation gate before any render. `pattern_materialize_candidates` surfaces reusable shapes from completed runs.
- **Workflow Runtime** — DAG execution with leases, retries, sealing. `.queue.json` specs are the wire format; `compose_plan_from_intent` is the LLM-driven authoring path.
- **Authority Layer** — durable rows that outlast any single run: `operator_decisions` (standing orders, supersession history), `primitive_catalog` (consistency-checked against code), `authority_operation_receipts` (one ledger row per operation).
- **Provider Adapters** — unified `ProviderAdapterContract` for Anthropic, OpenAI, Google, DeepSeek. Task-type routing (`auto/build`, `auto/review`, etc.) selects the best-fit model per job.
- **Storage** — Postgres + pgvector. State, knowledge graph, embeddings, operator control plane, bugs, receipts, decisions, primitives — all one database.

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
