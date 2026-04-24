# Praxis Engine Setup Guide

Complete installation and configuration reference.

## Prerequisites

- **Python 3.14** — the native operator wrappers are pinned to 3.14. Earlier versions are not supported.
- PostgreSQL 16+ with [pgvector](https://github.com/pgvector/pgvector) extension
- Node.js 18+ (Moon dashboard UI)
- At least one LLM provider API key (Anthropic, OpenAI, Google, or DeepSeek)

## One-command bootstrap (recommended)

For fresh clones:

```bash
./scripts/bootstrap
```

This script is idempotent. It resolves setup/registry DB authority, creates `.env` only from the selected target authority, creates the target Postgres database only for fresh local bootstrap, enables `pgvector`, creates `.venv`, installs dependencies, installs the config-backed Praxis launcher, runs `db-bootstrap` (full migration set plus fresh-install authority seed), starts the REST API, and validates/submits/streams `examples/bootstrap_smoke.queue.json`. The bootstrap smoke is deterministic and provider-independent; `examples/hello_world.queue.json` remains the provider demo. Skip the platform-specific instructions below unless you are debugging or intentionally diverging from it.

## Docker Setup

Requires Docker and Docker Compose. The compose stack does **not** include its own database. It uses `WORKFLOW_DATABASE_URL` from `.env` or the shell, so the database can be host-local, remote on the LAN, or any reachable Postgres 16+ instance with `pgvector`. If container networking needs a different DSN than native host tools do, set `PRAXIS_DOCKER_WORKFLOW_DATABASE_URL` to the container-reachable authority while leaving `WORKFLOW_DATABASE_URL` pointed at the native host authority.

```bash
# Start the cockpit services (semantic-backend + api-server + scheduler)
docker compose up -d

# Wait for healthy
docker compose ps

# From the host, prepare the database and dependencies
./scripts/bootstrap
```

Default compose startup is cockpit-only. The workflow worker is an explicit
execution-node profile so cockpit machines do not accidentally claim jobs from
the shared database.

Run the worker only on the machine that should execute workflow work:

```bash
docker compose --profile worker up -d --build workflow-worker
docker compose logs -f workflow-worker
```

For day-to-day control, `./scripts/praxis start worker` targets the same
execution-node service explicitly.

If nested Docker workers need to call the host-published API from Linux, set `PRAXIS_WORKFLOW_MCP_URL` to a reachable URL for `/mcp`. On Docker Desktop and OrbStack the default `http://host.docker.internal:8420/mcp` usually works. Set `PRAXIS_WORKFLOW_MCP_SIGNING_SECRET` in `.env` to the same random value for the API and worker containers; `openssl rand -hex 32` is enough for local setups.

On a two-machine LAN cluster, do not copy Apple Silicon Docker images from the
M1 to an Intel/AMD Dell. Rebuild the images on the Dell from the repo checkout
so the image architecture matches the worker host.

## macOS Native Setup

### Install Python 3.14 and Postgres

```bash
brew install python@3.14 postgresql@16

# Start Postgres
brew services start postgresql@16

# Create the praxis database
createdb praxis
```

### Install pgvector

Homebrew does not ship a first-party `pgvector` formula. Use one of:

```bash
# Option A — pgvector tap (simplest if available for your setup)
brew install pgvector/brew/pgvector

# Option B — build from source against brew's Postgres 16
git clone --branch v0.7.4 https://github.com/pgvector/pgvector.git /tmp/pgvector
cd /tmp/pgvector
PG_CONFIG=$(brew --prefix postgresql@16)/bin/pg_config make
PG_CONFIG=$(brew --prefix postgresql@16)/bin/pg_config make install
cd -

# Then enable in the praxis database
psql praxis -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

See the [pgvector install notes](https://github.com/pgvector/pgvector#installation-notes) if you hit errors.

### Store API Keys in Keychain

Praxis resolves keys in this precedence order: explicit env passed by a caller, **macOS Keychain** (account `praxis`, service `<ENV_VAR_NAME>`), process environment, then `.env` as a last-resort local fallback. On macOS, prefer Keychain — do not put real secrets in `.env`.

```bash
security add-generic-password -U -a "praxis" -s "ANTHROPIC_API_KEY" \
  -w "sk-ant-..."

security add-generic-password -U -a "praxis" -s "OPENAI_API_KEY" \
  -w "sk-..."

security add-generic-password -U -a "praxis" -s "GEMINI_API_KEY" \
  -w "AI..."
```

### Install and Run

The recommended path is `./scripts/bootstrap`. If you want to see the steps:

```bash
# Create venv + install deps
python3.14 -m venv .venv
source .venv/bin/activate
pip install -r Code\&DBs/Workflow/requirements.runtime.txt

# Bootstrap the DB (migrations + fresh-install authority seed)
./scripts/native-bootstrap.sh

# Launch the API server
#   PYTHONPATH is REQUIRED - the API module is rooted at Code&DBs/Workflow
PYTHONPATH="Code&DBs/Workflow" \
  python -m surfaces.api.server --host 0.0.0.0 --port 8420
```

## Linux Setup

### Install Python 3.14 and Postgres with pgvector

```bash
# Ubuntu/Debian — 24.04+ ships python3.14 in deadsnakes or build from source
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.14 python3.14-venv postgresql-16 postgresql-16-pgvector

sudo -u postgres createdb praxis
sudo -u postgres psql praxis -c "CREATE EXTENSION IF NOT EXISTS vector;"
sudo -u postgres psql -c "CREATE USER $(whoami) SUPERUSER;"   # so ./scripts/bootstrap can connect as your shell user
```

### Configure API Keys

On Linux, set environment variables (no Keychain equivalent). The runtime uses env vars directly:

```bash
# Add to ~/.bashrc or ~/.profile
export ANTHROPIC_API_KEY="sk-ant-..."
export OPENAI_API_KEY="sk-..."
export GEMINI_API_KEY="AI..."
export DEEPSEEK_API_KEY="sk-..."
```

### Install and Run

```bash
./scripts/bootstrap
```

Or manually:

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -r Code\&DBs/Workflow/requirements.runtime.txt

./scripts/native-bootstrap.sh

PYTHONPATH="Code&DBs/Workflow" \
  python -m surfaces.api.server --host 0.0.0.0 --port 8420
```

## Database Bootstrap

Migrations live in `Code&DBs/Databases/migrations/workflow/` — the repo is currently at migration **209**. They run in order, are idempotent, and are split into `canonical` (always applied) and `bootstrap_only` (applied on fresh instances). The classification is authored by `_generated_workflow_migration_authority.py`; fresh-install runtime rows are then reconciled from `config/runtime_profiles.json`.

```bash
# Apply everything — idempotent
./scripts/native-bootstrap.sh

# Verify
psql praxis -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';"
```

The schema covers: workflow runs, job state, execution leases, provider routing, knowledge graph, embeddings, operator control plane (`operator_decisions`), bug tracking, and integration registry.

## API Key Configuration

| Provider | Env Var | Required | Notes |
|----------|---------|----------|-------|
| Anthropic | `ANTHROPIC_API_KEY` | At least one provider required | Claude models |
| OpenAI | `OPENAI_API_KEY` | At least one provider required | GPT models |
| Google | `GEMINI_API_KEY` | At least one provider required | Gemini models |
| DeepSeek | `DEEPSEEK_API_KEY` | Optional | Research tasks only |
| GitHub | `GITHUB_TOKEN` | Optional | For GitHub integrations |
| Brave Search | `BRAVE_SEARCH_API_KEY` | Optional | For web search |

On macOS, store keys in Keychain (see above). On Linux, set as environment variables.

## Runtime Configuration

### runtime_profiles.json

Located at `config/runtime_profiles.json`. Defines provider routing policy:

```json
{
  "schema_version": 1,
  "default_runtime_profile": "praxis",
  "runtime_profiles": {
    "praxis": {
      "provider_names": ["openai", "anthropic", "google"],
      "allowed_models": [
        "gpt-5.4",
        "claude-opus-4-7",
        "claude-sonnet-4-6",
        "gemini-3.1-pro-preview"
      ],
      "repo_root": ".",
      "workdir": "."
    }
  }
}
```

### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKFLOW_DATABASE_URL` | (none) | PostgreSQL connection string |
| `PRAXIS_DOCKER_WORKFLOW_DATABASE_URL` | falls back to `WORKFLOW_DATABASE_URL` | Optional Docker-only PostgreSQL connection string for compose services when the native host DSN uses `localhost` or `127.0.0.1` |
| `PRAXIS_API_PORT` | `8420` | HTTP API port |
| `PRAXIS_API_HOST` | `0.0.0.0` | HTTP API bind address |
| `PRAXIS_API_URL` | (runtime target) | Client-facing API authority for remote/runtime-target clients; do not use the bind address as the client URL |
| `PRAXIS_DOCKER_IMAGE` | (unset) | Optional explicit debug image override for model sandboxes. Do not set this to `praxis-worker:latest`; the worker image is reserved for the workflow control service. |
| `PRAXIS_DOCKER_MEMORY` | `500m` | Memory limit for model sandboxes |
| `PRAXIS_DOCKER_CPUS` | `2` | CPU limit for Docker workers |
| `PRAXIS_CLI_AUTH_HOME` | `$HOME` | Host directory containing `.codex`, `.claude`, and `.gemini` auth files for Docker workers |
| `PRAXIS_WORKER_MAX_PARALLEL` | auto | Optional local workflow worker slot cap; unset derives slots from live CPU/RAM resources |
| `PRAXIS_WORKFLOW_MAX_CONCURRENT_NODES` | auto | Compatibility alias for the same optional worker slot cap |
| `PRAXIS_WORKFLOW_MCP_URL` | `http://host.docker.internal:8420/mcp` | MCP bridge URL for worker-launched model containers |
| `PRAXIS_WORKFLOW_MCP_SIGNING_SECRET` | (none) | Shared secret used by the API and worker to mint and verify workflow MCP session tokens |

### Runtime Target Setup

Praxis setup is one distributed control-plane surface. API and MCP own the
operation; CLI and website are clients of that authority.

The setup doctor also reports `complete_repo_package`. That is the machine
contract that this checkout contains the operator entrypoint, runtime,
migrations, API, MCP, CLI, website, runtime profiles, and derived skill exports
as one package. Missing pieces become blockers instead of tribal knowledge.

| Surface | Pointer |
|---------|---------|
| CLI client | `praxis setup doctor --json`, `praxis setup plan --json`, `praxis setup apply --yes --json` |
| Workflow CLI | `praxis workflow setup doctor --json` |
| MCP authority | `praxis_setup(action="doctor")` |
| API authority | `GET /api/setup/doctor`, `GET /api/setup/plan`, `POST /api/setup/apply` |
| Website client | Launcher readiness consumes `/api/launcher/status`, which includes the same runtime-target and sandbox contract from setup doctor |

SSH is not a setup path and not an authority layer. It is only build/deploy
transport for a selected target when artifacts or thin images must be built
there.

## MCP Setup

Add the Praxis MCP server to your Claude Code configuration:

### Claude Code (.mcp.json)

```json
{
  "mcpServers": {
    "praxis": {
      "command": "python",
      "args": ["-m", "surfaces.mcp.server"],
      "cwd": "/path/to/praxis/Code&DBs/Workflow",
      "env": {
        "WORKFLOW_DATABASE_URL": "<selected Praxis.db URL>"
      }
    }
  }
}
```

Verify with: `praxis_health()`

## Verification

`./scripts/bootstrap` runs the smoke as its final step. To re-verify manually:

```bash
# Native smoke — exercises the self-hosted flow end to end
./scripts/native-smoke.sh

# API health (requires running API server)
curl http://localhost:8420/api/health

# Full status
curl -X POST http://localhost:8420/orient

# Via the canonical CLI frontdoor
praxis workflow query "status"
praxis workflow health
praxis workflow tools list
praxis workflow tools search query

# Via MCP (in Claude Code)
praxis_health()
praxis_query(question="status")
```

Expected output from `/orient`: database connection status, registered providers, active workflows, MCP tool count, and current standing orders from `operator_decisions`.

## Troubleshooting

### Database connection refused

```
asyncpg.exceptions.ConnectionRefusedError
```

- Verify Postgres is running: `pg_isready -h localhost -p 5432`
- Check `WORKFLOW_DATABASE_URL` is set correctly
- For Docker: `docker compose ps` should show healthy

### pgvector extension missing

```
ERROR: extension "vector" is not available
```

- Docker: use `pgvector/pgvector:pg16` image (included in docker-compose.yml)
- macOS: use `brew install pgvector/brew/pgvector`, or build from source against `postgresql@16`
- Linux: `sudo apt install postgresql-16-pgvector`

Then: `psql praxis -c "CREATE EXTENSION IF NOT EXISTS vector;"`

### No provider configured

```
ProviderRegistryError: no eligible provider
```

At least one API key must be set. Verify:
- macOS: `security find-generic-password -a "praxis" -s "ANTHROPIC_API_KEY" -w`
- Linux: `echo $ANTHROPIC_API_KEY`

### Port already in use

```
OSError: [Errno 48] Address already in use
```

Change the port: `PRAXIS_API_PORT=8421` or find the existing process: `lsof -i :8420`

### Migration failures

Migrations are idempotent. Re-run:
```bash
./scripts/native-bootstrap.sh
```

If a specific migration fails, check the SQL file in `Code&DBs/Databases/migrations/workflow/` and the generated authority in `Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py`.
