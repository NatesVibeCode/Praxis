# Praxis Engine Setup Guide

Complete installation and configuration reference.

## Prerequisites

- Python 3.11+
- PostgreSQL 16+ with [pgvector](https://github.com/pgvector/pgvector) extension
- Node.js 18+ (for dashboard UI only)
- At least one LLM provider API key (Anthropic, OpenAI, Google, or DeepSeek)

## Docker Setup

The fastest path. Requires Docker and Docker Compose.

```bash
# Start Postgres with pgvector
docker compose up -d

# Wait for healthy
docker compose ps  # should show "healthy"

# Install Python deps
pip install -r Code\&DBs/Workflow/requirements.runtime.txt

# Run migrations
WORKFLOW_DATABASE_URL=postgresql://postgres@localhost:5432/praxis \
  python Code\&DBs/Workflow/storage/postgres/migrate.py

# Copy and edit env
cp .env.example .env
# Add your API keys to .env

# Launch
WORKFLOW_DATABASE_URL=postgresql://postgres@localhost:5432/praxis \
  python -m uvicorn surfaces.api.native_operator_surface:app \
    --host 0.0.0.0 --port 8420
```

## macOS Native Setup

### Install Postgres with pgvector

```bash
brew install postgresql@16
brew install pgvector

# Start Postgres
brew services start postgresql@16

# Create database and enable pgvector
createdb praxis
psql praxis -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Store API Keys in Keychain

Praxis reads API keys from macOS Keychain under the service name `praxis`:

```bash
# Store each key
security add-generic-password -a "praxis" -s "praxis" -l "ANTHROPIC_API_KEY" \
  -w "sk-ant-..." -U

security add-generic-password -a "praxis" -s "praxis" -l "OPENAI_API_KEY" \
  -w "sk-..." -U

security add-generic-password -a "praxis" -s "praxis" -l "GEMINI_API_KEY" \
  -w "AI..." -U
```

The runtime resolves keys via Keychain first, then falls back to environment variables.

### Install and Run

```bash
pip install -r Code\&DBs/Workflow/requirements.runtime.txt

# Set database URL
export WORKFLOW_DATABASE_URL=postgresql://$(whoami)@localhost:5432/praxis

# Run migrations
python Code\&DBs/Workflow/storage/postgres/migrate.py

# Launch
python -m uvicorn surfaces.api.native_operator_surface:app \
  --host 0.0.0.0 --port 8420
```

## Linux Setup

### Install Postgres with pgvector

```bash
# Ubuntu/Debian
sudo apt install postgresql-16 postgresql-16-pgvector

sudo -u postgres createdb praxis
sudo -u postgres psql praxis -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Configure API Keys

On Linux, use environment variables (no Keychain equivalent):

```bash
# Add to ~/.bashrc or ~/.profile
export ANTHROPIC_API_KEY="sk-ant-..."
export OPENAI_API_KEY="sk-..."
export GEMINI_API_KEY="AI..."
export DEEPSEEK_API_KEY="sk-..."
```

### Install and Run

```bash
pip install -r Code\&DBs/Workflow/requirements.runtime.txt

export WORKFLOW_DATABASE_URL=postgresql://localhost:5432/praxis

python Code\&DBs/Workflow/storage/postgres/migrate.py

python -m uvicorn surfaces.api.native_operator_surface:app \
  --host 0.0.0.0 --port 8420
```

## Database Bootstrap

Migrations live in `Code&DBs/Databases/migrations/workflow/` (001-028). They run in order and are idempotent.

```bash
# Run all pending migrations
python Code\&DBs/Workflow/storage/postgres/migrate.py

# Verify
psql praxis -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';"
```

The schema includes: workflow runs, job state, execution leases, provider routing, knowledge graph, embeddings, operator control plane, bug tracking, and integration registry.

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
        "claude-opus-4-6",
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
| `PRAXIS_API_PORT` | `8420` | HTTP API port |
| `PRAXIS_API_HOST` | `0.0.0.0` | HTTP API bind address |
| `PRAXIS_DOCKER_IMAGE` | `praxis-worker:latest` | Docker image for sandboxed execution. If unset and the default image is missing, Praxis will build it from `Code&DBs/Workflow/docker/praxis-worker.Dockerfile` on first use. |
| `PRAXIS_DOCKER_MEMORY` | `4g` | Memory limit for Docker workers |
| `PRAXIS_DOCKER_CPUS` | `2` | CPU limit for Docker workers |

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
        "WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis"
      }
    }
  }
}
```

Verify with: `praxis_health(action="check")`

## Verification

Run the built-in health check:

```bash
# API health
curl http://localhost:8420/health

# Full status (requires running server)
curl -X POST http://localhost:8420/orient

# Via MCP (in Claude Code)
praxis_health(action="check")
praxis_query("status")
```

Expected output from `/orient`: database connection status, registered providers, active workflows, MCP tool count.

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
- macOS: `brew install pgvector`
- Linux: `sudo apt install postgresql-16-pgvector`

Then: `psql praxis -c "CREATE EXTENSION IF NOT EXISTS vector;"`

### No provider configured

```
ProviderRegistryError: no eligible provider
```

At least one API key must be set. Verify:
- macOS: `security find-generic-password -s "praxis" -l "ANTHROPIC_API_KEY" -w`
- Linux: `echo $ANTHROPIC_API_KEY`

### Port already in use

```
OSError: [Errno 48] Address already in use
```

Change the port: `PRAXIS_API_PORT=8421` or find the existing process: `lsof -i :8420`

### Migration failures

Migrations are idempotent. Re-run:
```bash
python Code\&DBs/Workflow/storage/postgres/migrate.py
```

If a specific migration fails, check the SQL file in `Code&DBs/Databases/migrations/workflow/` for manual review.
