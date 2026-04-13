# Provider Configuration

Praxis Engine supports multiple LLM providers. The routing system selects the optimal provider and model for each job based on task type, cost, and capability.

## Supported Providers

| Provider | Env Var | API Protocol | Models |
|----------|---------|--------------|--------|
| Anthropic | `ANTHROPIC_API_KEY` | Anthropic Messages API | Claude Opus, Sonnet, Haiku |
| OpenAI | `OPENAI_API_KEY` | OpenAI Chat Completions | GPT-5.4, GPT-5.4-mini |
| Google | `GEMINI_API_KEY` | Google GenAI | Gemini 3.1 Pro, Gemini 3 Flash |
| DeepSeek | `DEEPSEEK_API_KEY` | OpenAI-compatible | DeepSeek R3 (research only) |

At least one provider must be configured. The engine routes jobs across all available providers.

## API Key Storage

### macOS (Keychain)

Keys are stored in macOS Keychain under the service name `praxis`:

```bash
security add-generic-password -a "praxis" -s "praxis" -l "ANTHROPIC_API_KEY" \
  -w "your-key-here" -U
```

### Environment Variables

Set keys as standard environment variables or in `.env`:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Resolution order: Keychain (macOS) > environment variable > `.env` file.

## Model Routing

### Task-Type Routing (auto/ prefixes)

When a job uses `auto/` routing, the engine selects the best model based on task type profiles stored in the provider registry database.

Each provider/model combination has capability scores for task types:

- **build** -- Code generation quality
- **review** -- Code analysis accuracy
- **architecture** -- System design reasoning
- **test** -- Test generation and terminal operation
- **refactor** -- Code restructuring
- **wiring** -- Simple tasks (optimizes for cost)
- **debate** -- Adversarial reasoning
- **research** -- Deep research (DeepSeek-only)

### Direct Model Selection

Bypass routing with explicit provider/model:

```json
{"agent": "anthropic/claude-opus-4-6"}
```

### Provider Fallback

If the primary provider for a job is unavailable (rate limit, outage), the engine falls back to the next eligible provider with matching capabilities.

## runtime_profiles.json

Located at `config/runtime_profiles.json`. Defines the provider pool and allowed models:

```json
{
  "schema_version": 1,
  "default_runtime_profile": "praxis",
  "runtime_profiles": {
    "praxis": {
      "instance_name": "praxis",
      "provider_names": ["openai", "anthropic", "google"],
      "allowed_models": [
        "gpt-5.4",
        "claude-opus-4-6",
        "claude-sonnet-4-6",
        "gemini-3.1-pro-preview",
        "gpt-5.4-mini",
        "gemini-3-flash-preview",
        "claude-haiku-4-5-20251001"
      ],
      "repo_root": ".",
      "workdir": "."
    }
  }
}
```

### Profile Fields

| Field | Description |
|-------|-------------|
| `provider_names` | Ordered list of enabled providers |
| `allowed_models` | Whitelist of model IDs the runtime may use |
| `repo_root` | Repository root for file operations |
| `workdir` | Working directory for job execution |
| `receipts_dir` | Where execution receipts are stored |
| `topology_dir` | Where runtime topology snapshots are stored |

## Provider Adapter Contract

All providers implement `ProviderAdapterContract`:

- Authentication and key resolution
- Request serialization for the provider's API protocol
- Response parsing and token usage extraction
- Streaming support
- Error handling and retry semantics

The adapter registry supports hot-reloading provider configuration from the database via `reload_from_db()`.

## Health and Monitoring

Check provider health:

```bash
# HTTP
curl http://localhost:8420/health

# MCP
praxis_health(action="check")
```

The health endpoint reports per-provider status, recent error rates, and budget consumption.
