# MCP Server Reference

Praxis Engine exposes 38 tools via the [Model Context Protocol](https://modelcontextprotocol.io/) for integration with Claude Code and other MCP clients.

## Setup

Add to `.mcp.json` (Claude Code) or equivalent MCP client configuration:

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

The MCP server connects to the same Postgres database as the HTTP API.

## Tools by Surface

### Workflow

| Tool | Description |
|------|-------------|
| `praxis_workflow` | Run, validate, or manage workflow specs |
| `praxis_workflow_validate` | Validate a workflow spec without executing |

### Operator

| Tool | Description |
|------|-------------|
| `praxis_status` | Current engine status |
| `praxis_maintenance` | Maintenance operations |
| `praxis_operator_view` | Read operator control plane state |
| `praxis_operator_write` | Write operator control plane state |
| `praxis_operator_closeout` | Close out completed work items |
| `praxis_operator_roadmap_view` | View roadmap and phase status |

### Knowledge

| Tool | Description |
|------|-------------|
| `praxis_recall` | Retrieve from knowledge graph |
| `praxis_ingest` | Add to knowledge graph |
| `praxis_graph` | Query knowledge graph structure |

### Evidence

| Tool | Description |
|------|-------------|
| `praxis_receipts` | Execution receipts and history |
| `praxis_constraints` | Constraint management |
| `praxis_friction` | Friction logging and analysis |

### Bugs

| Tool | Description |
|------|-------------|
| `praxis_bugs` | File, query, and resolve bugs |

### Discovery

| Tool | Description |
|------|-------------|
| `praxis_discover` | Semantic code search via embeddings |

### Query

| Tool | Description |
|------|-------------|
| `praxis_query` | General-purpose database queries |

### Health

| Tool | Description |
|------|-------------|
| `praxis_health` | Runtime health checks |

### Session

| Tool | Description |
|------|-------------|
| `praxis_session` | Session lifecycle management |
| `praxis_heartbeat` | Session heartbeat |
| `praxis_decompose` | Task decomposition |
| `praxis_research` | Quick knowledge graph research |

### Intent

| Tool | Description |
|------|-------------|
| `praxis_intent_match` | Match user intent to capabilities |
| `praxis_manifest_generate` | Generate workflow manifests from intent |
| `praxis_manifest_refine` | Refine generated manifests |

### Submission

| Tool | Description |
|------|-------------|
| `praxis_submit_code_change` | Submit code changes for review |
| `praxis_submit_research_result` | Submit research findings |
| `praxis_submit_artifact_bundle` | Submit artifact bundles |
| `praxis_get_submission` | Retrieve submission details |
| `praxis_review_submission` | Review a submission |

### Governance

| Tool | Description |
|------|-------------|
| `praxis_governance` | Governance policy queries |
| `praxis_heal` | Self-healing operations |

### Artifacts

| Tool | Description |
|------|-------------|
| `praxis_artifacts` | Artifact storage and retrieval |

### Wave

| Tool | Description |
|------|-------------|
| `praxis_wave` | Multi-run wave orchestration |

### Context

| Tool | Description |
|------|-------------|
| `praxis_context_shard` | Context sharding for large payloads |

### Connector

| Tool | Description |
|------|-------------|
| `praxis_connector` | Third-party API connector management |

## Protocol Details

- **Transport:** stdio (spawned as subprocess by MCP client)
- **Protocol version:** MCP 1.x
- **Serialization:** JSON-RPC over stdio
- **Authentication:** Inherits environment (database URL, API keys)

All tools accept a JSON object as input and return a JSON object. Errors are returned as MCP tool errors with descriptive messages.

## Example Calls

### Check status

```
praxis_query("status")
```

### Run a workflow

```
praxis_workflow(action="run", spec_path="specs/my_feature.queue.json")
```

### Search for existing code

```
praxis_discover(query="authentication middleware")
```

### File a bug

```
praxis_bugs(action="file", title="Login timeout", description="Auth flow times out after 30s")
```

### Ingest knowledge

```
praxis_ingest(content="The auth module uses JWT tokens with 24h expiry", tags=["auth", "jwt"])
```
