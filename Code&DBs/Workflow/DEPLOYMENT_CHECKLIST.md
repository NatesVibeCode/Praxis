# Praxis Workflow MCP Deployment Checklist

## Implementation Checklist

- [x] `scripts/mcp-server.sh` launches `python3 -m surfaces.mcp`
- [x] `surfaces/mcp/protocol.py` accepts framed MCP traffic and JSONL smoke-test traffic
- [x] `surfaces/mcp/catalog.py` provides the shared MCP tool catalog authority
- [x] `praxis_workflow` is documented and enforced as async kickoff only
- [x] `praxis_workflow(action='status')` exposes health heuristics and optional `kill_if_idle`
- [x] `docs/MCP.md` and `MCP_SERVER_INDEX.md` document the separate stream/status channels
- [x] orient output tells agents not to wait on launch

## Verification Steps

### 1. Launcher Works

```bash
cd "/path/to/praxis/Code&DBs/Workflow"
./scripts/mcp-server.sh <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
EOF
```

### 2. Tool Catalog Loads

```bash
cd "/path/to/praxis/Code&DBs/Workflow"
./scripts/mcp-server.sh <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
EOF
```

### 3. Workflow Spec Validation Works

```bash
cd "/path/to/praxis/Code&DBs/Workflow"
./scripts/mcp-server.sh <<'EOF'
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"praxis_workflow_validate","input":{"spec_path":"/path/to/praxis/config/specs/local_alpha/W0_workflow_preflight.json"}}}
EOF
```

## Client Config

```json
{
  "mcpServers": {
    "praxis-workflow": {
      "command": "/path/to/praxis/Code&DBs/Workflow/scripts/mcp-server.sh",
      "disabled": false
    }
  }
}
```

## Behavior Checklist For Models

- [x] `praxis_workflow(action='run')` is launch-only and returns quickly
- [x] live progress is consumed on a separate stream channel
- [x] launch should not block the client from issuing new commands
- [x] status polling is the truth surface for health, failure signals, and idle detection
- [x] legacy wait-style actions are not part of the contract

## Notes

- No checked-in `config/mcp_config.json` is required.
- No old single-file MCP shim entrypoint should be referenced in new docs.
- The generic MCP task adapter docs are separate from this repo's own workflow MCP surface.
