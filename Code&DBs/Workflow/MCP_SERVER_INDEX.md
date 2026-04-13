# Praxis Workflow MCP Server Index

## Current Surface

The active MCP server is the `surfaces.mcp` package, launched by
`scripts/mcp-server.sh`.

Key files:

- `scripts/mcp-server.sh` — stdio launcher
- `surfaces/mcp/__main__.py` — `python -m surfaces.mcp` entrypoint
- `surfaces/mcp/protocol.py` — JSON-RPC transport and MCP notifications
- `surfaces/mcp/registry.py` — auto-discovers tools from `surfaces/mcp/tools/`
- `surfaces/mcp/tools/workflow.py` — async workflow contract
- `surfaces/MCP_SERVER_README.md` — protocol and usage guide

## Workflow Contract

Models and clients should treat Praxis workflow execution this way:

1. Validate specs first with `praxis_workflow_validate(spec_path='...')` when the spec changed.
2. Launch with `praxis_workflow(action='run', spec_path='...')`.
3. Treat the launch response as kickoff only. It returns `run_id`, `stream_url`, and `status_url`.
4. Consume live progress on the separate stream channel.
5. Use `praxis_workflow(action='status', run_id=run_id)` for snapshots, failure heuristics, and optional idle cancellation.
6. Do not reintroduce wait-style blocking behavior in the front door.

The point is to keep launch cheap while workers execute elsewhere and status keeps flowing on a separate path.

## Transport Notes

- Stdio transport supports both Content-Length framing and JSONL input.
- The server can emit `notifications/progress` and `notifications/message` while a tool call is in flight.
- Tool inventory is auto-discovered from `surfaces/mcp/tools/`; it is not a fixed 4-tool surface anymore.

## Smoke Test

```bash
cd "<repo-root>/Code&DBs/Workflow"
./scripts/mcp-server.sh <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
EOF
```

## Config Snippet

```json
{
  "mcpServers": {
    "praxis-workflow": {
      "command": "<repo-root>/Code&DBs/Workflow/scripts/mcp-server.sh",
      "disabled": false
    }
  }
}
```

No checked-in `config/mcp_config.json` is required. Use the launcher path directly.
