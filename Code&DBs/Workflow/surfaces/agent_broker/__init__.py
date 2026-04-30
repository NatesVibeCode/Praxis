"""praxis-agentd: local agent broker for CLI/MCP traffic.

Transport, not authority. Boots the canonical MCP subsystems and dispatches
all tool calls through ``surfaces.mcp.protocol.handle_request`` →
``invoke_tool`` → ``operation_catalog_gateway`` so receipts, events,
trigger-checks, and credentials all behave identically to the api-server
``/mcp`` endpoint. The broker exists so that:

  * host CLI agents do not inherit host-shell credentials,
  * workflow-workers stop depending on the api-server for MCP traffic, and
  * the api-server stays for human-facing surfaces only.

Standing-order references:
  architecture-policy::auth::via-docker-creds-not-shell
  architecture-policy::surfaces::cli-mcp-parallel
"""
