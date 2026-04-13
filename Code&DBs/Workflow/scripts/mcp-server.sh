#!/usr/bin/env bash
# MCP server launcher for the current surfaces.mcp entrypoint.
# Supports stdio MCP traffic over either Content-Length framing or JSONL.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export PYTHONPATH="${PROJ_ROOT}:${PYTHONPATH:-}"
exec python3 -m surfaces.mcp
