#!/usr/bin/env bash
# MCP server launcher for the current surfaces.mcp entrypoint.
# Supports stdio MCP traffic over either Content-Length framing or JSONL.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJ_ROOT/.." && pwd)"

# Self-sufficient env load. Some MCP launchers (Claude.app's disclaimer helper
# in particular) bypass the praxis-mcp wrapper and exec this script directly,
# so we cannot rely on PRAXIS_WORKSPACE_BASE_PATH being pre-set. Without it,
# graph-capable workflow submits fail closed because registry rows like
# `base_path: ${PRAXIS_WORKSPACE_BASE_PATH}` cannot expand from env.
if [ -f "${REPO_ROOT}/scripts/_workflow_env.sh" ]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/scripts/_workflow_env.sh"
  workflow_load_repo_env >/dev/null 2>&1 || true
fi
: "${PRAXIS_WORKSPACE_BASE_PATH:=$REPO_ROOT}"
: "${PRAXIS_LAUNCHER_RESOLVED_REPO_ROOT:=$REPO_ROOT}"
export PRAXIS_WORKSPACE_BASE_PATH PRAXIS_LAUNCHER_RESOLVED_REPO_ROOT

export PYTHONPATH="${PROJ_ROOT}:${PYTHONPATH:-}"
exec python3 -m surfaces.mcp
