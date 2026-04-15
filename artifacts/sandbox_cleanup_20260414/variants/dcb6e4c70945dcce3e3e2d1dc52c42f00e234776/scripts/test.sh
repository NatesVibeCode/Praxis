#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}/Code&DBs/Workflow${PYTHONPATH:+:$PYTHONPATH}"
export WORKFLOW_DATABASE_URL="${WORKFLOW_DATABASE_URL:?"WORKFLOW_DATABASE_URL must be set"}"
export PATH="${PATH}"

exec python3 "${REPO_ROOT}/scripts/test_frontdoor.py" "$@"
