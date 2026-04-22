#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_ROOT}/scripts/_workflow_env.sh"
workflow_root="$(workflow_repo_workflow_root)"
workflow_load_repo_env
export PYTHONPATH="${workflow_root}${PYTHONPATH:+:$PYTHONPATH}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3.14 || command -v python3.13 || command -v python3)}"

exec "${PYTHON_BIN}" "${REPO_ROOT}/scripts/test_frontdoor.py" "$@"
