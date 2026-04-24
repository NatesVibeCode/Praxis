#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_ROOT}/scripts/_workflow_env.sh"
workflow_root="$(workflow_repo_workflow_root)"
workflow_load_repo_env
export PYTHONPATH="${workflow_root}${PYTHONPATH:+:$PYTHONPATH}"
PYTHON_BIN="${PYTHON_BIN:-$(workflow_python_bin)}"

exec "${PYTHON_BIN}" "${REPO_ROOT}/scripts/test_frontdoor.py" "$@"
