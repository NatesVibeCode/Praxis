#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_ROOT}/scripts/_workflow_env.sh"
workflow_load_repo_env
export PYTHONPATH="${REPO_ROOT}/Code&DBs/Workflow${PYTHONPATH:+:$PYTHONPATH}"
export PATH="${PATH}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3.14 || command -v python3.13 || command -v python3)}"

run_frontdoor() {
  "${PYTHON_BIN}" -m surfaces.cli.main "$@"
}

case "${1:-help}" in
  run)
    mkdir -p "${REPO_ROOT}/artifacts"
    RESULT_FILE="${REPO_ROOT}/artifacts/workflow_run_result.$(date +%s).$$.${RANDOM}.json"
    LAUNCH_ID="workflow-launch-$(date +%s)-$$"
    # Run detached so the workflow survives if the calling session dies
    nohup "${PYTHON_BIN}" -m surfaces.cli.workflow_cli run "${@:2}" \
      --job-id "${LAUNCH_ID}" \
      --result-file "${RESULT_FILE}" \
      >> "${REPO_ROOT}/artifacts/workflow.log" 2>&1 &
    WORKFLOW_PID=$!
    for _ in 1 2 3 4 5; do
      if [ -s "${RESULT_FILE}" ]; then
        "${PYTHON_BIN}" - "${RESULT_FILE}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
run_id = payload.get("run_id") or "unknown"
workflow_id = payload.get("workflow_id") or "unknown"
status = payload.get("status") or "unknown"
prefix = "Workflow replayed" if status == "replayed" else "Workflow submitted"
print(f"{prefix}: {run_id}")
print(f"Workflow ID: {workflow_id}")
print(f"Submission status: {status}")
print(f"Result file: {sys.argv[1]}")
PY
        echo "Use 'workflow.sh status' to check progress"
        exit 0
      fi
      if ! kill -0 "${WORKFLOW_PID}" 2>/dev/null; then
        break
      fi
      sleep 1
    done

    if [ ! -s "${RESULT_FILE}" ] && ! kill -0 "${WORKFLOW_PID}" 2>/dev/null; then
      echo "Workflow process exited before durable submission completed."
      echo "Result file: ${RESULT_FILE}"
      echo "Check artifacts/workflow.log for the launch error."
      exit 1
    fi

    echo "Workflow process started (PID ${WORKFLOW_PID}), awaiting durable submission result."
    echo "Result file: ${RESULT_FILE}"
    echo "Use 'workflow.sh status' to check progress"
    ;;
  run-managed)
    "${PYTHON_BIN}" -m surfaces.cli.workflow_cli run "${@:2}"
    ;;
  dry-run)
    "${PYTHON_BIN}" -m surfaces.cli.workflow_cli run --dry-run "${@:2}"
    ;;
  help|-h|--help)
    run_frontdoor --help
    echo ""
    echo "Detached helpers:"
    echo "  workflow.sh run <spec.json>         Detached workflow launch wrapper"
    echo "  workflow.sh run-managed <spec.json> Inline durable launch helper"
    echo "  workflow.sh dry-run <spec.json>     Durable runner dry-run helper"
    ;;
  *)
    run_frontdoor "$@"
    ;;
esac
