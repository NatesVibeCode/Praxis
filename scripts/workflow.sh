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

launch_detached() {
  local command=$1
  local launch_id_prefix=$2
  local result_file_base=$3
  local success_prefix=$4
  local emit_parent=$5
  shift 5

  mkdir -p "${REPO_ROOT}/artifacts"
  RESULT_FILE="${REPO_ROOT}/artifacts/${result_file_base}.$(date +%s).$$.${RANDOM}.json"
  LAUNCH_ID="${launch_id_prefix}-$(date +%s)-$$"

  # Run detached so the workflow survives if the calling session dies.
  nohup "${PYTHON_BIN}" -m surfaces.cli.main "${command}" "$@" \
    --job-id "${LAUNCH_ID}" \
    --result-file "${RESULT_FILE}" \
    >> "${REPO_ROOT}/artifacts/workflow.log" 2>&1 &
  WORKFLOW_PID=$!

  for _ in 1 2 3 4 5; do
    if [ -s "${RESULT_FILE}" ]; then
      "${PYTHON_BIN}" - "${RESULT_FILE}" "${success_prefix}" "${emit_parent}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
run_id = payload.get("run_id") or "unknown"
workflow_id = payload.get("workflow_id") or "unknown"
status = payload.get("status") or "unknown"
parent_run_id = payload.get("parent_run_id") or "unknown"
status_prefix = "Workflow replayed" if status == "replayed" else sys.argv[2]
emit_parent = sys.argv[3] == "1"

print(f"{status_prefix}: {run_id}")
print(f"Workflow ID: {workflow_id}")
if emit_parent:
    print(f"Parent run: {parent_run_id}")
print(f"Submission status: {status}")
print(f"Result file: {sys.argv[1]}")
PY
      echo "Use './scripts/praxis workflow active' to observe progress"
      exit 0
    fi
    if ! kill -0 "${WORKFLOW_PID}" 2>/dev/null; then
      break
    fi
    sleep 1
  done

  if [ ! -s "${RESULT_FILE}" ] && ! kill -0 "${WORKFLOW_PID}" 2>/dev/null; then
    echo "Workflow ${command} process exited before durable submission completed."
    echo "Result file: ${RESULT_FILE}"
    echo "Check artifacts/workflow.log for the launch error."
    exit 1
  fi

  echo "Workflow ${command} process started (PID ${WORKFLOW_PID}), awaiting durable submission result."
  echo "Result file: ${RESULT_FILE}"
  echo "Use './scripts/praxis workflow active' to check progress"
}

case "${1:-help}" in
  run)
    launch_detached run \
      "workflow-launch" \
      "workflow_run_result" \
      "Workflow submitted" \
      0 \
      "${@:2}"
    ;;
  spawn)
    launch_detached spawn \
      "workflow-spawn" \
      "workflow_spawn_result" \
      "Child workflow spawned" \
      1 \
      "${@:2}"
    ;;
  dry-run)
    run_frontdoor run --dry-run "${@:2}"
    ;;
  help|-h|--help)
    run_frontdoor --help
    echo ""
    echo "Canonical frontdoor:"
    echo "  ./scripts/praxis workflow <command>"
    echo "  ./scripts/praxis-workflow <command>"
    echo ""
    echo "Detached helpers:"
    echo "  workflow.sh run <spec.json>         Detached workflow launch wrapper"
    echo "  workflow.sh spawn <parent> <spec>   Detached child workflow spawn wrapper"
    echo "  workflow.sh dry-run <spec.json>     Durable runner dry-run helper"
    ;;
  *)
    run_frontdoor "$@"
    ;;
esac
