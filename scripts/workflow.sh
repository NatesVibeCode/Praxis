#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}/Code&DBs/Workflow${PYTHONPATH:+:$PYTHONPATH}"
export WORKFLOW_DATABASE_URL="${WORKFLOW_DATABASE_URL:?"WORKFLOW_DATABASE_URL must be set"}"
export PATH="${PATH}"

case "${1:-help}" in
  generate)
    python3 -m surfaces.cli.workflow_cli generate "${@:2}"
    ;;
  run)
    mkdir -p "${REPO_ROOT}/artifacts"
    RESULT_FILE="${REPO_ROOT}/artifacts/workflow_run_result.$(date +%s).$$.${RANDOM}.json"
    LAUNCH_ID="workflow-launch-$(date +%s)-$$"
    # Run detached so the workflow survives if the calling session dies
    nohup python3 -m surfaces.cli.workflow_cli run "${@:2}" \
      --job-id "${LAUNCH_ID}" \
      --result-file "${RESULT_FILE}" \
      >> "${REPO_ROOT}/artifacts/workflow.log" 2>&1 &
    WORKFLOW_PID=$!
    for _ in 1 2 3 4 5; do
      if [ -s "${RESULT_FILE}" ]; then
        python3 - "${RESULT_FILE}" <<'PY'
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
    # Called by MCP detached workflow launch — runs inline (caller handles detachment)
    # Passes through --job-id and --result-file for structured result tracking
        python3 -m surfaces.cli.workflow_cli run "${@:2}"
    ;;
  validate)
        python3 -m surfaces.cli.workflow_cli validate "${@:2}"
    ;;
  chain)
    python3 -m surfaces.cli.workflow_cli chain "${@:2}"
    ;;
  chain-status)
    python3 -m surfaces.cli.workflow_cli chain-status "${@:2}"
    ;;
  status)
        python3 -m surfaces.cli.workflow_cli status "${@:2}"
    ;;
  stream)
        python3 -m surfaces.cli.workflow_cli stream "${@:2}"
    ;;
  cancel)
        python3 -m surfaces.cli.workflow_cli cancel "${@:2}"
    ;;
  active)
        python3 -m surfaces.cli.workflow_cli active "${@:2}"
    ;;
  dry-run)
        python3 -m surfaces.cli.workflow_cli run --dry-run "${@:2}"
    ;;
  *)
    echo "Usage: workflow.sh {generate|run|run-managed|validate|chain|chain-status|status|active|stream|cancel|dry-run} [args]"
    echo ""
    echo "Commands:"
    echo "  generate <manifest.json> <output.json> Generate a workflow spec from a manifest file"
    echo "  run <spec.json>            Run a workflow spec (detached, survives session death)"
    echo "  run-managed <spec.json>    Run inline with --job-id/--result-file (used by MCP)"
    echo "  validate <spec.json>       Validate a workflow spec without running"
    echo "  chain <coordination.json>  Submit a durable chained multi-wave workflow program"
    echo "  chain-status [chain_id]    Show one chain or list recent chains"
    echo "  status                     Show recent workflow status from receipts"
    echo "  active                     Show currently active workflow runs"
    echo "  stream <run_id>            Stream one workflow run in the terminal"
    echo "  cancel <run_id>            Cancel an in-flight workflow run"
    echo "  dry-run <spec.json>        Simulate workflow execution without mutating state"
    ;;
esac
