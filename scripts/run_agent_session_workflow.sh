#!/bin/bash
# One-shot wrapper: launches the agent_session_service workflow via Praxis Engine.
# Guarded by a marker file so launchd's daily fire only runs it once;
# remove artifacts/agent_session_workflow.launched to let it run again.

set -euo pipefail

REPO="${PRAXIS_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
SPEC="$REPO/config/cascade/specs/W_agent_session_service_20260420.queue.json"
MARKER="$REPO/artifacts/agent_session_workflow.launched"
LOG_DIR="$REPO/artifacts"
LAUNCH_LOG="$LOG_DIR/agent_session_workflow_launch.log"
PRAXIS="${PRAXIS_BIN:-$(command -v praxis || true)}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] agent-session-spawn: $*" | tee -a "$LAUNCH_LOG"; }

mkdir -p "$LOG_DIR"

if [ -f "$MARKER" ]; then
  log "marker present at $MARKER — already launched; nothing to do"
  exit 0
fi

if [ ! -f "$SPEC" ]; then
  log "ERROR: spec not found at $SPEC"
  exit 1
fi

if [ -z "$PRAXIS" ]; then
  log "ERROR: praxis CLI not found; set PRAXIS_BIN"
  exit 1
fi

log "launching workflow: $SPEC"
cd "$REPO"

# Run the workflow; capture output but don't fail-fast — we want the marker written
# whether the workflow succeeds or not, so launchd doesn't re-fire nightly until
# the user explicitly clears the marker.
if "$PRAXIS" workflow run "$SPEC" >> "$LAUNCH_LOG" 2>&1; then
  log "workflow launched OK"
  echo "launched_at=$(ts)" > "$MARKER"
  exit 0
else
  rc=$?
  log "workflow launch failed rc=$rc — NOT writing marker; will retry tomorrow"
  exit "$rc"
fi
