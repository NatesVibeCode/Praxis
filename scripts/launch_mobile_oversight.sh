#!/bin/bash
# Launch the mobile_oversight 8-phase chain.
#
# Usage (with Claude Code closed, from a plain Terminal):
#   ./scripts/launch_mobile_oversight.sh              # submit the chain
#   ./scripts/launch_mobile_oversight.sh --dry-run    # validate only
#
# Once submitted, the already-loaded launchd agent com.praxis.agent-sessions
# (PID persistent, KeepAlive=true) will dispatch waves as dependencies clear.
# No separate advance-tick cron is needed until Phase 7's healer lands.
#
# Observe progress from any terminal:
#   praxis workflow query "chain status"
#   praxis workflow run-status <wave_run_id>

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

exec /opt/homebrew/bin/python3 scripts/launch_mobile_oversight_chain.py "$@"
