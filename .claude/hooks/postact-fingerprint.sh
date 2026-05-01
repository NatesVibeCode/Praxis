#!/usr/bin/env bash
# postact-fingerprint.sh — PostToolUse hook for action shape fingerprinting.
#
# DEFAULT-OFF. Coded and ready, but disabled until both:
#   1) An entry is added to .claude/settings.json (PostToolUse with matcher
#      "Bash|Edit|Write|MultiEdit|Read") pointing here.
#   2) PRAXIS_FINGERPRINT_ENABLED=1 is exported in the harness environment.
#
# Even when wired in settings.json, this short-circuits unless the env var
# is set, so wiring is reversible without removing the entry.
#
# Source surface tagging: PRAXIS_FINGERPRINT_SOURCE_SURFACE controls how
# fingerprints from this hook are tagged in action_fingerprints. Defaults
# to "claude-code:host". Sandbox harnesses should export
# "sandbox-worker:<agent>" or similar so cross-surface frequency counting
# in tool_opportunities_pending works correctly.
#
# Fail-open: any error in this hook silently exits 0. Tool calls never
# block on fingerprinting.

set -uo pipefail

if [[ "${PRAXIS_FINGERPRINT_ENABLED:-0}" != "1" ]]; then
    exit 0
fi

REPO_ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
IMPL="${REPO_ROOT}/.claude/hooks/postact_fingerprint.py"

if [[ ! -f "$IMPL" ]]; then
    exit 0
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" && -f "$REPO_ROOT/scripts/_workflow_env.sh" ]]; then
    # shellcheck source=/dev/null
    . "$REPO_ROOT/scripts/_workflow_env.sh"
    PYTHON_BIN="$(workflow_python_bin 2>/dev/null || true)"
fi
if [[ -z "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3 || true)"
fi
[[ -z "$PYTHON_BIN" ]] && exit 0

PAYLOAD="$(cat -)"

# Fire-and-forget. Never block the tool call.
LOG="${PRAXIS_FINGERPRINT_LOG:-${TMPDIR:-/tmp}/praxis-fingerprint.log}"
printf '%s' "$PAYLOAD" \
  | CLAUDE_PROJECT_DIR="$REPO_ROOT" \
    PRAXIS_FINGERPRINT_SOURCE_SURFACE="${PRAXIS_FINGERPRINT_SOURCE_SURFACE:-claude-code:host}" \
    nohup "$PYTHON_BIN" "$IMPL" >>"$LOG" 2>&1 &
disown
exit 0
