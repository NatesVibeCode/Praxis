#!/usr/bin/env bash
# preact-orient-friction.sh — PreToolUse hook entry point.
#
# Thin wrapper. Reads stdin (the Claude Code hook payload), forwards it to
# the sibling Python implementation. The Python script does the matching,
# friction emission, and additionalContext shaping.
#
# Exit codes:
#   0  — hook ran cleanly. stdout is the JSON hook response.
#   non-zero — hook errored. Claude Code treats hook errors as no-op
#              continue=true. We choose to fail open by default.

set -uo pipefail

REPO_ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
IMPL="${REPO_ROOT}/.claude/hooks/preact_orient_friction.py"

if [[ ! -f "$IMPL" ]]; then
  # Fail open. No matching, no injection, tool proceeds.
  exit 0
fi

# Per-session cooldown directory. Each PreToolUse hook invocation is a
# fresh subprocess so the in-process cooldown cache (in surfaces.policy)
# can't dedupe consecutive edits to the same file. Pointing at a stable
# per-session marker dir lets the matcher persist (decision_key, target)
# pairs across subprocess invocations. Session boundary defaults to PPID
# of the agent harness — close enough; the harness restarts on session
# rollover. Cleanup is the operator's job (rm -rf at session end).
# See BUG-3E9820C4.
session_marker="${CLAUDE_SESSION_ID:-${PPID}}"
export PRAXIS_SESSION_COOLDOWN_DIR="${TMPDIR:-/tmp}/praxis_cooldown_${session_marker}"

# Forward stdin (hook payload) to the Python implementation. CLAUDE_PROJECT_DIR
# is exported so the impl knows where to find the trigger registry and the
# praxis-agent binary.
CLAUDE_PROJECT_DIR="$REPO_ROOT" python3 "$IMPL"
