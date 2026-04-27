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

# Forward stdin (hook payload) to the Python implementation. CLAUDE_PROJECT_DIR
# is exported so the impl knows where to find the trigger registry and the
# praxis-agent binary.
CLAUDE_PROJECT_DIR="$REPO_ROOT" python3 "$IMPL"
