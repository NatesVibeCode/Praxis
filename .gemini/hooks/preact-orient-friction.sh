#!/usr/bin/env bash
# preact-orient-friction.sh (Gemini CLI BeforeTool hook)
#
# Mirrors the Claude Code hook one-for-one. Reads the BeforeTool payload on
# stdin, forwards it to the harness-neutral Python implementation. Hook
# payload shape is the same as Claude's: `{tool_name, tool_input}`. Response
# uses `hookSpecificOutput.additionalContext`. Confirmed against
# gemini-cli@0.39.1 bundle.
#
# The Python entry handles tool-name aliasing (run_shell_command → Bash,
# replace → Edit, write_file → Write, read_file → Read) inside
# `surfaces.policy._normalize_tool_name`, so the trigger registry stays
# harness-neutral.

set -uo pipefail

REPO_ROOT="${GEMINI_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
IMPL="${REPO_ROOT}/.gemini/hooks/preact_orient_friction.py"

if [[ ! -f "$IMPL" ]]; then
  exit 0  # fail open
fi

# Per-session cooldown — see BUG-3E9820C4. Persist (decision_key, target)
# fired pairs across subprocess hook invocations so consecutive edits to
# the same file don't surface the same advisory repeatedly.
session_marker="${GEMINI_SESSION_ID:-${CLAUDE_SESSION_ID:-${PPID}}}"
export PRAXIS_SESSION_COOLDOWN_DIR="${TMPDIR:-/tmp}/praxis_cooldown_${session_marker}"

GEMINI_PROJECT_DIR="$REPO_ROOT" python3 "$IMPL"
