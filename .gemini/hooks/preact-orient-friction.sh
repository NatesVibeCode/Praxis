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

GEMINI_PROJECT_DIR="$REPO_ROOT" python3 "$IMPL"
