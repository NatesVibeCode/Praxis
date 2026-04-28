#!/usr/bin/env bash
# preact-orient-friction.sh (Codex CLI PreToolUse hook)
#
# Codex hook entrypoint. The Python implementation records every match but
# writes stdout only for explicit, non-advisory context injection so routine
# tool calls do not create transcript noise.
#
# The Python entry handles tool-name aliasing
# (`local_shell` → Bash, `apply_patch` → Edit) so the trigger registry
# stays harness-neutral.
#
# This file is loaded via either:
#   - `~/.codex/hooks.json` (user-global) referencing this path
#   - `.codex/hooks.json` (project-local) referencing this path
#   - a Codex plugin manifest in the repo (`.codex-plugin/plugin.json`
#     with `"hooks": "./hooks.json"`)
# See `policy/HARNESS_INTEGRATION.md` for the wire-up recipe.

set -uo pipefail

REPO_ROOT="${CODEX_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
IMPL="${REPO_ROOT}/.codex/hooks/preact_orient_friction.py"

if [[ ! -f "$IMPL" ]]; then
  exit 0  # fail open
fi

# Per-session cooldown — see BUG-3E9820C4. Persist (decision_key, target)
# fired pairs across subprocess hook invocations so consecutive edits to
# the same file don't surface the same advisory repeatedly.
session_marker="${CODEX_SESSION_ID:-${CLAUDE_SESSION_ID:-${PPID}}}"
export PRAXIS_SESSION_COOLDOWN_DIR="${TMPDIR:-/tmp}/praxis_cooldown_${session_marker}"

CODEX_PROJECT_DIR="$REPO_ROOT" python3 "$IMPL"
