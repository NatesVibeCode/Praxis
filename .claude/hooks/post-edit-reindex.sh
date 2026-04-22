#!/usr/bin/env bash
# Background-debounce the discover reindex after Edit/Write events touch
# Praxis Workflow Python sources. Reads the hook payload from stdin, so files
# outside the watched tree silently no-op.
#
# Wired in .claude/settings.json under hooks.PostToolUse.

set -uo pipefail

PAYLOAD="$(cat -)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" && -f "$REPO_ROOT/scripts/_workflow_env.sh" ]]; then
    # shellcheck source=/dev/null
    . "$REPO_ROOT/scripts/_workflow_env.sh"
    PYTHON_BIN="$(workflow_python_bin 2>/dev/null || true)"
fi
if [[ -z "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3 || true)"
fi
if [[ -z "$PYTHON_BIN" ]]; then
    exit 0
fi

# Only react to Edit / Write / MultiEdit events.
TOOL_NAME="$(printf '%s' "$PAYLOAD" | "$PYTHON_BIN" -c 'import json,sys
try:
    print(json.loads(sys.stdin.read()).get("tool_name",""))
except Exception:
    print("")')"

case "$TOOL_NAME" in
    Edit|Write|MultiEdit) ;;
    *) exit 0 ;;
esac

# Pull file_path off the tool_input. If absent, exit quietly.
FILE_PATH="$(printf '%s' "$PAYLOAD" | "$PYTHON_BIN" -c 'import json,sys
try:
    payload = json.loads(sys.stdin.read())
    print((payload.get("tool_input") or {}).get("file_path",""))
except Exception:
    print("")')"

case "$FILE_PATH" in
    */Code\&DBs/Workflow/*.py) ;;
    *) exit 0 ;;
esac

# Debounce: a stamp file holds the next allowed run time.
STAMP="${TMPDIR:-/tmp}/praxis-discover-reindex.stamp"
NOW="$(date +%s)"
WINDOW=30
if [[ -f "$STAMP" ]]; then
    LAST="$(cat "$STAMP" 2>/dev/null || echo 0)"
    if (( NOW - LAST < WINDOW )); then
        exit 0
    fi
fi
echo "$NOW" > "$STAMP"

# Fire and forget; never block the tool call. Logs land in the stamp dir.
LOG="${TMPDIR:-/tmp}/praxis-discover-reindex.log"
PRAXIS_BIN="${PRAXIS_BIN:-}"
if [[ -z "$PRAXIS_BIN" ]]; then
    PRAXIS_BIN="$(command -v praxis || true)"
fi
if [[ -z "$PRAXIS_BIN" ]]; then
    PRAXIS_BIN="$REPO_ROOT/scripts/praxis"
fi
nohup "$PRAXIS_BIN" workflow discover reindex --yes \
    >>"$LOG" 2>&1 &
disown

exit 0
