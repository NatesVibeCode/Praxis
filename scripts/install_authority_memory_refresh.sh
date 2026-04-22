#!/usr/bin/env bash
# Install the authority-to-memory projection refresh LaunchAgent.
# Idempotent: safe to re-run after plist edits.
set -euo pipefail

REPO_ROOT="${PRAXIS_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
PLIST_NAME="com.praxis.authority-memory-refresh.plist"
SRC="$REPO_ROOT/scripts/$PLIST_NAME"
default_launchd_dir() {
    printf '%s' "$HOME"
    printf '%s' "/Library/LaunchAgents"
}

LAUNCHD_DIR="${PRAXIS_LAUNCHD_DIR:-$(default_launchd_dir)}"
DST="$LAUNCHD_DIR/$PLIST_NAME"
PYTHON_BIN="${PRAXIS_PYTHON_BIN:-$(command -v python3 || true)}"
DATABASE_URL="${WORKFLOW_DATABASE_URL:-}"

if [[ ! -f "$SRC" ]]; then
    echo "error: $SRC not found" >&2
    exit 1
fi

if [[ -z "$PYTHON_BIN" ]]; then
    echo "error: python3 not found; set PRAXIS_PYTHON_BIN" >&2
    exit 1
fi

if [[ -z "$DATABASE_URL" && -f "$REPO_ROOT/scripts/_workflow_env.sh" ]]; then
    # shellcheck source=/dev/null
    . "$REPO_ROOT/scripts/_workflow_env.sh"
    workflow_load_repo_env >/dev/null 2>&1 || true
    DATABASE_URL="${WORKFLOW_DATABASE_URL:-}"
fi

if [[ -z "$DATABASE_URL" && -f "$REPO_ROOT/.env" ]]; then
    DATABASE_URL="$(
        awk -F= '/^WORKFLOW_DATABASE_URL=/ {sub(/^[^=]*=/, ""); print; exit}' "$REPO_ROOT/.env"
    )"
fi

if [[ -z "$DATABASE_URL" ]]; then
    echo "error: WORKFLOW_DATABASE_URL not resolved; set it or create .env" >&2
    exit 1
fi

xml_escape() {
    printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"
}

sed_escape() {
    printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

mkdir -p "$LAUNCHD_DIR"
sed \
    -e "s|__PRAXIS_REPO_ROOT__|$(sed_escape "$(xml_escape "$REPO_ROOT")")|g" \
    -e "s|__PRAXIS_PYTHON_BIN__|$(sed_escape "$(xml_escape "$PYTHON_BIN")")|g" \
    -e "s|__WORKFLOW_DATABASE_URL__|$(sed_escape "$(xml_escape "$DATABASE_URL")")|g" \
    "$SRC" > "$DST"
launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"

echo "installed: $DST"
echo "status:"
launchctl list | grep com.praxis.authority-memory || true
echo ""
echo "cadence: every 30 minutes (RunAtLoad=true so it fires on load)"
echo "logs: $REPO_ROOT/artifacts/authority_memory_refresh.log"
echo "errs: $REPO_ROOT/artifacts/authority_memory_refresh.err"
