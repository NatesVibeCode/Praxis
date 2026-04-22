#!/usr/bin/env bash
# Install and activate the com.praxis.daily-heartbeat LaunchAgent.
# Idempotent: safe to re-run after plist edits.

set -euo pipefail

REPO="${PRAXIS_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
SRC="$REPO/scripts/com.praxis.daily-heartbeat.plist"
default_launchd_dir() {
  printf '%s' "$HOME"
  printf '%s' "/Library/LaunchAgents"
}

LAUNCHD_DIR="${PRAXIS_LAUNCHD_DIR:-$(default_launchd_dir)}"
DST="$LAUNCHD_DIR/com.praxis.daily-heartbeat.plist"
LOG_DIR="$REPO/artifacts/heartbeat"
PYTHON_BIN="${PRAXIS_PYTHON_BIN:-$(command -v python3 || true)}"
LAUNCHD_PATH="${PRAXIS_LAUNCHD_PATH:-$(getconf PATH 2>/dev/null || printf '/usr/bin:/bin:/usr/sbin:/sbin')}"
DATABASE_URL="${WORKFLOW_DATABASE_URL:-}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] install-daily-heartbeat: $*"; }

if [ ! -f "$SRC" ]; then
  log "ERROR: source plist not found at $SRC"
  exit 1
fi

if [ -z "$PYTHON_BIN" ]; then
  log "ERROR: python3 not found; set PRAXIS_PYTHON_BIN"
  exit 1
fi

if [ -z "$DATABASE_URL" ] && [ -f "$REPO/scripts/_workflow_env.sh" ]; then
  # shellcheck source=/dev/null
  . "$REPO/scripts/_workflow_env.sh"
  workflow_load_repo_env >/dev/null 2>&1 || true
  DATABASE_URL="${WORKFLOW_DATABASE_URL:-}"
fi

if [ -z "$DATABASE_URL" ] && [ -f "$REPO/.env" ]; then
  DATABASE_URL="$(
    awk -F= '/^WORKFLOW_DATABASE_URL=/ {sub(/^[^=]*=/, ""); print; exit}' "$REPO/.env"
  )"
fi

if [ -z "$DATABASE_URL" ]; then
  log "ERROR: WORKFLOW_DATABASE_URL not resolved; set it or create .env"
  exit 1
fi

xml_escape() {
  printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"
}

sed_escape() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

mkdir -p "$LAUNCHD_DIR" "$LOG_DIR"
sed \
  -e "s|__PRAXIS_REPO_ROOT__|$(sed_escape "$(xml_escape "$REPO")")|g" \
  -e "s|__PRAXIS_PYTHON_BIN__|$(sed_escape "$(xml_escape "$PYTHON_BIN")")|g" \
  -e "s|__PRAXIS_PATH__|$(sed_escape "$(xml_escape "$LAUNCHD_PATH")")|g" \
  "$SRC" > "$DST"
log "plist rendered to $DST"

launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"
log "launchd loaded — next fire is the 09:30 daily schedule"

echo "installed: $DST"
echo "logs: $LOG_DIR/daily_heartbeat.log"
echo "errs: $LOG_DIR/daily_heartbeat.err"
