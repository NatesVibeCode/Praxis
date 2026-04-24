#!/bin/bash
# Install and activate the com.praxis.agent-sessions LaunchAgent.
# Idempotent: safe to re-run after plist edits.

set -euo pipefail

REPO="${PRAXIS_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
SRC="$REPO/scripts/com.praxis.agent-sessions.plist"
default_launchd_dir() {
  printf '%s' "$HOME"
  printf '%s' "/Library/LaunchAgents"
}

LAUNCHD_DIR="${PRAXIS_LAUNCHD_DIR:-$(default_launchd_dir)}"
DST="$LAUNCHD_DIR/com.praxis.agent-sessions.plist"
HOST="${PRAXIS_AGENT_SESSIONS_HOST:-127.0.0.1}"
PORT="${PRAXIS_AGENT_SESSIONS_PORT:-8421}"
PYTHON_BIN="${PRAXIS_PYTHON_BIN:-$(command -v python3 || true)}"
LAUNCHD_PATH="${PRAXIS_LAUNCHD_PATH:-/opt/homebrew/bin:/usr/local/bin:$(getconf PATH 2>/dev/null || printf '/usr/bin:/bin:/usr/sbin:/sbin')}"
CLI_PROVIDER="${PRAXIS_AGENT_CLI_PROVIDER:-codex}"
CODEX_SANDBOX="${PRAXIS_AGENT_CODEX_SANDBOX:-workspace-write}"
DATABASE_URL="${WORKFLOW_DATABASE_URL:-}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] install-agent-sessions: $*"; }

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

mkdir -p "$LAUNCHD_DIR" "$REPO/artifacts"
sed \
  -e "s|__PRAXIS_REPO_ROOT__|$(sed_escape "$(xml_escape "$REPO")")|g" \
  -e "s|__PRAXIS_PYTHON_BIN__|$(sed_escape "$(xml_escape "$PYTHON_BIN")")|g" \
  -e "s|__PRAXIS_PATH__|$(sed_escape "$(xml_escape "$LAUNCHD_PATH")")|g" \
  -e "s|__PRAXIS_AGENT_CLI_PROVIDER__|$(sed_escape "$(xml_escape "$CLI_PROVIDER")")|g" \
  -e "s|__PRAXIS_AGENT_CODEX_SANDBOX__|$(sed_escape "$(xml_escape "$CODEX_SANDBOX")")|g" \
  -e "s|__PRAXIS_AGENT_SESSIONS_HOST__|$(sed_escape "$(xml_escape "$HOST")")|g" \
  -e "s|__PRAXIS_AGENT_SESSIONS_PORT__|$(sed_escape "$(xml_escape "$PORT")")|g" \
  -e "s|__WORKFLOW_DATABASE_URL__|$(sed_escape "$(xml_escape "$DATABASE_URL")")|g" \
  "$SRC" > "$DST"
log "plist rendered to $DST"

launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"
CHECK_HOST="$HOST"
case "$CHECK_HOST" in
  0.0.0.0|::|[::]) CHECK_HOST=127.0.0.1 ;;
esac
log "launchd loaded — waiting for $CHECK_HOST:$PORT"

for i in $(seq 1 20); do
  if curl -sf "http://$CHECK_HOST:$PORT/" >/dev/null 2>&1; then
    log "service is answering on $CHECK_HOST:$PORT"
    log "try: scripts/praxis-agent list"
    exit 0
  fi
  sleep 0.5
done

log "ERROR: service did not answer within 10s"
log "check logs: $REPO/artifacts/agent_sessions.err"
tail -20 "$REPO/artifacts/agent_sessions.err" 2>/dev/null || true
exit 1
