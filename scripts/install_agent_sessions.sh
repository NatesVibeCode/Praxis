#!/bin/bash
# Install and activate the com.praxis.agent-sessions LaunchAgent.
# Idempotent: safe to re-run after plist edits.

set -euo pipefail

REPO=/Users/nate/Praxis
SRC="$REPO/scripts/com.praxis.agent-sessions.plist"
DST="$HOME/Library/LaunchAgents/com.praxis.agent-sessions.plist"
PORT=8421

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] install-agent-sessions: $*"; }

if [ ! -f "$SRC" ]; then
  log "ERROR: source plist not found at $SRC"
  exit 1
fi

mkdir -p "$(dirname "$DST")"
cp "$SRC" "$DST"
log "plist copied to $DST"

launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"
log "launchd loaded — waiting for 127.0.0.1:$PORT"

for i in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:$PORT/agents" >/dev/null 2>&1; then
    log "service is answering on 127.0.0.1:$PORT"
    log "try: curl -X POST http://127.0.0.1:$PORT/agents -d '{\"prompt\":\"hello\"}'"
    exit 0
  fi
  sleep 0.5
done

log "ERROR: service did not answer within 10s"
log "check logs: $REPO/artifacts/agent_sessions.err"
tail -20 "$REPO/artifacts/agent_sessions.err" 2>/dev/null || true
exit 1
