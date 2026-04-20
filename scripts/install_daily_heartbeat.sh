#!/usr/bin/env bash
# Install and activate the com.praxis.daily-heartbeat LaunchAgent.
# Idempotent: safe to re-run after plist edits.

set -euo pipefail

REPO="/Users/nate/Praxis"
SRC="$REPO/scripts/com.praxis.daily-heartbeat.plist"
DST="$HOME/Library/LaunchAgents/com.praxis.daily-heartbeat.plist"
LOG_DIR="$REPO/artifacts/heartbeat"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] install-daily-heartbeat: $*"; }

if [ ! -f "$SRC" ]; then
  log "ERROR: source plist not found at $SRC"
  exit 1
fi

mkdir -p "$(dirname "$DST")" "$LOG_DIR"
cp "$SRC" "$DST"
log "plist copied to $DST"

launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"
log "launchd loaded — next fire is the 09:30 daily schedule"

echo "installed: $DST"
echo "logs: $LOG_DIR/daily_heartbeat.log"
echo "errs: $LOG_DIR/daily_heartbeat.err"
