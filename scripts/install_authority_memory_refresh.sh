#!/usr/bin/env bash
# Install the authority-to-memory projection refresh LaunchAgent.
# Idempotent: safe to re-run after plist edits.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_NAME="com.praxis.authority-memory-refresh.plist"
SRC="$REPO_ROOT/scripts/$PLIST_NAME"
DST="$HOME/Library/LaunchAgents/$PLIST_NAME"

if [[ ! -f "$SRC" ]]; then
    echo "error: $SRC not found" >&2
    exit 1
fi

cp "$SRC" "$DST"
launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"

echo "installed: $DST"
echo "status:"
launchctl list | grep com.praxis.authority-memory || true
echo ""
echo "cadence: every 30 minutes (RunAtLoad=true so it fires on load)"
echo "logs: $REPO_ROOT/artifacts/authority_memory_refresh.log"
echo "errs: $REPO_ROOT/artifacts/authority_memory_refresh.err"
