#!/usr/bin/env bash
# Remove the repo-local Postgres launchd user agent.
# Postgres stops and will no longer auto-start on login.
set -euo pipefail

PLIST_NAME="com.praxis.postgres.plist"
DEST="$HOME/Library/LaunchAgents/$PLIST_NAME"

if [ ! -f "$DEST" ]; then
    echo "Not installed: $DEST"
    exit 0
fi

# Unload
launchctl bootout "gui/$(id -u)/$PLIST_NAME" 2>/dev/null || true
rm -f "$DEST"
echo "Uninstalled: $PLIST_NAME"
echo "Postgres will no longer auto-start. Data is untouched."
