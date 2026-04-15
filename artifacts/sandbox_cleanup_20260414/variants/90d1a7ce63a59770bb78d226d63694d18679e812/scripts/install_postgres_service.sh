#!/usr/bin/env bash
# Install the repo-local Postgres cluster as a launchd user agent.
# After install, Postgres starts on login and auto-restarts if it crashes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMPLATE="$REPO_ROOT/config/com.praxis.postgres.plist.template"
PLIST_NAME="com.praxis.postgres.plist"
DEST="$HOME/Library/LaunchAgents/$PLIST_NAME"

PGDATA="$REPO_ROOT/Code&DBs/Databases/postgres-dev/data"
LOGFILE="$REPO_ROOT/Code&DBs/Databases/postgres-dev/log/postgres.log"

# Find pg_ctl
PG_CTL="$(command -v pg_ctl 2>/dev/null || echo pg_ctl)"
if [ ! -x "$PG_CTL" ]; then
    echo "ERROR: pg_ctl not found. Install PostgreSQL via Homebrew: brew install postgresql@14"
    exit 1
fi

if [ ! -d "$PGDATA" ]; then
    echo "ERROR: PGDATA directory not found: $PGDATA"
    exit 1
fi

# Ensure log directory exists
mkdir -p "$(dirname "$LOGFILE")"

# Render template
sed \
    -e "s|__PG_CTL__|$PG_CTL|g" \
    -e "s|__PGDATA__|$PGDATA|g" \
    -e "s|__LOGFILE__|$LOGFILE|g" \
    "$TEMPLATE" > "$DEST"

echo "Installed: $DEST"

# Unload if already loaded (ignore errors)
launchctl bootout "gui/$(id -u)/$PLIST_NAME" 2>/dev/null || true

# Load the agent
launchctl bootstrap "gui/$(id -u)" "$DEST"
echo "Loaded: $PLIST_NAME"
echo "Postgres will now start on login and auto-restart if stopped."
echo ""
echo "PGDATA: $PGDATA"
echo "Log:    $LOGFILE"
echo "pg_ctl: $PG_CTL"
