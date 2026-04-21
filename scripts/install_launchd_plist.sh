#!/usr/bin/env bash
# Render and install a Praxis launchd plist template from scripts/.

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: scripts/install_launchd_plist.sh com.praxis.NAME.plist" >&2
    exit 2
fi

REPO_ROOT="${PRAXIS_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
PLIST_NAME="$1"
SRC="$REPO_ROOT/scripts/$PLIST_NAME"
DST="$HOME/Library/LaunchAgents/$PLIST_NAME"
PYTHON_BIN="${PRAXIS_PYTHON_BIN:-$(command -v python3 || true)}"
LAUNCHD_PATH="${PRAXIS_LAUNCHD_PATH:-$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin}"
DATABASE_URL="${WORKFLOW_DATABASE_URL:-}"

if [[ "$PLIST_NAME" != com.praxis.*.plist ]]; then
    echo "error: plist must be a com.praxis.*.plist file" >&2
    exit 2
fi

if [[ ! -f "$SRC" ]]; then
    echo "error: source plist not found: $SRC" >&2
    exit 1
fi

if [[ -z "$PYTHON_BIN" ]]; then
    echo "error: python3 not found; set PRAXIS_PYTHON_BIN" >&2
    exit 1
fi

if [[ -z "$DATABASE_URL" && -f "$REPO_ROOT/.env" ]]; then
    DATABASE_URL="$(
        awk -F= '/^WORKFLOW_DATABASE_URL=/ {sub(/^[^=]*=/, ""); print; exit}' "$REPO_ROOT/.env"
    )"
fi
DATABASE_URL="${DATABASE_URL:-postgresql://localhost:5432/praxis}"

xml_escape() {
    printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"
}

sed_escape() {
    printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

mkdir -p "$(dirname "$DST")" "$REPO_ROOT/artifacts"
sed \
    -e "s|__PRAXIS_REPO_ROOT__|$(sed_escape "$(xml_escape "$REPO_ROOT")")|g" \
    -e "s|__PRAXIS_PYTHON_BIN__|$(sed_escape "$(xml_escape "$PYTHON_BIN")")|g" \
    -e "s|__PRAXIS_PATH__|$(sed_escape "$(xml_escape "$LAUNCHD_PATH")")|g" \
    -e "s|__WORKFLOW_DATABASE_URL__|$(sed_escape "$(xml_escape "$DATABASE_URL")")|g" \
    "$SRC" > "$DST"

plutil -lint "$DST" >/dev/null
launchctl unload "$DST" 2>/dev/null || true
launchctl load "$DST"

echo "installed: $DST"
