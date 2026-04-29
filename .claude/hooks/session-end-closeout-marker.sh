#!/usr/bin/env bash
# session-end-closeout-marker.sh — SessionEnd hook entry point.
#
# When a Claude Code session ends, write a small JSON marker under the
# user's pending-closeout dir. The next session's SessionStart hook reads
# the dir and surfaces "N closeouts pending" so the operator (or next-session
# agent) reviews via Skills/praxis-conversation-closeout/SKILL.md.
#
# Why a file marker, not a DB write:
#   - The closeout skill is a *reasoning* task (classify decisions, bugs,
#     roadmap items, evidence) — it can't be auto-fired by a hook.
#   - The hook's only durable job is to leave a breadcrumb so the next
#     session knows the previous one ended without explicit closeout.
#   - File markers are reversible (rm), survive Praxis.db downtime, and
#     don't require any CQRS write path. When a proper closeout-candidate
#     authority lands, this hook gets pointed at the gateway op instead.
#
# Markers are cleared by:
#   - bin/praxis-agent praxis_operator_closeout (success path), OR
#   - aging out — the surfacer ignores markers older than 7 days.
#
# Hook payload arrives on stdin as JSON: {session_id, transcript_path, ...}.
# We only need session_id; everything else is best-effort.

set -uo pipefail

REPO_ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
MARKER_DIR="${PRAXIS_PENDING_CLOSEOUT_DIR:-${HOME}/.praxis/pending_closeouts}"

mkdir -p "$MARKER_DIR" 2>/dev/null || exit 0

# Parse session_id from the hook payload. python3 is universally available
# on macOS + Linux dev machines; if it's somehow missing, skip silently.
if ! command -v python3 >/dev/null 2>&1; then
    exit 0
fi

session_id="$(python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    sid = str(data.get('session_id') or '').strip()
    print(sid if sid else '')
except Exception:
    pass
" 2>/dev/null || echo "")"

# Without a session_id we can't deduplicate per-session, so use ppid +
# timestamp as a fallback so markers are unique per termination event.
if [[ -z "$session_id" ]]; then
    session_id="anon_${PPID}_$(date +%s)"
fi

# Sanitize the session_id for use as a filename (no slashes, no colons).
safe_session_id="$(printf '%s' "$session_id" | tr '/:' '__')"
marker_path="${MARKER_DIR}/${safe_session_id}.json"

# Write the marker. We capture: session_id, end_iso, repo_root. Anything
# richer (turn count, tools used) needs PreToolUse-side counting, which
# is a separate piece of work; the marker is intentionally minimal.
end_iso="$(python3 -c "from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())" 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 - "$marker_path" "$session_id" "$end_iso" "$REPO_ROOT" <<'EOF' 2>/dev/null || exit 0
import json, sys
path, session_id, end_iso, repo_root = sys.argv[1:]
payload = {
    "session_id": session_id,
    "end_iso": end_iso,
    "repo_root": repo_root,
    "marker_version": 1,
}
with open(path, "w", encoding="utf-8") as fp:
    json.dump(payload, fp)
EOF

exit 0
