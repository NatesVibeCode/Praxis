#!/usr/bin/env bash
# session-start-standing-orders.sh — minimal session-start signal.
#
# Per /praxis-debate fork, round 3 (R1): the 25KB standing-orders dump that
# previously fired at every SessionStart was anti-pattern. Nobody read it,
# the agent ignored it by turn three. The JIT surfacing layer
# (.claude/hooks/preact-orient-friction.sh) is now the canonical surface —
# it injects the matching standing order at the moment of action, not at
# session boot.
#
# This hook now emits a one-line orient pointer so the agent knows where to
# look without paying the 25KB-per-session token tax. Standing orders are
# always available via:
#   - praxis_orient (MCP tool)
#   - operator_decisions table (filter decision_kind='architecture_policy')
#   - policy/operator-decision-triggers.json (structured projection)
#
# To dump the full set on demand, the operator can run:
#   PYTHONPATH=Code\&DBs/Workflow python3 -m surfaces.policy.list

set -uo pipefail

REPO_ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
TRIGGERS_JSON="$REPO_ROOT/policy/operator-decision-triggers.json"
MARKER_DIR="${PRAXIS_PENDING_CLOSEOUT_DIR:-${HOME}/.praxis/pending_closeouts}"

# Count the registered triggers as a single signal so the agent knows the
# JIT layer is wired and how many decisions back it.
TRIGGER_COUNT=0
if [[ -f "$TRIGGERS_JSON" ]] && command -v python3 >/dev/null 2>&1; then
    TRIGGER_COUNT="$(python3 -c "
import json, sys
try:
    with open('$TRIGGERS_JSON') as fp:
        data = json.load(fp)
    print(len(data.get('triggers') or []))
except Exception:
    print(0)
" 2>/dev/null || echo 0)"
fi

# Pending closeout markers from prior sessions. Surface only the count and
# the oldest age — full marker reading happens on demand via the closeout
# skill. Skip silently if python3 missing or marker dir absent.
PENDING_CLOSEOUT_LINE=""
if [[ -d "$MARKER_DIR" ]] && command -v python3 >/dev/null 2>&1; then
    PENDING_CLOSEOUT_LINE="$(python3 - "$MARKER_DIR" <<'EOF' 2>/dev/null || echo ""
import json, os, sys, time
from datetime import datetime, timezone, timedelta

marker_dir = sys.argv[1]
try:
    entries = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    for name in os.listdir(marker_dir):
        if not name.endswith(".json"):
            continue
        path = os.path.join(marker_dir, name)
        try:
            with open(path) as fp:
                data = json.load(fp)
            iso = data.get("end_iso", "")
            ts = datetime.fromisoformat(iso) if iso else None
        except Exception:
            ts = None
        if ts is None or ts < cutoff:
            # Stale or unparseable — age out silently.
            try:
                if ts is not None and ts < cutoff:
                    os.unlink(path)
            except OSError:
                pass
            continue
        entries.append(ts)
    if not entries:
        print("")
    else:
        entries.sort()
        oldest = entries[0]
        age_min = int((datetime.now(timezone.utc) - oldest).total_seconds() // 60)
        if age_min < 60:
            age_str = f"{age_min}m"
        else:
            age_str = f"{age_min // 60}h{age_min % 60:02d}m"
        print(f"{len(entries)}|{age_str}")
except Exception:
    print("")
EOF
)"
fi

cat <<EOF
## Standing Orders — JIT surfacing active

${TRIGGER_COUNT} operator decisions registered in policy/operator-decision-triggers.json.
The PreToolUse hook (.claude/hooks/preact-orient-friction.sh) surfaces matching
standing orders as additionalContext at the moment of action — at the actual
Bash/Edit/Write call, not at session boot.

When you see "⚠ STANDING ORDER MATCH" in your context, pause and read it.
The decision_key links to operator_decisions in Praxis.db; query via
praxis_orient or praxis_search for the full rationale.

Need the full list now? Operators run \`praxis_orient\` from the catalog.
Need to add a new trigger? See policy/HARNESS_INTEGRATION.md.

EOF

if [[ -n "$PENDING_CLOSEOUT_LINE" ]]; then
    PENDING_COUNT="${PENDING_CLOSEOUT_LINE%%|*}"
    PENDING_AGE="${PENDING_CLOSEOUT_LINE##*|}"
    cat <<EOF
## Pending closeouts — ${PENDING_COUNT} (oldest: ${PENDING_AGE} ago)

Prior session(s) ended without explicit closeout. Markers live at:
  ${MARKER_DIR}

Run the praxis-conversation-closeout skill if any of those sessions produced
real work that should persist (decisions, bugs, roadmap, fixes). Clear the
marker by deleting the file once the closeout is filed.

EOF
fi

exit 0
