#!/usr/bin/env bash
# refresh-decisions-snapshot.sh — export current operator_decisions
# (architecture_policy rows) to policy/operator-decisions-snapshot.json.
#
# Why this exists
#   Fresh clones have an empty Praxis.db. Agent context files reference
#   operator_decisions, but on a fresh clone there are no rows to reference.
#   The snapshot is the floor — committed in the repo, refreshed mechanically,
#   read by the bootstrap script when the DB is empty so /orient has
#   something to surface from turn one.
#
# Output
#   policy/operator-decisions-snapshot.json — deterministic, byte-stable
#   across runs (rows ordered, no timestamps in payload). The CI gate
#   compares the committed file against a fresh export and fails if they
#   diverge — same shape as the rendered .md gate from packet 1.
#
# Standing-order references
#   architecture-policy::deployment::docker-restart-caches-env  (uses praxis-agent)
#   architecture-policy::auth::via-docker-creds-not-shell       (uses praxis-agent)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTFILE="${REPO_ROOT}/policy/operator-decisions-snapshot.json"
PRAXIS_AGENT="${REPO_ROOT}/bin/praxis-agent"

if [[ ! -x "$PRAXIS_AGENT" ]]; then
  echo "refresh-decisions-snapshot: $PRAXIS_AGENT not executable" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTFILE")"

# Pull all active architecture_policy rows. praxis_operator_decisions
# action=list returns the canonical shape; we re-serialize with sorted keys
# and chronological key ordering so the file is byte-deterministic.
"$PRAXIS_AGENT" praxis_operator_decisions \
  --input-json '{"action":"list","decision_kind":"architecture_policy","active_only":true,"limit":500}' \
  | python3 -c "
import json, sys
raw = sys.stdin.read()
d = json.loads(raw)
rows = d.get('results') or d.get('operator_decisions') or []

# Strip per-export volatile fields. The snapshot is a stable artifact;
# created_at, updated_at, decided_at, effective_from change every export
# in degenerate ways. Keep the row's intrinsic identity and content only.
KEEP = (
    'decision_key', 'decision_kind', 'decision_status',
    'decision_source', 'decision_scope_kind', 'decision_scope_ref',
    'title', 'rationale', 'decided_by', 'scope_clamp',
)

clean = []
for r in rows:
    clean.append({k: r.get(k) for k in KEEP})

clean.sort(key=lambda r: r.get('decision_key') or '')

manifest = {
    '\$schema_version': 1,
    '\$description': 'Bootstrap snapshot of operator_decisions architecture_policy rows. Committed in the repo so a fresh clone can populate operator_decisions before the agent\'s first /orient call. Refreshed by scripts/refresh-decisions-snapshot.sh.',
    'count': len(clean),
    'decisions': clean,
}

print(json.dumps(manifest, indent=2, sort_keys=True))
" > "$OUTFILE"

WROTE="$(python3 -c "
import json
with open('$OUTFILE') as fp: d = json.load(fp)
print(d['count'])
")"

echo "→ wrote $WROTE decision rows to $OUTFILE"
