#!/usr/bin/env bash
# Fetch active standing orders from Praxis.db and emit them as
# session-start context. Wired in .claude/settings.json under hooks.SessionStart.
#
# Truth lives in operator_decisions (decision_kind='architecture_policy').
# This hook is a thin reader — the directive itself lives in Praxis.db so
# every harness (Claude, Codex, Gemini, Cursor) reads the same rows.

set -uo pipefail

REPO_ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
if [[ -z "${WORKFLOW_DATABASE_URL:-}" && -f "$REPO_ROOT/scripts/_workflow_env.sh" ]]; then
    # shellcheck source=/dev/null
    . "$REPO_ROOT/scripts/_workflow_env.sh"
    workflow_load_repo_env >/dev/null 2>&1 || true
fi

DB_URL="${WORKFLOW_DATABASE_URL:-}"
if [[ -z "$DB_URL" ]]; then
    exit 0
fi

PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "$PSQL_BIN" ]]; then
    PSQL_BIN="$(command -v psql || true)"
fi

if [[ -z "$PSQL_BIN" ]]; then
    exit 0
fi

if ! command -v "$PSQL_BIN" >/dev/null 2>&1; then
    # Silently no-op if psql isn't available; orient must never block session start.
    exit 0
fi

ROWS="$("$PSQL_BIN" "$DB_URL" -tAF $'\t' -c "
SELECT
    decision_scope_ref,
    decision_key,
    title,
    rationale
FROM operator_decisions
WHERE decision_kind = 'architecture_policy'
  AND decision_status IN ('decided', 'active')
  AND effective_from <= now()
  AND (effective_to IS NULL OR effective_to > now())
ORDER BY decided_at DESC
LIMIT 20;
" 2>/dev/null)"

if [[ -z "$ROWS" ]]; then
    exit 0
fi

printf '## Standing Orders (Praxis.db authority)\n\n'
printf 'These are active architecture-policy rows from `operator_decisions`. '
printf 'Every harness orienting via /orient receives the same directives. '
printf 'They bind this session.\n\n'

while IFS=$'\t' read -r scope slug title rationale; do
    [[ -z "$title" ]] && continue
    printf -- '- **%s / %s** — %s\n' "$scope" "$slug" "$title"
    printf '  %s\n' "$rationale"
done <<< "$ROWS"

printf '\nRefresh: `POST /orient` or re-run this hook.\n'

exit 0
