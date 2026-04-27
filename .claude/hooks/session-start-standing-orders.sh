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
    regexp_replace(title, E'\\\\s+', ' ', 'g'),
    regexp_replace(rationale, E'\\\\s+', ' ', 'g'),
    regexp_replace(
        COALESCE(
            (SELECT string_agg(value, ' | ')
             FROM jsonb_array_elements_text(scope_clamp -> 'applies_to') AS value),
            ''
        ),
        E'\\\\s+', ' ', 'g'
    ) AS applies_to_text,
    regexp_replace(
        COALESCE(
            (SELECT string_agg(value, ' | ')
             FROM jsonb_array_elements_text(scope_clamp -> 'does_not_apply_to') AS value),
            ''
        ),
        E'\\\\s+', ' ', 'g'
    ) AS does_not_apply_to_text
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
printf 'They bind this session. Quote the scope_clamp verbatim — never paraphrase.\n\n'

while IFS=$'\t' read -r scope slug title rationale applies_to_text does_not_apply_to_text; do
    [[ -z "$title" ]] && continue
    printf -- '- **%s / %s** — %s\n' "$scope" "$slug" "$title"
    printf '  %s\n' "$rationale"
    if [[ "$applies_to_text" == "pending_review" ]]; then
        printf '  ⚠ scope_clamp pending operator review (Moon Decisions panel)\n'
    elif [[ -n "$applies_to_text" ]]; then
        printf '  Applies to: %s\n' "$applies_to_text"
    fi
    if [[ -n "$does_not_apply_to_text" ]]; then
        printf '  Does NOT apply to: %s\n' "$does_not_apply_to_text"
    fi
done <<< "$ROWS"

printf '\nRefresh: `POST /orient` or re-run this hook.\n'

cat <<'EOF'

## Operator Tool Routing — concern → tool

When you encounter an authority symptom, USE THE NAMED TOOL. Do not write SQL or a migration to mutate these concerns. The operator tool surface is the only authoritative writer.

| Symptom | Tool | Action |
|---|---|---|
| `runtime_profile_route.not_admitted` | `praxis_provider_onboard` | onboard |
| `control_panel.transport_turned_off` / `transport_default_deny` | `praxis_access_control` | enable |
| `control_panel.model_access_method_turned_off` | `praxis_access_control` | enable |
| circuit breaker `manual_override_state=OPEN` | `praxis_circuits` | reset |
| `circuit_breaker.runtime_open` | `praxis_circuits` | list (then investigate failures) |
| credentials missing / API key not found | `praxis_provider_onboard` | re-onboard with `api_key_env_var` |
| `task_type_routing` rank-1 doesn't take effect | `praxis_provider_control_plane` | read — check `is_runnable` + `removal_reasons` BEFORE writing migrations |
| `workflow.submit` fails with `postgres.authority_unavailable` | `praxis_provider_control_plane` | read; the failed-submit response also carries `admission_diagnosis.rejection_rows` + `next_actions[].tool` |

If you find yourself drafting a migration for any of these concerns, stop and use the tool instead. Migrations are reserved for schema/structural changes, not operator-facing admission state.

EOF

exit 0
