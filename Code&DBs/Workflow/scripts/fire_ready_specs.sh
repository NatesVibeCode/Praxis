#!/usr/bin/env bash
# Fire every due *.queue.json spec staged under artifacts/workflow/ready/.
#
# The launcher keeps the directory as the discovery surface, but `workflow_spec_ready`
# is the authority for when a spec is eligible to fire. Rows stay staged until their
# `scheduled_at` is NULL or in the past, then the launcher fans them out through
# `praxis workflow run` and records the run lifecycle.
#
# Usage (from anywhere):
#   Code&DBs/Workflow/scripts/fire_ready_specs.sh
#   SEQUENTIAL=1 Code&DBs/Workflow/scripts/fire_ready_specs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck source=../../../scripts/_workflow_env.sh
source "$REPO_ROOT/scripts/_workflow_env.sh"
workflow_load_repo_env

READY_DIR="${REPO_ROOT}/Code&DBs/Workflow/artifacts/workflow/ready"
DB="${WORKFLOW_DATABASE_URL:?workflow database authority resolver returned no URL}"
LOG_DIR="${REPO_ROOT}/Code&DBs/Workflow/artifacts/workflow/ready/.logs"

mkdir -p "$LOG_DIR"
cd "$REPO_ROOT"

shopt -s nullglob
specs=("$READY_DIR"/*.queue.json)
shopt -u nullglob

ensure_stage_row() {
  local spec_id="$1" spec_path="$2"
  psql "$DB" -q -v spec_id="$spec_id" -v spec_path="$spec_path" <<'SQL'
INSERT INTO workflow_spec_ready (spec_id, spec_path, status)
VALUES (:'spec_id', :'spec_path', 'staged')
ON CONFLICT (spec_id) DO UPDATE SET
  spec_path = EXCLUDED.spec_path;
SQL
}

mark_fired() {
  local spec_id="$1" run_id="$2"
  psql "$DB" -q -v spec_id="$spec_id" -v run_id="$run_id" <<'SQL'
UPDATE workflow_spec_ready
SET status = 'fired', run_id = :'run_id', fired_at = now(), last_error = NULL
WHERE spec_id = :'spec_id';
SQL
}

mark_failed() {
  local spec_id="$1" err="$2"
  psql "$DB" -q -v spec_id="$spec_id" -v err="$err" <<'SQL'
UPDATE workflow_spec_ready
SET status = 'failed', last_error = :'err', fired_at = now()
WHERE spec_id = :'spec_id';
SQL
}

fire_one() {
  local spec_id="$1" spec_path="$2"
  local log_file="$LOG_DIR/$spec_id.log"

  echo "[$(date +%H:%M:%S)] firing $spec_id -> $log_file"

  # Use --json so we can accurately extract the run_id without scraping prose.
  if output=$(praxis workflow run "$spec_path" --json 2>&1 | tee "$log_file"); then
    local run_id
    run_id=$(printf '%s\n' "$output" | jq -r '.run_id // empty' || true)
    if [ -z "$run_id" ]; then
        # Fallback to older grep if jq fails or run_id missing in JSON
        run_id=$(printf '%s\n' "$output" | grep -oE 'run[-_:][A-Za-z0-9:_\.-]+' | head -1 || true)
    fi
    mark_fired "$spec_id" "${run_id:-unknown}"
    echo "[$(date +%H:%M:%S)] fired $spec_id (run_id=${run_id:-unknown})"
  else
    mark_failed "$spec_id" "see $log_file"
    echo "[$(date +%H:%M:%S)] FAILED $spec_id — see $log_file"
  fi
}

for spec_path in "${specs[@]}"; do
  spec_id=$(basename "$spec_path" .queue.json)
  ensure_stage_row "$spec_id" "$spec_path"
done

load_due_specs() {
  psql "$DB" -At -F $'\t' <<'SQL'
SELECT spec_id, spec_path
FROM workflow_spec_ready
WHERE status = 'staged'
  AND (scheduled_at IS NULL OR scheduled_at <= now())
ORDER BY scheduled_at NULLS FIRST, created_at ASC;
SQL
}

mapfile -t due_specs < <(load_due_specs)

if [ ${#due_specs[@]} -eq 0 ]; then
  if [ ${#specs[@]} -eq 0 ]; then
    echo "no ready specs in $READY_DIR"
  else
    echo "no staged specs are due yet"
  fi
  exit 0
fi

if [ "${SEQUENTIAL:-0}" = "1" ]; then
  for row in "${due_specs[@]}"; do
    IFS=$'\t' read -r spec_id spec_path <<< "$row"
    fire_one "$spec_id" "$spec_path"
  done
else
  for row in "${due_specs[@]}"; do
    IFS=$'\t' read -r spec_id spec_path <<< "$row"
    fire_one "$spec_id" "$spec_path" &
  done
  wait
fi

echo
echo "=== summary ==="
psql "$DB" -c "SELECT spec_id, status, scheduled_at, run_id, fired_at FROM workflow_spec_ready ORDER BY fired_at DESC NULLS LAST, created_at DESC LIMIT 20"
