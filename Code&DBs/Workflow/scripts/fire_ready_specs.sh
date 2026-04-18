#!/usr/bin/env bash
# Fire every *.queue.json spec staged under artifacts/workflow/ready/.
#
# Default: fan out in parallel, one workflow run per spec, write run_id back
# to workflow_spec_ready. Set SEQUENTIAL=1 to run one at a time.
#
# Usage (from anywhere):
#   /Users/nate/Praxis/Code\&DBs/Workflow/scripts/fire_ready_specs.sh
#   SEQUENTIAL=1 /Users/nate/Praxis/Code\&DBs/Workflow/scripts/fire_ready_specs.sh

set -euo pipefail

REPO_ROOT="/Users/nate/Praxis"
READY_DIR="${REPO_ROOT}/Code&DBs/Workflow/artifacts/workflow/ready"
DB="${WORKFLOW_DATABASE_URL:-postgresql://localhost:5432/praxis}"
LOG_DIR="${REPO_ROOT}/Code&DBs/Workflow/artifacts/workflow/ready/.logs"

mkdir -p "$LOG_DIR"
cd "$REPO_ROOT"

shopt -s nullglob
specs=("$READY_DIR"/*.queue.json)
shopt -u nullglob

if [ ${#specs[@]} -eq 0 ]; then
  echo "no ready specs in $READY_DIR"
  exit 0
fi

stage_row() {
  local spec_id="$1" spec_path="$2"
  psql "$DB" -q <<SQL
INSERT INTO workflow_spec_ready (spec_id, spec_path, status)
VALUES ('$spec_id', '$spec_path', 'firing')
ON CONFLICT (spec_id) DO UPDATE SET
  spec_path = EXCLUDED.spec_path,
  status = 'firing',
  last_error = NULL;
SQL
}

mark_fired() {
  local spec_id="$1" run_id="$2"
  psql "$DB" -q <<SQL
UPDATE workflow_spec_ready
SET status='fired', run_id='$run_id', fired_at=now(), last_error=NULL
WHERE spec_id='$spec_id';
SQL
}

mark_failed() {
  local spec_id="$1" err="$2"
  psql "$DB" -q <<SQL
UPDATE workflow_spec_ready
SET status='failed', last_error=\$err\$${err}\$err\$, fired_at=now()
WHERE spec_id='$spec_id';
SQL
}

fire_one() {
  local spec_path="$1"
  local spec_id
  spec_id=$(basename "$spec_path" .queue.json)
  local log_file="$LOG_DIR/$spec_id.log"

  stage_row "$spec_id" "$spec_path"
  echo "[$(date +%H:%M:%S)] firing $spec_id -> $log_file"

  if output=$(praxis workflow run "$spec_path" 2>&1 | tee "$log_file"); then
    local run_id
    run_id=$(printf '%s\n' "$output" | grep -oE 'run[-_:][A-Za-z0-9:_\.-]+' | head -1 || true)
    mark_fired "$spec_id" "${run_id:-unknown}"
    echo "[$(date +%H:%M:%S)] fired $spec_id (run_id=${run_id:-unknown})"
  else
    mark_failed "$spec_id" "see $log_file"
    echo "[$(date +%H:%M:%S)] FAILED $spec_id — see $log_file"
  fi
}

if [ "${SEQUENTIAL:-0}" = "1" ]; then
  for s in "${specs[@]}"; do fire_one "$s"; done
else
  for s in "${specs[@]}"; do fire_one "$s" & done
  wait
fi

echo
echo "=== summary ==="
psql "$DB" -c "SELECT spec_id, status, run_id, fired_at FROM workflow_spec_ready ORDER BY fired_at DESC NULLS LAST LIMIT 20"
