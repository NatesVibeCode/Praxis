#!/usr/bin/env bash
# render-policy-artifacts.sh — regenerate committed policy artifacts from
# sources of truth. The orchestrator. Each underlying renderer is a thin
# wrapper that knows one transform (registry → cursor rules, DB → snapshot,
# etc.) and writes its output deterministically.
#
# Sources of truth (hand-authored):
#   - operator_decisions table (Praxis.db)              ← the floor
#   - policy/operator-decision-triggers.json            ← structured projection
#   - .claude/CLAUDE.md, AGENTS.md, GEMINI.md           ← per-harness orient
#
# Generated outputs (CI-gated when feasible):
#   - policy/operator-decisions-snapshot.json           ← refresh-decisions-snapshot.sh (needs DB)
#   - .cursor/rules/*.mdc                               ← render-cursor-rules.sh (registry only)
#
# Flags:
#   --skip-snapshot        Skip DB-dependent renderers. CI uses this so
#                          the gate runs without WORKFLOW_DATABASE_URL.
#   --check                Render to a tmp dir and diff against committed.
#                          Exit non-zero on drift. Used by the CI gate +
#                          pre-commit hook.
#
# When run without flags: renders in-place. Operator runs this before
# committing changes to the registry / when new operator_decisions land.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SKIP_SNAPSHOT=0
CHECK_MODE=0
for arg in "$@"; do
  case "$arg" in
    --skip-snapshot) SKIP_SNAPSHOT=1 ;;
    --check)         CHECK_MODE=1 ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "render-policy-artifacts: unknown flag: $arg" >&2
      exit 2
      ;;
  esac
done

step() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
ok()   { printf '  \033[1;32m[ok]\033[0m %s\n' "$*"; }
warn() { printf '  \033[1;33m[warn]\033[0m %s\n' "$*"; }
fail() { printf '  \033[1;31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

run_renderer() {
  # $1 — path to renderer script
  # $2 — short name for logs
  local script="$1" name="$2"
  if [[ ! -x "$script" ]]; then
    fail "renderer not executable: $script"
  fi
  if "$script"; then
    ok "$name"
  else
    fail "$name failed"
  fi
}

# Cursor rules — pure transformation from registry. Always safe to run,
# no DB dependency. This is the load-bearing CI-gated artifact.
render_cursor_rules() {
  step "Cursor rules"
  run_renderer "$REPO_ROOT/scripts/render-cursor-rules.sh" "render cursor rules"
}

# Decisions snapshot — needs Praxis-agent / DB. Skip in CI; the operator
# re-runs locally when operator_decisions changes. Drift detection on
# the snapshot is a different gate (operator promise: re-run before commit).
render_decisions_snapshot() {
  if [[ "$SKIP_SNAPSHOT" -eq 1 ]]; then
    warn "decisions snapshot — skipped (--skip-snapshot)"
    return 0
  fi
  step "Decisions snapshot"
  if [[ ! -x "$REPO_ROOT/bin/praxis-agent" ]]; then
    warn "decisions snapshot — bin/praxis-agent not executable; skipping"
    return 0
  fi
  if ! docker compose ps --services --filter status=running 2>/dev/null | grep -q '^api-server$'; then
    warn "decisions snapshot — api-server service not running; skipping"
    return 0
  fi
  run_renderer "$REPO_ROOT/scripts/refresh-decisions-snapshot.sh" "refresh decisions snapshot"
}

if [[ "$CHECK_MODE" -eq 1 ]]; then
  # Drift detection: snapshot the tracked render outputs, re-render in
  # place, compare. Restore the snapshot on exit so a failed --check
  # never leaves the working tree dirty.
  step "Check mode — diffing fresh render against committed"

  scratch="$(mktemp -d -t praxis-render-check.XXXXXX)"
  trap 'rm -rf "$scratch"' EXIT

  # Capture committed state of generated paths.
  rsync -a --delete "$REPO_ROOT/.cursor/rules/" "$scratch/cursor-rules.committed/" 2>/dev/null || true

  # Re-render in place.
  render_cursor_rules

  # Diff fresh against committed snapshot.
  if diff -ruN "$scratch/cursor-rules.committed" "$REPO_ROOT/.cursor/rules" > "$scratch/cursor-rules.diff"; then
    ok "Cursor rules — clean (committed matches fresh render)"
    diff_status=0
  else
    diff_status=1
    fail_msg=$(cat <<EOF
Cursor rules drift detected.

Re-run:
  scripts/render-policy-artifacts.sh

then commit the result. Showing first 80 lines of diff:
EOF
)
    echo "$fail_msg" >&2
    head -n 80 "$scratch/cursor-rules.diff" >&2
  fi

  # Restore committed state regardless of result so --check is read-only.
  rsync -a --delete "$scratch/cursor-rules.committed/" "$REPO_ROOT/.cursor/rules/" 2>/dev/null || true

  if [[ "$diff_status" -ne 0 ]]; then
    exit 1
  fi
  ok "All checks clean"
  exit 0
fi

# Default: render-in-place.
render_cursor_rules
render_decisions_snapshot

step "Done"
ok "policy artifacts re-rendered"
echo
echo "Next: review with 'git status' and 'git diff', then commit."
