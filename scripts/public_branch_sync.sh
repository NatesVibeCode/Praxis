#!/bin/bash
# Sync the local public branch to origin/public.
# Single-user repo: auto-commit any dirty state to a snapshot commit before
# fast-forwarding, then push. Never drops work, never blocks on dirty tree.

set -euo pipefail

REPO="${PRAXIS_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] public-sync: $*"; }

log "starting"

git fetch origin public --quiet

local_sha=$(git rev-parse refs/heads/public 2>/dev/null || echo "none")
remote_sha=$(git rev-parse refs/remotes/origin/public)
log "local=$local_sha origin=$remote_sha"

current_branch=$(git rev-parse --abbrev-ref HEAD)

# Auto-commit any dirty state on public so it survives the sync. Worktrees
# checked out on other branches are untouched — only the main checkout's
# public branch is affected here.
if [ "$current_branch" = "public" ]; then
  if ! git diff --quiet HEAD -- \
     || ! git diff --cached --quiet -- \
     || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    log "dirty tree detected — auto-committing snapshot"
    git add -A
    git -c commit.gpgsign=false commit -m "public-sync: autosnap $(ts)" --quiet || log "nothing to commit after add"
    local_sha=$(git rev-parse refs/heads/public)
    log "post-autosnap local=$local_sha"
  fi
fi

if [ "$local_sha" = "$remote_sha" ]; then
  log "already in sync"
  exit 0
fi

# Decide direction: if local is ahead of origin, push; if behind, pull; if
# diverged, rebase local onto origin and push.
ahead=$(git rev-list --count "$remote_sha..$local_sha" 2>/dev/null || echo 0)
behind=$(git rev-list --count "$local_sha..$remote_sha" 2>/dev/null || echo 0)
log "ahead=$ahead behind=$behind"

if [ "$current_branch" = "public" ]; then
  if [ "$behind" = "0" ] && [ "$ahead" != "0" ]; then
    log "local ahead — pushing"
    git push origin public
  elif [ "$ahead" = "0" ] && [ "$behind" != "0" ]; then
    log "local behind — fast-forwarding"
    git merge --ff-only "$remote_sha"
  else
    log "diverged — rebasing local onto origin then pushing"
    git rebase "$remote_sha" || {
      log "ERROR: rebase conflict — aborting; manual intervention required"
      git rebase --abort || true
      exit 1
    }
    git push origin public
  fi
else
  # Not on public: just fast-forward the ref if safely possible.
  if [ "$ahead" = "0" ] && [ "$behind" != "0" ]; then
    log "HEAD is $current_branch — fast-forwarding public ref to origin"
    git update-ref refs/heads/public "$remote_sha" "$local_sha"
  else
    log "HEAD is $current_branch with local-ahead public — leaving alone; run from public checkout to push"
  fi
fi

log "done — public now at $(git rev-parse refs/heads/public)"
