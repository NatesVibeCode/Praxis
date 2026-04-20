#!/bin/bash
# Sync /Users/nate/Praxis local public branch to origin/public.
# Skip-when-dirty: refuses to reset if working tree has uncommitted changes,
# so in-flight work across sessions/worktrees is never discarded.

set -euo pipefail

REPO=/Users/nate/Praxis
cd "$REPO"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] public-sync: $*"; }

log "starting"

git fetch origin public --quiet

local_sha=$(git rev-parse refs/heads/public 2>/dev/null || echo "none")
remote_sha=$(git rev-parse refs/remotes/origin/public)
log "local=$local_sha origin=$remote_sha"

if [ "$local_sha" = "$remote_sha" ]; then
  log "already in sync"
  exit 0
fi

if ! git diff --quiet HEAD -- \
   || ! git diff --cached --quiet -- \
   || [ -n "$(git ls-files --others --exclude-standard)" ]; then
  log "SKIP: working tree is dirty (in-flight work present)"
  log "hint: commit/stash your changes, then run this script manually to sync"
  exit 0
fi

current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" = "public" ]; then
  log "on public — resetting to origin/public"
  git reset --hard "$remote_sha"
else
  log "HEAD is $current_branch — fast-forwarding public ref only"
  git update-ref refs/heads/public "$remote_sha" "$local_sha"
fi

log "done — public now at $(git rev-parse refs/heads/public)"
