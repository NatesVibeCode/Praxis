#!/usr/bin/env bash
# install-git-hooks.sh — wire the repo's tracked hooks dir into git.
#
# Idempotent. Run once after a fresh clone (or after rotating clones)
# so the policy-artifact drift gate fires on every commit.
#
# What this does:
#   git config core.hooksPath .githooks
#
# That points git at the in-repo `.githooks/` directory instead of the
# default per-clone `.git/hooks/`. The hooks live with the code, get
# updated when you pull, and apply to every clone of this repo.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [[ ! -d ".githooks" ]]; then
  echo "install-git-hooks: .githooks/ not present in $REPO_ROOT" >&2
  exit 1
fi

current="$(git config --local core.hooksPath || true)"
if [[ "$current" == ".githooks" ]]; then
  echo "git hooks already pointing at .githooks (no change)"
else
  git config --local core.hooksPath .githooks
  echo "git hooks now point at .githooks (was: ${current:-default})"
fi

# Ensure each tracked hook is executable. git doesn't enforce x bit on
# checked-in files across all platforms.
for hook in .githooks/*; do
  [[ -f "$hook" ]] || continue
  chmod +x "$hook"
done
echo "hooks installed: $(ls .githooks | xargs)"
