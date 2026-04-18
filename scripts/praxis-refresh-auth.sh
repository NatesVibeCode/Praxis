#!/usr/bin/env bash
# Extract Claude Code OAuth token from macOS Keychain, export to env, and
# restart the worker container so it inherits the token. No plaintext
# credential files are written — the token travels:
#   Keychain → shell env → docker env → ephemeral CLI container env
# Every step is in-memory. `--rm` on ephemeral containers ensures the env
# var dies with the container.
#
# Run this after `claude login` on the host, or whenever auto-routed
# Anthropic jobs start returning 401.

set -euo pipefail

if ! command -v security >/dev/null 2>&1; then
  echo "error: 'security' CLI not available (macOS only)" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "error: 'jq' required — install via 'brew install jq'" >&2
  exit 1
fi

# Extract the OAuth access token from Keychain. The Keychain item is created
# by `claude login` under the generic-password service name
# "Claude Code-credentials" and stores a JSON blob:
#   {"claudeAiOauth":{"accessToken":"sk-ant-oat01-...","refreshToken":...,"expiresAt":...}}
token="$(security find-generic-password -s 'Claude Code-credentials' -w 2>/dev/null \
  | jq -r '.claudeAiOauth.accessToken // empty')"

if [ -z "${token}" ]; then
  echo "error: Claude Code OAuth token not found in Keychain." >&2
  echo "       Run 'claude login' on the host first, then rerun this script." >&2
  exit 1
fi

export CLAUDE_CODE_OAUTH_TOKEN="${token}"

# Also do Codex and Gemini here if/when they migrate to Keychain.
# (Currently .codex/auth.json and .gemini/* are still file-based — the existing
# volume mounts in docker-compose.yml handle those.)

cd "$(dirname "$0")/.."
docker-compose up -d --force-recreate workflow-worker >&2

echo "auth-refreshed=true worker-restarted=true expires-at=$(
  security find-generic-password -s 'Claude Code-credentials' -w 2>/dev/null \
    | jq -r '.claudeAiOauth.expiresAt // 0' \
    | awk '{print strftime("%Y-%m-%d %H:%M:%S UTC", $1/1000)}'
)"
