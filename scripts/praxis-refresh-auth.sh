#!/usr/bin/env bash
# Deprecated compatibility wrapper.
#
# The canonical resolver is scripts/praxis-up, which:
#   1. exports host-resolved auth (including Claude's macOS Keychain token)
#   2. recreates the long-lived control containers
#   3. re-runs praxis_cli_auth_doctor in the worker and fails closed if auth
#      is still unhealthy
#
# Keep this wrapper so older operator notes and agent memories still land on
# the one current path instead of preserving parallel auth-fix rituals.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "scripts/praxis-refresh-auth.sh is deprecated; delegating to scripts/praxis-up" >&2
exec "${REPO_ROOT}/scripts/praxis-up" api-server workflow-worker scheduler
