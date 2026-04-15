#!/usr/bin/env bash

# Shared env bootstrap for repo-local workflow frontdoors.
# Loads checked-in .env when present, then falls back to the repo-local
# workflow database contract so cold shells still resolve an authority.

workflow_env_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_env_dotenv="${workflow_env_repo_root}/.env"
workflow_env_contract="${workflow_env_repo_root}/config/PRAXIS_NATIVE_INSTANCE_ENV.contract"

workflow_load_repo_env() {
  if [ -f "${workflow_env_dotenv}" ]; then
    set -a
    # shellcheck disable=SC1090
    . "${workflow_env_dotenv}"
    set +a
  fi

  if [ -z "${WORKFLOW_DATABASE_URL:-}" ] && [ -f "${workflow_env_contract}" ]; then
    WORKFLOW_DATABASE_URL="$(
      awk -F= '
        /^[[:space:]]*#/ { next }
        /^[[:space:]]*WORKFLOW_DATABASE_URL=/ {
          sub(/^[^=]*=/, "", $0)
          sub(/^[[:space:]]+/, "", $0)
          sub(/[[:space:]]+$/, "", $0)
          print
          exit
        }
      ' "${workflow_env_contract}"
    )"
  fi

  if [ -z "${WORKFLOW_DATABASE_URL:-}" ]; then
    WORKFLOW_DATABASE_URL="postgresql://localhost:5432/praxis"
  fi

  export WORKFLOW_DATABASE_URL
}
