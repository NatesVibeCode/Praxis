#!/usr/bin/env bash

# Shared env bootstrap for workflow frontdoors.
# Loads checked-in .env when present, but never invents a native localhost
# authority. Callers must provide an explicit sandbox-owned database URL.

workflow_detect_script_path() {
  if [ -n "${BASH_SOURCE[0]:-}" ]; then
    printf '%s\n' "${BASH_SOURCE[0]}"
    return 0
  fi
  printf '%s\n' "$0"
}

if [ -f "${PWD}/docker-compose.yml" ] && [ -f "${PWD}/scripts/_workflow_env.sh" ]; then
  workflow_env_repo_root="$(cd "${PWD}" && pwd)"
else
  workflow_env_script_path="$(workflow_detect_script_path)"
  workflow_env_repo_root="$(cd "$(dirname "${workflow_env_script_path}")/.." && pwd)"
fi
workflow_env_dotenv="${workflow_env_repo_root}/.env"

workflow_resolve_docker_database_url() {
  local compose_file="${workflow_env_repo_root}/docker-compose.yml"
  [ -f "${compose_file}" ] || return 1
  command -v docker >/dev/null 2>&1 || return 1

  local postgres_container=""
  postgres_container="$(docker compose -f "${compose_file}" ps -q postgres 2>/dev/null | head -n 1)"
  [ -n "${postgres_container}" ] || return 1

  local container_state=""
  container_state="$(
    docker inspect \
      --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "${postgres_container}" 2>/dev/null || true
  )"
  case "${container_state}" in
    healthy|running) ;;
    *) return 1 ;;
  esac

  local published=""
  published="$(docker compose -f "${compose_file}" port postgres 5432 2>/dev/null | head -n 1)"
  [ -n "${published}" ] || return 1

  local docker_host="${published%:*}"
  local docker_port="${published##*:}"
  docker_host="${docker_host#\[}"
  docker_host="${docker_host%\]}"
  case "${docker_host}" in
    ""|"0.0.0.0"|"::")
      docker_host="127.0.0.1"
      ;;
  esac
  [ -n "${docker_port}" ] || return 1

  printf 'postgresql://postgres@%s:%s/praxis\n' "${docker_host}" "${docker_port}"
}

workflow_load_repo_env() {
  if [ -f "${workflow_env_dotenv}" ]; then
    set -a
    # shellcheck disable=SC1090
    . "${workflow_env_dotenv}"
    set +a
  fi

  if [ -z "${WORKFLOW_DATABASE_URL:-}" ]; then
    WORKFLOW_DATABASE_URL="$(workflow_resolve_docker_database_url || true)"
  fi

  if [ -z "${WORKFLOW_DATABASE_URL:-}" ]; then
    echo "WORKFLOW_DATABASE_URL must be set explicitly by Docker or Cloudflare authority; native localhost fallback is disabled." >&2
    return 1
  fi

  export WORKFLOW_DATABASE_URL
}
