#!/usr/bin/env bash

# Shared helper for the repo-local native lifecycle wrappers.
# These wrappers stay thin: resolve the checked-in workflow package, extend
# PYTHONPATH explicitly, and exec the native operator surface.

native_operator_common_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
native_operator_repo_root="$(cd "$native_operator_common_dir/.." && pwd)"

# shellcheck source=_workflow_env.sh
source "$native_operator_repo_root/scripts/_workflow_env.sh"

native_operator_workflow_root="$native_operator_repo_root/CodeDBs/Workflow"
native_operator_runtime_profiles_config="$native_operator_repo_root/config/runtime_profiles.json"
native_operator_runtime_profile_ref="praxis"
native_operator_instance_name="praxis"
native_operator_workflow_database_url=""
native_operator_local_postgres_data_dir="$native_operator_repo_root/CodeDBs/Databases/postgres-dev/data"
native_operator_receipts_dir="$native_operator_repo_root/artifacts/runtime_receipts"
native_operator_topology_dir="$native_operator_repo_root/artifacts/runtime_topology"

native_operator_contract_value() {
  if [ "$#" -ne 1 ]; then
    echo "usage: native_operator_contract_value <key>" >&2
    return 2
  fi

  local key="$1"
  case "$key" in
    WORKFLOW_DATABASE_URL)
      printf '%s\n' "$native_operator_workflow_database_url"
      ;;
    PRAXIS_LOCAL_POSTGRES_DATA_DIR)
      printf '%s\n' "$native_operator_local_postgres_data_dir"
      ;;
    PRAXIS_RUNTIME_PROFILE)
      printf '%s\n' "$native_operator_runtime_profile_ref"
      ;;
    PRAXIS_INSTANCE_NAME)
      printf '%s\n' "$native_operator_instance_name"
      ;;
    PRAXIS_RECEIPTS_DIR)
      printf '%s\n' "$native_operator_receipts_dir"
      ;;
    PRAXIS_TOPOLOGY_DIR)
      printf '%s\n' "$native_operator_topology_dir"
      ;;
    *)
      echo "native operator wrappers do not define contract key $key" >&2
      return 1
      ;;
  esac
}

native_operator_contract_keys() {
  cat <<'EOF'
WORKFLOW_DATABASE_URL
PRAXIS_LOCAL_POSTGRES_DATA_DIR
PRAXIS_RUNTIME_PROFILE
PRAXIS_INSTANCE_NAME
PRAXIS_RECEIPTS_DIR
PRAXIS_TOPOLOGY_DIR
EOF
}

native_operator_require_file() {
  if [ "$#" -ne 2 ]; then
    echo "usage: native_operator_require_file <path> <label>" >&2
    return 2
  fi

  local path="$1"
  local label="$2"
  if [ ! -f "$path" ]; then
    echo "native operator wrappers require $label at $path" >&2
    return 1
  fi
}

native_operator_assert_expected_env() {
  if [ "$#" -ne 2 ]; then
    echo "usage: native_operator_assert_expected_env <env_name> <expected_value>" >&2
    return 2
  fi

  local env_name="$1"
  local expected_value="$2"
  local actual_is_set=0
  if [ "${!env_name+x}" = x ]; then
    actual_is_set=1
  fi
  local actual_value="${!env_name-}"
  if [ "$actual_is_set" -eq 1 ] && [ "$actual_value" != "$expected_value" ]; then
    echo "native operator wrappers reject ambient $env_name override: expected '$expected_value' got '$actual_value'" >&2
    return 1
  fi
}

native_operator_derive_env() {
  native_operator_require_file \
    "$native_operator_runtime_profiles_config" \
    "checked-in runtime profiles config" || return 1

  native_operator_assert_expected_env PRAXIS_RUNTIME_PROFILES_CONFIG "$native_operator_runtime_profiles_config" || return 1
  export PRAXIS_RUNTIME_PROFILES_CONFIG="$native_operator_runtime_profiles_config"

  workflow_load_repo_env || return 1
  native_operator_workflow_database_url="$WORKFLOW_DATABASE_URL"

  local contract_key
  local contract_value
  while IFS= read -r contract_key; do
    [ -n "$contract_key" ] || continue
    contract_value="$(native_operator_contract_value "$contract_key")" || return 1
    native_operator_assert_expected_env "$contract_key" "$contract_value" || return 1
    export "$contract_key=$contract_value"
  done < <(native_operator_contract_keys)
}

native_operator_python_is_supported() {
  "$1" -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 14) else 1)' \
    >/dev/null 2>&1
}

native_operator_python() {
  # Native wrappers are pinned to the machine-owned 3.14 interpreter.
  local candidate="python3.14"
  if command -v "$candidate" >/dev/null 2>&1 && native_operator_python_is_supported "$candidate"; then
    printf '%s\n' "$candidate"
    return 0
  fi

  echo "native operator wrappers require Python 3.14 on PATH" >&2
  return 127
}

native_operator_exec() {
  if [ "$#" -lt 1 ]; then
    echo "usage: native_operator_exec <command> [args...]" >&2
    return 2
  fi

  local command="$1"
  shift
  local python_bin
  python_bin="$(native_operator_python)"
  native_operator_derive_env || return 1

  PYTHONPATH="$native_operator_workflow_root${PYTHONPATH:+:$PYTHONPATH}" \
    exec "$python_bin" \
      -c 'from surfaces.cli.native_operator import main; import sys; raise SystemExit(main(sys.argv[1:]))' \
      "$command" "$@"
}
