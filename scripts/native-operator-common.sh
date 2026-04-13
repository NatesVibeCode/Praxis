#!/usr/bin/env bash

# Shared helper for the repo-local native lifecycle wrappers.
# These wrappers stay thin: resolve the checked-in workflow package, extend
# PYTHONPATH explicitly, and exec the native operator surface.

native_operator_common_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
native_operator_repo_root="$(cd "$native_operator_common_dir/.." && pwd)"
native_operator_workflow_root="$native_operator_repo_root/Code&DBs/Workflow"
native_operator_runtime_profiles_config="$native_operator_repo_root/config/runtime_profiles.json"
native_operator_runtime_contract="$native_operator_repo_root/config/PRAXIS_NATIVE_INSTANCE_ENV.contract"

native_operator_contract_value() {
  if [ "$#" -ne 1 ]; then
    echo "usage: native_operator_contract_value <key>" >&2
    return 2
  fi

  # The checked-in contract is authoritative; no shell defaults here.
  local key="$1"
  local value=""

  if [ -f "$native_operator_runtime_contract" ]; then
    value="$(
      awk -F= -v key="$key" '
        /^[[:space:]]*#/ { next }
        index($0, "=") == 0 { next }
        {
          current_key = $1
          sub(/^[[:space:]]+/, "", current_key)
          sub(/[[:space:]]+$/, "", current_key)
          if (current_key != key) {
            next
          }
          sub(/^[^=]*=/, "", $0)
          sub(/^[[:space:]]+/, "", $0)
          sub(/[[:space:]]+$/, "", $0)
          print
          exit
        }
      ' "$native_operator_runtime_contract"
    )"
  fi

  if [ -n "$value" ]; then
    printf '%s\n' "$value"
    return 0
  fi

  echo "native operator wrappers require $key in $native_operator_runtime_contract" >&2
  return 1
}

native_operator_contract_keys() {
  awk -F= '
    /^[[:space:]]*#/ { next }
    index($0, "=") == 0 { next }
    {
      key = $1
      sub(/^[[:space:]]+/, "", key)
      sub(/[[:space:]]+$/, "", key)
      if (key != "") {
        print key
      }
    }
  ' "$native_operator_runtime_contract"
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
  native_operator_require_file \
    "$native_operator_runtime_contract" \
    "checked-in native runtime contract" || return 1

  native_operator_assert_expected_env PRAXIS_RUNTIME_PROFILES_CONFIG "$native_operator_runtime_profiles_config" || return 1
  export PRAXIS_RUNTIME_PROFILES_CONFIG="$native_operator_runtime_profiles_config"

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
