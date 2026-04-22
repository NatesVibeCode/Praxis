#!/usr/bin/env bash

# Shared env bootstrap for workflow shell frontdoors.
# The Python workflow resolver owns database authority. This shell helper only
# asks Python for the canonical answer and exports it for callers like test.sh.

workflow_detect_script_path() {
  if [ -n "${BASH_SOURCE[0]:-}" ]; then
    printf '%s\n' "${BASH_SOURCE[0]}"
    return 0
  fi
  printf '%s\n' "$0"
}

if [ -n "${WORKFLOW_ENV_REPO_ROOT:-}" ]; then
  workflow_env_repo_root="$(cd "${WORKFLOW_ENV_REPO_ROOT}" && pwd)"
elif [ -n "${PRAXIS_LAUNCHER_RESOLVED_REPO_ROOT:-}" ]; then
  workflow_env_repo_root="$(cd "${PRAXIS_LAUNCHER_RESOLVED_REPO_ROOT}" && pwd)"
elif [ -f "${PWD}/docker-compose.yml" ] && [ -f "${PWD}/scripts/_workflow_env.sh" ]; then
  workflow_env_repo_root="$(cd "${PWD}" && pwd)"
else
  workflow_env_script_path="$(workflow_detect_script_path)"
  workflow_env_repo_root="$(cd "$(dirname "${workflow_env_script_path}")/.." && pwd)"
fi

workflow_python_bin() {
  if [ -n "${PYTHON_BIN:-}" ] && command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    printf '%s\n' "${PYTHON_BIN}"
    return 0
  fi
  command -v python3.14 >/dev/null 2>&1 && { command -v python3.14; return 0; }
  command -v python3.13 >/dev/null 2>&1 && { command -v python3.13; return 0; }
  command -v python3 >/dev/null 2>&1 && { command -v python3; return 0; }
  return 1
}

workflow_repo_workflow_root() {
  local python_bin
  python_bin="$(workflow_python_bin)" || {
    echo "python3 is required to resolve workflow root authority." >&2
    return 1
  }

  WORKFLOW_ENV_REPO_ROOT="${workflow_env_repo_root}" "${python_bin}" - <<'PY'
import json
import os
from pathlib import Path

repo_root = Path(os.environ["WORKFLOW_ENV_REPO_ROOT"])
layout = json.loads((repo_root / "config" / "workspace_layout.json").read_text(encoding="utf-8"))
code_tree = str(layout["code_tree"]["canonical"])
workflow_subdir = str(layout["subdirs"]["workflow"])
print(repo_root / code_tree / workflow_subdir)
PY
}

workflow_resolve_database_env_json() {
  local python_bin
  python_bin="$(workflow_python_bin)" || {
    echo "python3 is required to resolve workflow database authority." >&2
    return 1
  }

  local workflow_root
  workflow_root="$(workflow_repo_workflow_root)" || return 1
  local pythonpath="${workflow_root}"
  if [ -n "${PYTHONPATH:-}" ]; then
    pythonpath="${pythonpath}:${PYTHONPATH}"
  fi

  REPO_ROOT="${workflow_env_repo_root}" PYTHONPATH="${pythonpath}" "${python_bin}" - <<'PY'
import json
import os
from pathlib import Path

from surfaces._workflow_database import workflow_database_authority_for_repo

repo_root = Path(os.environ["REPO_ROOT"])
authority = workflow_database_authority_for_repo(repo_root, env=os.environ)
print(
    json.dumps(
        {
            "database_url": authority.database_url,
            "authority_source": authority.source,
        }
    )
)
PY
}

workflow_load_repo_env() {
  local resolved
  resolved="$(workflow_resolve_database_env_json)" || return 1

  if [ -z "${resolved}" ]; then
    echo "workflow database authority resolution returned no payload." >&2
    return 1
  fi

  local python_bin
  python_bin="$(workflow_python_bin)" || {
    echo "python3 is required to parse workflow authority output." >&2
    return 1
  }

  local parsed
  parsed="$(
    WORKFLOW_AUTHORITY_JSON="${resolved}" "${python_bin}" - <<'PY'
import json
import os
import sys

payload = json.loads(os.environ["WORKFLOW_AUTHORITY_JSON"])
database_url = str(payload.get("database_url") or "").strip()
authority_source = str(payload.get("authority_source") or "").strip()
if not database_url:
    sys.stderr.write("canonical workflow resolver returned an empty WORKFLOW_DATABASE_URL\n")
    raise SystemExit(1)
print(database_url)
print(authority_source)
PY
  )" || return 1

  WORKFLOW_DATABASE_URL="$(printf '%s\n' "${parsed}" | sed -n '1p')"
  WORKFLOW_DATABASE_AUTHORITY_SOURCE="$(printf '%s\n' "${parsed}" | sed -n '2p')"

  export WORKFLOW_DATABASE_URL
  export WORKFLOW_DATABASE_AUTHORITY_SOURCE

  if [ -z "${PRAXIS_WORKSPACE_BASE_PATH:-}" ] && [ -f "${workflow_env_repo_root}/.env" ]; then
    PRAXIS_WORKSPACE_BASE_PATH="$(
      awk -F= '/^PRAXIS_WORKSPACE_BASE_PATH=/ {sub(/^[^=]*=/, ""); print; exit}' \
        "${workflow_env_repo_root}/.env"
    )"
    if [ -n "${PRAXIS_WORKSPACE_BASE_PATH}" ]; then
      export PRAXIS_WORKSPACE_BASE_PATH
    fi
  fi
}
