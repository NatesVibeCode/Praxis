#!/usr/bin/env bash
# Launch the DAG Workflow REST API server with port rollover, reload, and auto-heal.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOW_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

HOST="${PRAXIS_API_HOST:-0.0.0.0}"
BASE_PORT="${PRAXIS_API_PORT:-8420}"
RELOAD_SETTING="${PRAXIS_API_RELOAD:-1}"
RESTART_DELAY_S="${PRAXIS_API_RESTART_DELAY_S:-2}"
PORT_SCAN_LIMIT="${PRAXIS_API_PORT_SCAN_LIMIT:-50}"

export WORKFLOW_DATABASE_URL="${WORKFLOW_DATABASE_URL:?"WORKFLOW_DATABASE_URL must be set"}"
export PATH="${PATH}"

stop_requested=0
child_pid=""
active_port=""

is_truthy() {
  case "$1" in
    1|[Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn]) return 0 ;;
    *) return 1 ;;
  esac
}

find_open_port() {
  "${PYTHON_BIN}" - "${HOST}" "$1" "${PORT_SCAN_LIMIT}" <<'PY'
import socket
import sys

host = sys.argv[1]
start = int(sys.argv[2])
limit = int(sys.argv[3])
bind_host = "0.0.0.0" if host == "0.0.0.0" else socket.gethostbyname(host)

for candidate in range(start, start + limit):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((bind_host, candidate))
        except OSError:
            continue
    print(candidate)
    raise SystemExit(0)

raise SystemExit(
    f"Could not find an open API port after {limit} attempts starting at {start}"
)
PY
}

handle_signal() {
  stop_requested=1
  if [ -n "${child_pid}" ] && kill -0 "${child_pid}" 2>/dev/null; then
    kill -TERM "${child_pid}" 2>/dev/null || true
  fi
}

trap handle_signal INT TERM

while true; do
  port_hint="${active_port:-${BASE_PORT}}"
  active_port="$(find_open_port "${port_hint}")"
  export PRAXIS_API_PORT="${active_port}"

  server_args=(-m surfaces.api.server --host "${HOST}" --port "${active_port}")
  reload_label="disabled"
  if is_truthy "${RELOAD_SETTING}"; then
    server_args+=(--reload --reload-dir "${WORKFLOW_ROOT}")
    reload_label="enabled"
  fi

  echo "Starting DAG Workflow API"
  echo "  Host: ${HOST}"
  echo "  Port: ${active_port}"
  echo "  Reload: ${reload_label}"
  echo "  Docs: http://localhost:${active_port}/docs"
  echo ""

  set +e
  PYTHONPATH="${WORKFLOW_ROOT}" "${PYTHON_BIN}" "${server_args[@]}" &
  child_pid=$!
  wait "${child_pid}"
  exit_code=$?
  set -e
  child_pid=""

  if [ "${stop_requested}" -eq 1 ]; then
    exit 0
  fi

  echo "[api-server] exited unexpectedly with code ${exit_code}; retrying in ${RESTART_DELAY_S}s" >&2
  sleep "${RESTART_DELAY_S}"
done
