#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LAUNCHER="${REPO_ROOT}/scripts/praxis"

usage() {
  cat <<'EOF'
usage: supervisor.sh {install|uninstall|status|logs|restart}

Legacy compatibility wrapper around ./scripts/praxis for service management.
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

case "${1:-}" in
  -h|--help|help)
    usage
    exit 0
    ;;
  install|uninstall|status|logs|restart)
    ;;
  *)
    echo "error: unknown supervisor subcommand: $1" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ ! -x "${LAUNCHER}" ]]; then
  echo "error: Praxis launcher not found at ${LAUNCHER}" >&2
  exit 1
fi

exec "${LAUNCHER}" "$@"
