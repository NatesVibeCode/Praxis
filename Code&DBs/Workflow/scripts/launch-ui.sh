#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"

echo "[legacy] launch-ui.sh now delegates to ./scripts/praxis launch"
exec "${ROOT_DIR}/scripts/praxis" launch "$@"
