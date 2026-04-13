#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=native-operator-common.sh
source "$script_dir/native-operator-common.sh"

native_operator_exec bootstrap "$@"
