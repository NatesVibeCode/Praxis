#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

case "${1:-help}" in
  run-managed)
    shift
    exec "${REPO_ROOT}/scripts/praxis" workflow run "$@"
    ;;
  spawn-managed)
    shift
    exec "${REPO_ROOT}/scripts/praxis" workflow spawn "$@"
    ;;
  dry-run)
    shift
    exec "${REPO_ROOT}/scripts/praxis" workflow run --dry-run "$@"
    ;;
  help|-h|--help)
    exec "${REPO_ROOT}/scripts/praxis" workflow --help
    ;;
  *)
    exec "${REPO_ROOT}/scripts/praxis" workflow "$@"
    ;;
esac
