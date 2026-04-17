"""Tiny native ops wrappers for boring local workflow operations.

These wrappers are intentionally thin:

- they delegate to existing repo-local helpers
- they do not own runtime truth
- they give operators a stable repo-local command surface
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Mapping

from runtime.instance import (
    native_instance_contract,
)
from storage.dev_postgres import (
    local_postgres_bootstrap,
    local_postgres_health,
    local_postgres_restart,
)

def _instance_contract(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    return native_instance_contract(env=env)


def _emit(payload: Mapping[str, Any]) -> int:
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


def show_instance_contract(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return the repo-local native instance contract."""

    return _instance_contract(env)


def db_health(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return the repo-local Postgres health snapshot."""

    status = local_postgres_health() if env is None else local_postgres_health(env=env)
    return status.to_json()


def db_bootstrap(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return the repo-local Postgres bootstrap snapshot."""

    status = local_postgres_bootstrap() if env is None else local_postgres_bootstrap(env=env)
    return status.to_json()


def db_restart(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return the repo-local Postgres restart snapshot."""

    status = local_postgres_restart() if env is None else local_postgres_restart(env=env)
    return status.to_json()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repo-local native workflow ops wrappers")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("db-health")
    subparsers.add_parser("db-bootstrap")
    subparsers.add_parser("db-restart")
    subparsers.add_parser("show-instance-contract")

    args = parser.parse_args(argv)
    if args.command == "db-health":
        return _emit(db_health())
    if args.command == "db-bootstrap":
        return _emit(db_bootstrap())
    if args.command == "db-restart":
        return _emit(db_restart())
    if args.command == "show-instance-contract":
        return _emit(show_instance_contract())
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":  # pragma: no cover - manual operator entrypoint
    raise SystemExit(main())
