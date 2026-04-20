#!/usr/bin/env python3
"""Run one daily heartbeat cycle.

Usage:
  WORKFLOW_DATABASE_URL=<url> python3 scripts/daily_heartbeat.py [--scope SCOPE] [--triggered-by SRC]

Scopes: all (default) | providers | connectors | credentials | mcp
Triggered-by: launchd | cli | mcp | http | test  (default: launchd)

When WORKFLOW_DATABASE_URL is absent, the script resolves the workflow database
through the shared runtime authority resolver.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime._workflow_database import resolve_runtime_database_url
from runtime.daily_heartbeat import run_daily_heartbeat


def _configure_workflow_database_env() -> str:
    configured = str(os.environ.get("WORKFLOW_DATABASE_URL") or "").strip()
    if not configured:
        configured = str(resolve_runtime_database_url(repo_root=REPO_ROOT))
        os.environ["WORKFLOW_DATABASE_URL"] = configured
    return configured


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one daily heartbeat cycle.")
    parser.add_argument(
        "--scope",
        choices=["all", "providers", "connectors", "credentials", "mcp"],
        default="all",
    )
    parser.add_argument(
        "--triggered-by",
        choices=["launchd", "cli", "mcp", "http", "test"],
        default="launchd",
    )
    return parser.parse_args()


async def main() -> int:
    args = _parse_args()
    _configure_workflow_database_env()
    result = await run_daily_heartbeat(
        scope=args.scope,
        triggered_by=args.triggered_by,
    )
    print(json.dumps(result.to_json(), indent=2, default=str))
    return 0 if result.status in ("succeeded", "partial") else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
