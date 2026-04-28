#!/usr/bin/env python3
"""Run one daily heartbeat cycle through the catalog-backed heartbeat surface.

Usage:
  WORKFLOW_DATABASE_URL=<url> python3 scripts/daily_heartbeat.py [--scope SCOPE] [--triggered-by SRC]

Scopes: all (default) | providers | connectors | credentials | mcp | model_retirement
Triggered-by: launchd | cli | mcp | http | test  (default: launchd)

This wrapper stays intentionally thin so launchd/manual entrypoints reuse the
same operation-catalog path as MCP and the CLI alias.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
sys.path.insert(0, str(WORKFLOW_ROOT))

from surfaces.cli.mcp_tools import run_cli_tool


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one daily heartbeat cycle.")
    parser.add_argument(
        "--scope",
        choices=["all", "providers", "connectors", "credentials", "mcp", "model_retirement"],
        default="all",
    )
    parser.add_argument(
        "--triggered-by",
        choices=["launchd", "cli", "mcp", "http", "test"],
        default="launchd",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    exit_code, payload = run_cli_tool(
        "praxis_daily_heartbeat",
        {
            "scope": args.scope,
            "triggered_by": args.triggered_by,
        },
    )
    print(json.dumps(payload, indent=2, default=str))
    if exit_code != 0:
        return exit_code
    return 0 if payload.get("status") in ("succeeded", "partial") else 1


if __name__ == "__main__":
    sys.exit(main())
