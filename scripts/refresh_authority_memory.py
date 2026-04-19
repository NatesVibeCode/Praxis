#!/usr/bin/env python3
"""Run the authority-to-memory projection refresher once.

Usage:
  WORKFLOW_DATABASE_URL=<url> python3 scripts/refresh_authority_memory.py

When WORKFLOW_DATABASE_URL is absent, the script resolves the workflow database
through the shared runtime authority resolver.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime._workflow_database import resolve_runtime_database_url
from runtime.authority_memory_projection import refresh_authority_memory_projection


def _configure_workflow_database_env() -> str:
    configured = str(os.environ.get("WORKFLOW_DATABASE_URL") or "").strip()
    if not configured:
        configured = str(resolve_runtime_database_url(repo_root=REPO_ROOT))
        os.environ["WORKFLOW_DATABASE_URL"] = configured
    return configured


async def main() -> None:
    _configure_workflow_database_env()
    result = await refresh_authority_memory_projection()
    print(json.dumps(result.to_json(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
