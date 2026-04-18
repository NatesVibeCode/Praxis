#!/usr/bin/env python3
"""Run the authority-to-memory projection refresher once.

Usage:
  WORKFLOW_DATABASE_URL=<url> python3 scripts/refresh_authority_memory.py

The connection url must point at the praxis postgres — if you have a local
postgres shadowing port 5432, use `praxis-postgres-1.orb.local:5432` (OrbStack
hostname).
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

os.environ.setdefault(
    "WORKFLOW_DATABASE_URL",
    "postgresql://postgres@praxis-postgres-1.orb.local:5432/praxis",
)

from runtime.authority_memory_projection import refresh_authority_memory_projection


async def main() -> None:
    result = await refresh_authority_memory_projection()
    print(json.dumps(result.to_json(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
