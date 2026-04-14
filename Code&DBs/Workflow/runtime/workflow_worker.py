"""Entry point for the unified workflow worker.

Usage (programmatic/CLI)::

    python -m runtime.workflow_worker
"""

from __future__ import annotations

import logging
import os
import sys

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# __main__ — run as standalone process (launchd / CLI)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    from runtime.workflow.unified import run_worker_loop

    pool = get_workflow_pool()
    conn = SyncPostgresConnection(pool)
    # __file__ = .../Code&DBs/Workflow/runtime/workflow_worker.py
    # repo root = .../Praxis (3 levels up from runtime/)
    _here = os.path.abspath(__file__)
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_here))))

    _log.info("Starting unified workflow worker (pid=%d, repo=%s)", os.getpid(), repo_root)
    run_worker_loop(conn, repo_root, poll_interval=2.0)
