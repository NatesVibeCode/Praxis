"""Entry point for the unified workflow worker.

Usage (programmatic/CLI)::

    python -m runtime.workflow_worker
"""

from __future__ import annotations

import logging
import os
import sys

from runtime.dependency_contract import require_runtime_dependencies

_log = logging.getLogger(__name__)


def _repo_root_from_runtime_file(file_path: str) -> str:
    """Resolve the Praxis workspace root from this runtime entrypoint."""

    here = os.path.abspath(file_path)
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(here))))


def _build_worker_connection():
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    pool = get_workflow_pool()
    return SyncPostgresConnection(pool)


def _run_worker_loop(conn, repo_root: str, *, poll_interval: float = 2.0) -> None:
    from runtime.workflow.unified import run_worker_loop

    run_worker_loop(conn, repo_root, poll_interval=poll_interval)


def start_worker(*, poll_interval: float = 2.0, file_path: str | None = None) -> None:
    """Start the unified workflow worker with an explicit dependency contract."""

    report = require_runtime_dependencies(scope="workflow_worker")
    conn = _build_worker_connection()
    repo_root = _repo_root_from_runtime_file(file_path or __file__)
    _log.info(
        "Starting unified workflow worker (pid=%d, repo=%s, manifest=%s)",
        os.getpid(),
        repo_root,
        report["manifest_path"],
    )
    _run_worker_loop(conn, repo_root, poll_interval=poll_interval)


def main(argv: list[str] | None = None) -> int:
    del argv
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    start_worker()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
