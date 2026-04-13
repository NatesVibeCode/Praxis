#!/usr/bin/env python3
"""CLI entry point for the heartbeat runner.

Usage:
    python3 run_heartbeat.py --db path/to/knowledge.db [--interval 300] [--once]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure the Workflow package root is on sys.path so bare imports work.
_WORKFLOW_ROOT = Path(__file__).resolve().parent.parent
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.heartbeat_runner import HeartbeatRunner


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run heartbeat maintenance cycles against a memory engine DB."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the memory engine SQLite database.",
    )
    parser.add_argument(
        "--results",
        required=False,
        default="",
        help="Deprecated compatibility flag. Heartbeat summaries are now DB-backed.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between cycles (default: 300).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cycle and exit.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    runner = HeartbeatRunner(engine_db_path=args.db, results_dir=args.results)

    if args.once:
        runner.run_once()
    else:
        runner.run_loop(interval_seconds=args.interval)


if __name__ == "__main__":
    main()
