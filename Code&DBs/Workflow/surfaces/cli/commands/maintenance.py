"""Maintenance CLI command handlers."""

from __future__ import annotations

from typing import TextIO

from runtime.failure_category_backfill import (
    backfill_failure_categories,
    render_failure_category_backfill_report,
)
from surfaces.cli._db import cli_sync_conn
from surfaces.cli.mcp_tools import print_json


def _maintenance_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow maintenance <backfill-failure-categories|help> [--json] [--yes]",
            "",
            "Maintenance authority:",
            "  workflow maintenance backfill-failure-categories [--json] [--yes]",
            "",
            "Backfills workflow_jobs and receipt_meta failure classification fields from canonical receipts.",
        ]
    )


def _maintenance_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_maintenance_help_text() + "\n")
        return 2

    action = args[0].strip().lower().replace("_", "-")
    if action != "backfill-failure-categories":
        stdout.write(f"unknown maintenance action: {args[0]}\n")
        stdout.write(_maintenance_help_text() + "\n")
        return 2

    as_json = False
    confirmed = False
    for token in args[1:]:
        if token == "--json":
            as_json = True
        elif token == "--yes":
            confirmed = True
        else:
            stdout.write(f"unexpected argument: {token}\n")
            return 2

    if not confirmed:
        stdout.write("confirmation required: rerun with --yes\n")
        return 2

    try:
        conn = cli_sync_conn()
        payload = backfill_failure_categories(conn)
    except Exception as exc:
        print_json(stdout, {"error": str(exc)})
        return 1

    if as_json:
        print_json(stdout, payload)
        return 0

    render_failure_category_backfill_report(payload, stdout=stdout)
    return 0


__all__ = ["_maintenance_command"]
