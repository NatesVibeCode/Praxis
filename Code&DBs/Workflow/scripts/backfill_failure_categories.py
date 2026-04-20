"""Backfill: classify unclassified receipt/job rows from canonical receipts.

Legacy receipt_search is no longer authority; this script now reads from
`receipts` and writes:
- `receipts.outputs.failure_classification` (jsonb)
- `workflow_jobs.failure_category/failure_zone/is_transient`
- `receipt_meta.failure_category/failure_zone/is_transient`

Run with:
    WORKFLOW_DATABASE_URL="$WORKFLOW_DATABASE_URL" \
    PYTHONPATH='Code&DBs/Workflow' python3 Code&DBs/Workflow/scripts/backfill_failure_categories.py
"""
from __future__ import annotations

import sys

from runtime.failure_category_backfill import (
    backfill_failure_categories,
    render_failure_category_backfill_report,
)
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool


def main() -> None:
    conn = SyncPostgresConnection(get_workflow_pool())
    payload = backfill_failure_categories(conn)
    render_failure_category_backfill_report(payload, stdout=sys.stdout)


if __name__ == "__main__":
    main()
