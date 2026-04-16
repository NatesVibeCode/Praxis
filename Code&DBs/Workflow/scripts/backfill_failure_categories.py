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

import json

from runtime.failure_classifier import classify_failure
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

ZONE_MAP = {
    "timeout": "external", "rate_limit": "external", "provider_error": "external",
    "network_error": "external", "infrastructure": "external",
    "credential_error": "config", "model_error": "config", "input_error": "config",
    "context_overflow": "internal", "parse_error": "internal", "sandbox_error": "internal",
    "scope_violation": "internal", "verification_failed": "internal",
}


def main() -> None:
    conn = SyncPostgresConnection(get_workflow_pool())

    # 1. Classify all failed/error canonical receipts missing classification.
    rows = conn.execute(
        """
        SELECT receipt_id, failure_code, outputs
        FROM receipts
        WHERE lower(status) IN ('failed', 'error')
          AND COALESCE(failure_code, '') <> ''
          AND (
                jsonb_typeof(outputs->'failure_classification') IS DISTINCT FROM 'object'
                OR outputs->'failure_classification' = '{}'::jsonb
              )
        """,
    )
    print(f"Unclassified receipts rows: {len(rows)}")

    updated = 0
    for r in rows:
        fc = str(r["failure_code"] or "")
        outputs = r["outputs"] if isinstance(r["outputs"], dict) else {}
        classification = classify_failure(fc, outputs=outputs or None)
        category = classification.category.value
        zone = ZONE_MAP.get(category, "")
        existing_outputs = dict(outputs)
        existing_outputs["failure_classification"] = classification.to_dict()
        existing_outputs["failure_zone"] = zone
        existing_outputs["is_transient"] = classification.is_transient

        conn.execute(
            """
            UPDATE receipts
            SET outputs = $2::jsonb
            WHERE receipt_id = $1
            """,
            r["receipt_id"],
            json.dumps(existing_outputs),
        )
        updated += 1

    print(f"Updated receipts: {updated}")

    # 2. Backfill workflow_jobs from canonical receipt projections.
    conn.execute(
        """
        WITH receipt_proj AS (
            SELECT
                receipt_id,
                run_id,
                node_id AS label,
                COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, '') AS agent,
                COALESCE(outputs->'failure_classification'->>'category', '') AS failure_category,
                COALESCE(outputs->>'failure_zone', '') AS failure_zone,
                COALESCE((outputs->>'is_transient')::boolean, false) AS is_transient
            FROM receipts
            WHERE lower(status) IN ('failed', 'error')
        )
        UPDATE workflow_jobs wj
        SET failure_category = rp.failure_category,
            failure_zone = rp.failure_zone,
            is_transient = rp.is_transient
        FROM receipt_proj rp
        WHERE wj.receipt_id = rp.receipt_id
          AND COALESCE(wj.failure_category, '') = ''
          AND COALESCE(rp.failure_category, '') <> ''
        """,
    )
    jobs_remaining = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM workflow_jobs
        WHERE lower(status) IN ('failed', 'error', 'dead_letter')
          AND COALESCE(last_error_code, '') <> ''
          AND COALESCE(failure_category, '') = ''
        """,
    )
    print(f"workflow_jobs unclassified remaining: {jobs_remaining[0]['cnt']}")

    # 3. Backfill receipt_meta from canonical receipt projections (label+agent).
    conn.execute(
        """
        WITH receipt_proj AS (
            SELECT
                node_id AS label,
                COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, '') AS agent,
                COALESCE(outputs->'failure_classification'->>'category', '') AS failure_category,
                COALESCE(outputs->>'failure_zone', '') AS failure_zone,
                COALESCE((outputs->>'is_transient')::boolean, false) AS is_transient
            FROM receipts
            WHERE lower(status) IN ('failed', 'error')
        )
        UPDATE receipt_meta rm
        SET failure_category = rp.failure_category,
            failure_zone = rp.failure_zone,
            is_transient = rp.is_transient
        FROM receipt_proj rp
        WHERE rm.label = rp.label
          AND rm.agent = rp.agent
          AND lower(COALESCE(rm.status, '')) IN ('failed', 'error')
          AND COALESCE(rm.failure_category, '') = ''
          AND COALESCE(rp.failure_category, '') <> ''
        """,
    )
    meta_remaining = conn.execute(
        """
        SELECT COUNT(*) as cnt
        FROM receipt_meta
        WHERE lower(COALESCE(status, '')) IN ('failed', 'error')
          AND COALESCE(failure_category, '') = ''
        """,
    )
    print(f"receipt_meta unclassified remaining: {meta_remaining[0]['cnt']}")

    # 4. Final verification
    final = conn.execute(
        """
        SELECT
            COALESCE(outputs->'failure_classification'->>'category', '') AS failure_category,
            COUNT(*) AS cnt
        FROM receipts
        WHERE lower(status) IN ('failed', 'error')
          AND COALESCE(outputs->'failure_classification'->>'category', '') <> ''
        GROUP BY 1
        ORDER BY cnt DESC
        """,
    )
    print("\n=== Final receipts breakdown ===")
    for r in final:
        print(f"  {r['failure_category']:20s} {r['cnt']}")

    zone_check = conn.execute(
        """
        SELECT
            COALESCE(outputs->>'failure_zone', '') AS failure_zone,
            COUNT(*) AS cnt
        FROM receipts
        WHERE lower(status) IN ('failed', 'error')
          AND COALESCE(outputs->>'failure_zone', '') <> ''
        GROUP BY 1
        ORDER BY cnt DESC
        """,
    )
    print("\n=== Zone breakdown (from column) ===")
    for r in zone_check:
        print(f"  {r['failure_zone']:10s} {r['cnt']}")

    unclassified = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM receipts
        WHERE lower(status) IN ('failed', 'error')
          AND COALESCE(failure_code, '') <> ''
          AND COALESCE(outputs->'failure_classification'->>'category', '') = ''
        """,
    )
    print(f"\nRemaining unclassified: {unclassified[0]['cnt']}")


if __name__ == "__main__":
    main()
