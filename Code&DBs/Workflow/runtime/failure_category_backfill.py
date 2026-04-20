"""Backfill failure classification fields from canonical receipt data."""

from __future__ import annotations

import json
from typing import Any, Mapping, TextIO

from runtime.failure_classifier import classify_failure


ZONE_MAP = {
    "timeout": "external",
    "rate_limit": "external",
    "provider_error": "external",
    "network_error": "external",
    "infrastructure": "external",
    "credential_error": "config",
    "model_error": "config",
    "input_error": "config",
    "context_overflow": "internal",
    "parse_error": "internal",
    "sandbox_error": "internal",
    "scope_violation": "internal",
    "verification_failed": "internal",
}


def _failure_zone_for_category(category: str) -> str:
    return ZONE_MAP.get(category, "")


def _count_rows(rows: Any) -> int:
    if rows is None:
        return 0
    if isinstance(rows, (list, tuple)):
        return len(rows)
    try:
        return len(rows)
    except TypeError:
        return sum(1 for _ in rows)


def _first_int(rows: Any, *, field: str) -> int:
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, Mapping):
        try:
            return int(row.get(field, 0) or 0)
        except (TypeError, ValueError):
            return 0
    return 0


def backfill_failure_categories(conn: Any) -> dict[str, Any]:
    """Backfill failure category, zone, and transient fields from receipts."""

    receipts = conn.execute(
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

    updated_receipts = 0
    for receipt in receipts:
        failure_code = str(receipt["failure_code"] or "")
        outputs = receipt["outputs"] if isinstance(receipt["outputs"], dict) else {}
        classification = classify_failure(failure_code, outputs=outputs or None)
        category = classification.category.value
        zone = _failure_zone_for_category(category)
        existing_outputs = dict(outputs)
        existing_outputs["failure_classification"] = classification.to_dict()
        existing_outputs["failure_zone"] = zone
        existing_outputs["is_transient"] = classification.is_transient

        updated_receipts += _count_rows(
            conn.execute(
                """
                UPDATE receipts
                SET outputs = $2::jsonb
                WHERE receipt_id = $1
                RETURNING receipt_id
                """,
                receipt["receipt_id"],
                json.dumps(existing_outputs),
            ),
        )

    workflow_jobs_updated = _count_rows(
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
            RETURNING wj.id
            """,
        ),
    )
    jobs_remaining = _first_int(
        conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM workflow_jobs
            WHERE lower(status) IN ('failed', 'error', 'dead_letter')
              AND COALESCE(last_error_code, '') <> ''
              AND COALESCE(failure_category, '') = ''
            """,
        ),
        field="cnt",
    )

    receipt_meta_updated = _count_rows(
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
            RETURNING rm.id
            """,
        ),
    )
    meta_remaining = _first_int(
        conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM receipt_meta
            WHERE lower(COALESCE(status, '')) IN ('failed', 'error')
              AND COALESCE(failure_category, '') = ''
            """,
        ),
        field="cnt",
    )

    final_breakdown_rows = conn.execute(
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
    ) or []
    zone_breakdown_rows = conn.execute(
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
    ) or []
    remaining_unclassified = _first_int(
        conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM receipts
            WHERE lower(status) IN ('failed', 'error')
              AND COALESCE(failure_code, '') <> ''
              AND COALESCE(outputs->'failure_classification'->>'category', '') = ''
            """,
        ),
        field="cnt",
    )

    return {
        "receipts_scanned": len(receipts),
        "receipts_updated": updated_receipts,
        "workflow_jobs_updated": workflow_jobs_updated,
        "workflow_jobs_remaining": jobs_remaining,
        "receipt_meta_updated": receipt_meta_updated,
        "receipt_meta_remaining": meta_remaining,
        "final_breakdown": [
            {
                "failure_category": str(row.get("failure_category") or ""),
                "count": int(row.get("cnt") or 0),
            }
            for row in final_breakdown_rows
            if isinstance(row, Mapping)
        ],
        "zone_breakdown": [
            {
                "failure_zone": str(row.get("failure_zone") or ""),
                "count": int(row.get("cnt") or 0),
            }
            for row in zone_breakdown_rows
            if isinstance(row, Mapping)
        ],
        "remaining_unclassified": remaining_unclassified,
    }


def render_failure_category_backfill_report(payload: Mapping[str, Any], *, stdout: TextIO) -> None:
    """Render a concise operator-facing backfill summary."""

    stdout.write(f"Unclassified receipts rows: {int(payload.get('receipts_scanned') or 0)}\n")
    stdout.write(f"Updated receipts: {int(payload.get('receipts_updated') or 0)}\n")
    stdout.write(f"workflow_jobs updated: {int(payload.get('workflow_jobs_updated') or 0)}\n")
    stdout.write(
        f"workflow_jobs unclassified remaining: {int(payload.get('workflow_jobs_remaining') or 0)}\n"
    )
    stdout.write(f"receipt_meta updated: {int(payload.get('receipt_meta_updated') or 0)}\n")
    stdout.write(
        f"receipt_meta unclassified remaining: {int(payload.get('receipt_meta_remaining') or 0)}\n"
    )

    stdout.write("\n=== Final receipts breakdown ===\n")
    for row in payload.get("final_breakdown") or []:
        if not isinstance(row, Mapping):
            continue
        stdout.write(f"  {str(row.get('failure_category') or ''):20s} {int(row.get('count') or 0)}\n")

    stdout.write("\n=== Zone breakdown (from column) ===\n")
    for row in payload.get("zone_breakdown") or []:
        if not isinstance(row, Mapping):
            continue
        stdout.write(f"  {str(row.get('failure_zone') or ''):10s} {int(row.get('count') or 0)}\n")

    stdout.write(
        f"\nRemaining unclassified: {int(payload.get('remaining_unclassified') or 0)}\n"
    )


__all__ = [
    "ZONE_MAP",
    "backfill_failure_categories",
    "render_failure_category_backfill_report",
]
