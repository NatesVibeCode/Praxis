"""Backfill: classify all unclassified receipt/job rows using failure_classifier.

Populates failure_category, failure_zone, and is_transient columns.

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

    # 1. Classify all unclassified receipt_search rows
    rows = conn.execute(
        "SELECT id, failure_code, raw_json FROM receipt_search "
        "WHERE failure_code != $1 AND failure_category = $1",
        "",
    )
    print(f"Unclassified receipt_search rows: {len(rows)}")

    updated = 0
    for r in rows:
        fc = r["failure_code"]
        raw = r["raw_json"] or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                raw = {}

        outputs: dict = {}
        if isinstance(raw, dict):
            if "stderr" in raw:
                outputs["stderr"] = raw["stderr"]
            if "exit_code" in raw:
                outputs["exit_code"] = raw["exit_code"]

        classification = classify_failure(fc, outputs=outputs or None)
        category = classification.category.value
        zone = ZONE_MAP.get(category, "")

        conn.execute(
            "UPDATE receipt_search "
            "SET failure_category = $1, failure_zone = $2, is_transient = $3 "
            "WHERE id = $4",
            category, zone, classification.is_transient, r["id"],
        )
        updated += 1

    print(f"Updated receipt_search: {updated}")

    # 2. Backfill receipt_meta from receipt_search (join on label+agent)
    conn.execute(
        "UPDATE receipt_meta rm "
        "SET failure_category = rs.failure_category, "
        "    failure_zone = rs.failure_zone, "
        "    is_transient = rs.is_transient "
        "FROM receipt_search rs "
        "WHERE rm.label = rs.label AND rm.agent = rs.agent "
        "AND rm.status IN ($1, $2) "
        "AND rs.failure_category != $3 AND rm.failure_category = $3",
        "failed", "error", "",
    )
    meta_remaining = conn.execute(
        "SELECT COUNT(*) as cnt FROM receipt_meta "
        "WHERE status IN ($1, $2) AND failure_category = $3",
        "failed", "error", "",
    )
    print(f"receipt_meta unclassified remaining: {meta_remaining[0]['cnt']}")

    # 3. Classify all unclassified workflow_jobs
    job_rows = conn.execute(
        "SELECT id, last_error_code, stdout_preview FROM workflow_jobs "
        "WHERE status IN ($1, $2) AND failure_category = $3 "
        "AND last_error_code IS NOT NULL AND last_error_code != $3",
        "failed", "dead_letter", "",
    )
    print(f"Unclassified workflow_jobs: {len(job_rows)}")

    job_updated = 0
    for j in job_rows:
        classification = classify_failure(
            j["last_error_code"],
            outputs={"stderr": j.get("stdout_preview", "") or ""},
        )
        category = classification.category.value
        zone = ZONE_MAP.get(category, "")
        conn.execute(
            "UPDATE workflow_jobs "
            "SET failure_category = $1, failure_zone = $2, is_transient = $3 "
            "WHERE id = $4",
            category, zone, classification.is_transient, j["id"],
        )
        job_updated += 1

    print(f"Updated workflow_jobs: {job_updated}")

    # 4. Final verification
    final = conn.execute(
        "SELECT failure_category, COUNT(*) as cnt "
        "FROM receipt_search WHERE failure_category != $1 "
        "GROUP BY failure_category ORDER BY cnt DESC",
        "",
    )
    print("\n=== Final receipt_search breakdown ===")
    for r in final:
        print(f"  {r['failure_category']:20s} {r['cnt']}")

    zone_check = conn.execute(
        "SELECT failure_zone, COUNT(*) as cnt "
        "FROM receipt_search "
        "WHERE failure_zone != $1 "
        "GROUP BY failure_zone ORDER BY cnt DESC",
        "",
    )
    print("\n=== Zone breakdown (from column) ===")
    for r in zone_check:
        print(f"  {r['failure_zone']:10s} {r['cnt']}")

    unclassified = conn.execute(
        "SELECT COUNT(*) as cnt FROM receipt_search "
        "WHERE failure_code != $1 AND failure_category = $1",
        "",
    )
    print(f"\nRemaining unclassified: {unclassified[0]['cnt']}")


if __name__ == "__main__":
    main()
