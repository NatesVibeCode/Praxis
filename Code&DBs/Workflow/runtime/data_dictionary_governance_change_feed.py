"""Change-feed driven incremental governance.

A trigger on the three authority tables appends to
`data_dictionary_governance_change_ledger` on every mutation, keyed by
`affected_object_kind`. This runtime drains unprocessed rows, runs a
focused governance check on each distinct affected object, and files
bugs when new violations surface — all in-between scheduled scans.

Drain semantics:

* Claim the oldest N unprocessed rows in one SELECT.
* Group by `affected_object_kind` (one object can have many ledger
  entries if multiple tags / stewards / rules changed at once).
* For each distinct object, run scoped versions of the three policy
  SQL checks bound to that object only.
* File any new violations via the normal bug-filing path, with
  `triggered_by='change_feed'` so the audit table distinguishes
  real-time scans from scheduled ones.
* Mark every drained ledger row `processed_at=now()` with the
  `processed_scan_id` linking to the scan that covered it.

The drain is idempotent and safe to call from any cadence: heartbeat
cycles, manual MCP actions, or (eventually) a dedicated listener.
"""
from __future__ import annotations

from typing import Any

from runtime.bug_tracker import BugTracker
from runtime.data_dictionary_governance import (
    GovernanceViolation,
    _downstream_count,
    _nearest_upstream_owner,
    _maybe_record_scan,
    file_violation_bugs,
)


_DEFAULT_DRAIN_BATCH = 100

_SQL_CLAIM_PENDING = """
SELECT change_id, affected_object_kind, source_table, change_kind, observed_at
FROM data_dictionary_governance_change_ledger
WHERE processed_at IS NULL
ORDER BY observed_at ASC, change_id ASC
LIMIT $1
"""

_SQL_UNOWNED_TAGS_FOR_OBJECT = """
-- Mirror of runtime.data_dictionary_governance._SQL_UNOWNED_TAGS scoped to
-- one object. Operator-layer tags only (see that module for rationale).
SELECT DISTINCT c.object_kind, c.tag_key
FROM data_dictionary_classifications_effective c
LEFT JOIN data_dictionary_stewardship_effective s
  ON s.object_kind = c.object_kind
 AND s.steward_kind = 'owner'
WHERE c.object_kind = $1
  AND c.tag_key IN ('pii', 'sensitive')
  AND c.effective_source = 'operator'
  AND s.object_kind IS NULL
"""

_SQL_FAILING_ERROR_RULES_FOR_OBJECT = """
WITH latest AS (
    SELECT DISTINCT ON (object_kind, field_path, rule_kind)
        object_kind, field_path, rule_kind, status, started_at
    FROM data_dictionary_quality_runs
    WHERE object_kind = $1
    ORDER BY object_kind, field_path, rule_kind, started_at DESC
)
SELECT r.object_kind, r.rule_kind, r.field_path, l.status, l.started_at
FROM data_dictionary_quality_rules_effective r
JOIN latest l
  ON l.object_kind = r.object_kind
 AND l.field_path = r.field_path
 AND l.rule_kind = r.rule_kind
WHERE r.object_kind = $1
  AND r.severity = 'error'
  AND r.enabled = TRUE
  AND l.status IN ('fail', 'error')
"""


def _scan_object(conn: Any, object_kind: str) -> list[GovernanceViolation]:
    """Same three policies as `scan_violations`, but scoped to one object."""
    out: list[GovernanceViolation] = []

    for row in conn.execute(_SQL_UNOWNED_TAGS_FOR_OBJECT, object_kind) or []:
        tag = str(row.get("tag_key") or "").strip()
        if not tag:
            continue
        policy = "pii_without_owner" if tag == "pii" else "sensitive_without_owner"
        out.append(GovernanceViolation(
            policy=policy,
            object_kind=object_kind,
            details={"tag_key": tag},
        ))

    for row in conn.execute(_SQL_FAILING_ERROR_RULES_FOR_OBJECT, object_kind) or []:
        rule = str(row.get("rule_kind") or "").strip()
        if not rule:
            continue
        out.append(GovernanceViolation(
            policy="error_rule_failing",
            object_kind=object_kind,
            rule_kind=rule,
            details={
                "field_path": str(row.get("field_path") or ""),
                "status": str(row.get("status") or ""),
                "last_run_at": str(row.get("started_at") or ""),
            },
        ))

    return out


def _mark_processed(
    conn: Any,
    *,
    change_ids: list[int],
    scan_id: str | None,
) -> None:
    if not change_ids:
        return
    conn.execute(
        """
        UPDATE data_dictionary_governance_change_ledger
        SET processed_at = now(),
            processed_scan_id = $2::uuid
        WHERE change_id = ANY($1::bigint[])
        """,
        change_ids,
        scan_id,  # may be None — cast allows NULL
    )


def pending_count(conn: Any) -> int:
    rows = conn.execute(
        "SELECT COUNT(*)::int AS c FROM data_dictionary_governance_change_ledger "
        "WHERE processed_at IS NULL"
    )
    return int(rows[0]["c"]) if rows else 0


def drain_change_feed(
    conn: Any,
    *,
    tracker: BugTracker | None = None,
    limit: int = _DEFAULT_DRAIN_BATCH,
    triggered_by: str = "change_feed",
) -> dict[str, Any]:
    """Drain the unprocessed ledger, scan affected objects, file bugs.

    Returns:
      {
        "drained": N,                  # ledger rows marked processed
        "objects_scanned": M,          # distinct object_kinds checked
        "total_violations": V,         # violations found across M objects
        "by_policy": {...},            # counts per policy
        "filed_bugs": [...],           # newly filed
        "skipped_existing": [...],     # already-open
        "filing_errors": [...],
        "scan_id": <uuid> | None,
      }
    """
    limit = max(1, min(1000, int(limit or _DEFAULT_DRAIN_BATCH)))

    pending = conn.execute(_SQL_CLAIM_PENDING, limit) or []
    pending_rows = [dict(r) for r in pending]
    if not pending_rows:
        return {
            "drained": 0,
            "objects_scanned": 0,
            "total_violations": 0,
            "by_policy": {},
            "filed_bugs": [],
            "skipped_existing": [],
            "filing_errors": [],
            "scan_id": None,
        }

    change_ids = [int(r["change_id"]) for r in pending_rows]
    affected_objects = sorted({r["affected_object_kind"] for r in pending_rows})

    # Focused scan on each affected object.
    violations: list[GovernanceViolation] = []
    for obj in affected_objects:
        violations.extend(_scan_object(conn, obj))

    by_policy: dict[str, int] = {}
    for v in violations:
        by_policy[v.policy] = by_policy.get(v.policy, 0) + 1

    filed_bugs: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    if tracker is not None and violations:
        result = file_violation_bugs(conn, tracker, violations)
        filed_bugs = result["filed"]
        skipped = result["skipped"]
        errors = result["errors"]

    # Always persist a scan audit row — the drain *is* a scan, and
    # operators should be able to correlate drained objects with scans
    # via the processed_scan_id ledger column.
    summary_out: dict[str, Any] = {}
    _maybe_record_scan(
        conn,
        triggered_by=triggered_by,
        dry_run=tracker is None,
        total_violations=len(violations),
        by_policy=by_policy,
        violations=[v.to_payload() for v in violations],
        filed_bugs=filed_bugs,
        bugs_skipped=len(skipped),
        bugs_errored=len(errors),
        summary_out=summary_out,
    )
    scan_id = summary_out.get("scan_id")

    _mark_processed(conn, change_ids=change_ids, scan_id=scan_id)

    return {
        "drained": len(change_ids),
        "objects_scanned": len(affected_objects),
        "affected_objects": affected_objects,
        "total_violations": len(violations),
        "by_policy": by_policy,
        "filed_bugs": filed_bugs,
        "skipped_existing": skipped,
        "filing_errors": errors,
        "scan_id": scan_id,
    }


def peek_pending(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    """Read-only preview of what the next drain would cover."""
    rows = conn.execute(
        """
        SELECT change_id, affected_object_kind, source_table, change_kind, observed_at
        FROM data_dictionary_governance_change_ledger
        WHERE processed_at IS NULL
        ORDER BY observed_at ASC, change_id ASC
        LIMIT $1
        """,
        max(1, min(500, int(limit or 20))),
    )
    items = []
    objects: set[str] = set()
    for r in rows:
        items.append({
            "change_id": int(r["change_id"]),
            "affected_object_kind": r["affected_object_kind"],
            "source_table": r["source_table"],
            "change_kind": r["change_kind"],
            "observed_at": (
                r["observed_at"].isoformat()
                if hasattr(r["observed_at"], "isoformat") else str(r["observed_at"])
            ),
        })
        objects.add(r["affected_object_kind"])
    return {
        "total_pending": pending_count(conn),
        "showing": len(items),
        "distinct_objects": len(objects),
        "items": items,
    }


__all__ = [
    "drain_change_feed",
    "peek_pending",
    "pending_count",
]
