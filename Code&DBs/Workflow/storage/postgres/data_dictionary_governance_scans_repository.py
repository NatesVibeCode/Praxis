"""Sync Postgres repository for governance scan audit records."""
from __future__ import annotations

import json
from typing import Any


def insert_scan(
    conn: Any,
    *,
    triggered_by: str,
    dry_run: bool,
    total_violations: int,
    bugs_filed: int,
    bugs_skipped: int,
    bugs_errored: int,
    by_policy: dict[str, int],
    violations: list[dict[str, Any]],
    filed_bug_ids: list[str],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_governance_scans (
            triggered_by, dry_run, total_violations,
            bugs_filed, bugs_skipped, bugs_errored,
            by_policy, violations, filed_bug_ids, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10::jsonb)
        RETURNING scan_id::text, scanned_at, triggered_by, dry_run,
                  total_violations, bugs_filed, bugs_skipped, bugs_errored,
                  by_policy, violations, filed_bug_ids, metadata
        """,
        triggered_by, dry_run, total_violations,
        bugs_filed, bugs_skipped, bugs_errored,
        json.dumps(by_policy or {}),
        json.dumps(violations or []),
        list(filed_bug_ids or []),
        json.dumps(metadata or {}),
    )
    return dict(row)


def fetch_scan_by_id(conn: Any, scan_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT scan_id::text, scanned_at, triggered_by, dry_run,
               total_violations, bugs_filed, bugs_skipped, bugs_errored,
               by_policy, violations, filed_bug_ids, metadata
        FROM data_dictionary_governance_scans
        WHERE scan_id = $1::uuid
        """,
        scan_id,
    )
    return dict(row) if row else None


def list_scans(
    conn: Any,
    *,
    limit: int = 50,
    triggered_by: str | None = None,
) -> list[dict[str, Any]]:
    if triggered_by:
        rows = conn.execute(
            """
            SELECT scan_id::text, scanned_at, triggered_by, dry_run,
                   total_violations, bugs_filed, bugs_skipped, bugs_errored,
                   by_policy
            FROM data_dictionary_governance_scans
            WHERE triggered_by = $1
            ORDER BY scanned_at DESC
            LIMIT $2
            """,
            triggered_by, max(1, min(500, int(limit or 50))),
        )
    else:
        rows = conn.execute(
            """
            SELECT scan_id::text, scanned_at, triggered_by, dry_run,
                   total_violations, bugs_filed, bugs_skipped, bugs_errored,
                   by_policy
            FROM data_dictionary_governance_scans
            ORDER BY scanned_at DESC
            LIMIT $1
            """,
            max(1, min(500, int(limit or 50))),
        )
    return [dict(r) for r in rows]


def fetch_scans_for_bug(conn: Any, bug_id: str) -> list[dict[str, Any]]:
    """Reverse lookup: every scan that ever filed this bug."""
    rows = conn.execute(
        """
        SELECT s.scan_id::text, s.scanned_at, s.triggered_by, s.total_violations
        FROM data_dictionary_governance_scans s
        WHERE $1 = ANY(s.filed_bug_ids)
        ORDER BY s.scanned_at DESC
        """,
        bug_id,
    )
    return [dict(r) for r in rows]


def link_bug_to_scan(
    conn: Any,
    *,
    bug_id: str,
    scan_id: str,
    role: str = "discovered_by",
) -> None:
    """Add a bug_evidence_links row pointing the bug at the scan that found it."""
    import uuid

    conn.execute(
        """
        INSERT INTO bug_evidence_links
            (bug_evidence_link_id, bug_id, evidence_kind, evidence_ref,
             evidence_role, created_at, created_by)
        VALUES ($1, $2, 'governance_scan', $3, $4, now(),
                'governance_compliance_heartbeat')
        ON CONFLICT (bug_id, evidence_kind, evidence_ref, evidence_role)
        DO NOTHING
        """,
        f"BEL-{uuid.uuid4().hex[:12]}",
        bug_id, scan_id, role,
    )


__all__ = [
    "fetch_scan_by_id",
    "fetch_scans_for_bug",
    "insert_scan",
    "link_bug_to_scan",
    "list_scans",
]
