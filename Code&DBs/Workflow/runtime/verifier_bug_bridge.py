"""Bug promotion and evidence-linking helpers for verifier/healer control-plane runs."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


_CONTROL_PLANE_AUTO_BUG_THRESHOLD = 3
_CONTROL_PLANE_BUG_WINDOW_SQL = "7 days"


def _connection(conn: "SyncPostgresConnection | None" = None) -> "SyncPostgresConnection":
    if conn is not None:
        return conn
    from storage.postgres.connection import ensure_postgres_available

    return ensure_postgres_available()


def _optional_connection(conn: "SyncPostgresConnection | None" = None) -> "SyncPostgresConnection | None":
    if conn is not None:
        return conn
    try:
        return _connection()
    except Exception:
        return None


def _normalize_bug_tag(value: str) -> str:
    text = re.sub(r"[^a-z0-9._:/-]+", "-", str(value).strip().lower())
    return text.strip("-") or "none"


def _control_plane_failure_signature(status: str, outputs: dict[str, Any]) -> str:
    if status in {"passed", "succeeded"}:
        return ""
    detail = (
        outputs.get("failure_signature")
        or outputs.get("error")
        or outputs.get("exception_type")
        or outputs.get("summary")
        or outputs
    )
    if isinstance(detail, (dict, list)):
        text = json.dumps(detail, sort_keys=True, default=str)
    else:
        text = str(detail or status)
    return " ".join(text.split())[:500]


def _control_plane_bug_fingerprint(
    *,
    kind: str,
    primary_ref: str,
    target_kind: str,
    target_ref: str,
    status: str,
    outputs: dict[str, Any],
) -> str | None:
    signature = _control_plane_failure_signature(status, outputs)
    if not signature:
        return None
    payload = {
        "kind": kind,
        "primary_ref": primary_ref,
        "target_kind": target_kind,
        "target_ref": target_ref,
        "signature": signature,
        "status": status,
    }
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:20]


def annotate_control_plane_outputs(
    *,
    kind: str,
    primary_ref: str,
    target_kind: str,
    target_ref: str,
    status: str,
    outputs: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(outputs)
    signature = _control_plane_failure_signature(status, normalized)
    if signature and "failure_signature" not in normalized:
        normalized["failure_signature"] = signature
    fingerprint = _control_plane_bug_fingerprint(
        kind=kind,
        primary_ref=primary_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        status=status,
        outputs=normalized,
    )
    if fingerprint:
        normalized["control_plane_bug_kind"] = kind
        normalized["control_plane_bug_fingerprint"] = fingerprint
    return normalized


def _recent_verification_failure_count(
    *,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    fingerprint: str,
    conn: "SyncPostgresConnection | None" = None,
) -> int:
    db = _optional_connection(conn)
    if db is None:
        return 0
    row = db.fetchrow(
        f"""
        SELECT COUNT(*) AS recent_failures
        FROM verification_runs
        WHERE verifier_ref = $1
          AND target_kind = $2
          AND target_ref = $3
          AND status IN ('failed', 'error')
          AND COALESCE(outputs->>'control_plane_bug_fingerprint', '') = $4
          AND attempted_at >= NOW() - INTERVAL '{_CONTROL_PLANE_BUG_WINDOW_SQL}'
        """,
        verifier_ref,
        target_kind,
        target_ref,
        fingerprint,
    ) or {}
    return int(row.get("recent_failures") or 0)


def _recent_healing_failure_count(
    *,
    healer_ref: str,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    fingerprint: str,
    conn: "SyncPostgresConnection | None" = None,
) -> int:
    db = _optional_connection(conn)
    if db is None:
        return 0
    row = db.fetchrow(
        f"""
        SELECT COUNT(*) AS recent_failures
        FROM healing_runs
        WHERE healer_ref = $1
          AND verifier_ref = $2
          AND target_kind = $3
          AND target_ref = $4
          AND status IN ('failed', 'error')
          AND COALESCE(outputs->>'control_plane_bug_fingerprint', '') = $5
          AND attempted_at >= NOW() - INTERVAL '{_CONTROL_PLANE_BUG_WINDOW_SQL}'
        """,
        healer_ref,
        verifier_ref,
        target_kind,
        target_ref,
        fingerprint,
    ) or {}
    return int(row.get("recent_failures") or 0)


def _load_open_bug_by_fingerprint(
    *,
    fingerprint: str,
    conn: "SyncPostgresConnection | None" = None,
):
    db = _optional_connection(conn)
    if db is None:
        return None
    from runtime.bug_tracker import BugTracker

    tracker = BugTracker(db)
    matches = tracker.list_bugs(
        open_only=True,
        tags=(f"control-plane-fingerprint:{fingerprint}",),
        limit=5,
    )
    return matches[0] if matches else None


def _link_bug_evidence(
    *,
    bug_id: str,
    evidence_kind: str,
    evidence_ref: str,
    evidence_role: str,
    notes: str,
    conn: "SyncPostgresConnection | None" = None,
) -> None:
    db = _optional_connection(conn)
    if db is None or not bug_id or not evidence_ref:
        return
    db.execute(
        """
        INSERT INTO bug_evidence_links (
            bug_evidence_link_id,
            bug_id,
            evidence_kind,
            evidence_ref,
            evidence_role,
            created_at,
            created_by,
            notes
        ) VALUES (
            $1, $2, $3, $4, $5, NOW(), $6, $7
        )
        ON CONFLICT (bug_id, evidence_kind, evidence_ref, evidence_role) DO NOTHING
        """,
        f"bug_evidence_link:{uuid.uuid4().hex}",
        bug_id,
        evidence_kind,
        evidence_ref,
        evidence_role,
        "verifier_authority",
        notes,
    )


def _file_control_plane_bug(
    *,
    kind: str,
    primary_ref: str,
    primary_display_name: str,
    status: str,
    target_kind: str,
    target_ref: str,
    fingerprint: str,
    recent_failures: int,
    outputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
):
    db = _optional_connection(conn)
    if db is None:
        return None
    from runtime.bug_tracker import BugCategory, BugSeverity, BugTracker

    tracker = BugTracker(db)
    severity = BugSeverity.P1 if status == "error" else BugSeverity.P2
    target_label = f"{target_kind}:{target_ref}" if target_ref else target_kind
    signature = _control_plane_failure_signature(status, outputs) or status
    title = f"{kind.capitalize()} failure: {primary_display_name} [{target_label}]"
    description = (
        f"{kind.capitalize()} control-plane failure for {primary_ref} on {target_label}. "
        f"Observed status={status}. Recent matching failures in the last {_CONTROL_PLANE_BUG_WINDOW_SQL}: "
        f"{recent_failures}. Failure signature: {signature}"
    )
    bug, _similar_bugs = tracker.file_bug(
        title=title,
        severity=severity,
        category=BugCategory.VERIFY,
        description=description,
        filed_by="verifier_authority",
        tags=(
            "control-plane",
            f"control-plane-kind:{_normalize_bug_tag(kind)}",
            f"control-plane-fingerprint:{fingerprint}",
            f"primary-ref:{_normalize_bug_tag(primary_ref)}",
            f"target-kind:{_normalize_bug_tag(target_kind)}",
            f"target-ref:{hashlib.sha1(target_ref.encode('utf-8')).hexdigest()[:12] if target_ref else 'none'}",
        ),
    )
    return bug


def _latest_failed_verification_fingerprint(
    *,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    db = _optional_connection(conn)
    if db is None:
        return None
    row = db.fetchrow(
        """
        SELECT COALESCE(outputs->>'control_plane_bug_fingerprint', '') AS fingerprint
        FROM verification_runs
        WHERE verifier_ref = $1
          AND target_kind = $2
          AND target_ref = $3
          AND status IN ('failed', 'error')
          AND COALESCE(outputs->>'control_plane_bug_fingerprint', '') <> ''
        ORDER BY attempted_at DESC
        LIMIT 1
        """,
        verifier_ref,
        target_kind,
        target_ref,
    ) or {}
    fingerprint = str(row.get("fingerprint") or "").strip()
    return fingerprint or None


def maybe_resolve_verifier_bug(
    *,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    healing_run_id: str | None,
    post_verification: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    if not healing_run_id or str(post_verification.get("status") or "") != "passed":
        return None
    fingerprint = _latest_failed_verification_fingerprint(
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        conn=conn,
    )
    if not fingerprint:
        return None
    bug = _load_open_bug_by_fingerprint(fingerprint=fingerprint, conn=conn)
    if bug is None:
        return None
    _link_bug_evidence(
        bug_id=bug.bug_id,
        evidence_kind="healing_run",
        evidence_ref=healing_run_id,
        evidence_role="attempted_fix",
        notes=f"Healer run repaired verifier target {target_kind}:{target_ref or 'global'}.",
        conn=conn,
    )
    verification_run_id = str(post_verification.get("verification_run_id") or "").strip()
    if verification_run_id:
        _link_bug_evidence(
            bug_id=bug.bug_id,
            evidence_kind="verification_run",
            evidence_ref=verification_run_id,
            evidence_role="validates_fix",
            notes=f"Verifier {verifier_ref} passed after healing.",
            conn=conn,
        )
    db = _optional_connection(conn)
    if db is None:
        return None
    from runtime.bug_tracker import BugStatus, BugTracker

    tracker = BugTracker(db)
    resolved = tracker.resolve(bug.bug_id, BugStatus.FIXED)
    return resolved.bug_id if resolved is not None else bug.bug_id
