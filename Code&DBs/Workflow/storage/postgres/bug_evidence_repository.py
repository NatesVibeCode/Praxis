"""Explicit sync Postgres repository for bug-evidence mutations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import uuid

from .validators import PostgresWriteError, _optional_text, _require_text, _require_utc


_ALLOWED_EVIDENCE_ROLES = frozenset({"observed_in", "attempted_fix", "validates_fix"})


def _require_evidence_role(value: object, *, field_name: str) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized not in _ALLOWED_EVIDENCE_ROLES:
        raise PostgresWriteError(
            "bug_evidence.invalid_submission",
            f"{field_name} must be one of {', '.join(sorted(_ALLOWED_EVIDENCE_ROLES))}",
            details={"field": field_name},
        )
    return normalized


class PostgresBugEvidenceRepository:
    """Owns canonical bug-evidence link mutations."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def upsert_bug_evidence_link(
        self,
        *,
        bug_id: str,
        evidence_kind: str,
        evidence_ref: str,
        evidence_role: str,
        created_by: str = "bug_tracker",
        notes: str | None = None,
        bug_evidence_link_id: str | None = None,
        created_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        normalized_bug_id = _require_text(bug_id, field_name="bug_id")
        normalized_evidence_kind = _require_text(
            evidence_kind,
            field_name="evidence_kind",
        )
        normalized_evidence_ref = _require_text(
            evidence_ref,
            field_name="evidence_ref",
        )
        normalized_evidence_role = _require_evidence_role(
            evidence_role,
            field_name="evidence_role",
        )
        normalized_created_by = _require_text(
            created_by,
            field_name="created_by",
        )
        normalized_notes = _optional_text(notes, field_name="notes")
        normalized_link_id = _optional_text(
            bug_evidence_link_id,
            field_name="bug_evidence_link_id",
        ) or f"bug_evidence_link:{uuid.uuid4().hex}"
        normalized_created_at = (
            _require_utc(created_at, field_name="created_at")
            if created_at is not None
            else datetime.now(timezone.utc)
        )
        rows = self._conn.execute(
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
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (bug_id, evidence_kind, evidence_ref, evidence_role)
            DO UPDATE SET
                notes = COALESCE(bug_evidence_links.notes, EXCLUDED.notes)
            RETURNING *
            """,
            normalized_link_id,
            normalized_bug_id,
            normalized_evidence_kind,
            normalized_evidence_ref,
            normalized_evidence_role,
            normalized_created_at,
            normalized_created_by,
            normalized_notes,
        )
        return None if not rows else dict(rows[0])


__all__ = ["PostgresBugEvidenceRepository"]
