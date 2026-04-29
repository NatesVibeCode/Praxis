"""Postgres authority for repo-policy onboarding contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

from .connection import SyncPostgresConnection
from .validators import _encode_jsonb, _require_text


_DISCLOSURE_FIELDS = {
    "bug": "bug_disclosure_count",
    "pattern": "pattern_disclosure_count",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


@dataclass(frozen=True, slots=True)
class RepoPolicyContractRecord:
    repo_policy_contract_id: str
    repo_root: str
    status: str
    current_revision_id: str | None
    current_revision_no: int
    current_contract_hash: str | None
    disclosure_repeat_limit: int
    bug_disclosure_count: int
    pattern_disclosure_count: int
    contract_body: dict[str, Any]
    change_reason: str | None
    created_by: str | None
    created_at: datetime | None
    updated_at: datetime | None


def _record_from_row(row: Mapping[str, Any] | None) -> RepoPolicyContractRecord | None:
    if row is None:
        return None
    return RepoPolicyContractRecord(
        repo_policy_contract_id=str(row["repo_policy_contract_id"]),
        repo_root=str(row["repo_root"]),
        status=str(row["status"]),
        current_revision_id=(
            str(row["current_revision_id"]) if row.get("current_revision_id") is not None else None
        ),
        current_revision_no=int(row.get("current_revision_no") or 0),
        current_contract_hash=(
            str(row["current_contract_hash"]) if row.get("current_contract_hash") is not None else None
        ),
        disclosure_repeat_limit=int(row.get("disclosure_repeat_limit") or 0),
        bug_disclosure_count=int(row.get("bug_disclosure_count") or 0),
        pattern_disclosure_count=int(row.get("pattern_disclosure_count") or 0),
        contract_body=_json_object(row.get("contract_body")),
        change_reason=str(row["change_reason"]) if row.get("change_reason") is not None else None,
        created_by=str(row["created_by"]) if row.get("created_by") is not None else None,
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


class PostgresRepoPolicyContractRepository:
    """Owns repo-policy onboarding contract persistence."""

    def __init__(self, conn: SyncPostgresConnection) -> None:
        self._conn = conn

    def get_current(self, *, repo_root: str) -> RepoPolicyContractRecord | None:
        row = self._conn.fetchrow(
            """
            SELECT head.repo_policy_contract_id,
                   head.repo_root,
                   head.status,
                   head.current_revision_id,
                   head.current_revision_no,
                   head.current_contract_hash,
                   head.disclosure_repeat_limit,
                   head.bug_disclosure_count,
                   head.pattern_disclosure_count,
                   rev.contract_body,
                   rev.change_reason,
                   rev.created_by,
                   head.created_at,
                   head.updated_at
              FROM operator_repo_policy_contracts AS head
              LEFT JOIN operator_repo_policy_contract_revisions AS rev
                ON rev.repo_policy_contract_revision_id = head.current_revision_id
             WHERE head.repo_root = $1
             LIMIT 1
            """,
            _require_text(repo_root, field_name="repo_root"),
        )
        return _record_from_row(row)

    def upsert_contract(
        self,
        *,
        repo_policy_contract_id: str,
        repo_root: str,
        status: str,
        revision_id: str,
        contract_hash: str,
        contract_body: dict[str, Any],
        created_by: str,
        change_reason: str,
        disclosure_repeat_limit: int,
    ) -> RepoPolicyContractRecord:
        normalized_repo_root = _require_text(repo_root, field_name="repo_root")
        current = self.get_current(repo_root=normalized_repo_root)
        revision_no = (current.current_revision_no + 1) if current is not None else 1
        current_status = _require_text(status, field_name="status")
        now = _utc_now()

        self._conn.execute("BEGIN")
        try:
            if current is None:
                self._conn.execute(
                    """
                    INSERT INTO operator_repo_policy_contracts (
                        repo_policy_contract_id,
                        repo_root,
                        status,
                        current_revision_id,
                        current_revision_no,
                        current_contract_hash,
                        disclosure_repeat_limit,
                        bug_disclosure_count,
                        pattern_disclosure_count,
                        created_at,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, $8, $8)
                    """,
                    _require_text(repo_policy_contract_id, field_name="repo_policy_contract_id"),
                    normalized_repo_root,
                    current_status,
                    _require_text(revision_id, field_name="revision_id"),
                    revision_no,
                    _require_text(contract_hash, field_name="contract_hash"),
                    max(0, int(disclosure_repeat_limit)),
                    now,
                )
            else:
                self._conn.execute(
                    """
                    UPDATE operator_repo_policy_contracts
                       SET status = $2,
                           current_revision_id = $3,
                           current_revision_no = $4,
                           current_contract_hash = $5,
                           disclosure_repeat_limit = $6,
                           updated_at = $7
                     WHERE repo_policy_contract_id = $1
                    """,
                    current.repo_policy_contract_id,
                    current_status,
                    _require_text(revision_id, field_name="revision_id"),
                    revision_no,
                    _require_text(contract_hash, field_name="contract_hash"),
                    max(0, int(disclosure_repeat_limit)),
                    now,
                )
            self._conn.execute(
                """
                INSERT INTO operator_repo_policy_contract_revisions (
                    repo_policy_contract_revision_id,
                    repo_policy_contract_id,
                    revision_no,
                    parent_revision_id,
                    contract_hash,
                    contract_body,
                    change_reason,
                    created_by,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
                """,
                _require_text(revision_id, field_name="revision_id"),
                (
                    current.repo_policy_contract_id
                    if current is not None
                    else _require_text(repo_policy_contract_id, field_name="repo_policy_contract_id")
                ),
                revision_no,
                current.current_revision_id if current is not None else None,
                _require_text(contract_hash, field_name="contract_hash"),
                _encode_jsonb(contract_body, field_name="contract_body"),
                _require_text(change_reason, field_name="change_reason"),
                _require_text(created_by, field_name="created_by"),
                now,
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        refreshed = self.get_current(repo_root=normalized_repo_root)
        if refreshed is None:
            raise RuntimeError(f"failed to read back repo policy contract for {normalized_repo_root!r}")
        return refreshed

    def increment_disclosure(
        self,
        *,
        repo_root: str,
        disclosure_kind: str,
    ) -> RepoPolicyContractRecord | None:
        field_name = _DISCLOSURE_FIELDS.get(str(disclosure_kind or "").strip().lower())
        if field_name is None:
            raise ValueError(f"unknown disclosure_kind={disclosure_kind!r}")
        row = self._conn.fetchrow(
            f"""
            UPDATE operator_repo_policy_contracts
               SET {field_name} = {field_name} + 1,
                   updated_at = now()
             WHERE repo_root = $1
               AND {field_name} < disclosure_repeat_limit
             RETURNING repo_policy_contract_id
            """,
            _require_text(repo_root, field_name="repo_root"),
        )
        if row is None:
            return self.get_current(repo_root=repo_root)
        return self.get_current(repo_root=repo_root)


__all__ = [
    "PostgresRepoPolicyContractRepository",
    "RepoPolicyContractRecord",
]
