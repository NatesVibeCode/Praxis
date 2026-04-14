"""Explicit sync Postgres repository for durable control-command mutations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
    _require_utc,
)


def _row_dict(row: object, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "postgres.write_failed",
            f"{operation} returned no row",
        )
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return dict(row)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        pass
    raise PostgresWriteError(
        "postgres.write_failed",
        f"{operation} returned an invalid row type",
        details={"operation": operation, "row_type": type(row).__name__},
    )


class PostgresCommandRepository:
    """Owns canonical control-command inserts and updates."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def insert_control_command(
        self,
        *,
        command_id: str,
        command_type: str,
        command_status: str,
        requested_by_kind: str,
        requested_by_ref: str,
        requested_at: datetime,
        approved_at: datetime | None,
        approved_by: str | None,
        idempotency_key: str,
        risk_level: str,
        payload: Mapping[str, Any],
        result_ref: str | None,
        error_code: str | None,
        error_detail: str | None,
        created_at: datetime,
        updated_at: datetime,
    ) -> dict[str, Any] | None:
        normalized_approved_at = (
            None if approved_at is None else _require_utc(approved_at, field_name="approved_at")
        )
        normalized_approved_by = _optional_text(approved_by, field_name="approved_by")
        normalized_result_ref = _optional_text(result_ref, field_name="result_ref")
        normalized_error_code = _optional_text(error_code, field_name="error_code")
        normalized_error_detail = _optional_text(error_detail, field_name="error_detail")

        rows = self._conn.execute(
            """
            INSERT INTO control_commands (
                command_id,
                command_type,
                command_status,
                requested_by_kind,
                requested_by_ref,
                requested_at,
                approved_at,
                approved_by,
                idempotency_key,
                risk_level,
                payload,
                result_ref,
                error_code,
                error_detail,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING
                command_id,
                command_type,
                command_status,
                requested_by_kind,
                requested_by_ref,
                requested_at,
                approved_at,
                approved_by,
                idempotency_key,
                risk_level,
                payload,
                result_ref,
                error_code,
                error_detail,
                created_at,
                updated_at
            """,
            _require_text(command_id, field_name="command_id"),
            _require_text(command_type, field_name="command_type"),
            _require_text(command_status, field_name="command_status"),
            _require_text(requested_by_kind, field_name="requested_by_kind"),
            _require_text(requested_by_ref, field_name="requested_by_ref"),
            _require_utc(requested_at, field_name="requested_at"),
            normalized_approved_at,
            normalized_approved_by,
            _require_text(idempotency_key, field_name="idempotency_key"),
            _require_text(risk_level, field_name="risk_level"),
            _encode_jsonb(
                _require_mapping(payload, field_name="payload"),
                field_name="payload",
            ),
            normalized_result_ref,
            normalized_error_code,
            normalized_error_detail,
            _require_utc(created_at, field_name="created_at"),
            _require_utc(updated_at, field_name="updated_at"),
        )
        if not rows:
            return None
        return _row_dict(rows[0], operation="insert_control_command")

    def update_control_command(
        self,
        *,
        command_id: str,
        command_status: str,
        approved_at: datetime | None,
        approved_by: str | None,
        payload: Mapping[str, Any],
        result_ref: str | None,
        error_code: str | None,
        error_detail: str | None,
    ) -> dict[str, Any]:
        normalized_approved_at = (
            None if approved_at is None else _require_utc(approved_at, field_name="approved_at")
        )
        normalized_approved_by = _optional_text(approved_by, field_name="approved_by")
        normalized_result_ref = _optional_text(result_ref, field_name="result_ref")
        normalized_error_code = _optional_text(error_code, field_name="error_code")
        normalized_error_detail = _optional_text(error_detail, field_name="error_detail")

        rows = self._conn.execute(
            """
            UPDATE control_commands
            SET command_status = $2,
                approved_at = $3,
                approved_by = $4,
                payload = $5::jsonb,
                result_ref = $6,
                error_code = $7,
                error_detail = $8,
                updated_at = now()
            WHERE command_id = $1
            RETURNING
                command_id,
                command_type,
                command_status,
                requested_by_kind,
                requested_by_ref,
                requested_at,
                approved_at,
                approved_by,
                idempotency_key,
                risk_level,
                payload,
                result_ref,
                error_code,
                error_detail,
                created_at,
                updated_at
            """,
            _require_text(command_id, field_name="command_id"),
            _require_text(command_status, field_name="command_status"),
            normalized_approved_at,
            normalized_approved_by,
            _encode_jsonb(
                _require_mapping(payload, field_name="payload"),
                field_name="payload",
            ),
            normalized_result_ref,
            normalized_error_code,
            normalized_error_detail,
        )
        if not rows:
            raise PostgresWriteError(
                "postgres.write_failed",
                "update_control_command returned no row",
                details={"command_id": _require_text(command_id, field_name="command_id")},
            )
        return _row_dict(rows[0], operation="update_control_command")


__all__ = ["PostgresCommandRepository"]
