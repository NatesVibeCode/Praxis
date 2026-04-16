"""Read-only Postgres repository for operation catalog authority."""

from __future__ import annotations

from typing import Any

from contracts.operation_catalog import (
    normalize_operation_idempotency_policy,
    normalize_operation_kind,
    normalize_operation_posture,
    normalize_operation_source_kind,
)

from .validators import PostgresWriteError, _optional_text, _require_text


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PostgresWriteError(
            "operation_catalog.invalid_submission",
            f"{field_name} must be a boolean",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PostgresWriteError(
            "operation_catalog.invalid_submission",
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_operation_row(row: dict[str, Any]) -> dict[str, Any]:
    try:
        return {
            "operation_ref": _require_text(row.get("operation_ref"), field_name="operation_ref"),
            "operation_name": _require_text(row.get("operation_name"), field_name="operation_name"),
            "source_kind": normalize_operation_source_kind(row.get("source_kind")),
            "operation_kind": normalize_operation_kind(row.get("operation_kind")),
            "http_method": _require_text(row.get("http_method"), field_name="http_method"),
            "http_path": _require_text(row.get("http_path"), field_name="http_path"),
            "input_model_ref": _require_text(
                row.get("input_model_ref"),
                field_name="input_model_ref",
            ),
            "handler_ref": _require_text(row.get("handler_ref"), field_name="handler_ref"),
            "authority_ref": _require_text(row.get("authority_ref"), field_name="authority_ref"),
            "projection_ref": _optional_text(row.get("projection_ref"), field_name="projection_ref"),
            "posture": normalize_operation_posture(row.get("posture"), allow_none=True),
            "idempotency_policy": normalize_operation_idempotency_policy(
                row.get("idempotency_policy"),
                allow_none=True,
            ),
            "enabled": _require_bool(row.get("enabled"), field_name="enabled"),
            "binding_revision": _require_text(
                row.get("binding_revision"),
                field_name="binding_revision",
            ),
            "decision_ref": _require_text(row.get("decision_ref"), field_name="decision_ref"),
        }
    except ValueError as exc:
        raise PostgresWriteError(
            "operation_catalog.invalid_submission",
            str(exc),
        ) from exc


def _normalize_source_policy_row(row: dict[str, Any]) -> dict[str, Any]:
    try:
        return {
            "policy_ref": _require_text(row.get("policy_ref"), field_name="policy_ref"),
            "source_kind": normalize_operation_source_kind(row.get("source_kind")),
            "posture": str(normalize_operation_posture(row.get("posture"))),
            "idempotency_policy": str(
                normalize_operation_idempotency_policy(row.get("idempotency_policy"))
            ),
            "enabled": _require_bool(row.get("enabled"), field_name="enabled"),
            "binding_revision": _require_text(
                row.get("binding_revision"),
                field_name="binding_revision",
            ),
            "decision_ref": _require_text(row.get("decision_ref"), field_name="decision_ref"),
        }
    except ValueError as exc:
        raise PostgresWriteError(
            "operation_catalog.invalid_submission",
            str(exc),
        ) from exc


def load_operation_catalog_record(conn: Any, *, operation_ref: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT * FROM operation_catalog_registry WHERE operation_ref = $1",
        _require_text(operation_ref, field_name="operation_ref"),
    )
    return None if row is None else _normalize_operation_row(dict(row))


def load_operation_catalog_record_by_name(conn: Any, *, operation_name: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT * FROM operation_catalog_registry WHERE operation_name = $1",
        _require_text(operation_name, field_name="operation_name"),
    )
    return None if row is None else _normalize_operation_row(dict(row))


def list_operation_catalog_records(
    conn: Any,
    *,
    source_kind: str | None = None,
    include_disabled: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_limit = _require_int(limit, field_name="limit")
    if normalized_limit <= 0:
        raise PostgresWriteError(
            "operation_catalog.invalid_submission",
            "limit must be a positive integer",
            details={"field": "limit"},
        )

    params: list[Any] = []
    clauses = ["SELECT * FROM operation_catalog_registry WHERE TRUE"]
    if source_kind is not None:
        params.append(normalize_operation_source_kind(source_kind))
        clauses.append(f"AND source_kind = ${len(params)}")
    if not include_disabled:
        clauses.append("AND enabled = TRUE")
    params.append(normalized_limit)
    clauses.append(f"ORDER BY operation_name LIMIT ${len(params)}")
    rows = conn.fetch("\n".join(clauses), *params)
    return [_normalize_operation_row(dict(row)) for row in rows or []]


def list_operation_source_policy_records(
    conn: Any,
    *,
    include_disabled: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_limit = _require_int(limit, field_name="limit")
    if normalized_limit <= 0:
        raise PostgresWriteError(
            "operation_catalog.invalid_submission",
            "limit must be a positive integer",
            details={"field": "limit"},
        )

    clauses = ["SELECT * FROM operation_catalog_source_policy_registry WHERE TRUE"]
    params: list[Any] = []
    if not include_disabled:
        clauses.append("AND enabled = TRUE")
    params.append(normalized_limit)
    clauses.append(f"ORDER BY source_kind LIMIT ${len(params)}")
    rows = conn.fetch("\n".join(clauses), *params)
    return [_normalize_source_policy_row(dict(row)) for row in rows or []]


__all__ = [
    "list_operation_catalog_records",
    "list_operation_source_policy_records",
    "load_operation_catalog_record",
    "load_operation_catalog_record_by_name",
]
