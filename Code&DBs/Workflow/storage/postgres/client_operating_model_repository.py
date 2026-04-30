"""Postgres persistence for Client Operating Model operator-view snapshots."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


_VIEW_KIND_PREFIX = "client_operating_model.operator_surface."


def _rows(rows: Any) -> list[dict[str, Any]]:
    return [_normalize_row(row) for row in rows or []]


def _normalize_row(row: Any) -> dict[str, Any]:
    payload = dict(row or {})
    for key in (
        "freshness_json",
        "permission_scope_json",
        "evidence_refs_json",
        "correlation_ids_json",
        "operator_view_json",
    ):
        value = payload.get(key)
        if isinstance(value, str):
            try:
                payload[key] = json.loads(value)
            except json.JSONDecodeError:
                continue
    return payload


def _canonical_json(value: Mapping[str, Any], *, field_name: str) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError as exc:
        raise PostgresWriteError(
            "client_operating_model.snapshot_not_json_serializable",
            f"{field_name} must be JSON serializable",
            details={"field": field_name},
        ) from exc


def _snapshot_digest(operator_view: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_json(operator_view, field_name="operator_view").encode("utf-8")
    ).hexdigest()


def _view_name(operator_view: Mapping[str, Any], *, explicit_view: str | None) -> str:
    if explicit_view:
        return explicit_view
    kind = _require_text(operator_view.get("kind"), field_name="operator_view.kind")
    if not kind.startswith(_VIEW_KIND_PREFIX) or not kind.endswith(".v1"):
        raise PostgresWriteError(
            "client_operating_model.invalid_view_kind",
            "operator_view.kind must be a Client Operating Model v1 surface kind",
            details={"kind": kind},
        )
    return kind.removeprefix(_VIEW_KIND_PREFIX).removesuffix(".v1")


def _mapping_or_empty(value: object, *, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    return _require_mapping(value, field_name=field_name)


def _string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PostgresWriteError(
            "client_operating_model.invalid_snapshot_payload",
            f"{field_name} must be a list",
            details={"field": field_name},
        )
    return [str(item).strip() for item in value if str(item).strip()]


def persist_operator_view_snapshot(
    conn: Any,
    *,
    operator_view: dict[str, Any],
    view: str | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one JSON-ready Client Operating Model operator-view snapshot."""

    payload = dict(_require_mapping(operator_view, field_name="operator_view"))
    view_name = _view_name(payload, explicit_view=view)
    view_id = _require_text(payload.get("view_id") or payload.get("stable_id"), field_name="operator_view.view_id")
    state = _require_text(payload.get("state"), field_name="operator_view.state")
    freshness = dict(_mapping_or_empty(payload.get("freshness"), field_name="operator_view.freshness"))
    permission_scope = dict(
        _mapping_or_empty(payload.get("permission_scope"), field_name="operator_view.permission_scope")
    )
    scope_ref = str(permission_scope.get("scope_ref") or "global").strip() or "global"
    digest = _snapshot_digest(payload)

    row = conn.fetchrow(
        """
        INSERT INTO client_operating_model_operator_view_snapshots (
            snapshot_digest,
            snapshot_ref,
            view_name,
            view_id,
            scope_ref,
            state,
            freshness_json,
            permission_scope_json,
            evidence_refs_json,
            correlation_ids_json,
            operator_view_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12, $13
        )
        ON CONFLICT (snapshot_digest) DO UPDATE SET
            snapshot_ref = EXCLUDED.snapshot_ref,
            view_name = EXCLUDED.view_name,
            view_id = EXCLUDED.view_id,
            scope_ref = EXCLUDED.scope_ref,
            state = EXCLUDED.state,
            freshness_json = EXCLUDED.freshness_json,
            permission_scope_json = EXCLUDED.permission_scope_json,
            evidence_refs_json = EXCLUDED.evidence_refs_json,
            correlation_ids_json = EXCLUDED.correlation_ids_json,
            operator_view_json = EXCLUDED.operator_view_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            snapshot_digest,
            snapshot_ref,
            view_name,
            view_id,
            scope_ref,
            state,
            freshness_json,
            permission_scope_json,
            evidence_refs_json,
            correlation_ids_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        digest,
        f"client_operating_model_operator_view_snapshot:{digest}",
        view_name,
        view_id,
        scope_ref,
        state,
        _encode_jsonb(freshness, field_name="freshness"),
        _encode_jsonb(permission_scope, field_name="permission_scope"),
        _encode_jsonb(_string_list(payload.get("evidence_refs"), field_name="operator_view.evidence_refs"), field_name="evidence_refs"),
        _encode_jsonb(_string_list(payload.get("correlation_ids"), field_name="operator_view.correlation_ids"), field_name="correlation_ids"),
        _encode_jsonb(payload, field_name="operator_view"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    if row is None:
        raise PostgresWriteError(
            "client_operating_model.snapshot_write_failed",
            "operator-view snapshot insert returned no row",
        )
    return _normalize_row(row)


def list_operator_view_snapshots(
    conn: Any,
    *,
    snapshot_ref: str | None = None,
    snapshot_digest: str | None = None,
    view: str | None = None,
    scope_ref: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Read stored Client Operating Model operator-view snapshots."""

    rows = conn.fetch(
        """
        SELECT
            snapshot_digest,
            snapshot_ref,
            view_name,
            view_id,
            scope_ref,
            state,
            freshness_json,
            permission_scope_json,
            evidence_refs_json,
            correlation_ids_json,
            operator_view_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM client_operating_model_operator_view_snapshots
         WHERE ($1::text IS NULL OR snapshot_ref = $1::text)
           AND ($2::text IS NULL OR snapshot_digest = $2::text)
           AND ($3::text IS NULL OR view_name = $3::text)
           AND ($4::text IS NULL OR scope_ref = $4::text)
         ORDER BY created_at DESC, snapshot_digest
         LIMIT $5
        """,
        _optional_text(snapshot_ref, field_name="snapshot_ref"),
        _optional_text(snapshot_digest, field_name="snapshot_digest"),
        _optional_text(view, field_name="view"),
        _optional_text(scope_ref, field_name="scope_ref"),
        limit,
    )
    return _rows(rows)


__all__ = [
    "list_operator_view_snapshots",
    "persist_operator_view_snapshot",
]
