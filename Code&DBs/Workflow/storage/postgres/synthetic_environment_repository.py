"""Postgres persistence for Synthetic Environment authority."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


_ENVIRONMENT_JSON_COLUMNS = (
    "seed_state_json",
    "current_state_json",
    "permissions_json",
    "metadata_json",
)
_EFFECT_JSON_COLUMNS = (
    "target_refs_json",
    "changed_fields_json",
    "effect_json",
)


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _normalize_datetime_value(value: Any) -> str | Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return value


def _parse_datetime_value(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "postgres.invalid_submission",
                f"{field_name} must be an ISO timestamp",
                details={"field": field_name},
            ) from exc
    else:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be an ISO timestamp",
            details={"field": field_name},
        )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_row(row: Any, *, json_columns: tuple[str, ...]) -> dict[str, Any]:
    payload = dict(row or {})
    for key in json_columns:
        if key in payload:
            payload[key] = _normalize_json_value(payload.get(key))
    for key in ("clock_time", "created_at", "updated_at"):
        if key in payload:
            payload[key] = _normalize_datetime_value(payload.get(key))
    return payload


def _normalize_rows(rows: Any, *, json_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return [_normalize_row(row, json_columns=json_columns) for row in rows or []]


def _environment_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    environment = dict(row)
    environment["seed_state"] = environment.pop("seed_state_json", {})
    environment["current_state"] = environment.pop("current_state_json", {})
    environment["permissions"] = environment.pop("permissions_json", {})
    environment["metadata"] = environment.pop("metadata_json", {})
    return environment


def _effect_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    effect = dict(row)
    effect["target_refs"] = effect.pop("target_refs_json", [])
    effect["changed_fields"] = effect.pop("changed_fields_json", {})
    effect["effect"] = effect.pop("effect_json", {})
    return effect


def persist_synthetic_environment(
    conn: Any,
    *,
    environment: dict[str, Any],
    effect: dict[str, Any] | None = None,
    receipt_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one environment head and optionally append one effect."""

    payload = dict(_require_mapping(environment, field_name="environment"))
    environment_ref = _require_text(payload.get("environment_ref"), field_name="environment.environment_ref")
    row = conn.fetchrow(
        """
        INSERT INTO synthetic_environments (
            environment_ref,
            namespace,
            source_dataset_ref,
            seed,
            lifecycle_state,
            clock_time,
            seed_state_digest,
            current_state_digest,
            record_count,
            current_record_count,
            dirty_record_count,
            seed_state_json,
            current_state_json,
            permissions_json,
            metadata_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6::timestamptz, $7, $8, $9, $10, $11,
            $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16, $17
        )
        ON CONFLICT (environment_ref) DO UPDATE SET
            namespace = EXCLUDED.namespace,
            source_dataset_ref = EXCLUDED.source_dataset_ref,
            seed = EXCLUDED.seed,
            lifecycle_state = EXCLUDED.lifecycle_state,
            clock_time = EXCLUDED.clock_time,
            seed_state_digest = EXCLUDED.seed_state_digest,
            current_state_digest = EXCLUDED.current_state_digest,
            record_count = EXCLUDED.record_count,
            current_record_count = EXCLUDED.current_record_count,
            dirty_record_count = EXCLUDED.dirty_record_count,
            seed_state_json = EXCLUDED.seed_state_json,
            current_state_json = EXCLUDED.current_state_json,
            permissions_json = EXCLUDED.permissions_json,
            metadata_json = EXCLUDED.metadata_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        environment_ref,
        _require_text(payload.get("namespace"), field_name="environment.namespace"),
        _require_text(payload.get("source_dataset_ref"), field_name="environment.source_dataset_ref"),
        _require_text(payload.get("seed"), field_name="environment.seed"),
        _require_text(payload.get("lifecycle_state"), field_name="environment.lifecycle_state"),
        _parse_datetime_value(payload.get("clock_time"), field_name="environment.clock_time"),
        _require_text(payload.get("seed_state_digest"), field_name="environment.seed_state_digest"),
        _require_text(payload.get("current_state_digest"), field_name="environment.current_state_digest"),
        int(payload.get("record_count") or 0),
        int(payload.get("current_record_count") or 0),
        int(payload.get("dirty_record_count") or 0),
        _encode_jsonb(payload.get("seed_state") or {}, field_name="environment.seed_state"),
        _encode_jsonb(payload.get("current_state") or {}, field_name="environment.current_state"),
        _encode_jsonb(payload.get("permissions") or {}, field_name="environment.permissions"),
        _encode_jsonb(payload.get("metadata") or {}, field_name="environment.metadata"),
        _optional_text(payload.get("observed_by_ref"), field_name="environment.observed_by_ref"),
        _optional_text(payload.get("source_ref"), field_name="environment.source_ref"),
    )
    if row is None:
        raise PostgresWriteError(
            "synthetic_environment.environment_write_failed",
            "synthetic environment insert returned no row",
        )
    if effect is not None:
        append_synthetic_environment_effect(conn, effect=effect, receipt_ref=receipt_ref)
    return _environment_row_to_domain(_normalize_row(row, json_columns=_ENVIRONMENT_JSON_COLUMNS))


def append_synthetic_environment_effect(
    conn: Any,
    *,
    effect: dict[str, Any],
    receipt_ref: str | None = None,
) -> dict[str, Any]:
    payload = dict(_require_mapping(effect, field_name="effect"))
    row = conn.fetchrow(
        """
        INSERT INTO synthetic_environment_effects (
            effect_ref,
            environment_ref,
            sequence_number,
            effect_type,
            action,
            event_ref,
            actor_ref,
            target_refs_json,
            before_state_digest,
            after_state_digest,
            changed_record_count,
            changed_fields_json,
            reversible,
            receipt_ref,
            effect_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12::jsonb,
            $13, $14, $15::jsonb
        )
        ON CONFLICT (effect_ref) DO UPDATE SET
            receipt_ref = COALESCE(synthetic_environment_effects.receipt_ref, EXCLUDED.receipt_ref)
        RETURNING *
        """,
        _require_text(payload.get("effect_ref"), field_name="effect.effect_ref"),
        _require_text(payload.get("environment_ref"), field_name="effect.environment_ref"),
        int(payload.get("sequence_number") or 0),
        _require_text(payload.get("effect_type"), field_name="effect.effect_type"),
        _require_text(payload.get("action"), field_name="effect.action"),
        _optional_text(payload.get("event_ref"), field_name="effect.event_ref"),
        _require_text(payload.get("actor_ref"), field_name="effect.actor_ref"),
        _encode_jsonb(payload.get("target_refs") or [], field_name="effect.target_refs"),
        _optional_text(payload.get("before_state_digest"), field_name="effect.before_state_digest"),
        _require_text(payload.get("after_state_digest"), field_name="effect.after_state_digest"),
        int(payload.get("changed_record_count") or 0),
        _encode_jsonb(payload.get("changed_fields") or {}, field_name="effect.changed_fields"),
        bool(payload.get("reversible")),
        _optional_text(receipt_ref, field_name="receipt_ref"),
        _encode_jsonb(payload.get("effect") or {}, field_name="effect.effect"),
    )
    if row is None:
        raise PostgresWriteError(
            "synthetic_environment.effect_write_failed",
            "synthetic environment effect insert returned no row",
        )
    return _effect_row_to_domain(_normalize_row(row, json_columns=_EFFECT_JSON_COLUMNS))


def load_synthetic_environment(
    conn: Any,
    *,
    environment_ref: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
          FROM synthetic_environments
         WHERE environment_ref = $1
        """,
        _require_text(environment_ref, field_name="environment_ref"),
    )
    if row is None:
        return None
    return _environment_row_to_domain(_normalize_row(row, json_columns=_ENVIRONMENT_JSON_COLUMNS))


def list_synthetic_environments(
    conn: Any,
    *,
    namespace: str | None = None,
    source_dataset_ref: str | None = None,
    lifecycle_state: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM synthetic_environments
         WHERE ($1::text IS NULL OR namespace = $1)
           AND ($2::text IS NULL OR source_dataset_ref = $2)
           AND ($3::text IS NULL OR lifecycle_state = $3)
         ORDER BY updated_at DESC
         LIMIT $4
        """,
        _optional_text(namespace, field_name="namespace"),
        _optional_text(source_dataset_ref, field_name="source_dataset_ref"),
        _optional_text(lifecycle_state, field_name="lifecycle_state"),
        int(limit),
    )
    return [
        _environment_row_to_domain(row)
        for row in _normalize_rows(rows, json_columns=_ENVIRONMENT_JSON_COLUMNS)
    ]


def list_synthetic_environment_effects(
    conn: Any,
    *,
    environment_ref: str,
    effect_type: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM synthetic_environment_effects
         WHERE environment_ref = $1
           AND ($2::text IS NULL OR effect_type = $2)
         ORDER BY sequence_number DESC
         LIMIT $3
        """,
        _require_text(environment_ref, field_name="environment_ref"),
        _optional_text(effect_type, field_name="effect_type"),
        int(limit),
    )
    return [
        _effect_row_to_domain(row)
        for row in _normalize_rows(rows, json_columns=_EFFECT_JSON_COLUMNS)
    ]


def next_synthetic_environment_effect_sequence(
    conn: Any,
    *,
    environment_ref: str,
) -> int:
    value = conn.fetchval(
        """
        SELECT COALESCE(MAX(sequence_number), 0) + 1
          FROM synthetic_environment_effects
         WHERE environment_ref = $1
        """,
        _require_text(environment_ref, field_name="environment_ref"),
    )
    return int(value or 1)


__all__ = [
    "append_synthetic_environment_effect",
    "list_synthetic_environment_effects",
    "list_synthetic_environments",
    "load_synthetic_environment",
    "next_synthetic_environment_effect_sequence",
    "persist_synthetic_environment",
]
