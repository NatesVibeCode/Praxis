"""Postgres persistence for object-truth evidence."""

from __future__ import annotations

import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "object_truth.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key in {"source_metadata", "object_version"}):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def persist_object_version(
    conn: Any,
    *,
    object_version: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one deterministic object-version packet and its field evidence."""

    version = dict(_require_mapping(object_version, field_name="object_version"))
    identity = dict(_require_mapping(version.get("identity"), field_name="object_version.identity"))
    object_version_digest = _require_text(
        version.get("object_version_digest"),
        field_name="object_version.object_version_digest",
    )
    field_observations = version.get("field_observations")
    if not isinstance(field_observations, list):
        raise PostgresWriteError(
            "object_truth.invalid_object_version",
            "object_version.field_observations must be a list",
            details={"field": "object_version.field_observations"},
        )

    row = conn.fetchrow(
        """
        INSERT INTO object_truth_object_versions (
            object_version_digest,
            object_version_ref,
            system_ref,
            object_ref,
            identity_digest,
            identity_values_json,
            payload_digest,
            schema_snapshot_digest,
            source_metadata_json,
            hierarchy_signals_json,
            object_version_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12, $13
        )
        ON CONFLICT (object_version_digest) DO UPDATE SET
            object_version_ref = EXCLUDED.object_version_ref,
            system_ref = EXCLUDED.system_ref,
            object_ref = EXCLUDED.object_ref,
            identity_digest = EXCLUDED.identity_digest,
            identity_values_json = EXCLUDED.identity_values_json,
            payload_digest = EXCLUDED.payload_digest,
            schema_snapshot_digest = EXCLUDED.schema_snapshot_digest,
            source_metadata_json = EXCLUDED.source_metadata_json,
            hierarchy_signals_json = EXCLUDED.hierarchy_signals_json,
            object_version_json = EXCLUDED.object_version_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            object_version_digest,
            object_version_ref,
            system_ref,
            object_ref,
            identity_digest,
            payload_digest,
            schema_snapshot_digest,
            source_metadata_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        object_version_digest,
        f"object_truth_object_version:{object_version_digest}",
        _require_text(version.get("system_ref"), field_name="object_version.system_ref"),
        _require_text(version.get("object_ref"), field_name="object_version.object_ref"),
        _require_text(identity.get("identity_digest"), field_name="object_version.identity.identity_digest"),
        _encode_jsonb(
            dict(_require_mapping(identity.get("identity_values"), field_name="object_version.identity.identity_values")),
            field_name="identity_values",
        ),
        _require_text(version.get("payload_digest"), field_name="object_version.payload_digest"),
        _optional_text(version.get("schema_snapshot_digest"), field_name="object_version.schema_snapshot_digest"),
        _encode_jsonb(
            dict(_require_mapping(version.get("source_metadata"), field_name="object_version.source_metadata")),
            field_name="source_metadata",
        ),
        _encode_jsonb(
            dict(_require_mapping(version.get("hierarchy_signals"), field_name="object_version.hierarchy_signals")),
            field_name="hierarchy_signals",
        ),
        _encode_jsonb(version, field_name="object_version"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )

    conn.execute(
        """
        DELETE FROM object_truth_field_observations
         WHERE object_version_digest = $1
        """,
        object_version_digest,
    )
    for observation in field_observations:
        obs = dict(_require_mapping(observation, field_name="field_observation"))
        conn.execute(
            """
            INSERT INTO object_truth_field_observations (
                object_version_digest,
                field_path,
                field_kind,
                presence,
                cardinality_kind,
                cardinality_count,
                sensitive,
                normalized_value_digest,
                redacted_value_preview_json,
                observation_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb
            )
            """,
            object_version_digest,
            _require_text(obs.get("field_path"), field_name="field_observation.field_path"),
            _require_text(obs.get("field_kind"), field_name="field_observation.field_kind"),
            _require_text(obs.get("presence"), field_name="field_observation.presence"),
            _require_text(obs.get("cardinality_kind"), field_name="field_observation.cardinality_kind"),
            obs.get("cardinality_count"),
            bool(obs.get("sensitive", False)),
            _require_text(
                obs.get("normalized_value_digest"),
                field_name="field_observation.normalized_value_digest",
            ),
            _encode_jsonb(obs.get("redacted_value_preview"), field_name="redacted_value_preview"),
            _encode_jsonb(obs, field_name="field_observation"),
        )

    persisted = _normalize_row(row, operation="persist_object_version")
    persisted["field_observation_count"] = len(field_observations)
    return persisted


def persist_schema_snapshot(
    conn: Any,
    *,
    schema_snapshot: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one deterministic normalized schema snapshot."""

    snapshot = dict(_require_mapping(schema_snapshot, field_name="schema_snapshot"))
    schema_digest = _require_text(
        snapshot.get("schema_digest"),
        field_name="schema_snapshot.schema_digest",
    )
    fields = snapshot.get("fields")
    if not isinstance(fields, list):
        raise PostgresWriteError(
            "object_truth.invalid_schema_snapshot",
            "schema_snapshot.fields must be a list",
            details={"field": "schema_snapshot.fields"},
        )

    row = conn.fetchrow(
        """
        INSERT INTO object_truth_schema_snapshots (
            schema_snapshot_digest,
            schema_snapshot_ref,
            system_ref,
            object_ref,
            field_count,
            schema_snapshot_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7, $8
        )
        ON CONFLICT (schema_snapshot_digest) DO UPDATE SET
            schema_snapshot_ref = EXCLUDED.schema_snapshot_ref,
            system_ref = EXCLUDED.system_ref,
            object_ref = EXCLUDED.object_ref,
            field_count = EXCLUDED.field_count,
            schema_snapshot_json = EXCLUDED.schema_snapshot_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            schema_snapshot_digest,
            schema_snapshot_ref,
            system_ref,
            object_ref,
            field_count,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        schema_digest,
        f"object_truth_schema_snapshot:{schema_digest}",
        _require_text(snapshot.get("system_ref"), field_name="schema_snapshot.system_ref"),
        _require_text(snapshot.get("object_ref"), field_name="schema_snapshot.object_ref"),
        len(fields),
        _encode_jsonb(snapshot, field_name="schema_snapshot"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    return _normalize_row(row, operation="persist_schema_snapshot")


def load_object_version(
    conn: Any,
    *,
    object_version_digest: str,
) -> dict[str, Any] | None:
    """Load a stored object-version packet by digest."""

    row = conn.fetchrow(
        """
        SELECT object_version_json
          FROM object_truth_object_versions
         WHERE object_version_digest = $1
        """,
        _require_text(object_version_digest, field_name="object_version_digest"),
    )
    if row is None:
        return None
    payload = dict(row).get("object_version_json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PostgresWriteError(
                "object_truth.invalid_stored_object_version",
                "stored object_version_json is not valid JSON",
            ) from exc
    if not isinstance(payload, dict):
        raise PostgresWriteError(
            "object_truth.invalid_stored_object_version",
            "stored object_version_json must be a JSON object",
        )
    return payload


def persist_comparison_run(
    conn: Any,
    *,
    comparison: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one deterministic comparison result between stored object versions."""

    payload = dict(_require_mapping(comparison, field_name="comparison"))
    comparison_digest = _require_text(
        payload.get("comparison_digest"),
        field_name="comparison.comparison_digest",
    )
    summary = dict(_require_mapping(payload.get("summary"), field_name="comparison.summary"))
    freshness = dict(_require_mapping(payload.get("freshness"), field_name="comparison.freshness"))

    row = conn.fetchrow(
        """
        INSERT INTO object_truth_comparison_runs (
            comparison_run_digest,
            comparison_run_ref,
            comparison_digest,
            left_object_version_digest,
            right_object_version_digest,
            left_identity_digest,
            right_identity_digest,
            summary_json,
            freshness_json,
            comparison_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11, $12
        )
        ON CONFLICT (comparison_run_digest) DO UPDATE SET
            comparison_run_ref = EXCLUDED.comparison_run_ref,
            comparison_digest = EXCLUDED.comparison_digest,
            left_object_version_digest = EXCLUDED.left_object_version_digest,
            right_object_version_digest = EXCLUDED.right_object_version_digest,
            left_identity_digest = EXCLUDED.left_identity_digest,
            right_identity_digest = EXCLUDED.right_identity_digest,
            summary_json = EXCLUDED.summary_json,
            freshness_json = EXCLUDED.freshness_json,
            comparison_json = EXCLUDED.comparison_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            comparison_run_digest,
            comparison_run_ref,
            comparison_digest,
            left_object_version_digest,
            right_object_version_digest,
            left_identity_digest,
            right_identity_digest,
            summary_json,
            freshness_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        comparison_digest,
        f"object_truth_comparison_run:{comparison_digest}",
        comparison_digest,
        _require_text(
            payload.get("left_object_version_digest"),
            field_name="comparison.left_object_version_digest",
        ),
        _require_text(
            payload.get("right_object_version_digest"),
            field_name="comparison.right_object_version_digest",
        ),
        _require_text(payload.get("left_identity_digest"), field_name="comparison.left_identity_digest"),
        _require_text(payload.get("right_identity_digest"), field_name="comparison.right_identity_digest"),
        _encode_jsonb(summary, field_name="comparison.summary"),
        _encode_jsonb(freshness, field_name="comparison.freshness"),
        _encode_jsonb(payload, field_name="comparison"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    return _normalize_row(row, operation="persist_comparison_run")


__all__ = [
    "load_object_version",
    "persist_comparison_run",
    "persist_object_version",
    "persist_schema_snapshot",
]
