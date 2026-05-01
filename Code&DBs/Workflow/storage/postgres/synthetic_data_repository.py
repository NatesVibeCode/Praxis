"""Postgres persistence for Synthetic Data authority."""

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


_DATASET_JSON_COLUMNS = (
    "source_object_truth_refs_json",
    "scenario_pack_refs_json",
    "object_counts_json",
    "name_plan_json",
    "generation_spec_json",
    "schema_contract_json",
    "quality_report_json",
    "permissions_json",
    "metadata_json",
)
_RECORD_JSON_COLUMNS = ("fields_json", "name_components_json", "lineage_json", "quality_flags_json")


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _normalize_row(row: Any, *, json_columns: tuple[str, ...]) -> dict[str, Any]:
    payload = dict(row or {})
    for key in json_columns:
        if key in payload:
            payload[key] = _normalize_json_value(payload.get(key))
    return payload


def _normalize_rows(rows: Any, *, json_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return [_normalize_row(row, json_columns=json_columns) for row in rows or []]


def _dataset_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    dataset = dict(row)
    dataset["source_object_truth_refs"] = dataset.pop("source_object_truth_refs_json", [])
    dataset["scenario_pack_refs"] = dataset.pop("scenario_pack_refs_json", [])
    dataset["object_counts"] = dataset.pop("object_counts_json", {})
    dataset["name_plan"] = dataset.pop("name_plan_json", {})
    dataset["generation_spec"] = dataset.pop("generation_spec_json", {})
    dataset["schema_contract"] = dataset.pop("schema_contract_json", {})
    dataset["quality_report"] = dataset.pop("quality_report_json", {})
    dataset["permissions"] = dataset.pop("permissions_json", {})
    dataset["metadata"] = dataset.pop("metadata_json", {})
    return dataset


def _record_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    record = dict(row)
    record["fields"] = record.pop("fields_json", {})
    record["name_components"] = record.pop("name_components_json", {})
    record["lineage"] = record.pop("lineage_json", {})
    record["quality_flags"] = record.pop("quality_flags_json", [])
    return record


def persist_synthetic_dataset(
    conn: Any,
    *,
    dataset: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one synthetic dataset and replace its record projection."""

    payload = dict(_require_mapping(dataset, field_name="dataset"))
    dataset_ref = _require_text(payload.get("dataset_ref"), field_name="dataset.dataset_ref")
    quality_report = dict(_require_mapping(payload.get("quality_report"), field_name="dataset.quality_report"))
    row = conn.fetchrow(
        """
        INSERT INTO synthetic_data_sets (
            dataset_ref,
            namespace,
            workflow_ref,
            source_context_ref,
            source_object_truth_refs_json,
            generator_ref,
            generator_version,
            seed,
            domain_pack,
            locale_ref,
            privacy_mode,
            evidence_tier,
            scenario_pack_refs_json,
            object_counts_json,
            record_count,
            quality_state,
            quality_score,
            name_plan_json,
            generation_spec_json,
            schema_contract_json,
            quality_report_json,
            permissions_json,
            metadata_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10,
            $11, $12, $13::jsonb, $14::jsonb, $15, $16, $17,
            $18::jsonb, $19::jsonb, $20::jsonb, $21::jsonb, $22::jsonb,
            $23::jsonb, $24, $25
        )
        ON CONFLICT (dataset_ref) DO UPDATE SET
            namespace = EXCLUDED.namespace,
            workflow_ref = EXCLUDED.workflow_ref,
            source_context_ref = EXCLUDED.source_context_ref,
            source_object_truth_refs_json = EXCLUDED.source_object_truth_refs_json,
            generator_ref = EXCLUDED.generator_ref,
            generator_version = EXCLUDED.generator_version,
            seed = EXCLUDED.seed,
            domain_pack = EXCLUDED.domain_pack,
            locale_ref = EXCLUDED.locale_ref,
            privacy_mode = EXCLUDED.privacy_mode,
            evidence_tier = EXCLUDED.evidence_tier,
            scenario_pack_refs_json = EXCLUDED.scenario_pack_refs_json,
            object_counts_json = EXCLUDED.object_counts_json,
            record_count = EXCLUDED.record_count,
            quality_state = EXCLUDED.quality_state,
            quality_score = EXCLUDED.quality_score,
            name_plan_json = EXCLUDED.name_plan_json,
            generation_spec_json = EXCLUDED.generation_spec_json,
            schema_contract_json = EXCLUDED.schema_contract_json,
            quality_report_json = EXCLUDED.quality_report_json,
            permissions_json = EXCLUDED.permissions_json,
            metadata_json = EXCLUDED.metadata_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        dataset_ref,
        _require_text(payload.get("namespace"), field_name="dataset.namespace"),
        _optional_text(payload.get("workflow_ref"), field_name="dataset.workflow_ref"),
        _optional_text(payload.get("source_context_ref"), field_name="dataset.source_context_ref"),
        _encode_jsonb(payload.get("source_object_truth_refs") or [], field_name="source_object_truth_refs"),
        _require_text(payload.get("generator_ref"), field_name="dataset.generator_ref"),
        _require_text(payload.get("generator_version"), field_name="dataset.generator_version"),
        _require_text(payload.get("seed"), field_name="dataset.seed"),
        _require_text(payload.get("domain_pack"), field_name="dataset.domain_pack"),
        _require_text(payload.get("locale_ref"), field_name="dataset.locale_ref"),
        _require_text(payload.get("privacy_mode"), field_name="dataset.privacy_mode"),
        _require_text(payload.get("evidence_tier"), field_name="dataset.evidence_tier"),
        _encode_jsonb(payload.get("scenario_pack_refs") or [], field_name="scenario_pack_refs"),
        _encode_jsonb(payload.get("object_counts") or {}, field_name="object_counts"),
        int(payload.get("record_count") or len(payload.get("records") or [])),
        _require_text(payload.get("quality_state") or quality_report.get("quality_state"), field_name="quality_state"),
        float(payload.get("quality_score") or quality_report.get("quality_score") or 0.0),
        _encode_jsonb(payload.get("name_plan") or {}, field_name="name_plan"),
        _encode_jsonb(payload.get("generation_spec") or {}, field_name="generation_spec"),
        _encode_jsonb(payload.get("schema_contract") or {}, field_name="schema_contract"),
        _encode_jsonb(quality_report, field_name="quality_report"),
        _encode_jsonb(payload.get("permissions") or {}, field_name="permissions"),
        _encode_jsonb(payload.get("metadata") or {}, field_name="metadata"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    if row is None:
        raise PostgresWriteError(
            "synthetic_data.dataset_write_failed",
            "synthetic dataset insert returned no row",
        )
    records = [dict(item) for item in payload.get("records") or []]
    conn.execute("DELETE FROM synthetic_data_records WHERE dataset_ref = $1", dataset_ref)
    if records:
        conn.execute_many(
            """
            INSERT INTO synthetic_data_records (
                record_ref,
                dataset_ref,
                object_kind,
                object_slug,
                ordinal,
                display_name,
                name_ref,
                fields_json,
                name_components_json,
                lineage_json,
                quality_flags_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb
            )
            ON CONFLICT (record_ref) DO UPDATE SET
                dataset_ref = EXCLUDED.dataset_ref,
                object_kind = EXCLUDED.object_kind,
                object_slug = EXCLUDED.object_slug,
                ordinal = EXCLUDED.ordinal,
                display_name = EXCLUDED.display_name,
                name_ref = EXCLUDED.name_ref,
                fields_json = EXCLUDED.fields_json,
                name_components_json = EXCLUDED.name_components_json,
                lineage_json = EXCLUDED.lineage_json,
                quality_flags_json = EXCLUDED.quality_flags_json,
                updated_at = now()
            """,
            [
                (
                    _require_text(record.get("record_ref"), field_name="record.record_ref"),
                    dataset_ref,
                    _require_text(record.get("object_kind"), field_name="record.object_kind"),
                    _require_text(record.get("object_slug"), field_name="record.object_slug"),
                    int(record.get("ordinal")),
                    _require_text(record.get("display_name"), field_name="record.display_name"),
                    _require_text(record.get("name_ref"), field_name="record.name_ref"),
                    _encode_jsonb(record.get("fields") or {}, field_name="record.fields"),
                    _encode_jsonb(record.get("name_components") or {}, field_name="record.name_components"),
                    _encode_jsonb(record.get("lineage") or {}, field_name="record.lineage"),
                    _encode_jsonb(record.get("quality_flags") or [], field_name="record.quality_flags"),
                )
                for record in records
            ],
        )
    persisted = _dataset_row_to_domain(_normalize_row(row, json_columns=_DATASET_JSON_COLUMNS))
    persisted["records"] = list_synthetic_records(conn, dataset_ref=dataset_ref, limit=min(len(records), 500))
    return persisted


def load_synthetic_dataset(
    conn: Any,
    *,
    dataset_ref: str,
    include_records: bool = True,
    limit: int = 500,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
          FROM synthetic_data_sets
         WHERE dataset_ref = $1
        """,
        _require_text(dataset_ref, field_name="dataset_ref"),
    )
    if row is None:
        return None
    dataset = _dataset_row_to_domain(_normalize_row(row, json_columns=_DATASET_JSON_COLUMNS))
    if include_records:
        dataset["records"] = list_synthetic_records(conn, dataset_ref=dataset_ref, limit=limit)
    return dataset


def list_synthetic_datasets(
    conn: Any,
    *,
    namespace: str | None = None,
    source_context_ref: str | None = None,
    quality_state: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM synthetic_data_sets
         WHERE ($1::text IS NULL OR namespace = $1)
           AND ($2::text IS NULL OR source_context_ref = $2)
           AND ($3::text IS NULL OR quality_state = $3)
         ORDER BY updated_at DESC
         LIMIT $4
        """,
        _optional_text(namespace, field_name="namespace"),
        _optional_text(source_context_ref, field_name="source_context_ref"),
        _optional_text(quality_state, field_name="quality_state"),
        int(limit),
    )
    return [
        _dataset_row_to_domain(row)
        for row in _normalize_rows(rows, json_columns=_DATASET_JSON_COLUMNS)
    ]


def list_synthetic_records(
    conn: Any,
    *,
    dataset_ref: str,
    object_kind: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT *
          FROM synthetic_data_records
         WHERE dataset_ref = $1
           AND ($2::text IS NULL OR object_kind = $2)
         ORDER BY object_kind ASC, ordinal ASC
         LIMIT $3
        """,
        _require_text(dataset_ref, field_name="dataset_ref"),
        _optional_text(object_kind, field_name="object_kind"),
        int(limit),
    )
    return [
        _record_row_to_domain(row)
        for row in _normalize_rows(rows, json_columns=_RECORD_JSON_COLUMNS)
    ]


__all__ = [
    "list_synthetic_datasets",
    "list_synthetic_records",
    "load_synthetic_dataset",
    "persist_synthetic_dataset",
]
