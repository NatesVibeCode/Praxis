"""Shared data-dictionary query contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

DATA_DICTIONARY_CONTRACT_VERSION = 1
DATA_DICTIONARY_QUERY_PATH = "/api/operator/data-dictionary"
DATA_DICTIONARY_AUTHORITY = "runtime.cqrs.queries.data_dictionary.handle_query_data_dictionary"
DATA_DICTIONARY_QUERY_MODEL = "runtime.cqrs.queries.data_dictionary.QueryDataDictionary"
DATA_DICTIONARY_TABLE_PROJECTION = "memory_entities"
DATA_DICTIONARY_RELATIONSHIP_PROJECTION = "memory_edges"
_RELATIONSHIP_KEYS = ("depends_on", "referenced_by")


def _normalize_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_list(value: object) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _normalize_iso(value: object) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    return None


def data_dictionary_contract_descriptor() -> dict[str, Any]:
    """Return the stable contract descriptor for data-dictionary responses."""

    return {
        "name": "data_dictionary",
        "version": DATA_DICTIONARY_CONTRACT_VERSION,
        "item_kind": "table",
        "authority": DATA_DICTIONARY_AUTHORITY,
        "query_model": DATA_DICTIONARY_QUERY_MODEL,
        "query_path": DATA_DICTIONARY_QUERY_PATH,
        "sources": {
            "table_projection": DATA_DICTIONARY_TABLE_PROJECTION,
            "relationship_projection": DATA_DICTIONARY_RELATIONSHIP_PROJECTION,
        },
        "response_fields": [
            "routed_to",
            "contract_version",
            "contract",
            "scope",
            "requested_table",
            "tables",
            "count",
            "total_tables",
            "generated_at",
            "freshness",
            "hint",
            "matches",
            "message",
        ],
        "table_fields": [
            "entity_id",
            "name",
            "summary",
            "columns",
            "column_count",
            "indexes",
            "triggers",
            "used_by",
            "approx_rows",
            "valid_values",
            "pg_notify_channels",
            "updated_at",
            "relationships",
            "relationship_counts",
            "lifecycle",
        ],
        "relationship_fields": ["table", "relation", "metadata"],
        "freshness_fields": [
            "generated_at",
            "projection_updated_at_min",
            "projection_updated_at_max",
        ],
    }


def build_data_dictionary_table(
    *,
    entity_id: str,
    name: str,
    summary: str | None,
    columns: object,
    column_count: int,
    indexes: object,
    triggers: object,
    used_by: object,
    approx_rows: object,
    valid_values: object,
    pg_notify_channels: object,
    updated_at: object,
    relationships: Mapping[str, Sequence[Mapping[str, Any]]] | None,
    detail_level: str,
) -> dict[str, Any]:
    """Normalize one table payload into the shared dictionary contract."""

    normalized_relationships = {
        key: [
            {
                "table": str(edge.get("table") or ""),
                "relation": str(edge.get("relation") or ""),
                "metadata": _normalize_mapping(edge.get("metadata")),
            }
            for edge in list((relationships or {}).get(key, []))
            if str(edge.get("table") or "").strip()
        ]
        for key in _RELATIONSHIP_KEYS
    }
    updated_at_iso = _normalize_iso(updated_at)
    return {
        "entity_id": entity_id,
        "name": name,
        "summary": summary,
        "columns": _normalize_list(columns),
        "column_count": max(0, int(column_count)),
        "indexes": _normalize_list(indexes),
        "triggers": _normalize_list(triggers),
        "used_by": _normalize_mapping(used_by),
        "approx_rows": approx_rows,
        "valid_values": _normalize_mapping(valid_values),
        "pg_notify_channels": _normalize_list(pg_notify_channels),
        "updated_at": updated_at_iso,
        "relationships": normalized_relationships,
        "relationship_counts": {
            key: len(normalized_relationships[key]) for key in _RELATIONSHIP_KEYS
        },
        "lifecycle": {
            "detail_level": detail_level,
            "projection_updated_at": updated_at_iso,
            "contract_version": DATA_DICTIONARY_CONTRACT_VERSION,
        },
    }


def build_data_dictionary_freshness(
    *,
    generated_at: datetime,
    projection_updated_ats: Sequence[object],
) -> dict[str, Any]:
    """Summarize freshness over the underlying dictionary projection."""

    projection_times = [
        iso_value
        for iso_value in (_normalize_iso(value) for value in projection_updated_ats)
        if iso_value is not None
    ]
    generated_at_iso = _normalize_iso(generated_at)
    return {
        "generated_at": generated_at_iso,
        "projection_updated_at_min": min(projection_times) if projection_times else None,
        "projection_updated_at_max": max(projection_times) if projection_times else None,
    }


def build_data_dictionary_response(
    *,
    scope: str,
    requested_table: str | None,
    tables: Sequence[Mapping[str, Any]],
    total_tables: int,
    generated_at: datetime,
    projection_updated_ats: Sequence[object],
    hint: str | None = None,
    matches: Sequence[str] | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    """Build the versioned top-level data-dictionary response envelope."""

    normalized_requested_table = (
        requested_table.strip() if isinstance(requested_table, str) and requested_table.strip() else None
    )
    generated_at_iso = _normalize_iso(generated_at)
    response: dict[str, Any] = {
        "routed_to": "data_dictionary",
        "contract_version": DATA_DICTIONARY_CONTRACT_VERSION,
        "contract": data_dictionary_contract_descriptor(),
        "scope": scope,
        "requested_table": normalized_requested_table,
        "tables": [dict(table) for table in tables],
        "count": len(tables),
        "total_tables": total_tables,
        "generated_at": generated_at_iso,
        "freshness": build_data_dictionary_freshness(
            generated_at=generated_at,
            projection_updated_ats=projection_updated_ats,
        ),
    }
    if hint:
        response["hint"] = hint
    if matches:
        response["matches"] = [str(match) for match in matches]
    if message:
        response["message"] = message
    return response
