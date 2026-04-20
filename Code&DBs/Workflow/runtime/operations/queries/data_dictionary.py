"""Query handlers for the unified data dictionary.

Two flows live here:
- Legacy `QueryDataDictionary(table_name=...)` — reads the per-table projection
  in `memory_entities` (kept so existing callers continue to work).
- New `QueryDataDictionaryObject(object_kind=...)` — reads the merged
  `data_dictionary_effective` view and returns {object, fields, layers}.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from contracts.data_dictionary import (
    build_data_dictionary_response,
    build_data_dictionary_table,
)
from pydantic import BaseModel
from runtime.data_dictionary import (
    DataDictionaryBoundaryError,
    describe_object,
    list_object_kinds,
)
_ENTITY_PREFIX_SEPARATORS = ("::", ":")


def _as_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _as_iso(value: object) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return None


def _entity_kind(entity_id: str) -> str:
    for separator in _ENTITY_PREFIX_SEPARATORS:
        if separator in entity_id:
            return entity_id.split(separator, 1)[0]
    return ""


def _entity_label(entity_id: str) -> str:
    for separator in _ENTITY_PREFIX_SEPARATORS:
        if separator in entity_id:
            return entity_id.split(separator, 1)[1]
    return entity_id


def _load_entity_contexts(conn: Any, entity_ids: list[str]) -> dict[str, dict[str, Any]]:
    ids = [entity_id for entity_id in dict.fromkeys(entity_ids) if entity_id]
    if not ids:
        return {}
    rows = conn.execute(
        "SELECT id, entity_type, name, content FROM memory_entities WHERE id = ANY($1::text[])",
        ids,
    )
    contexts: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        contexts[str(row["id"])] = {
            "entity_id": str(row["id"]),
            "entity_type": str(row.get("entity_type") or ""),
            "name": str(row.get("name") or ""),
            "summary": str(row.get("content") or ""),
        }
    return contexts


def _extract_edges_by_direction(
    conn,
    table_entity_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = conn.execute(
        "SELECT source_id, target_id, relation_type, metadata "
        "FROM memory_edges "
        "WHERE source_id = $1 OR target_id = $1",
        table_entity_id,
    )
    entity_contexts = _load_entity_contexts(
        conn,
        [row["source_id"] for row in rows or []] + [row["target_id"] for row in rows or []],
    )

    def _edge_payload(
        *,
        source_id: str,
        target_id: str,
        relation: str,
        metadata: dict[str, Any],
        direction: str,
        other_id: str,
    ) -> dict[str, Any]:
        context = entity_contexts.get(other_id, {})
        name = str(context.get("name") or _entity_label(other_id) or other_id)
        return {
            "entity_id": other_id,
            "entity_type": str(context.get("entity_type") or _entity_kind(other_id) or ""),
            "name": name,
            "summary": str(context.get("summary") or ""),
            "table": name,
            "relation": relation,
            "direction": direction,
            "source_id": source_id,
            "target_id": target_id,
            "metadata": metadata,
        }

    outgoing: list[dict[str, Any]] = []
    incoming: list[dict[str, Any]] = []
    for row in rows or []:
        relation = str(row["relation_type"] or "")
        metadata = _as_dict(row["metadata"])
        source_id = str(row["source_id"] or "")
        target_id = str(row["target_id"] or "")
        if source_id == table_entity_id:
            outgoing.append(
                _edge_payload(
                    source_id=source_id,
                    target_id=target_id,
                    relation=relation,
                    metadata=metadata,
                    direction="depends_on",
                    other_id=target_id,
                )
            )
            continue
        if target_id == table_entity_id:
            incoming.append(
                _edge_payload(
                    source_id=source_id,
                    target_id=target_id,
                    relation=relation,
                    metadata=metadata,
                    direction="referenced_by",
                    other_id=source_id,
                )
            )
    return outgoing, incoming


def _pick_candidate_rows(
    rows: list[dict[str, Any]],
    table_name: str | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not table_name:
        return rows, []

    normalized = table_name.strip().lower().strip("`\"' ")
    if not normalized:
        return rows, []

    exact: list[dict[str, Any]] = []
    fuzzy: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("name") or "").strip().lower()
        if normalized == name:
            exact.append(row)
            continue
        if normalized in name:
            fuzzy.append(row)

    if exact:
        return exact, []
    if fuzzy:
        return fuzzy[:20], [str(r["name"]) for r in fuzzy[:20]]
    return [], []


def _build_overview(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _as_dict(row["metadata"])
    columns = metadata.get("columns", [])
    return build_data_dictionary_table(
        entity_id=str(row["id"]),
        name=str(row["name"]),
        summary=row.get("content"),
        columns=[],
        column_count=len(columns) if isinstance(columns, list) else 0,
        indexes=[],
        triggers=[],
        used_by={},
        approx_rows=metadata.get("approx_rows", 0),
        valid_values=metadata.get("valid_values", {}),
        pg_notify_channels=[],
        updated_at=_as_iso(row.get("updated_at")),
        relationships={},
        detail_level="overview",
    )


def _build_detail(
    conn: Any,
    row: dict[str, Any],
    include_relationships: bool = True,
) -> dict[str, Any]:
    metadata = _as_dict(row["metadata"])
    relationships: dict[str, Any] = {}
    if include_relationships:
        entity_id = f"table:{row['name']}"
        outgoing, incoming = _extract_edges_by_direction(conn, entity_id)
        if outgoing:
            relationships["depends_on"] = outgoing
        if incoming:
            relationships["referenced_by"] = incoming
    columns = metadata.get("columns", [])
    return build_data_dictionary_table(
        entity_id=str(row["id"]),
        name=str(row["name"]),
        summary=row.get("content"),
        columns=columns,
        column_count=len(columns) if isinstance(columns, list) else 0,
        indexes=metadata.get("indexes", []),
        triggers=metadata.get("triggers", []),
        used_by=metadata.get("used_by", {}),
        approx_rows=metadata.get("approx_rows", 0),
        valid_values=metadata.get("valid_values", {}),
        pg_notify_channels=metadata.get("pg_notify_channels", []),
        updated_at=_as_iso(row.get("updated_at")),
        relationships=relationships,
        detail_level="detail",
    )


class QueryDataDictionary(BaseModel):
    table_name: str | None = None
    include_relationships: bool = True


def handle_query_data_dictionary(
    query: QueryDataDictionary,
    subsystems: Any,
) -> dict[str, Any]:
    env = getattr(subsystems, "_postgres_env", None)
    resolved_env = env() if callable(env) else None

    conn = None
    if hasattr(subsystems, "get_pg_conn") and callable(subsystems.get_pg_conn):
        conn = subsystems.get_pg_conn()
    elif resolved_env is not None:
        conn = resolved_env.get("pg_conn")
    if conn is None:
        raise RuntimeError("No Postgres connection available for data dictionary query.")

    rows = conn.execute(
        "SELECT id, name, content, metadata, updated_at "
        "FROM memory_entities "
        "WHERE entity_type = 'table' AND NOT archived "
        "ORDER BY name",
    )
    generated_at = datetime.now(timezone.utc)
    projection_updated_ats = [row.get("updated_at") for row in rows or []]
    if not rows:
        return build_data_dictionary_response(
            scope="empty",
            requested_table=query.table_name,
            tables=[],
            total_tables=0,
            generated_at=generated_at,
            projection_updated_ats=projection_updated_ats,
            message="No table entities are currently projected. Run heartbeat to refresh schema metadata.",
        )

    table_rows = [
        {
            "id": row["id"],
            "name": row["name"],
            "content": row["content"],
            "metadata": row["metadata"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]
    selected_rows, ambiguous = _pick_candidate_rows(table_rows, query.table_name)

    if not selected_rows:
        matches = [row["name"] for row in table_rows]
        return build_data_dictionary_response(
            scope="missing",
            requested_table=query.table_name,
            tables=[],
            total_tables=len(table_rows),
            generated_at=generated_at,
            projection_updated_ats=projection_updated_ats,
            hint="No exact table match. Try one of: " + ", ".join(matches[:20]),
        )

    # Detail mode when a specific table resolves cleanly.
    if query.table_name and len(selected_rows) == 1:
        result_tables = [
            _build_detail(conn, selected_rows[0], query.include_relationships)
        ]
        scope = "table"
    else:
        result_tables = [_build_overview(row) for row in selected_rows]
        scope = "tables"

    response = build_data_dictionary_response(
        scope=scope,
        requested_table=query.table_name,
        tables=result_tables,
        total_tables=len(table_rows),
        generated_at=generated_at,
        projection_updated_ats=projection_updated_ats,
    )
    if ambiguous:
        response["hint"] = (
            f"Multiple tables matched '{query.table_name}'. "
            "Use an exact table name for detailed schema."
        )
        response["matches"] = ambiguous
    return response


class QueryDataDictionaryObject(BaseModel):
    object_kind: str | None = None
    category: str | None = None
    include_layers: bool = False


def _resolve_conn(subsystems: Any) -> Any:
    env = getattr(subsystems, "_postgres_env", None)
    resolved_env = env() if callable(env) else None
    if hasattr(subsystems, "get_pg_conn") and callable(subsystems.get_pg_conn):
        conn = subsystems.get_pg_conn()
    elif resolved_env is not None:
        conn = resolved_env.get("pg_conn")
    else:
        conn = None
    if conn is None:
        raise RuntimeError("No Postgres connection available for data dictionary query.")
    return conn


def handle_query_data_dictionary_object(
    query: QueryDataDictionaryObject,
    subsystems: Any,
) -> dict[str, Any]:
    """Query the unified data dictionary by object_kind.

    - With no `object_kind`, returns the catalog of object kinds (optionally
      filtered by `category`).
    - With `object_kind`, returns the merged field list (operator > inferred >
      auto) and, optionally, the raw per-source layers.
    """
    conn = _resolve_conn(subsystems)
    generated_at = datetime.now(timezone.utc).isoformat()
    try:
        if not (query.object_kind and query.object_kind.strip()):
            objects = list_object_kinds(conn, category=query.category)
            return {
                "routed_to": "data_dictionary_object",
                "scope": "catalog",
                "generated_at": generated_at,
                "objects": objects,
                "count": len(objects),
                "category": query.category,
            }
        payload = describe_object(
            conn,
            object_kind=query.object_kind,
            include_layers=query.include_layers,
        )
        return {
            "routed_to": "data_dictionary_object",
            "scope": "object",
            "generated_at": generated_at,
            "object_kind": query.object_kind,
            **payload,
        }
    except DataDictionaryBoundaryError as exc:
        return {
            "routed_to": "data_dictionary_object",
            "scope": "error",
            "generated_at": generated_at,
            "error": str(exc),
            "status_code": exc.status_code,
        }
