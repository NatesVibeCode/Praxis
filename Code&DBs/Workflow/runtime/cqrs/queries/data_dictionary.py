"""Query handlers for a DB-backed data dictionary projected into memory_entities."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from contracts.data_dictionary import (
    build_data_dictionary_response,
    build_data_dictionary_table,
)
from pydantic import BaseModel

from ..registry import registry


_TABLE_PREFIX = "table:"


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


def _as_table_name(entity_id: str) -> str | None:
    if entity_id.startswith(_TABLE_PREFIX):
        return entity_id[len(_TABLE_PREFIX):]
    return None


def _extract_edges_by_direction(conn, table_entity_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = conn.execute(
        "SELECT source_id, target_id, relation_type, metadata "
        "FROM memory_edges "
        "WHERE source_id = $1 OR target_id = $1",
        table_entity_id,
    )
    outgoing: list[dict[str, Any]] = []
    incoming: list[dict[str, Any]] = []
    for row in rows or []:
        relation = str(row["relation_type"] or "")
        metadata = _as_dict(row["metadata"])
        source_table = _as_table_name(str(row["source_id"] or ""))
        target_table = _as_table_name(str(row["target_id"] or ""))
        if row["source_id"] == table_entity_id and target_table:
            outgoing.append({
                "table": target_table,
                "relation": relation,
                "metadata": metadata,
            })
            continue
        if row["target_id"] == table_entity_id and source_table:
            incoming.append({
                "table": source_table,
                "relation": relation,
                "metadata": metadata,
            })
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


registry.register(
    path="/api/operator/data-dictionary",
    method="GET",
    command_class=QueryDataDictionary,
    handler=handle_query_data_dictionary,
    description=(
        "Read DB-backed table projections from memory_entities, including columns, "
        "indexes, triggers, valid values, and FK relationships."
    ),
)
