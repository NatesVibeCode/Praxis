"""Compiled object schema helpers over DB-native object field authority."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else str(value).strip() if value is not None else ""


def _json_value(value: Any, *, default: Any = None) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return value if value is not None else default


def _json_list(value: Any) -> list[Any]:
    value = _json_value(value, default=[])
    return list(value) if isinstance(value, list) else []


def _field_kind(value: Any) -> str:
    normalized = _text(value).lower()
    if normalized == "string":
        return "text"
    return normalized or "text"


def _normalize_field_row(row: dict[str, Any]) -> dict[str, Any]:
    default_value = _json_value(row.get("default_value"), default=None)
    normalized = {
        "name": _text(row.get("field_name")),
        "label": _text(row.get("label")) or _text(row.get("field_name")),
        "type": _field_kind(row.get("field_kind")),
        "description": _text(row.get("description")),
        "required": bool(row.get("required")),
        "default": default_value,
        "options": [str(item) for item in _json_list(row.get("options")) if isinstance(item, str)],
        "display_order": int(row.get("display_order") or 100),
        "retired": row.get("retired_at") is not None,
    }
    return normalized


def _property_definitions(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    for field in fields:
        item: dict[str, Any] = {
            "name": field["name"],
            "label": field["label"],
            "type": field["type"],
            "required": bool(field["required"]),
            "display_order": int(field.get("display_order") or 100),
        }
        if field.get("description"):
            item["description"] = field["description"]
        if field.get("default") is not None:
            item["default"] = field["default"]
        if field.get("options"):
            item["options"] = list(field["options"])
        definitions.append(item)
    return definitions


def _compile_object_type(
    type_row: dict[str, Any],
    field_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    compiled_fields = [_normalize_field_row(row) for row in field_rows]
    compiled_fields.sort(key=lambda item: (int(item.get("display_order") or 100), item["name"]))
    return {
        "type_id": _text(type_row.get("type_id")),
        "name": _text(type_row.get("name")),
        "description": _text(type_row.get("description")),
        "icon": _text(type_row.get("icon")),
        "created_at": type_row.get("created_at"),
        "fields": compiled_fields,
        "property_definitions": _property_definitions(compiled_fields),
    }


def load_compiled_object_type(conn: Any, *, type_id: str, include_retired: bool = False) -> dict[str, Any] | None:
    type_row = conn.fetchrow(
        "SELECT type_id, name, description, icon, created_at FROM object_types WHERE type_id = $1",
        _text(type_id),
    )
    if type_row is None:
        return None
    if include_retired:
        field_rows = conn.execute(
            """
            SELECT type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, retired_at
              FROM object_field_registry
             WHERE type_id = $1
             ORDER BY display_order ASC, field_name ASC
            """,
            _text(type_id),
        )
    else:
        field_rows = conn.execute(
            """
            SELECT type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, retired_at
              FROM object_field_registry
             WHERE type_id = $1
               AND retired_at IS NULL
             ORDER BY display_order ASC, field_name ASC
            """,
            _text(type_id),
        )
    return _compile_object_type(dict(type_row), [dict(row) for row in (field_rows or [])])


def list_compiled_object_types(
    conn: Any,
    *,
    query: str = "",
    limit: int = 100,
    include_retired: bool = False,
) -> list[dict[str, Any]]:
    normalized_query = _text(query)
    if normalized_query:
        type_rows = conn.execute(
            """
            SELECT type_id, name, description, icon, created_at
              FROM object_types
             WHERE search_vector @@ plainto_tsquery('english', $1)
             ORDER BY name
             LIMIT $2
            """,
            normalized_query,
            limit,
        )
    else:
        type_rows = conn.execute(
            """
            SELECT type_id, name, description, icon, created_at
              FROM object_types
             ORDER BY name
             LIMIT $1
            """,
            limit,
        )
    compiled_types = [dict(row) for row in (type_rows or [])]
    if not compiled_types:
        return []
    compiled_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if include_retired:
        field_rows = conn.execute(
            """
            SELECT type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, retired_at
              FROM object_field_registry
             ORDER BY type_id ASC, display_order ASC, field_name ASC
            """
        )
    else:
        field_rows = conn.execute(
            """
            SELECT type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, retired_at
              FROM object_field_registry
             WHERE retired_at IS NULL
             ORDER BY type_id ASC, display_order ASC, field_name ASC
            """
        )
    for row in field_rows or []:
        item = dict(row)
        compiled_by_type[_text(item.get("type_id"))].append(item)
    return [
        _compile_object_type(type_row, compiled_by_type.get(_text(type_row.get("type_id")), []))
        for type_row in compiled_types
    ]


def list_compiled_object_fields(
    conn: Any,
    *,
    type_id: str,
    include_retired: bool = False,
) -> list[dict[str, Any]]:
    compiled = load_compiled_object_type(conn, type_id=type_id, include_retired=include_retired)
    if compiled is None:
        return []
    return list(compiled.get("fields") or [])

