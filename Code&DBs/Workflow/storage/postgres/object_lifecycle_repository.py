"""Explicit sync Postgres repository for object and document lifecycle writes."""

from __future__ import annotations

from typing import Any

from .validators import PostgresWriteError, _encode_jsonb, _require_text


def _row_dict(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "object_lifecycle.write_failed",
            f"{operation} returned no row",
        )
    return dict(row)


def _json_document(
    value: object,
    *,
    field_name: str,
    allow_list: bool = False,
) -> dict[str, Any] | list[Any]:
    if isinstance(value, dict):
        return dict(value)
    if allow_list and isinstance(value, list):
        return list(value)
    raise PostgresWriteError(
        "object_lifecycle.invalid_submission",
        (
            f"{field_name} must be an object or list"
            if allow_list
            else f"{field_name} must be an object"
        ),
        details={"field": field_name},
    )


def object_type_exists(conn: Any, *, type_id: str) -> bool:
    return bool(
        conn.fetchval(
            "SELECT 1 FROM object_types WHERE type_id = $1",
            _require_text(type_id, field_name="type_id"),
        )
    )


def load_object_type_record(conn: Any, *, type_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT type_id, name, description, icon, property_definitions, created_at "
        "FROM object_types WHERE type_id = $1",
        _require_text(type_id, field_name="type_id"),
    )
    return None if row is None else dict(row)


def create_object_type_record(
    conn: Any,
    *,
    type_id: str,
    name: str,
    description: str = "",
    icon: str = "",
    property_definitions: object | None = None,
) -> dict[str, Any]:
    row = conn.fetchrow(
        "INSERT INTO object_types (type_id, name, description, icon, property_definitions) "
        "VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING *",
        _require_text(type_id, field_name="type_id"),
        _require_text(name, field_name="name"),
        str(description or ""),
        str(icon or ""),
        _encode_jsonb(
            _json_document(
                property_definitions if property_definitions is not None else {},
                field_name="property_definitions",
                allow_list=True,
            ),
            field_name="property_definitions",
        ),
    )
    return _row_dict(row, operation="creating object type")


def upsert_object_type_record(
    conn: Any,
    *,
    type_id: str,
    name: str,
    description: str = "",
    icon: str = "",
    property_definitions: object | None = None,
) -> dict[str, Any]:
    row = conn.fetchrow(
        "INSERT INTO object_types (type_id, name, description, icon, property_definitions) "
        "VALUES ($1, $2, $3, $4, $5::jsonb) "
        "ON CONFLICT (type_id) DO UPDATE SET "
        "name = EXCLUDED.name, "
        "description = EXCLUDED.description, "
        "icon = EXCLUDED.icon, "
        "property_definitions = EXCLUDED.property_definitions "
        "RETURNING *",
        _require_text(type_id, field_name="type_id"),
        _require_text(name, field_name="name"),
        str(description or ""),
        str(icon or ""),
        _encode_jsonb(
            _json_document(
                property_definitions if property_definitions is not None else {},
                field_name="property_definitions",
                allow_list=True,
            ),
            field_name="property_definitions",
        ),
    )
    return _row_dict(row, operation="upserting object type")


def delete_object_type_record(
    conn: Any,
    *,
    type_id: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "DELETE FROM object_types WHERE type_id = $1 RETURNING type_id",
        _require_text(type_id, field_name="type_id"),
    )
    if row is None:
        return None
    return dict(row)


def ensure_object_type_record(
    conn: Any,
    *,
    type_id: str,
    name: str,
    description: str = "",
    icon: str = "",
    property_definitions: object | None = None,
) -> None:
    conn.execute(
        "INSERT INTO object_types (type_id, name, description, icon, property_definitions) "
        "VALUES ($1, $2, $3, $4, $5::jsonb) "
        "ON CONFLICT (type_id) DO NOTHING",
        _require_text(type_id, field_name="type_id"),
        _require_text(name, field_name="name"),
        str(description or ""),
        str(icon or ""),
        _encode_jsonb(
            _json_document(
                property_definitions if property_definitions is not None else {},
                field_name="property_definitions",
                allow_list=True,
            ),
            field_name="property_definitions",
        ),
    )


def create_object_record(
    conn: Any,
    *,
    object_id: str,
    type_id: str,
    properties: object | None = None,
) -> dict[str, Any]:
    row = conn.fetchrow(
        "INSERT INTO objects (object_id, type_id, properties) "
        "VALUES ($1, $2, $3::jsonb) RETURNING *",
        _require_text(object_id, field_name="object_id"),
        _require_text(type_id, field_name="type_id"),
        _encode_jsonb(
            _json_document(
                properties if properties is not None else {},
                field_name="properties",
            ),
            field_name="properties",
        ),
    )
    return _row_dict(row, operation="creating object")


def load_object_record(conn: Any, *, object_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT object_id, type_id, properties, status, created_at, updated_at "
        "FROM objects WHERE object_id = $1",
        _require_text(object_id, field_name="object_id"),
    )
    return None if row is None else dict(row)


def update_object_properties_record(
    conn: Any,
    *,
    object_id: str,
    properties: object,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "UPDATE objects SET properties = properties || $2::jsonb, updated_at = now() "
        "WHERE object_id = $1 RETURNING *",
        _require_text(object_id, field_name="object_id"),
        _encode_jsonb(
            _json_document(properties, field_name="properties"),
            field_name="properties",
        ),
    )
    return dict(row) if row is not None else None


def mark_object_deleted(conn: Any, *, object_id: str) -> None:
    conn.execute(
        "UPDATE objects SET status = 'deleted', updated_at = now() WHERE object_id = $1",
        _require_text(object_id, field_name="object_id"),
    )


def attach_document_record(
    conn: Any,
    *,
    document_id: str,
    card_id: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "UPDATE objects "
        "SET properties = jsonb_set("
        "properties, "
        "'{attached_to}', "
        "(COALESCE(properties->'attached_to', '[]'::jsonb) || to_jsonb($2::text))"
        "), "
        "updated_at = now() "
        "WHERE object_id = $1 AND type_id = 'doc_type_document' "
        "RETURNING object_id, properties",
        _require_text(document_id, field_name="document_id"),
        _require_text(card_id, field_name="card_id"),
    )
    return dict(row) if row is not None else None
