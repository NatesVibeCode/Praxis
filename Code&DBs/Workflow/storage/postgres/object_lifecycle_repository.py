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
        "SELECT type_id, name, description, icon, created_at "
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
) -> dict[str, Any]:
    row = conn.fetchrow(
        "INSERT INTO object_types (type_id, name, description, icon) "
        "VALUES ($1, $2, $3, $4) RETURNING *",
        _require_text(type_id, field_name="type_id"),
        _require_text(name, field_name="name"),
        str(description or ""),
        str(icon or ""),
    )
    return _row_dict(row, operation="creating object type")


def upsert_object_type_record(
    conn: Any,
    *,
    type_id: str,
    name: str,
    description: str = "",
    icon: str = "",
) -> dict[str, Any]:
    row = conn.fetchrow(
        "INSERT INTO object_types (type_id, name, description, icon) "
        "VALUES ($1, $2, $3, $4) "
        "ON CONFLICT (type_id) DO UPDATE SET "
        "name = EXCLUDED.name, "
        "description = EXCLUDED.description, "
        "icon = EXCLUDED.icon "
        "RETURNING *",
        _require_text(type_id, field_name="type_id"),
        _require_text(name, field_name="name"),
        str(description or ""),
        str(icon or ""),
    )
    return _row_dict(row, operation="upserting object type")


def replace_object_field_records(
    conn: Any,
    *,
    type_id: str,
    fields: list[dict[str, Any]],
    binding_revision: str = "object_schema.fields.v1",
    decision_ref: str = "object_schema.field_registry.runtime_owner",
) -> None:
    normalized_type_id = _require_text(type_id, field_name="type_id")
    conn.execute(
        "DELETE FROM object_field_registry WHERE type_id = $1",
        normalized_type_id,
    )
    if not fields:
        return
    rows: list[tuple[Any, ...]] = []
    for index, field in enumerate(fields):
        rows.append(
            (
                normalized_type_id,
                _require_text(
                    field.get("name") or field.get("field_name"),
                    field_name=f"fields[{index}].name",
                ),
                str(field.get("label") or field.get("name") or field.get("field_name") or ""),
                _require_text(field.get("type") or field.get("field_kind"), field_name=f"fields[{index}].type"),
                str(field.get("description") or ""),
                bool(field.get("required")),
                _encode_jsonb(field.get("default"), field_name=f"fields[{index}].default"),
                _encode_jsonb(
                    _json_document(field.get("options") or [], field_name=f"fields[{index}].options", allow_list=True),
                    field_name=f"fields[{index}].options",
                ),
                int(field.get("display_order") or (index + 1) * 10),
                str(field.get("binding_revision") or binding_revision),
                str(field.get("decision_ref") or decision_ref),
            )
        )
    conn.execute_many(
        """
        INSERT INTO object_field_registry (
            type_id,
            field_name,
            label,
            field_kind,
            description,
            required,
            default_value,
            options,
            display_order,
            binding_revision,
            decision_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11)
        """,
        rows,
    )


def upsert_object_field_record(
    conn: Any,
    *,
    type_id: str,
    field_name: str,
    label: str = "",
    field_kind: str,
    description: str = "",
    required: bool = False,
    default_value: object | None = None,
    options: object | None = None,
    display_order: int = 100,
    binding_revision: str = "object_schema.fields.v1",
    decision_ref: str = "object_schema.field_registry.runtime_owner",
) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        INSERT INTO object_field_registry (
            type_id,
            field_name,
            label,
            field_kind,
            description,
            required,
            default_value,
            options,
            display_order,
            binding_revision,
            decision_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11)
        ON CONFLICT (type_id, field_name) DO UPDATE SET
            label = EXCLUDED.label,
            field_kind = EXCLUDED.field_kind,
            description = EXCLUDED.description,
            required = EXCLUDED.required,
            default_value = EXCLUDED.default_value,
            options = EXCLUDED.options,
            display_order = EXCLUDED.display_order,
            binding_revision = EXCLUDED.binding_revision,
            decision_ref = EXCLUDED.decision_ref,
            retired_at = NULL,
            updated_at = now()
        RETURNING type_id, field_name, label, field_kind, description, required,
                  default_value, options, display_order, binding_revision, decision_ref, retired_at
        """,
        _require_text(type_id, field_name="type_id"),
        _require_text(field_name, field_name="field_name"),
        str(label or ""),
        _require_text(field_kind, field_name="field_kind"),
        str(description or ""),
        bool(required),
        _encode_jsonb(default_value, field_name="default_value"),
        _encode_jsonb(
            _json_document(options if options is not None else [], field_name="options", allow_list=True),
            field_name="options",
        ),
        int(display_order),
        str(binding_revision),
        str(decision_ref),
    )
    return _row_dict(row, operation="upserting object field")


def list_object_field_records(
    conn: Any,
    *,
    type_id: str,
    include_retired: bool = False,
) -> list[dict[str, Any]]:
    normalized_type_id = _require_text(type_id, field_name="type_id")
    if include_retired:
        rows = conn.execute(
            """
            SELECT type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, binding_revision, decision_ref, retired_at
              FROM object_field_registry
             WHERE type_id = $1
             ORDER BY retired_at NULLS FIRST, display_order ASC, field_name ASC
            """,
            normalized_type_id,
        )
    else:
        rows = conn.execute(
            """
            SELECT type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, binding_revision, decision_ref, retired_at
              FROM object_field_registry
             WHERE type_id = $1
               AND retired_at IS NULL
             ORDER BY display_order ASC, field_name ASC
            """,
            normalized_type_id,
        )
    return [dict(row) for row in rows or []]


def retire_object_field_record(
    conn: Any,
    *,
    type_id: str,
    field_name: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        UPDATE object_field_registry
           SET retired_at = now(),
               updated_at = now()
         WHERE type_id = $1
           AND field_name = $2
           AND retired_at IS NULL
         RETURNING type_id, field_name, label, field_kind, description, required,
                   default_value, options, display_order, binding_revision, decision_ref, retired_at
        """,
        _require_text(type_id, field_name="type_id"),
        _require_text(field_name, field_name="field_name"),
    )
    return None if row is None else dict(row)


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
    fields: list[dict[str, Any]] | None = None,
) -> None:
    normalized_type_id = _require_text(type_id, field_name="type_id")
    missing = not object_type_exists(conn, type_id=normalized_type_id)
    conn.execute(
        "INSERT INTO object_types (type_id, name, description, icon) "
        "VALUES ($1, $2, $3, $4) "
        "ON CONFLICT (type_id) DO NOTHING",
        normalized_type_id,
        _require_text(name, field_name="name"),
        str(description or ""),
        str(icon or ""),
    )
    if missing:
        replace_object_field_records(
            conn,
            type_id=normalized_type_id,
            fields=list(fields or []),
            binding_revision="object_schema.fields.ensure.v1",
            decision_ref="object_schema.field_registry.ensure_owner",
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
