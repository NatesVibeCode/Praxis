"""Runtime ownership for object and document lifecycle mutations."""

from __future__ import annotations

import json
import uuid
from typing import Any

from runtime.object_schema import (
    list_compiled_object_fields,
    list_compiled_object_types,
    load_compiled_object_type,
)
from storage.postgres.object_lifecycle_repository import (
    attach_document_record,
    create_object_record,
    create_object_type_record,
    delete_object_type_record,
    load_object_record,
    mark_object_deleted,
    object_type_exists,
    replace_object_field_records,
    retire_object_field_record,
    upsert_object_field_record,
    upsert_object_type_record,
    update_object_properties_record,
)
from storage.postgres.validators import PostgresWriteError

_ALLOWED_DOCUMENT_TYPES = frozenset(
    {
        "policy",
        "sop",
        "evidence",
        "context",
        "reference",
    }
)
_ALLOWED_OBJECT_FIELD_KINDS = frozenset(
    {"text", "number", "boolean", "enum", "json", "date", "datetime", "reference"}
)


class ObjectLifecycleBoundaryError(RuntimeError):
    """Raised when object/document lifecycle ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _properties(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ObjectLifecycleBoundaryError(f"{field_name} must be an object")
    return dict(value)


def _field_kind(value: Any, *, field_name: str) -> str:
    normalized = _text(value).lower()
    if not normalized:
        raise ObjectLifecycleBoundaryError(f"{field_name} is required")
    aliases = {
        "string": "text",
        "str": "text",
        "varchar": "text",
        "integer": "number",
        "int": "number",
        "float": "number",
        "double": "number",
        "decimal": "number",
        "bool": "boolean",
        "object": "json",
        "array": "json",
        "list": "json",
        "map": "json",
        "dict": "json",
        "jsonb": "json",
        "timestamp": "datetime",
        "ref": "reference",
    }
    canonical = aliases.get(normalized, normalized)
    if canonical not in _ALLOWED_OBJECT_FIELD_KINDS:
        raise ObjectLifecycleBoundaryError(
            f"{field_name} must be one of: {', '.join(sorted(_ALLOWED_OBJECT_FIELD_KINDS))}"
        )
    return canonical


def _coerce_field_definition(*, value: Any, index: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ObjectLifecycleBoundaryError(
            f"fields[{index}] must be an object"
        )
    field_name = _text(value.get("field_name")) or _text(value.get("name"))
    if not field_name:
        raise ObjectLifecycleBoundaryError(
            f"fields[{index}].name is required"
        )
    normalized: dict[str, Any] = {
        "name": field_name,
        "label": _text(value.get("label")) or field_name,
        "type": _field_kind(value.get("field_kind") or value.get("type"), field_name=f"fields[{index}].type"),
        "description": _text(value.get("description")),
        "display_order": int(value.get("display_order") or (index + 1) * 10),
    }
    if "required" in value:
        normalized["required"] = bool(value.get("required"))
    if normalized["type"] == "enum" or "options" in value or "values" in value:
        options = value.get("options")
        if options is None:
            options = value.get("values")
        if not isinstance(options, list) or not all(isinstance(item, str) for item in options):
            raise ObjectLifecycleBoundaryError(
                f"fields[{index}].options must be a list of strings"
            )
        normalized["options"] = list(options)
    if "default" in value and value.get("default") is not None:
        normalized["default"] = value.get("default")
    return normalized


def _fields(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, dict) and isinstance(value.get("fields"), list):
        return [_coerce_field_definition(value=item, index=index) for index, item in enumerate(value.get("fields") or [])]
    if isinstance(value, list):
        return [_coerce_field_definition(value=item, index=index) for index, item in enumerate(value)]
    if isinstance(value, dict):
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(value.values()):
            normalized.append(_coerce_field_definition(value=item, index=index))
        return normalized
    raise ObjectLifecycleBoundaryError("fields must be an object or list")


def _string_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ObjectLifecycleBoundaryError(f"{field_name} must be a list of strings")
    return list(value)


def _slug_prefix(name: str) -> str:
    normalized = "-".join(segment for segment in name.strip().lower().split() if segment)
    return normalized or "object-type"


def _parse_properties(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise ObjectLifecycleBoundaryError(str(exc), status_code=status_code) from exc


def create_object_type(
    conn: Any,
    *,
    name: Any,
    description: Any = "",
    fields: Any = None,
    icon: Any = "",
) -> dict[str, Any]:
    normalized_name = _text(name)
    if not normalized_name:
        raise ObjectLifecycleBoundaryError("name is required")
    if description is not None and not isinstance(description, str):
        raise ObjectLifecycleBoundaryError("description must be a string")
    if icon is not None and not isinstance(icon, str):
        raise ObjectLifecycleBoundaryError("icon must be a string")
    normalized_fields = _fields(fields)

    try:
        row = create_object_type_record(
            conn,
            type_id=f"{_slug_prefix(normalized_name)}-{uuid.uuid4().hex[:6]}",
            name=normalized_name,
            description=description or "",
            icon=icon or "",
        )
        replace_object_field_records(
            conn,
            type_id=row["type_id"],
            fields=normalized_fields,
        )
        created = load_compiled_object_type(conn, type_id=row["type_id"])
        if created is None:
            raise ObjectLifecycleBoundaryError("created object type could not be reloaded", status_code=500)
        return created
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def upsert_object_type(
    conn: Any,
    *,
    type_id: Any = None,
    name: Any,
    description: Any = "",
    fields: Any = None,
    icon: Any = "",
) -> dict[str, Any]:
    normalized_name = _text(name)
    if not normalized_name:
        raise ObjectLifecycleBoundaryError("name is required")
    if description is not None and not isinstance(description, str):
        raise ObjectLifecycleBoundaryError("description must be a string")
    if icon is not None and not isinstance(icon, str):
        raise ObjectLifecycleBoundaryError("icon must be a string")
    normalized_type_id = _text(type_id) or f"{_slug_prefix(normalized_name)}-{uuid.uuid4().hex[:6]}"
    normalized_fields = _fields(fields)

    try:
        upsert_object_type_record(
            conn,
            type_id=normalized_type_id,
            name=normalized_name,
            description=description or "",
            icon=icon or "",
        )
        replace_object_field_records(
            conn,
            type_id=normalized_type_id,
            fields=normalized_fields,
        )
        updated = load_compiled_object_type(conn, type_id=normalized_type_id)
        if updated is None:
            raise ObjectLifecycleBoundaryError("upserted object type could not be reloaded", status_code=500)
        return updated
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def create_object(
    conn: Any,
    *,
    type_id: Any,
    properties: Any,
) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")
    normalized_properties = _properties(properties, field_name="properties")

    try:
        if not object_type_exists(conn, type_id=normalized_type_id):
            raise ObjectLifecycleBoundaryError(f"Object type not found: {normalized_type_id}", status_code=404)
        return create_object_record(
            conn,
            object_id="obj-" + uuid.uuid4().hex[:12],
            type_id=normalized_type_id,
            properties=normalized_properties,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def update_object(
    conn: Any,
    *,
    object_id: Any,
    properties: Any,
) -> dict[str, Any]:
    normalized_object_id = _text(object_id)
    if not normalized_object_id:
        raise ObjectLifecycleBoundaryError("object_id is required")
    normalized_properties = _properties(properties, field_name="properties")

    try:
        row = update_object_properties_record(
            conn,
            object_id=normalized_object_id,
            properties=normalized_properties,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise ObjectLifecycleBoundaryError(f"Object not found: {normalized_object_id}", status_code=404)
    return row


def get_object_type(conn: Any, *, type_id: Any) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")
    row = load_compiled_object_type(conn, type_id=normalized_type_id)
    if row is None:
        raise ObjectLifecycleBoundaryError(f"Object type not found: {normalized_type_id}", status_code=404)
    return row


def delete_object_type(conn: Any, *, type_id: Any) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")

    try:
        row = delete_object_type_record(conn, type_id=normalized_type_id)
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise ObjectLifecycleBoundaryError(
            f"Object type not found: {normalized_type_id}",
            status_code=404,
        )
    return {"type_id": row["type_id"], "deleted": True}


def list_object_types(
    conn: Any,
    *,
    query: Any = "",
    limit: int = 100,
) -> dict[str, Any]:
    if not isinstance(limit, int) or limit <= 0:
        raise ObjectLifecycleBoundaryError("limit must be a positive integer")
    rows = list_compiled_object_types(conn, query=_text(query), limit=limit)
    return {"types": rows, "count": len(rows)}


def list_object_fields(
    conn: Any,
    *,
    type_id: Any,
    include_retired: Any = False,
) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")
    if load_compiled_object_type(conn, type_id=normalized_type_id, include_retired=True) is None:
        raise ObjectLifecycleBoundaryError(f"Object type not found: {normalized_type_id}", status_code=404)
    fields = list_compiled_object_fields(conn, type_id=normalized_type_id, include_retired=bool(include_retired))
    return {"type_id": normalized_type_id, "fields": fields, "count": len(fields)}


def upsert_object_field(
    conn: Any,
    *,
    type_id: Any,
    field_name: Any,
    field_kind: Any,
    label: Any = "",
    description: Any = "",
    required: Any = False,
    default_value: Any = None,
    options: Any = None,
    display_order: Any = 100,
) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")
    if not object_type_exists(conn, type_id=normalized_type_id):
        raise ObjectLifecycleBoundaryError(f"Object type not found: {normalized_type_id}", status_code=404)
    try:
        row = upsert_object_field_record(
            conn,
            type_id=normalized_type_id,
            field_name=_text(field_name),
            label=_text(label),
            field_kind=_field_kind(field_kind, field_name="field_kind"),
            description=_text(description),
            required=bool(required),
            default_value=default_value,
            options=options if options is not None else [],
            display_order=int(display_order or 100),
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    fields = list_compiled_object_fields(conn, type_id=normalized_type_id, include_retired=True)
    matched = next((field for field in fields if field["name"] == row["field_name"]), None)
    if matched is None:
        raise ObjectLifecycleBoundaryError("upserted object field could not be reloaded", status_code=500)
    return {"type_id": normalized_type_id, "field": matched}


def retire_object_field(
    conn: Any,
    *,
    type_id: Any,
    field_name: Any,
) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    normalized_field_name = _text(field_name)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")
    if not normalized_field_name:
        raise ObjectLifecycleBoundaryError("field_name is required")
    try:
        row = retire_object_field_record(
            conn,
            type_id=normalized_type_id,
            field_name=normalized_field_name,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise ObjectLifecycleBoundaryError(
            f"Object field not found: {normalized_type_id}.{normalized_field_name}",
            status_code=404,
        )
    return {"type_id": normalized_type_id, "field_name": normalized_field_name, "retired": True}


def get_object(conn: Any, *, object_id: Any) -> dict[str, Any]:
    normalized_object_id = _text(object_id)
    if not normalized_object_id:
        raise ObjectLifecycleBoundaryError("object_id is required")
    row = load_object_record(conn, object_id=normalized_object_id)
    if row is None:
        raise ObjectLifecycleBoundaryError(f"Object not found: {normalized_object_id}", status_code=404)
    return row


def list_objects(
    conn: Any,
    *,
    type_id: Any,
    status: Any = "active",
    query: Any = "",
    limit: int = 100,
) -> dict[str, Any]:
    normalized_type_id = _text(type_id)
    if not normalized_type_id:
        raise ObjectLifecycleBoundaryError("type_id is required")
    normalized_status = _text(status) or "active"
    if not isinstance(limit, int) or limit <= 0:
        raise ObjectLifecycleBoundaryError("limit must be a positive integer")
    normalized_query = _text(query)
    if normalized_query:
        rows = conn.execute(
            "SELECT object_id, type_id, properties, status, created_at, updated_at "
            "FROM objects WHERE type_id = $1 AND status = $2 "
            "AND search_vector @@ plainto_tsquery('english', $3) "
            "ORDER BY updated_at DESC, object_id ASC LIMIT $4",
            normalized_type_id,
            normalized_status,
            normalized_query,
            limit,
        )
    else:
        rows = conn.execute(
            "SELECT object_id, type_id, properties, status, created_at, updated_at "
            "FROM objects WHERE type_id = $1 AND status = $2 "
            "ORDER BY updated_at DESC, object_id ASC LIMIT $3",
            normalized_type_id,
            normalized_status,
            limit,
        )
    return {"objects": [dict(row) for row in rows], "count": len(rows), "type_id": normalized_type_id}


def delete_object(
    conn: Any,
    *,
    object_id: Any,
) -> dict[str, Any]:
    normalized_object_id = _text(object_id)
    if not normalized_object_id:
        raise ObjectLifecycleBoundaryError("object_id is required")

    try:
        mark_object_deleted(conn, object_id=normalized_object_id)
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    return {"deleted": True}


def create_document(
    conn: Any,
    *,
    title: Any,
    content: Any,
    doc_type: Any,
    tags: Any,
    attached_to: Any,
) -> dict[str, Any]:
    normalized_title = _text(title)
    if not normalized_title:
        raise ObjectLifecycleBoundaryError("title is required")
    if not isinstance(content, str) or not content:
        raise ObjectLifecycleBoundaryError("content is required")
    normalized_doc_type = _text(doc_type)
    if normalized_doc_type not in _ALLOWED_DOCUMENT_TYPES:
        raise ObjectLifecycleBoundaryError(
            "doc_type must be one of: " + ", ".join(sorted(_ALLOWED_DOCUMENT_TYPES))
        )
    normalized_tags = _string_list(tags, field_name="tags")
    normalized_attached_to = _string_list(attached_to, field_name="attached_to")

    try:
        if not object_type_exists(conn, type_id="doc_type_document"):
            raise ObjectLifecycleBoundaryError("Object type not found: doc_type_document", status_code=404)
        row = create_object_record(
            conn,
            object_id="obj-" + uuid.uuid4().hex[:12],
            type_id="doc_type_document",
            properties={
                "title": normalized_title,
                "content": content,
                "doc_type": normalized_doc_type,
                "tags": normalized_tags,
                "version": 1,
                "attached_to": normalized_attached_to,
            },
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)

    props = _parse_properties(row.get("properties"))
    return {
        "document": {
            "id": row["object_id"],
            "title": props.get("title", ""),
            "doc_type": props.get("doc_type", ""),
        }
    }


def attach_document(
    conn: Any,
    *,
    document_id: Any,
    card_id: Any,
) -> dict[str, Any]:
    normalized_document_id = _text(document_id)
    if not normalized_document_id:
        raise ObjectLifecycleBoundaryError("document id is required")
    normalized_card_id = _text(card_id)
    if not normalized_card_id:
        raise ObjectLifecycleBoundaryError("card_id is required")

    try:
        row = attach_document_record(
            conn,
            document_id=normalized_document_id,
            card_id=normalized_card_id,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise ObjectLifecycleBoundaryError(f"Document not found: {normalized_document_id}", status_code=404)

    props = _parse_properties(row.get("properties"))
    attached = props.get("attached_to", [])
    if not isinstance(attached, list):
        attached = []
    return {"id": row["object_id"], "attached_to": attached}


__all__ = [
    "ObjectLifecycleBoundaryError",
    "attach_document",
    "create_document",
    "create_object",
    "create_object_type",
    "delete_object",
    "get_object",
    "get_object_type",
    "list_objects",
    "list_object_types",
    "update_object",
    "upsert_object_type",
]
