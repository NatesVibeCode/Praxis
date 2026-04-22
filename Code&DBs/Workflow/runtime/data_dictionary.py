"""Runtime authority for the unified data dictionary.

Three layers coexist in `data_dictionary_entries`:
- `auto`     — projector-derived, rewritten on every projection pass
- `inferred` — sampler-derived (e.g. row examples, observed enum values)
- `operator` — hand-edited, highest precedence, never overwritten by projectors

Projectors call `apply_projection()` to replace their own auto/inferred rows
idempotently. Operators mutate through `set_operator_override()` and
`clear_operator_override()`. Reads go through `describe_object()` which
returns merged rows from `data_dictionary_effective`.
"""

from __future__ import annotations

from typing import Any, Iterable

from storage.postgres.data_dictionary_repository import (
    count_entries_by_source,
    delete_entry,
    delete_object,
    get_object,
    list_effective_entries,
    list_entries,
    list_objects,
    replace_auto_entries,
    upsert_entry,
    upsert_object,
)
from storage.postgres.validators import PostgresWriteError

_ALLOWED_CATEGORIES = frozenset({
    "table", "object_type", "integration", "dataset", "ingest",
    "decision", "receipt", "tool", "object", "command", "event",
    "projection", "service_bus_channel", "feedback_stream", "definition",
    "runtime_target",
})
_ALLOWED_FIELD_KINDS = frozenset({
    "text", "number", "boolean", "enum", "json", "date", "datetime",
    "reference", "array", "object",
})
_FIELD_KIND_ALIASES = {
    "string": "text", "str": "text", "varchar": "text",
    "int": "number", "integer": "number", "float": "number",
    "decimal": "number", "double": "number",
    "bool": "boolean",
    "dict": "object", "map": "object", "jsonb": "json",
    "list": "array", "tuple": "array",
    "timestamp": "datetime", "timestamptz": "datetime",
    "ref": "reference", "fk": "reference",
}


class DataDictionaryBoundaryError(RuntimeError):
    """Raised when a data dictionary authority call is rejected."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def normalize_field_kind(value: Any) -> str:
    candidate = _text(value).lower() or "text"
    candidate = _FIELD_KIND_ALIASES.get(candidate, candidate)
    if candidate not in _ALLOWED_FIELD_KINDS:
        return "text"
    return candidate


def _raise_storage(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise DataDictionaryBoundaryError(str(exc), status_code=status_code) from exc


# --- projector-facing API -------------------------------------------------


def apply_projection(
    conn: Any,
    *,
    object_kind: str,
    category: str,
    entries: Iterable[dict[str, Any]],
    source: str = "auto",
    label: str = "",
    summary: str = "",
    origin_ref: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Idempotently write projector-derived rows for one object_kind.

    Upserts the object row, then replaces all entries with `source` matching
    the given source. Other sources (notably `operator`) are left untouched.
    """
    kind = _text(object_kind)
    if not kind:
        raise DataDictionaryBoundaryError("object_kind is required")
    if category not in _ALLOWED_CATEGORIES:
        raise DataDictionaryBoundaryError(
            f"category must be one of: {', '.join(sorted(_ALLOWED_CATEGORIES))}"
        )
    if source not in ("auto", "inferred"):
        raise DataDictionaryBoundaryError(
            "apply_projection only writes auto/inferred layers"
        )

    normalized_entries: list[dict[str, Any]] = []
    for index, raw in enumerate(entries or []):
        if not isinstance(raw, dict):
            raise DataDictionaryBoundaryError(
                f"entries[{index}] must be an object"
            )
        path = _text(raw.get("field_path"))
        if not path:
            raise DataDictionaryBoundaryError(
                f"entries[{index}].field_path is required"
            )
        normalized_entries.append({
            "field_path": path,
            "field_kind": normalize_field_kind(raw.get("field_kind")),
            "label": _text(raw.get("label")),
            "description": _text(raw.get("description")),
            "required": bool(raw.get("required", False)),
            "default_value": raw.get("default_value"),
            "valid_values": raw.get("valid_values") if raw.get("valid_values") is not None else [],
            "examples": raw.get("examples") if raw.get("examples") is not None else [],
            "deprecation_notes": _text(raw.get("deprecation_notes")),
            "display_order": int(raw.get("display_order") or (index + 1) * 10),
            "origin_ref": raw.get("origin_ref") or {},
            "metadata": raw.get("metadata") or {},
        })

    try:
        upsert_object(
            conn,
            object_kind=kind,
            label=label or kind,
            category=category,
            summary=summary,
            origin_ref=origin_ref or {},
            metadata=metadata or {},
        )
        written = replace_auto_entries(
            conn,
            object_kind=kind,
            source=source,
            entries=normalized_entries,
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)

    return {"object_kind": kind, "source": source, "entries_written": written}


# --- operator-facing API --------------------------------------------------


def set_operator_override(
    conn: Any,
    *,
    object_kind: str,
    field_path: str,
    field_kind: Any = None,
    label: Any = None,
    description: Any = None,
    required: Any = None,
    default_value: Any = None,
    valid_values: Any = None,
    examples: Any = None,
    deprecation_notes: Any = None,
    display_order: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    """Write an operator-layer row that takes precedence over auto/inferred.

    Any field left as None is read from the effective (auto/inferred) view so
    the operator row is a coherent full descriptor even when the operator is
    only overriding one attribute.
    """
    kind = _text(object_kind)
    path = _text(field_path)
    if not kind:
        raise DataDictionaryBoundaryError("object_kind is required")
    if not path:
        raise DataDictionaryBoundaryError("field_path is required")

    current = {}
    for row in list_effective_entries(conn, object_kind=kind):
        if row.get("field_path") == path:
            current = row
            break

    try:
        upsert_object(
            conn,
            object_kind=kind,
            label=_text(current.get("label") or kind),
            category=_text(_object_category(conn, kind) or "object"),
            summary="",
        )
        row = upsert_entry(
            conn,
            object_kind=kind,
            field_path=path,
            source="operator",
            field_kind=normalize_field_kind(
                field_kind if field_kind is not None else current.get("field_kind")
            ),
            label=_text(label if label is not None else current.get("label")),
            description=_text(description if description is not None else current.get("description")),
            required=bool(required if required is not None else current.get("required", False)),
            default_value=default_value if default_value is not None else current.get("default_value"),
            valid_values=valid_values if valid_values is not None else current.get("valid_values") or [],
            examples=examples if examples is not None else current.get("examples") or [],
            deprecation_notes=_text(deprecation_notes if deprecation_notes is not None else current.get("deprecation_notes")),
            display_order=int(
                display_order if display_order is not None else current.get("display_order") or 100
            ),
            origin_ref={"source": "operator"},
            metadata=metadata if metadata is not None else {},
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"object_kind": kind, "field_path": path, "entry": dict(row)}


def clear_operator_override(
    conn: Any,
    *,
    object_kind: str,
    field_path: str,
) -> dict[str, Any]:
    kind = _text(object_kind)
    path = _text(field_path)
    if not kind:
        raise DataDictionaryBoundaryError("object_kind is required")
    if not path:
        raise DataDictionaryBoundaryError("field_path is required")
    try:
        removed = delete_entry(
            conn,
            object_kind=kind,
            field_path=path,
            source="operator",
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"object_kind": kind, "field_path": path, "removed": removed}


# --- read API -------------------------------------------------------------


def list_object_kinds(conn: Any, *, category: str | None = None) -> list[dict[str, Any]]:
    normalized_category = _text(category) or None
    if normalized_category and normalized_category not in _ALLOWED_CATEGORIES:
        raise DataDictionaryBoundaryError(
            f"category must be one of: {', '.join(sorted(_ALLOWED_CATEGORIES))}"
        )
    rows = list_objects(conn, category=normalized_category)
    for row in rows:
        row["entries_by_source"] = count_entries_by_source(
            conn, object_kind=str(row.get("object_kind"))
        )
    return rows


def describe_object(
    conn: Any,
    *,
    object_kind: str,
    include_layers: bool = False,
) -> dict[str, Any]:
    kind = _text(object_kind)
    if not kind:
        raise DataDictionaryBoundaryError("object_kind is required")
    header = get_object(conn, object_kind=kind)
    if header is None:
        raise DataDictionaryBoundaryError(
            f"data dictionary: unknown object_kind {kind!r}",
            status_code=404,
        )
    effective = list_effective_entries(conn, object_kind=kind)
    response: dict[str, Any] = {
        "object": header,
        "fields": effective,
        "entries_by_source": count_entries_by_source(conn, object_kind=kind),
    }
    if include_layers:
        response["layers"] = list_entries(conn, object_kind=kind)
    return response


def _object_category(conn: Any, object_kind: str) -> str | None:
    row = get_object(conn, object_kind=object_kind)
    return str(row["category"]) if row else None


__all__ = [
    "DataDictionaryBoundaryError",
    "apply_projection",
    "clear_operator_override",
    "describe_object",
    "list_object_kinds",
    "normalize_field_kind",
    "set_operator_override",
]
