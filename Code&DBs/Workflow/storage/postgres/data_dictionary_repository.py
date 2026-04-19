"""Sync Postgres repository for the unified data dictionary authority.

Writes the auto/inferred layers produced by projectors and the operator layer
produced through runtime/data_dictionary.py. Reads expose both the per-source
rows and the merged (`data_dictionary_effective`) view.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from .validators import PostgresWriteError, _require_text

_VALID_SOURCES = frozenset({"auto", "inferred", "operator"})
_VALID_FIELD_KINDS = frozenset({
    "text", "number", "boolean", "enum", "json", "date", "datetime",
    "reference", "array", "object",
})
_VALID_CATEGORIES = frozenset({
    "table", "object_type", "integration", "dataset", "ingest",
    "decision", "receipt", "tool", "object",
})


def _encode_jsonb(value: Any, *, default: str = "{}") -> str:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            json.loads(stripped)
        except (TypeError, ValueError):
            return default
        return stripped
    return default


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


# --- object kinds --------------------------------------------------------


def upsert_object(
    conn: Any,
    *,
    object_kind: str,
    label: str = "",
    category: str = "object",
    summary: str = "",
    origin_ref: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    kind = _require_text(object_kind, field_name="object_kind")
    if category not in _VALID_CATEGORIES:
        raise PostgresWriteError(
            "data_dictionary.invalid_submission",
            f"category must be one of {sorted(_VALID_CATEGORIES)}",
            details={"field": "category", "value": category},
        )
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_objects (
            object_kind, label, category, summary, origin_ref, metadata
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
        ON CONFLICT (object_kind) DO UPDATE
           SET label = EXCLUDED.label,
               category = EXCLUDED.category,
               summary = EXCLUDED.summary,
               origin_ref = EXCLUDED.origin_ref,
               metadata = EXCLUDED.metadata
        RETURNING *
        """,
        kind,
        label or "",
        category,
        summary or "",
        _encode_jsonb(origin_ref),
        _encode_jsonb(metadata),
    )
    return _row(row)


def list_objects(conn: Any, *, category: str | None = None) -> list[dict[str, Any]]:
    if category:
        rows = conn.execute(
            "SELECT object_kind, label, category, summary, origin_ref, metadata, "
            "created_at, updated_at "
            "FROM data_dictionary_objects WHERE category = $1 ORDER BY object_kind",
            category,
        )
    else:
        rows = conn.execute(
            "SELECT object_kind, label, category, summary, origin_ref, metadata, "
            "created_at, updated_at "
            "FROM data_dictionary_objects ORDER BY category, object_kind",
        )
    return [dict(r) for r in rows or []]


def get_object(conn: Any, *, object_kind: str) -> dict[str, Any] | None:
    kind = _require_text(object_kind, field_name="object_kind")
    row = conn.fetchrow(
        "SELECT object_kind, label, category, summary, origin_ref, metadata, "
        "created_at, updated_at FROM data_dictionary_objects WHERE object_kind = $1",
        kind,
    )
    return dict(row) if row is not None else None


def delete_object(conn: Any, *, object_kind: str) -> bool:
    kind = _require_text(object_kind, field_name="object_kind")
    row = conn.fetchrow(
        "DELETE FROM data_dictionary_objects WHERE object_kind = $1 RETURNING object_kind",
        kind,
    )
    return row is not None


# --- entries -------------------------------------------------------------


def upsert_entry(
    conn: Any,
    *,
    object_kind: str,
    field_path: str,
    source: str,
    field_kind: str = "text",
    label: str = "",
    description: str = "",
    required: bool = False,
    default_value: Any = None,
    valid_values: Any = None,
    examples: Any = None,
    deprecation_notes: str = "",
    display_order: int = 100,
    origin_ref: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    kind = _require_text(object_kind, field_name="object_kind")
    path = _require_text(field_path, field_name="field_path")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    if field_kind not in _VALID_FIELD_KINDS:
        raise PostgresWriteError(
            "data_dictionary.invalid_submission",
            f"field_kind must be one of {sorted(_VALID_FIELD_KINDS)}",
            details={"field": "field_kind", "value": field_kind},
        )
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_entries (
            object_kind, field_path, source,
            field_kind, label, description, required,
            default_value, valid_values, examples,
            deprecation_notes, display_order, origin_ref, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8::jsonb, $9::jsonb, $10::jsonb,
            $11, $12, $13::jsonb, $14::jsonb
        )
        ON CONFLICT (object_kind, field_path, source) DO UPDATE
           SET field_kind = EXCLUDED.field_kind,
               label = EXCLUDED.label,
               description = EXCLUDED.description,
               required = EXCLUDED.required,
               default_value = EXCLUDED.default_value,
               valid_values = EXCLUDED.valid_values,
               examples = EXCLUDED.examples,
               deprecation_notes = EXCLUDED.deprecation_notes,
               display_order = EXCLUDED.display_order,
               origin_ref = EXCLUDED.origin_ref,
               metadata = EXCLUDED.metadata
        RETURNING *
        """,
        kind,
        path,
        source,
        field_kind,
        label or "",
        description or "",
        bool(required),
        _encode_jsonb(default_value, default="null"),
        _encode_jsonb(valid_values, default="[]"),
        _encode_jsonb(examples, default="[]"),
        deprecation_notes or "",
        int(display_order) if display_order is not None else 100,
        _encode_jsonb(origin_ref),
        _encode_jsonb(metadata),
    )
    return _row(row)


def replace_auto_entries(
    conn: Any,
    *,
    object_kind: str,
    source: str,
    entries: Iterable[dict[str, Any]],
) -> int:
    """Idempotently replace entries for (object_kind, source).

    Deletes rows whose field_path no longer appears in `entries`, then upserts
    each provided entry. Only callable for `auto` and `inferred` sources —
    `operator` rows are hand-managed and never get wholesale-replaced.
    """
    if source == "operator":
        raise PostgresWriteError(
            "data_dictionary.invalid_submission",
            "replace_auto_entries refuses to bulk-replace operator rows",
            details={"field": "source"},
        )
    kind = _require_text(object_kind, field_name="object_kind")
    entries_list = list(entries)
    keep_paths = [str(e["field_path"]) for e in entries_list if e.get("field_path")]
    if keep_paths:
        conn.execute(
            """
            DELETE FROM data_dictionary_entries
             WHERE object_kind = $1 AND source = $2
               AND NOT (field_path = ANY($3::text[]))
            """,
            kind,
            source,
            keep_paths,
        )
    else:
        conn.execute(
            "DELETE FROM data_dictionary_entries WHERE object_kind = $1 AND source = $2",
            kind,
            source,
        )
    written = 0
    for entry in entries_list:
        upsert_entry(conn, object_kind=kind, source=source, **{
            key: entry[key] for key in (
                "field_path", "field_kind", "label", "description", "required",
                "default_value", "valid_values", "examples", "deprecation_notes",
                "display_order", "origin_ref", "metadata",
            ) if key in entry
        })
        written += 1
    return written


def delete_entry(
    conn: Any,
    *,
    object_kind: str,
    field_path: str,
    source: str,
) -> bool:
    kind = _require_text(object_kind, field_name="object_kind")
    path = _require_text(field_path, field_name="field_path")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    row = conn.fetchrow(
        "DELETE FROM data_dictionary_entries "
        "WHERE object_kind = $1 AND field_path = $2 AND source = $3 "
        "RETURNING field_path",
        kind,
        path,
        source,
    )
    return row is not None


def list_entries(
    conn: Any,
    *,
    object_kind: str,
    source: str | None = None,
) -> list[dict[str, Any]]:
    kind = _require_text(object_kind, field_name="object_kind")
    if source:
        if source not in _VALID_SOURCES:
            raise PostgresWriteError(
                "data_dictionary.invalid_submission",
                f"source must be one of {sorted(_VALID_SOURCES)}",
                details={"field": "source", "value": source},
            )
        rows = conn.execute(
            "SELECT * FROM data_dictionary_entries "
            "WHERE object_kind = $1 AND source = $2 "
            "ORDER BY display_order, field_path",
            kind,
            source,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_entries "
            "WHERE object_kind = $1 "
            "ORDER BY field_path, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind,
        )
    return [dict(r) for r in rows or []]


def list_effective_entries(
    conn: Any,
    *,
    object_kind: str,
) -> list[dict[str, Any]]:
    kind = _require_text(object_kind, field_name="object_kind")
    rows = conn.execute(
        "SELECT * FROM data_dictionary_effective "
        "WHERE object_kind = $1 "
        "ORDER BY display_order, field_path",
        kind,
    )
    return [dict(r) for r in rows or []]


def count_entries_by_source(conn: Any, *, object_kind: str) -> dict[str, int]:
    kind = _require_text(object_kind, field_name="object_kind")
    rows = conn.execute(
        "SELECT source, COUNT(*) AS n FROM data_dictionary_entries "
        "WHERE object_kind = $1 GROUP BY source",
        kind,
    )
    return {str(r["source"]): int(r["n"]) for r in rows or []}


__all__ = [
    "upsert_object",
    "list_objects",
    "get_object",
    "delete_object",
    "upsert_entry",
    "replace_auto_entries",
    "delete_entry",
    "list_entries",
    "list_effective_entries",
    "count_entries_by_source",
]
