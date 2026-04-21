"""Sync Postgres repository for data dictionary classifications / tags.

Key/value labels on (object_kind, field_path) with three layered sources:
auto (projector heuristics), inferred (sampler), operator (hand-curated).
Reads expose the merged effective view plus raw per-source rows.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from .validators import PostgresWriteError, _require_text

_VALID_SOURCES = frozenset({"auto", "inferred", "operator"})


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


def upsert_classification(
    conn: Any,
    *,
    object_kind: str,
    tag_key: str,
    source: str,
    tag_value: str = "",
    field_path: str = "",
    confidence: float = 1.0,
    origin_ref: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    kind = _require_text(object_kind, field_name="object_kind")
    key = _require_text(tag_key, field_name="tag_key")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_classifications.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    conf = max(0.0, min(1.0, float(confidence)))
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_classifications (
            object_kind, field_path, tag_key, tag_value, source,
            confidence, origin_ref, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb
        )
        ON CONFLICT (object_kind, field_path, tag_key, source) DO UPDATE
           SET tag_value = EXCLUDED.tag_value,
               confidence = EXCLUDED.confidence,
               origin_ref = EXCLUDED.origin_ref,
               metadata = EXCLUDED.metadata
        RETURNING *
        """,
        kind, field_path or "", key, tag_value or "", source,
        conf, _encode_jsonb(origin_ref), _encode_jsonb(metadata),
    )
    return _row(row)


def replace_projected_classifications(
    conn: Any,
    *,
    source: str,
    projector_tag: str,
    entries: Iterable[dict[str, Any]],
) -> int:
    """Idempotently replace classifications written by a single projector."""
    if source == "operator":
        raise PostgresWriteError(
            "data_dictionary_classifications.invalid_submission",
            "replace_projected_classifications refuses to bulk-replace operator rows",
            details={"field": "source"},
        )
    tag = _require_text(projector_tag, field_name="projector_tag")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_classifications.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    entries_list = list(entries)
    keep_keys = [
        (
            str(e.get("object_kind") or ""),
            str(e.get("field_path") or ""),
            str(e.get("tag_key") or ""),
        )
        for e in entries_list
    ]
    obj_kinds = [k[0] for k in keep_keys]
    field_paths = [k[1] for k in keep_keys]
    tag_keys = [k[2] for k in keep_keys]
    if keep_keys:
        conn.execute(
            """
            DELETE FROM data_dictionary_classifications
             WHERE source = $1
               AND origin_ref ->> 'projector' = $2
               AND NOT EXISTS (
                   SELECT 1 FROM unnest($3::text[], $4::text[], $5::text[])
                     AS keep(ok, fp, tk)
                   WHERE keep.ok = data_dictionary_classifications.object_kind
                     AND keep.fp = data_dictionary_classifications.field_path
                     AND keep.tk = data_dictionary_classifications.tag_key
               )
            """,
            source, tag, obj_kinds, field_paths, tag_keys,
        )
    else:
        conn.execute(
            """
            DELETE FROM data_dictionary_classifications
             WHERE source = $1 AND origin_ref ->> 'projector' = $2
            """,
            source, tag,
        )
    written = 0
    for entry in entries_list:
        upsert_classification(
            conn,
            source=source,
            object_kind=str(entry.get("object_kind") or ""),
            field_path=str(entry.get("field_path") or ""),
            tag_key=str(entry.get("tag_key") or ""),
            tag_value=str(entry.get("tag_value") or ""),
            confidence=float(entry.get("confidence", 1.0)),
            origin_ref=entry.get("origin_ref") or {"projector": tag},
            metadata=entry.get("metadata") or {},
        )
        written += 1
    return written


def delete_classification(
    conn: Any,
    *,
    object_kind: str,
    tag_key: str,
    source: str,
    field_path: str = "",
) -> bool:
    kind = _require_text(object_kind, field_name="object_kind")
    key = _require_text(tag_key, field_name="tag_key")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_classifications.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    row = conn.fetchrow(
        """
        DELETE FROM data_dictionary_classifications
         WHERE object_kind = $1 AND field_path = $2
           AND tag_key = $3 AND source = $4
         RETURNING object_kind
        """,
        kind, field_path or "", key, source,
    )
    return row is not None


def list_classifications_for(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
) -> list[dict[str, Any]]:
    """Effective tags on an object (or one of its fields)."""
    kind = _require_text(object_kind, field_name="object_kind")
    if field_path is None:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_classifications_effective "
            "WHERE object_kind = $1 ORDER BY field_path, tag_key",
            kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_classifications_effective "
            "WHERE object_kind = $1 AND field_path = $2 "
            "ORDER BY tag_key",
            kind, field_path,
        )
    return [dict(r) for r in rows or []]


def list_by_tag(
    conn: Any,
    *,
    tag_key: str,
    tag_value: str | None = None,
) -> list[dict[str, Any]]:
    """Effective classifications filtered by tag_key / tag_value — useful
    for compliance reports like "list every PII email field"."""
    key = _require_text(tag_key, field_name="tag_key")
    if tag_value:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_classifications_effective "
            "WHERE tag_key = $1 AND tag_value = $2 "
            "ORDER BY object_kind, field_path",
            key, tag_value,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_classifications_effective "
            "WHERE tag_key = $1 "
            "ORDER BY object_kind, field_path",
            key,
        )
    return [dict(r) for r in rows or []]


def list_classification_layers(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
) -> list[dict[str, Any]]:
    """Raw per-source rows (auto/inferred/operator)."""
    kind = _require_text(object_kind, field_name="object_kind")
    if field_path is None:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_classifications "
            "WHERE object_kind = $1 "
            "ORDER BY field_path, tag_key, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_classifications "
            "WHERE object_kind = $1 AND field_path = $2 "
            "ORDER BY tag_key, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind, field_path,
        )
    return [dict(r) for r in rows or []]


def count_classifications_by_source(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT source, COUNT(*) AS n "
        "FROM data_dictionary_classifications GROUP BY source"
    )
    return {str(r["source"]): int(r["n"]) for r in rows or []}


def list_classification_tag_catalog(conn: Any) -> list[dict[str, Any]]:
    """Return distinct tag_key/tag_value/source counts for operator visibility."""
    rows = conn.execute(
        """
        SELECT tag_key, tag_value, source, COUNT(*)::int AS rows
        FROM data_dictionary_classifications
        GROUP BY tag_key, tag_value, source
        ORDER BY tag_key, tag_value, source
        """
    )
    return [
        {
            "tag_key": r.get("tag_key"),
            "tag_value": r.get("tag_value") or "",
            "source": r.get("source"),
            "rows": int(r.get("rows") or 0),
        }
        for r in rows or []
    ]


__all__ = [
    "upsert_classification",
    "replace_projected_classifications",
    "delete_classification",
    "list_classifications_for",
    "list_by_tag",
    "list_classification_layers",
    "count_classifications_by_source",
    "list_classification_tag_catalog",
]
