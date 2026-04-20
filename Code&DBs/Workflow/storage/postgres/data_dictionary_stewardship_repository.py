"""Sync Postgres repository for data dictionary stewardship.

Stewards (owners / approvers / contacts / publishers / consumers) on
(object_kind, field_path). Three layered sources: auto (heuristics),
inferred (behavioural signal), operator (hand-curated). Multiple
stewards per kind are allowed — the unique key includes steward_id.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from .validators import PostgresWriteError, _require_text

_VALID_SOURCES = frozenset({"auto", "inferred", "operator"})
_VALID_STEWARD_TYPES = frozenset({"person", "team", "agent", "role", "service"})


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


def upsert_steward(
    conn: Any,
    *,
    object_kind: str,
    steward_kind: str,
    steward_id: str,
    source: str,
    steward_type: str = "person",
    field_path: str = "",
    confidence: float = 1.0,
    origin_ref: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    kind = _require_text(object_kind, field_name="object_kind")
    sk = _require_text(steward_kind, field_name="steward_kind")
    sid = _require_text(steward_id, field_name="steward_id")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_stewardship.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    if steward_type not in _VALID_STEWARD_TYPES:
        raise PostgresWriteError(
            "data_dictionary_stewardship.invalid_submission",
            f"steward_type must be one of {sorted(_VALID_STEWARD_TYPES)}",
            details={"field": "steward_type", "value": steward_type},
        )
    conf = max(0.0, min(1.0, float(confidence)))
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_stewardship (
            object_kind, field_path, steward_kind, steward_id,
            steward_type, source, confidence, origin_ref, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb
        )
        ON CONFLICT (object_kind, field_path, steward_kind, steward_id, source) DO UPDATE
           SET steward_type = EXCLUDED.steward_type,
               confidence = EXCLUDED.confidence,
               origin_ref = EXCLUDED.origin_ref,
               metadata = EXCLUDED.metadata
        RETURNING *
        """,
        kind, field_path or "", sk, sid,
        steward_type, source, conf,
        _encode_jsonb(origin_ref), _encode_jsonb(metadata),
    )
    return _row(row)


def replace_projected_stewards(
    conn: Any,
    *,
    source: str,
    projector_tag: str,
    entries: Iterable[dict[str, Any]],
) -> int:
    """Idempotently replace stewards written by a single projector."""
    if source == "operator":
        raise PostgresWriteError(
            "data_dictionary_stewardship.invalid_submission",
            "replace_projected_stewards refuses to bulk-replace operator rows",
            details={"field": "source"},
        )
    tag = _require_text(projector_tag, field_name="projector_tag")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_stewardship.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    entries_list = list(entries)
    keep_keys = [
        (
            str(e.get("object_kind") or ""),
            str(e.get("field_path") or ""),
            str(e.get("steward_kind") or ""),
            str(e.get("steward_id") or ""),
        )
        for e in entries_list
    ]
    obj_kinds = [k[0] for k in keep_keys]
    field_paths = [k[1] for k in keep_keys]
    steward_kinds = [k[2] for k in keep_keys]
    steward_ids = [k[3] for k in keep_keys]
    if keep_keys:
        conn.execute(
            """
            DELETE FROM data_dictionary_stewardship
             WHERE source = $1
               AND origin_ref ->> 'projector' = $2
               AND NOT EXISTS (
                   SELECT 1
                     FROM unnest($3::text[], $4::text[], $5::text[], $6::text[])
                       AS keep(ok, fp, sk, sid)
                    WHERE keep.ok  = data_dictionary_stewardship.object_kind
                      AND keep.fp  = data_dictionary_stewardship.field_path
                      AND keep.sk  = data_dictionary_stewardship.steward_kind
                      AND keep.sid = data_dictionary_stewardship.steward_id
               )
            """,
            source, tag, obj_kinds, field_paths, steward_kinds, steward_ids,
        )
    else:
        conn.execute(
            """
            DELETE FROM data_dictionary_stewardship
             WHERE source = $1 AND origin_ref ->> 'projector' = $2
            """,
            source, tag,
        )
    written = 0
    for entry in entries_list:
        upsert_steward(
            conn,
            source=source,
            object_kind=str(entry.get("object_kind") or ""),
            field_path=str(entry.get("field_path") or ""),
            steward_kind=str(entry.get("steward_kind") or ""),
            steward_id=str(entry.get("steward_id") or ""),
            steward_type=str(entry.get("steward_type") or "person"),
            confidence=float(entry.get("confidence", 1.0)),
            origin_ref=entry.get("origin_ref") or {"projector": tag},
            metadata=entry.get("metadata") or {},
        )
        written += 1
    return written


def delete_steward(
    conn: Any,
    *,
    object_kind: str,
    steward_kind: str,
    steward_id: str,
    source: str,
    field_path: str = "",
) -> bool:
    kind = _require_text(object_kind, field_name="object_kind")
    sk = _require_text(steward_kind, field_name="steward_kind")
    sid = _require_text(steward_id, field_name="steward_id")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_stewardship.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    row = conn.fetchrow(
        """
        DELETE FROM data_dictionary_stewardship
         WHERE object_kind = $1 AND field_path = $2
           AND steward_kind = $3 AND steward_id = $4 AND source = $5
         RETURNING object_kind
        """,
        kind, field_path or "", sk, sid, source,
    )
    return row is not None


def list_stewards_for(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
) -> list[dict[str, Any]]:
    """Effective stewards on an object (or one of its fields)."""
    kind = _require_text(object_kind, field_name="object_kind")
    if field_path is None:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_stewardship_effective "
            "WHERE object_kind = $1 "
            "ORDER BY field_path, steward_kind, steward_id",
            kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_stewardship_effective "
            "WHERE object_kind = $1 AND field_path = $2 "
            "ORDER BY steward_kind, steward_id",
            kind, field_path,
        )
    return [dict(r) for r in rows or []]


def list_assets_owned_by(
    conn: Any,
    *,
    steward_id: str,
    steward_kind: str | None = None,
) -> list[dict[str, Any]]:
    """Effective stewardships filtered by steward_id — useful for
    "what does alice@company own?" reports."""
    sid = _require_text(steward_id, field_name="steward_id")
    if steward_kind:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_stewardship_effective "
            "WHERE steward_id = $1 AND steward_kind = $2 "
            "ORDER BY object_kind, field_path",
            sid, steward_kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_stewardship_effective "
            "WHERE steward_id = $1 "
            "ORDER BY object_kind, field_path, steward_kind",
            sid,
        )
    return [dict(r) for r in rows or []]


def list_steward_layers(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
) -> list[dict[str, Any]]:
    """Raw per-source rows (auto/inferred/operator)."""
    kind = _require_text(object_kind, field_name="object_kind")
    if field_path is None:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_stewardship "
            "WHERE object_kind = $1 "
            "ORDER BY field_path, steward_kind, steward_id, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_stewardship "
            "WHERE object_kind = $1 AND field_path = $2 "
            "ORDER BY steward_kind, steward_id, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind, field_path,
        )
    return [dict(r) for r in rows or []]


def count_stewards_by_source(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT source, COUNT(*) AS n "
        "FROM data_dictionary_stewardship GROUP BY source"
    )
    return {str(r["source"]): int(r["n"]) for r in rows or []}


def count_stewards_by_kind(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT steward_kind, COUNT(*) AS n "
        "FROM data_dictionary_stewardship_effective GROUP BY steward_kind"
    )
    return {str(r["steward_kind"]): int(r["n"]) for r in rows or []}


__all__ = [
    "upsert_steward",
    "replace_projected_stewards",
    "delete_steward",
    "list_stewards_for",
    "list_assets_owned_by",
    "list_steward_layers",
    "count_stewards_by_source",
    "count_stewards_by_kind",
]
