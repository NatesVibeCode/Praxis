"""Sync Postgres repository for data dictionary lineage edges.

Writes the auto/inferred layers produced by `lineage_projector` and the
operator layer produced through `runtime/data_dictionary_lineage.py`.
Reads expose both per-source rows and the merged effective view.

Edge model: directed (src -> dst) with an `edge_kind` discriminator and
optional field-path granularity (`field_path = ''` means object-level).
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from .validators import PostgresWriteError, _require_text

_VALID_SOURCES = frozenset({"auto", "inferred", "operator"})
_VALID_EDGE_KINDS = frozenset({
    "references", "derives_from", "projects_to", "ingests_from",
    "produces", "consumes", "promotes_to", "same_as",
    "dispatches", "governed_by",
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


def upsert_edge(
    conn: Any,
    *,
    src_object_kind: str,
    dst_object_kind: str,
    edge_kind: str,
    source: str,
    src_field_path: str = "",
    dst_field_path: str = "",
    confidence: float = 1.0,
    origin_ref: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    src = _require_text(src_object_kind, field_name="src_object_kind")
    dst = _require_text(dst_object_kind, field_name="dst_object_kind")
    src_field = src_field_path or ""
    dst_field = dst_field_path or ""
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_lineage.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    if edge_kind not in _VALID_EDGE_KINDS:
        raise PostgresWriteError(
            "data_dictionary_lineage.invalid_submission",
            f"edge_kind must be one of {sorted(_VALID_EDGE_KINDS)}",
            details={"field": "edge_kind", "value": edge_kind},
        )
    if src == dst and src_field == dst_field:
        raise PostgresWriteError(
            "data_dictionary_lineage.invalid_submission",
            "src and dst cannot be identical (self-loop)",
            details={"field": "dst_object_kind", "value": dst},
        )
    conf = max(0.0, min(1.0, float(confidence)))
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_lineage (
            src_object_kind, src_field_path,
            dst_object_kind, dst_field_path,
            edge_kind, source,
            confidence, origin_ref, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8::jsonb, $9::jsonb
        )
        ON CONFLICT (src_object_kind, src_field_path,
                     dst_object_kind, dst_field_path,
                     edge_kind, source) DO UPDATE
           SET confidence = EXCLUDED.confidence,
               origin_ref = EXCLUDED.origin_ref,
               metadata   = EXCLUDED.metadata
        RETURNING *
        """,
        src, src_field, dst, dst_field, edge_kind, source,
        conf, _encode_jsonb(origin_ref), _encode_jsonb(metadata),
    )
    return _row(row)


def replace_projected_edges(
    conn: Any,
    *,
    source: str,
    projector_tag: str,
    edges: Iterable[dict[str, Any]],
) -> int:
    """Idempotently replace edges written by a single projector.

    Deletes every (source, origin_ref->>'projector' = projector_tag) edge whose
    (src, src_field, dst, dst_field, edge_kind) is NOT present in `edges`,
    then upserts each provided edge. Safe for auto/inferred only.
    """
    if source == "operator":
        raise PostgresWriteError(
            "data_dictionary_lineage.invalid_submission",
            "replace_projected_edges refuses to bulk-replace operator rows",
            details={"field": "source"},
        )
    tag = _require_text(projector_tag, field_name="projector_tag")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_lineage.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    edges_list = list(edges)
    keep_keys = [
        (
            str(e.get("src_object_kind") or ""),
            str(e.get("src_field_path") or ""),
            str(e.get("dst_object_kind") or ""),
            str(e.get("dst_field_path") or ""),
            str(e.get("edge_kind") or ""),
        )
        for e in edges_list
    ]
    # Build parallel arrays for the anti-join; empty arrays are fine.
    src_kinds = [k[0] for k in keep_keys]
    src_fields = [k[1] for k in keep_keys]
    dst_kinds = [k[2] for k in keep_keys]
    dst_fields = [k[3] for k in keep_keys]
    edge_kinds = [k[4] for k in keep_keys]
    if keep_keys:
        conn.execute(
            """
            DELETE FROM data_dictionary_lineage
             WHERE source = $1
               AND origin_ref ->> 'projector' = $2
               AND NOT EXISTS (
                   SELECT 1 FROM unnest(
                       $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]
                   ) AS keep(sk, sf, dk, df, ek)
                   WHERE keep.sk = data_dictionary_lineage.src_object_kind
                     AND keep.sf = data_dictionary_lineage.src_field_path
                     AND keep.dk = data_dictionary_lineage.dst_object_kind
                     AND keep.df = data_dictionary_lineage.dst_field_path
                     AND keep.ek = data_dictionary_lineage.edge_kind
               )
            """,
            source, tag, src_kinds, src_fields, dst_kinds, dst_fields, edge_kinds,
        )
    else:
        conn.execute(
            """
            DELETE FROM data_dictionary_lineage
             WHERE source = $1 AND origin_ref ->> 'projector' = $2
            """,
            source, tag,
        )
    written = 0
    for edge in edges_list:
        upsert_edge(
            conn,
            source=source,
            src_object_kind=str(edge.get("src_object_kind") or ""),
            src_field_path=str(edge.get("src_field_path") or ""),
            dst_object_kind=str(edge.get("dst_object_kind") or ""),
            dst_field_path=str(edge.get("dst_field_path") or ""),
            edge_kind=str(edge.get("edge_kind") or ""),
            confidence=float(edge.get("confidence", 1.0)),
            origin_ref=edge.get("origin_ref") or {"projector": tag},
            metadata=edge.get("metadata") or {},
        )
        written += 1
    return written


def delete_edge(
    conn: Any,
    *,
    src_object_kind: str,
    dst_object_kind: str,
    edge_kind: str,
    source: str,
    src_field_path: str = "",
    dst_field_path: str = "",
) -> bool:
    src = _require_text(src_object_kind, field_name="src_object_kind")
    dst = _require_text(dst_object_kind, field_name="dst_object_kind")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_lineage.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    row = conn.fetchrow(
        """
        DELETE FROM data_dictionary_lineage
         WHERE src_object_kind = $1 AND src_field_path = $2
           AND dst_object_kind = $3 AND dst_field_path = $4
           AND edge_kind = $5 AND source = $6
         RETURNING src_object_kind
        """,
        src, src_field_path or "", dst, dst_field_path or "", edge_kind, source,
    )
    return row is not None


def list_edges_from(
    conn: Any,
    *,
    src_object_kind: str,
    edge_kind: str | None = None,
) -> list[dict[str, Any]]:
    """Effective outbound edges from an object (merged view)."""
    src = _require_text(src_object_kind, field_name="src_object_kind")
    if edge_kind:
        if edge_kind not in _VALID_EDGE_KINDS:
            raise PostgresWriteError(
                "data_dictionary_lineage.invalid_submission",
                f"edge_kind must be one of {sorted(_VALID_EDGE_KINDS)}",
                details={"field": "edge_kind", "value": edge_kind},
            )
        rows = conn.execute(
            "SELECT * FROM data_dictionary_lineage_effective "
            "WHERE src_object_kind = $1 AND edge_kind = $2 "
            "ORDER BY dst_object_kind, dst_field_path",
            src, edge_kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_lineage_effective "
            "WHERE src_object_kind = $1 "
            "ORDER BY edge_kind, dst_object_kind, dst_field_path",
            src,
        )
    return [dict(r) for r in rows or []]


def list_edges_to(
    conn: Any,
    *,
    dst_object_kind: str,
    edge_kind: str | None = None,
) -> list[dict[str, Any]]:
    """Effective inbound edges into an object (merged view)."""
    dst = _require_text(dst_object_kind, field_name="dst_object_kind")
    if edge_kind:
        if edge_kind not in _VALID_EDGE_KINDS:
            raise PostgresWriteError(
                "data_dictionary_lineage.invalid_submission",
                f"edge_kind must be one of {sorted(_VALID_EDGE_KINDS)}",
                details={"field": "edge_kind", "value": edge_kind},
            )
        rows = conn.execute(
            "SELECT * FROM data_dictionary_lineage_effective "
            "WHERE dst_object_kind = $1 AND edge_kind = $2 "
            "ORDER BY src_object_kind, src_field_path",
            dst, edge_kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_lineage_effective "
            "WHERE dst_object_kind = $1 "
            "ORDER BY edge_kind, src_object_kind, src_field_path",
            dst,
        )
    return [dict(r) for r in rows or []]


def list_edges_layers(
    conn: Any,
    *,
    src_object_kind: str | None = None,
    dst_object_kind: str | None = None,
) -> list[dict[str, Any]]:
    """Raw per-source rows (auto/inferred/operator) for debugging/override UI."""
    where: list[str] = []
    params: list[Any] = []
    if src_object_kind:
        params.append(src_object_kind)
        where.append(f"src_object_kind = ${len(params)}")
    if dst_object_kind:
        params.append(dst_object_kind)
        where.append(f"dst_object_kind = ${len(params)}")
    sql = "SELECT * FROM data_dictionary_lineage"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += (
        " ORDER BY src_object_kind, src_field_path, "
        "dst_object_kind, dst_field_path, edge_kind, "
        "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
        "WHEN 'auto' THEN 2 ELSE 3 END"
    )
    rows = conn.execute(sql, *params)
    return [dict(r) for r in rows or []]


def count_edges_by_source(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT source, COUNT(*) AS n FROM data_dictionary_lineage GROUP BY source"
    )
    return {str(r["source"]): int(r["n"]) for r in rows or []}


__all__ = [
    "upsert_edge",
    "replace_projected_edges",
    "delete_edge",
    "list_edges_from",
    "list_edges_to",
    "list_edges_layers",
    "count_edges_by_source",
]
