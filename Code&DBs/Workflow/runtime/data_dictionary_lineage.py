"""Runtime authority for data dictionary lineage.

Three layers coexist in `data_dictionary_lineage`:
- `auto`     — projector-derived (FK graph, view deps, dataset_promotions,
               ingest manifests, workflow step I/O)
- `inferred` — sampler-derived (value-overlap, usage-based)
- `operator` — hand-authored, highest precedence

Projectors call `apply_projected_edges()` to replace their own auto/inferred
rows idempotently (keyed on origin_ref.projector). Operators mutate through
`set_operator_edge()` / `clear_operator_edge()`. Read paths go through
`describe_edges()` which returns merged rows from
`data_dictionary_lineage_effective` plus a one-hop neighborhood walk.
"""

from __future__ import annotations

from typing import Any, Iterable

from storage.postgres.data_dictionary_lineage_repository import (
    count_edges_by_source,
    delete_edge,
    list_edges_from,
    list_edges_layers,
    list_edges_to,
    replace_projected_edges,
    upsert_edge,
)
from storage.postgres.data_dictionary_repository import get_object
from storage.postgres.validators import PostgresWriteError

_ALLOWED_EDGE_KINDS = frozenset({
    "references", "derives_from", "projects_to", "ingests_from",
    "produces", "consumes", "promotes_to", "same_as",
})


class DataDictionaryLineageError(RuntimeError):
    """Raised when a lineage authority call is rejected."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _raise_storage(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise DataDictionaryLineageError(str(exc), status_code=status_code) from exc


def _ensure_object_known(conn: Any, object_kind: str, *, role: str) -> None:
    row = get_object(conn, object_kind=object_kind)
    if row is None:
        raise DataDictionaryLineageError(
            f"{role} object_kind {object_kind!r} is not registered in the data dictionary",
            status_code=404,
        )


# --- projector-facing API -------------------------------------------------


def apply_projected_edges(
    conn: Any,
    *,
    projector_tag: str,
    edges: Iterable[dict[str, Any]],
    source: str = "auto",
) -> dict[str, Any]:
    """Idempotently write edges for one projector.

    Each edge is a dict with:
        src_object_kind, dst_object_kind, edge_kind  (required)
        src_field_path, dst_field_path               (optional, defaults "")
        confidence, origin_ref, metadata              (optional)
    """
    tag = _text(projector_tag)
    if not tag:
        raise DataDictionaryLineageError("projector_tag is required")
    if source not in ("auto", "inferred"):
        raise DataDictionaryLineageError(
            "apply_projected_edges only writes auto/inferred layers"
        )

    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(edges or []):
        if not isinstance(raw, dict):
            raise DataDictionaryLineageError(
                f"edges[{index}] must be an object"
            )
        src = _text(raw.get("src_object_kind"))
        dst = _text(raw.get("dst_object_kind"))
        kind = _text(raw.get("edge_kind"))
        if not src or not dst or not kind:
            raise DataDictionaryLineageError(
                f"edges[{index}] requires src_object_kind, dst_object_kind, edge_kind"
            )
        if kind not in _ALLOWED_EDGE_KINDS:
            raise DataDictionaryLineageError(
                f"edges[{index}].edge_kind={kind!r} not in {sorted(_ALLOWED_EDGE_KINDS)}"
            )
        origin_ref = dict(raw.get("origin_ref") or {})
        origin_ref.setdefault("projector", tag)
        normalized.append({
            "src_object_kind": src,
            "src_field_path": _text(raw.get("src_field_path")),
            "dst_object_kind": dst,
            "dst_field_path": _text(raw.get("dst_field_path")),
            "edge_kind": kind,
            "confidence": float(raw.get("confidence", 1.0)),
            "origin_ref": origin_ref,
            "metadata": raw.get("metadata") or {},
        })

    try:
        written = replace_projected_edges(
            conn,
            source=source,
            projector_tag=tag,
            edges=normalized,
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"projector": tag, "source": source, "edges_written": written}


# --- operator-facing API --------------------------------------------------


def set_operator_edge(
    conn: Any,
    *,
    src_object_kind: str,
    dst_object_kind: str,
    edge_kind: str,
    src_field_path: str = "",
    dst_field_path: str = "",
    confidence: float = 1.0,
    metadata: Any = None,
) -> dict[str, Any]:
    """Write an operator-layer edge that outranks auto/inferred."""
    src = _text(src_object_kind)
    dst = _text(dst_object_kind)
    kind = _text(edge_kind)
    if not src or not dst:
        raise DataDictionaryLineageError(
            "src_object_kind and dst_object_kind are required"
        )
    if kind not in _ALLOWED_EDGE_KINDS:
        raise DataDictionaryLineageError(
            f"edge_kind must be one of: {', '.join(sorted(_ALLOWED_EDGE_KINDS))}"
        )
    _ensure_object_known(conn, src, role="src")
    _ensure_object_known(conn, dst, role="dst")
    try:
        row = upsert_edge(
            conn,
            src_object_kind=src,
            src_field_path=_text(src_field_path),
            dst_object_kind=dst,
            dst_field_path=_text(dst_field_path),
            edge_kind=kind,
            source="operator",
            confidence=confidence,
            origin_ref={"source": "operator"},
            metadata=metadata or {},
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"edge": dict(row)}


def clear_operator_edge(
    conn: Any,
    *,
    src_object_kind: str,
    dst_object_kind: str,
    edge_kind: str,
    src_field_path: str = "",
    dst_field_path: str = "",
) -> dict[str, Any]:
    src = _text(src_object_kind)
    dst = _text(dst_object_kind)
    kind = _text(edge_kind)
    if not src or not dst or not kind:
        raise DataDictionaryLineageError(
            "src_object_kind, dst_object_kind, and edge_kind are required"
        )
    try:
        removed = delete_edge(
            conn,
            src_object_kind=src,
            src_field_path=_text(src_field_path),
            dst_object_kind=dst,
            dst_field_path=_text(dst_field_path),
            edge_kind=kind,
            source="operator",
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {
        "src_object_kind": src, "dst_object_kind": dst,
        "edge_kind": kind, "removed": removed,
    }


# --- read API -------------------------------------------------------------


def describe_edges(
    conn: Any,
    *,
    object_kind: str,
    direction: str = "both",
    edge_kind: str | None = None,
    include_layers: bool = False,
) -> dict[str, Any]:
    """Return effective edges touching `object_kind` (one hop)."""
    kind = _text(object_kind)
    if not kind:
        raise DataDictionaryLineageError("object_kind is required")
    direction = (direction or "both").lower()
    if direction not in ("upstream", "downstream", "both"):
        raise DataDictionaryLineageError(
            "direction must be 'upstream', 'downstream', or 'both'"
        )
    ek = _text(edge_kind) or None
    if ek and ek not in _ALLOWED_EDGE_KINDS:
        raise DataDictionaryLineageError(
            f"edge_kind must be one of: {', '.join(sorted(_ALLOWED_EDGE_KINDS))}"
        )

    upstream: list[dict[str, Any]] = []
    downstream: list[dict[str, Any]] = []
    if direction in ("upstream", "both"):
        upstream = list_edges_to(conn, dst_object_kind=kind, edge_kind=ek)
    if direction in ("downstream", "both"):
        downstream = list_edges_from(conn, src_object_kind=kind, edge_kind=ek)

    response: dict[str, Any] = {
        "object_kind": kind,
        "upstream": upstream,
        "downstream": downstream,
    }
    if include_layers:
        response["layers_upstream"] = list_edges_layers(
            conn, dst_object_kind=kind
        )
        response["layers_downstream"] = list_edges_layers(
            conn, src_object_kind=kind
        )
    return response


def walk_impact(
    conn: Any,
    *,
    object_kind: str,
    direction: str = "downstream",
    max_depth: int = 5,
    edge_kind: str | None = None,
) -> dict[str, Any]:
    """Walk the lineage graph and return reachable nodes + traversed edges.

    Direction:
      downstream  — follow outbound edges from object_kind
      upstream    — follow inbound edges into object_kind
    """
    root = _text(object_kind)
    if not root:
        raise DataDictionaryLineageError("object_kind is required")
    if direction not in ("upstream", "downstream"):
        raise DataDictionaryLineageError(
            "direction must be 'upstream' or 'downstream'"
        )
    depth_limit = max(1, min(10, int(max_depth or 5)))
    ek = _text(edge_kind) or None

    visited: set[str] = {root}
    frontier: list[tuple[str, int]] = [(root, 0)]
    edges_out: list[dict[str, Any]] = []

    while frontier:
        current, depth = frontier.pop(0)
        if depth >= depth_limit:
            continue
        if direction == "downstream":
            neighbors = list_edges_from(
                conn, src_object_kind=current, edge_kind=ek
            )
            next_key = "dst_object_kind"
        else:
            neighbors = list_edges_to(
                conn, dst_object_kind=current, edge_kind=ek
            )
            next_key = "src_object_kind"
        for edge in neighbors:
            edges_out.append(edge)
            nxt = str(edge.get(next_key) or "")
            if nxt and nxt not in visited:
                visited.add(nxt)
                frontier.append((nxt, depth + 1))

    return {
        "root": root,
        "direction": direction,
        "max_depth": depth_limit,
        "nodes": sorted(visited),
        "edges": edges_out,
    }


def lineage_summary(conn: Any) -> dict[str, Any]:
    """Counts useful for health dashboards."""
    return {"edges_by_source": count_edges_by_source(conn)}


__all__ = [
    "DataDictionaryLineageError",
    "apply_projected_edges",
    "clear_operator_edge",
    "describe_edges",
    "lineage_summary",
    "set_operator_edge",
    "walk_impact",
]
