"""HTTP surface for the data dictionary lineage authority.

GET  /api/data-dictionary/lineage                       — graph summary
GET  /api/data-dictionary/lineage/<object_kind>         — one-hop neighborhood
     ?direction=upstream|downstream|both
     ?edge_kind=<kind>
     ?include_layers=1
GET  /api/data-dictionary/lineage/<object_kind>/impact  — walk reachable graph
     ?direction=upstream|downstream  (default downstream)
     ?max_depth=<int 1..10>
     ?edge_kind=<kind>
POST /api/data-dictionary/lineage/reproject             — run projector now
PUT  /api/data-dictionary/lineage                       — upsert operator edge
DELETE /api/data-dictionary/lineage                     — clear operator edge

Request / response bodies on PUT and DELETE are JSON, keyed by the edge
identifier tuple. Path parameters are URL-encoded so object_kind values
that contain '/' or ':' round-trip intact.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from runtime.data_dictionary_lineage import (
    DataDictionaryLineageError,
    clear_operator_edge,
    describe_edges,
    lineage_summary,
    set_operator_edge,
    walk_impact,
)

from ._shared import RouteEntry, _exact, _prefix, _prefix_suffix, _read_json_body


_PREFIX = "/api/data-dictionary/lineage"


def _query(path: str) -> dict[str, str]:
    parsed = urlparse(path)
    out: dict[str, str] = {}
    for pair in parsed.query.split("&"):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        if key:
            out[key] = unquote(value)
    return out


def _tail_segments(path: str) -> list[str]:
    parsed = urlparse(path)
    raw = parsed.path
    if not raw.startswith(_PREFIX):
        return []
    tail = raw[len(_PREFIX):].lstrip("/")
    return [unquote(seg) for seg in tail.split("/") if seg]


def _send_error(request: Any, exc: Exception) -> None:
    if isinstance(exc, DataDictionaryLineageError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


# ── GET handlers ─────────────────────────────────────────────────────


def _handle_summary(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, lineage_summary(pg))
    except Exception as exc:
        _send_error(request, exc)


def _handle_describe(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    if len(segments) != 1 or segments[0] == "reproject":
        request._send_json(404, {"error": "not found"})
        return
    params = _query(path)
    include_layers = params.get("include_layers", "").lower() in ("1", "true", "yes")
    try:
        pg = request.subsystems.get_pg_conn()
        payload = describe_edges(
            pg,
            object_kind=segments[0],
            direction=params.get("direction") or "both",
            edge_kind=params.get("edge_kind") or None,
            include_layers=include_layers,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


def _handle_impact(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    if len(segments) != 2 or segments[1] != "impact":
        request._send_json(404, {"error": "not found"})
        return
    params = _query(path)
    try:
        max_depth = int(params.get("max_depth", "5") or 5)
    except ValueError:
        request._send_json(400, {"error": "max_depth must be int"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        payload = walk_impact(
            pg,
            object_kind=segments[0],
            direction=params.get("direction") or "downstream",
            max_depth=max_depth,
            edge_kind=params.get("edge_kind") or None,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


# ── POST: reproject ──────────────────────────────────────────────────


def _handle_reproject(request: Any, path: str) -> None:
    del path
    try:
        from memory.data_dictionary_lineage_projector import (
            DataDictionaryLineageProjector,
        )

        projector = DataDictionaryLineageProjector(request.subsystems.get_pg_conn())
        result = projector.run()
        request._send_json(
            200,
            {
                "ok": getattr(result, "ok", True),
                "duration_ms": getattr(result, "duration_ms", None),
                "error": getattr(result, "error", None),
            },
        )
    except Exception as exc:
        _send_error(request, exc)


# ── PUT: set operator edge ──────────────────────────────────────────


def _handle_set_edge(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
    except Exception as exc:
        request._send_json(400, {"error": f"invalid JSON: {exc}"})
        return
    if not isinstance(body, dict):
        request._send_json(400, {"error": "body must be an object"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        result = set_operator_edge(
            pg,
            src_object_kind=str(body.get("src_object_kind") or ""),
            src_field_path=str(body.get("src_field_path") or ""),
            dst_object_kind=str(body.get("dst_object_kind") or ""),
            dst_field_path=str(body.get("dst_field_path") or ""),
            edge_kind=str(body.get("edge_kind") or ""),
            confidence=float(body.get("confidence", 1.0)),
            metadata=body.get("metadata"),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── DELETE: clear operator edge ────────────────────────────────────


def _handle_clear_edge(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
    except Exception as exc:
        request._send_json(400, {"error": f"invalid JSON: {exc}"})
        return
    if not isinstance(body, dict):
        request._send_json(400, {"error": "body must be an object"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        result = clear_operator_edge(
            pg,
            src_object_kind=str(body.get("src_object_kind") or ""),
            src_field_path=str(body.get("src_field_path") or ""),
            dst_object_kind=str(body.get("dst_object_kind") or ""),
            dst_field_path=str(body.get("dst_field_path") or ""),
            edge_kind=str(body.get("edge_kind") or ""),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── Route tables ────────────────────────────────────────────────────
#
# Route order matters: the more-specific `impact` suffix must be listed
# before the generic `<object_kind>` prefix so the neighborhood handler
# does not swallow impact-walk requests.


DATA_DICTIONARY_LINEAGE_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_summary),
    (_prefix_suffix(_PREFIX + "/", "/impact"), _handle_impact),
    (_prefix(_PREFIX + "/"), _handle_describe),
]

DATA_DICTIONARY_LINEAGE_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/reproject"), _handle_reproject),
]

DATA_DICTIONARY_LINEAGE_PUT_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_set_edge),
]

DATA_DICTIONARY_LINEAGE_DELETE_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_clear_edge),
]


__all__ = [
    "DATA_DICTIONARY_LINEAGE_DELETE_ROUTES",
    "DATA_DICTIONARY_LINEAGE_GET_ROUTES",
    "DATA_DICTIONARY_LINEAGE_POST_ROUTES",
    "DATA_DICTIONARY_LINEAGE_PUT_ROUTES",
]
