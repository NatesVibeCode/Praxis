"""HTTP surface for cross-axis data-dictionary impact analysis.

GET /api/data-dictionary/impact/<object_kind>
    ?direction=downstream|upstream
    &max_depth=5
    &edge_kind=<optional lineage edge filter>

Returns the lineage blast-radius plus per-node classifications, stewards,
quality rules, and latest-run status, with an aggregate rollup across
the whole radius.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from runtime.data_dictionary_impact import (
    DataDictionaryImpactError,
    impact_analysis,
)

from ._shared import RouteEntry, _prefix


_PREFIX = "/api/data-dictionary/impact"


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
    if isinstance(exc, DataDictionaryImpactError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_impact(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    if len(segments) != 1:
        request._send_json(404, {"error": "not found"})
        return
    params = _query(path)
    direction = params.get("direction", "downstream").strip() or "downstream"
    try:
        max_depth = int(params.get("max_depth", "5") or 5)
    except ValueError:
        request._send_json(400, {"error": "max_depth must be int"})
        return
    edge_kind = params.get("edge_kind") or None
    try:
        pg = request.subsystems.get_pg_conn()
        payload = impact_analysis(
            pg,
            object_kind=segments[0],
            direction=direction,
            max_depth=max_depth,
            edge_kind=edge_kind,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


DATA_DICTIONARY_IMPACT_GET_ROUTES: list[RouteEntry] = [
    (_prefix(_PREFIX + "/"), _handle_impact),
]


__all__ = ["DATA_DICTIONARY_IMPACT_GET_ROUTES"]
