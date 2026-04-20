"""HTTP surface for the data dictionary stewardship authority.

GET  /api/data-dictionary/stewardship                       — summary
GET  /api/data-dictionary/stewardship/by-steward            — reverse lookup
     ?steward_id=<id>[&steward_kind=<kind>]
GET  /api/data-dictionary/stewardship/<object_kind>         — effective stewards
     ?field_path=<path>
     ?include_layers=1
POST /api/data-dictionary/stewardship/reproject             — run projector now
PUT  /api/data-dictionary/stewardship                       — upsert operator steward
DELETE /api/data-dictionary/stewardship                     — clear operator steward

PUT/DELETE bodies are JSON keyed by the
(object_kind, field_path, steward_kind, steward_id) tuple.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from runtime.data_dictionary_stewardship import (
    DataDictionaryStewardshipError,
    clear_operator_steward,
    describe_stewards,
    find_by_steward,
    set_operator_steward,
    stewardship_summary,
)

from ._shared import RouteEntry, _exact, _prefix, _read_json_body


_PREFIX = "/api/data-dictionary/stewardship"


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
    if isinstance(exc, DataDictionaryStewardshipError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


# ── GET handlers ─────────────────────────────────────────────────────


def _handle_summary(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, stewardship_summary(pg))
    except Exception as exc:
        _send_error(request, exc)


def _handle_by_steward(request: Any, path: str) -> None:
    params = _query(path)
    steward_id = params.get("steward_id") or ""
    if not steward_id:
        request._send_json(400, {"error": "steward_id query parameter is required"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        payload = find_by_steward(
            pg,
            steward_id=steward_id,
            steward_kind=params.get("steward_kind") or None,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


def _handle_describe(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    if len(segments) != 1 or segments[0] in ("reproject", "by-steward"):
        request._send_json(404, {"error": "not found"})
        return
    params = _query(path)
    include_layers = params.get("include_layers", "").lower() in ("1", "true", "yes")
    field_path = params.get("field_path")
    try:
        pg = request.subsystems.get_pg_conn()
        payload = describe_stewards(
            pg,
            object_kind=segments[0],
            field_path=field_path,
            include_layers=include_layers,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


# ── POST: reproject ──────────────────────────────────────────────────


def _handle_reproject(request: Any, path: str) -> None:
    del path
    try:
        from memory.data_dictionary_stewardship_projector import (
            DataDictionaryStewardshipProjector,
        )

        projector = DataDictionaryStewardshipProjector(
            request.subsystems.get_pg_conn()
        )
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


# ── PUT: set operator steward ───────────────────────────────────────


def _handle_set(request: Any, path: str) -> None:
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
        result = set_operator_steward(
            pg,
            object_kind=str(body.get("object_kind") or ""),
            field_path=str(body.get("field_path") or ""),
            steward_kind=str(body.get("steward_kind") or ""),
            steward_id=str(body.get("steward_id") or ""),
            steward_type=str(body.get("steward_type") or "person"),
            confidence=float(body.get("confidence", 1.0)),
            metadata=body.get("metadata"),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── DELETE: clear operator steward ─────────────────────────────────


def _handle_clear(request: Any, path: str) -> None:
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
        result = clear_operator_steward(
            pg,
            object_kind=str(body.get("object_kind") or ""),
            field_path=str(body.get("field_path") or ""),
            steward_kind=str(body.get("steward_kind") or ""),
            steward_id=str(body.get("steward_id") or ""),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── Route tables ────────────────────────────────────────────────────


DATA_DICTIONARY_STEWARDSHIP_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_summary),
    (_exact(_PREFIX + "/by-steward"), _handle_by_steward),
    (_prefix(_PREFIX + "/"), _handle_describe),
]

DATA_DICTIONARY_STEWARDSHIP_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/reproject"), _handle_reproject),
]

DATA_DICTIONARY_STEWARDSHIP_PUT_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_set),
]

DATA_DICTIONARY_STEWARDSHIP_DELETE_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_clear),
]


__all__ = [
    "DATA_DICTIONARY_STEWARDSHIP_DELETE_ROUTES",
    "DATA_DICTIONARY_STEWARDSHIP_GET_ROUTES",
    "DATA_DICTIONARY_STEWARDSHIP_POST_ROUTES",
    "DATA_DICTIONARY_STEWARDSHIP_PUT_ROUTES",
]
