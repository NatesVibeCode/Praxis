"""HTTP surface for the unified data dictionary authority.

GET  /api/data-dictionary                    — catalog of object kinds (?category=)
GET  /api/data-dictionary/<object_kind>      — merged field list (?include_layers=1)
POST /api/data-dictionary/reproject          — refresh the data-dictionary authority now
PUT  /api/data-dictionary/<kind>/<path>      — upsert an operator override
DELETE /api/data-dictionary/<kind>/<path>    — clear an operator override
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from runtime.data_dictionary import (
    DataDictionaryBoundaryError,
    clear_operator_override,
    describe_object,
    list_object_kinds,
    set_operator_override,
)
from memory.data_dictionary_refresh import refresh_data_dictionary_authority

from ._shared import RouteEntry, _exact, _prefix, _read_json_body


_PREFIX = "/api/data-dictionary"


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
    if isinstance(exc, DataDictionaryBoundaryError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


# ── GET handlers ─────────────────────────────────────────────────────


def _handle_list(request: Any, path: str) -> None:
    params = _query(path)
    try:
        pg = request.subsystems.get_pg_conn()
        rows = list_object_kinds(pg, category=params.get("category") or None)
        request._send_json(200, {"objects": rows, "count": len(rows)})
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
        payload = describe_object(
            pg, object_kind=segments[0], include_layers=include_layers
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


# ── POST: reproject ──────────────────────────────────────────────────


def _handle_reproject(request: Any, path: str) -> None:
    del path
    try:
        result = refresh_data_dictionary_authority(request.subsystems.get_pg_conn())
        request._send_json(
            200,
            {
                "ok": bool(result.get("ok", True)),
                "duration_ms": result.get("duration_ms"),
                "error": result.get("error"),
                "modules": result.get("modules", []),
            },
        )
    except Exception as exc:
        _send_error(request, exc)


# ── PUT: operator override ───────────────────────────────────────────


def _handle_set_override(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    if len(segments) != 2:
        request._send_json(404, {"error": "not found"})
        return
    object_kind, field_path = segments
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
        result = set_operator_override(
            pg,
            object_kind=object_kind,
            field_path=field_path,
            field_kind=body.get("field_kind"),
            label=body.get("label"),
            description=body.get("description"),
            required=body.get("required"),
            default_value=body.get("default_value"),
            valid_values=body.get("valid_values"),
            examples=body.get("examples"),
            deprecation_notes=body.get("deprecation_notes"),
            display_order=body.get("display_order"),
            metadata=body.get("metadata"),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── DELETE: clear override ───────────────────────────────────────────


def _handle_clear_override(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    if len(segments) != 2:
        request._send_json(404, {"error": "not found"})
        return
    object_kind, field_path = segments
    try:
        pg = request.subsystems.get_pg_conn()
        result = clear_operator_override(
            pg, object_kind=object_kind, field_path=field_path
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


DATA_DICTIONARY_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_list),
    (_prefix(_PREFIX + "/"), _handle_describe),
]

DATA_DICTIONARY_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/reproject"), _handle_reproject),
]

DATA_DICTIONARY_PUT_ROUTES: list[RouteEntry] = [
    (_prefix(_PREFIX + "/"), _handle_set_override),
]

DATA_DICTIONARY_DELETE_ROUTES: list[RouteEntry] = [
    (_prefix(_PREFIX + "/"), _handle_clear_override),
]


__all__ = [
    "DATA_DICTIONARY_DELETE_ROUTES",
    "DATA_DICTIONARY_GET_ROUTES",
    "DATA_DICTIONARY_POST_ROUTES",
    "DATA_DICTIONARY_PUT_ROUTES",
]
