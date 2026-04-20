"""HTTP surface for the data dictionary classification / tag authority.

GET  /api/data-dictionary/classifications                       — summary
GET  /api/data-dictionary/classifications/by-tag                — compliance query
     ?tag_key=<key>[&tag_value=<value>]
GET  /api/data-dictionary/classifications/<object_kind>         — effective tags
     ?field_path=<path>
     ?include_layers=1
POST /api/data-dictionary/classifications/reproject             — run projector now
PUT  /api/data-dictionary/classifications                       — upsert operator tag
DELETE /api/data-dictionary/classifications                     — clear operator tag

The PUT/DELETE bodies are JSON keyed by the (object_kind, field_path, tag_key)
tuple. Path parameters are URL-encoded so object_kind values that contain
'/' or ':' round-trip intact through percent-encoding.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from runtime.data_dictionary_classifications import (
    DataDictionaryClassificationError,
    classification_summary,
    clear_operator_classification,
    describe_classifications,
    find_by_tag,
    set_operator_classification,
)

from ._shared import RouteEntry, _exact, _prefix, _read_json_body


_PREFIX = "/api/data-dictionary/classifications"


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
    if isinstance(exc, DataDictionaryClassificationError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


# ── GET handlers ─────────────────────────────────────────────────────


def _handle_summary(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, classification_summary(pg))
    except Exception as exc:
        _send_error(request, exc)


def _handle_by_tag(request: Any, path: str) -> None:
    params = _query(path)
    tag_key = params.get("tag_key") or ""
    if not tag_key:
        request._send_json(400, {"error": "tag_key query parameter is required"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        payload = find_by_tag(
            pg,
            tag_key=tag_key,
            tag_value=params.get("tag_value") or None,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


def _handle_describe(request: Any, path: str) -> None:
    segments = _tail_segments(path)
    # `by-tag` and `reproject` are handled by their own exact routes; if we
    # end up here with one of them, or with a multi-segment tail, treat as
    # not-found.
    if len(segments) != 1 or segments[0] in ("reproject", "by-tag"):
        request._send_json(404, {"error": "not found"})
        return
    params = _query(path)
    include_layers = params.get("include_layers", "").lower() in ("1", "true", "yes")
    field_path = params.get("field_path")
    try:
        pg = request.subsystems.get_pg_conn()
        payload = describe_classifications(
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
        from memory.data_dictionary_classifications_projector import (
            DataDictionaryClassificationsProjector,
        )

        projector = DataDictionaryClassificationsProjector(
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


# ── PUT: set operator tag ───────────────────────────────────────────


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
        result = set_operator_classification(
            pg,
            object_kind=str(body.get("object_kind") or ""),
            field_path=str(body.get("field_path") or ""),
            tag_key=str(body.get("tag_key") or ""),
            tag_value=str(body.get("tag_value") or ""),
            confidence=float(body.get("confidence", 1.0)),
            metadata=body.get("metadata"),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── DELETE: clear operator tag ─────────────────────────────────────


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
        result = clear_operator_classification(
            pg,
            object_kind=str(body.get("object_kind") or ""),
            field_path=str(body.get("field_path") or ""),
            tag_key=str(body.get("tag_key") or ""),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── Route tables ────────────────────────────────────────────────────
#
# Route order matters: the exact `by-tag` and `reproject` routes must come
# before the generic `<object_kind>` prefix so those suffixes don't get
# interpreted as object_kind values.


DATA_DICTIONARY_CLASSIFICATIONS_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_summary),
    (_exact(_PREFIX + "/by-tag"), _handle_by_tag),
    (_prefix(_PREFIX + "/"), _handle_describe),
]

DATA_DICTIONARY_CLASSIFICATIONS_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/reproject"), _handle_reproject),
]

DATA_DICTIONARY_CLASSIFICATIONS_PUT_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_set),
]

DATA_DICTIONARY_CLASSIFICATIONS_DELETE_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_clear),
]


__all__ = [
    "DATA_DICTIONARY_CLASSIFICATIONS_DELETE_ROUTES",
    "DATA_DICTIONARY_CLASSIFICATIONS_GET_ROUTES",
    "DATA_DICTIONARY_CLASSIFICATIONS_POST_ROUTES",
    "DATA_DICTIONARY_CLASSIFICATIONS_PUT_ROUTES",
]
