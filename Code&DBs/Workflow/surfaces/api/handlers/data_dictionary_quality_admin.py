"""HTTP surface for the data dictionary quality-rules authority.

GET  /api/data-dictionary/quality                               — summary
GET  /api/data-dictionary/quality/rules                         — effective rules
     ?object_kind=<kind>&field_path=<path>&include_layers=1
GET  /api/data-dictionary/quality/runs                          — latest runs
     ?object_kind=<kind>&status=<pass|fail|error>&limit=<n>
GET  /api/data-dictionary/quality/runs/<object_kind>/<rule_kind>
     ?field_path=<path>&limit=<n>                              — run history
POST /api/data-dictionary/quality/reproject                     — run projector now
POST /api/data-dictionary/quality/evaluate                      — run evaluator now
     body: {"object_kind": "<optional filter>"}
PUT  /api/data-dictionary/quality                               — upsert operator rule
DELETE /api/data-dictionary/quality                             — clear operator rule
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from runtime.data_dictionary_quality import (
    DataDictionaryQualityError,
    clear_operator_rule,
    describe_rules,
    evaluate_all,
    latest_runs,
    quality_summary,
    run_history,
    set_operator_rule,
)

from ._shared import RouteEntry, _exact, _prefix, _read_json_body


_PREFIX = "/api/data-dictionary/quality"


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
    if isinstance(exc, DataDictionaryQualityError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


# ── GET handlers ─────────────────────────────────────────────────────


def _handle_summary(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, quality_summary(pg))
    except Exception as exc:
        _send_error(request, exc)


def _handle_list_rules(request: Any, path: str) -> None:
    params = _query(path)
    include_layers = params.get("include_layers", "").lower() in ("1", "true", "yes")
    try:
        pg = request.subsystems.get_pg_conn()
        payload = describe_rules(
            pg,
            object_kind=params.get("object_kind") or None,
            field_path=params.get("field_path"),
            include_layers=include_layers,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


def _handle_list_runs(request: Any, path: str) -> None:
    params = _query(path)
    try:
        limit = int(params.get("limit", "100") or 100)
    except ValueError:
        request._send_json(400, {"error": "limit must be int"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        payload = latest_runs(
            pg,
            object_kind=params.get("object_kind") or None,
            status=params.get("status") or None,
            limit=limit,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


def _handle_run_history(request: Any, path: str) -> None:
    # expected: /runs/<object_kind>/<rule_kind>
    segments = _tail_segments(path)
    if len(segments) != 3 or segments[0] != "runs":
        request._send_json(404, {"error": "not found"})
        return
    params = _query(path)
    try:
        limit = int(params.get("limit", "50") or 50)
    except ValueError:
        request._send_json(400, {"error": "limit must be int"})
        return
    try:
        pg = request.subsystems.get_pg_conn()
        payload = run_history(
            pg,
            object_kind=segments[1],
            rule_kind=segments[2],
            field_path=params.get("field_path") or "",
            limit=limit,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


# ── POST handlers ────────────────────────────────────────────────────


def _handle_reproject(request: Any, path: str) -> None:
    del path
    try:
        from memory.data_dictionary_quality_projector import (
            DataDictionaryQualityProjector,
        )

        projector = DataDictionaryQualityProjector(request.subsystems.get_pg_conn())
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


def _handle_evaluate(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    try:
        pg = request.subsystems.get_pg_conn()
        payload = evaluate_all(
            pg, object_kind=str(body.get("object_kind") or "") or None,
        )
        request._send_json(200, payload)
    except Exception as exc:
        _send_error(request, exc)


# ── PUT: set operator rule ──────────────────────────────────────────


def _handle_set_rule(request: Any, path: str) -> None:
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
        result = set_operator_rule(
            pg,
            object_kind=str(body.get("object_kind") or ""),
            field_path=str(body.get("field_path") or ""),
            rule_kind=str(body.get("rule_kind") or ""),
            expression=body.get("expression") or {},
            severity=str(body.get("severity") or "warning"),
            description=str(body.get("description") or ""),
            enabled=bool(body.get("enabled", True)),
            metadata=body.get("metadata"),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── DELETE: clear operator rule ─────────────────────────────────────


def _handle_clear_rule(request: Any, path: str) -> None:
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
        result = clear_operator_rule(
            pg,
            object_kind=str(body.get("object_kind") or ""),
            field_path=str(body.get("field_path") or ""),
            rule_kind=str(body.get("rule_kind") or ""),
        )
        request._send_json(200, result)
    except Exception as exc:
        _send_error(request, exc)


# ── Route tables ────────────────────────────────────────────────────
#
# Exact routes first, then prefix routes — `_handle_run_history` matches
# `/quality/runs/<obj>/<rk>` so the `_handle_list_runs` exact match for
# `/quality/runs` must come before it.


DATA_DICTIONARY_QUALITY_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_summary),
    (_exact(_PREFIX + "/rules"), _handle_list_rules),
    (_exact(_PREFIX + "/runs"), _handle_list_runs),
    (_prefix(_PREFIX + "/runs/"), _handle_run_history),
]

DATA_DICTIONARY_QUALITY_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/reproject"), _handle_reproject),
    (_exact(_PREFIX + "/evaluate"), _handle_evaluate),
]

DATA_DICTIONARY_QUALITY_PUT_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_set_rule),
]

DATA_DICTIONARY_QUALITY_DELETE_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX), _handle_clear_rule),
]


__all__ = [
    "DATA_DICTIONARY_QUALITY_DELETE_ROUTES",
    "DATA_DICTIONARY_QUALITY_GET_ROUTES",
    "DATA_DICTIONARY_QUALITY_POST_ROUTES",
    "DATA_DICTIONARY_QUALITY_PUT_ROUTES",
]
