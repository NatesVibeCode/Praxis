"""HTTP surface for the wiring + hard-path audit.

GET /api/data-dictionary/wiring-audit                 -> full audit
GET /api/data-dictionary/wiring-audit/hard-paths      -> paths only
GET /api/data-dictionary/wiring-audit/decisions       -> unreferenced decisions
GET /api/data-dictionary/wiring-audit/orphans         -> orphan tables
"""
from __future__ import annotations

from typing import Any

from runtime.data_dictionary_wiring_audit import (
    audit_code_orphan_tables,
    audit_hard_paths,
    audit_trend,
    audit_unreferenced_decisions,
    run_full_audit,
)

from ._shared import RouteEntry, _exact


_PREFIX = "/api/data-dictionary/wiring-audit"


def _handle_full(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, run_full_audit(pg))
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_hard_paths(request: Any, path: str) -> None:
    try:
        findings = audit_hard_paths()
        request._send_json(200, {
            "total": len(findings),
            "findings": [f.to_payload() for f in findings],
        })
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_decisions(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        findings = audit_unreferenced_decisions(pg)
        request._send_json(200, {
            "total": len(findings),
            "findings": [f.to_payload() for f in findings],
        })
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_trend(request: Any, path: str) -> None:
    try:
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(path).query)
        limit = int(qs.get("limit", ["50"])[0])
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, audit_trend(pg, limit=limit))
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_orphans(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        findings = audit_code_orphan_tables(pg)
        request._send_json(200, {
            "total": len(findings),
            "findings": [f.to_payload() for f in findings],
        })
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


DATA_DICTIONARY_WIRING_AUDIT_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/hard-paths"), _handle_hard_paths),
    (_exact(_PREFIX + "/decisions"), _handle_decisions),
    (_exact(_PREFIX + "/orphans"), _handle_orphans),
    (_exact(_PREFIX + "/trend"), _handle_trend),
    (_exact(_PREFIX), _handle_full),
]


__all__ = ["DATA_DICTIONARY_WIRING_AUDIT_GET_ROUTES"]
