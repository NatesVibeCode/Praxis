"""HTTP surface for the audit primitive — mirrors the MCP tool actions.

GET  /api/audit/playbook       -> structured usage guide
GET  /api/audit/registered     -> list audits + resolution patterns
GET  /api/audit/plan           -> dry-run: every finding + proposed action
POST /api/audit/apply          -> execute auto-safe actions
                                 body: {"only_patterns": [...], "max_per_pattern": N}
"""
from __future__ import annotations

import json
from typing import Any

from runtime.audit_primitive import (
    apply_autorunnable,
    derive_playbook,
    execute_all_contracts,
    execute_contract,
    plan_all,
    registered_audits,
    registered_contracts,
    registered_patterns,
)

# Eager registration so routes work on cold boot.
from runtime.audit_primitive_wiring import register_all as _register_wiring

_register_wiring()

from ._shared import RouteEntry, _exact, _read_json_body


_PREFIX = "/api/audit"


# Keep the playbook in sync with the MCP tool's copy by importing it
# directly. Single source of truth.
def _handle_playbook(request: Any, path: str) -> None:
    try:
        derived = derive_playbook()
        if derived.get("patterns_by_tier") and any(
            derived["patterns_by_tier"].values()
        ):
            request._send_json(200, {"playbook": derived})
            return
        from surfaces.mcp.tools.audit_primitive import _PLAYBOOK
        request._send_json(200, {"playbook": _PLAYBOOK})
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_registered(request: Any, path: str) -> None:
    try:
        audits = [
            {
                "audit_kind": c.audit_kind,
                "finding_kind": c.finding_kind,
                "default_pattern": c.default_pattern,
            }
            for c in registered_audits()
        ]
        patterns = [
            {
                "name": p.name,
                "applies_to": sorted(p.applies_to),
                "deterministic": p.deterministic,
                "has_executor": p.executor is not None,
            }
            for p in registered_patterns()
        ]
        request._send_json(200, {
            "audit_count": len(audits),
            "pattern_count": len(patterns),
            "audits": audits,
            "patterns": patterns,
        })
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_plan(request: Any, path: str) -> None:
    try:
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(path).query)
        max_tier = (qs.get("max_tier", [""])[0] or "").strip().lower() or None
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, plan_all(pg, max_tier=max_tier))
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_apply(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request) or {}
        raw = body.get("only_patterns")
        only = None
        if isinstance(raw, (list, tuple)):
            only = {str(s) for s in raw if str(s).strip()}
        elif isinstance(raw, str) and raw.strip():
            only = {s.strip() for s in raw.split(",") if s.strip()}
        max_per = int(body.get("max_per_pattern", 200))
        max_tier = (str(body.get("max_tier") or "").strip().lower()) or "none"
        pg = request.subsystems.get_pg_conn()
        request._send_json(
            200,
            apply_autorunnable(
                pg, only_patterns=only, max_per_pattern=max_per,
                max_tier=max_tier,
                authority_grant_ref=body.get("authority_grant_ref"),
            ),
        )
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_contracts(request: Any, path: str) -> None:
    try:
        rows = []
        for c in registered_contracts():
            rows.append({
                "name": c.name,
                "goal": c.goal,
                "verify": {"kind": c.verify.kind, "args": dict(c.verify.args)},
                "max_tier": c.max_tier,
                "allowed_patterns": sorted(c.allowed_patterns) if c.allowed_patterns else None,
                "max_iterations": c.max_iterations,
                "escalate_as_bug": c.escalate_as_bug,
            })
        request._send_json(200, {"count": len(rows), "contracts": rows})
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_execute_contract(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request) or {}
        name = str(body.get("name") or "").strip()
        if not name:
            request._send_json(400, {"error": "name is required"})
            return
        pg = request.subsystems.get_pg_conn()
        for c in registered_contracts():
            if c.name == name:
                request._send_json(200, execute_contract(pg, c))
                return
        request._send_json(404, {"error": f"unknown contract: {name}"})
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_execute_all_contracts(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, execute_all_contracts(pg))
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


AUDIT_PRIMITIVE_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/playbook"), _handle_playbook),
    (_exact(_PREFIX + "/registered"), _handle_registered),
    (_exact(_PREFIX + "/plan"), _handle_plan),
    (_exact(_PREFIX + "/contracts"), _handle_contracts),
]

AUDIT_PRIMITIVE_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/apply"), _handle_apply),
    (_exact(_PREFIX + "/execute_contract"), _handle_execute_contract),
    (_exact(_PREFIX + "/execute_all_contracts"), _handle_execute_all_contracts),
]


__all__ = [
    "AUDIT_PRIMITIVE_GET_ROUTES",
    "AUDIT_PRIMITIVE_POST_ROUTES",
]
