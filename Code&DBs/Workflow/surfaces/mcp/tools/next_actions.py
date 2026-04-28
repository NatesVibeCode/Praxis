"""Compatibility wrapper for the retired ``praxis_next_actions`` surface.

``praxis_next`` is the progressive front door. This legacy name delegates to
``operator.next`` with ``action='next'`` so old callers keep working without
creating a second next-action authority.
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems
from runtime.operations.queries.operator_next import OperatorNextQuery, handle_operator_next

from ..subsystems import _subs


_OPERATION_NAME = "operator.next"


def tool_praxis_next_actions(params: dict, _progress_emitter=None) -> dict[str, Any]:
    """Return progressive next-action guidance through the canonical front door."""

    payload = dict(params or {})
    payload["action"] = "next"
    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=1, message="Computing legal next actions")
    try:
        result = execute_operation_from_subsystems(
            _subs,
            operation_name=_OPERATION_NAME,
            payload=payload,
        )
        if isinstance(result, dict):
            result.setdefault("_meta", {})["dispatch_path"] = "gateway"
    except Exception as exc:
        try:
            direct = handle_operator_next(OperatorNextQuery(**payload), _subs)
        except Exception as handler_exc:
            return {
                "ok": False,
                "error": str(handler_exc),
                "error_code": "operator.next.failed",
                "gateway_error": f"{type(exc).__name__}: {exc}",
            }
        direct.setdefault("_meta", {})["dispatch_path"] = "direct_fallback"
        direct["_meta"]["gateway_error"] = f"{type(exc).__name__}: {exc}"
        result = direct
    if _progress_emitter:
        _progress_emitter.emit(progress=1, total=1, message="Done")
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_next_actions": (
        tool_praxis_next_actions,
        {
            "kind": "alias",
            "description": (
                "Deprecated compatibility alias for praxis_next(action='next'). "
                "Use praxis_next for progressive disclosure across next actions, "
                "launch gating, failure triage, manifest audit, toolsmith dedupe, "
                "and unlock-frontier analysis."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Operator intent or messy prose to ground against current authority.",
                    },
                    "workflow_id": {
                        "type": "string",
                        "description": "Optional workflow id anchor for workflow/build graph context.",
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Optional workflow run id anchor for runtime proof actions.",
                    },
                    "node_id": {
                        "type": "string",
                        "description": "Selected build graph node id when build_graph is provided.",
                    },
                    "build_graph": {
                        "type": "object",
                        "description": "Current workflow build graph; enables capability-catalog suggest-next.",
                    },
                    "source_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Authority refs such as BUG-* ids, receipt ids, or decision refs.",
                        "default": [],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum legal and blocked actions to return per group.",
                        "default": 8,
                    },
                    "include_blocked": {
                        "type": "boolean",
                        "description": "Include illegal-but-useful blocked moves and repair actions.",
                        "default": True,
                    },
                    "match_limit": {
                        "type": "integer",
                        "description": "Candidate limit for compile/search-style helper calls.",
                        "default": 5,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "stable",
                "recommended_alias": "next-actions",
                "replacement": "praxis_next",
                "when_to_use": "Legacy alias only; prefer praxis_next(action='next').",
                "when_not_to_use": "Do not build new workflows against this name.",
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Legacy next-actions call",
                        "input": {
                            "intent": "Fix workflow retries so every retry declares the failed receipt and retry delta."
                        },
                    }
                ],
            },
        },
    ),
}
