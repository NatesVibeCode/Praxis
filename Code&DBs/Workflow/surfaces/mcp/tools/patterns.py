"""Tools: praxis_patterns."""
from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems

from ..runtime_context import get_current_workflow_mcp_context
from ..subsystems import _subs


_READ_ACTIONS = frozenset({"list", "candidates", "evidence"})


def tool_praxis_patterns(params: dict) -> dict:
    """Inspect or materialize durable platform patterns."""
    action = str(params.get("action") or "list").strip().lower()

    if get_current_workflow_mcp_context() is not None:
        return {
            "ok": False,
            "error": "praxis_patterns is not permitted inside a workflow session.",
            "reason_code": "workflow_mcp.patterns_unscoped_system_read",
        }

    payload = {k: v for k, v in dict(params or {}).items() if k != "action"}
    if action in _READ_ACTIONS:
        payload["action"] = action
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator_patterns",
            payload=payload,
            requested_mode="query",
        )
    if action == "materialize":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="pattern_materialize_candidates",
            payload=payload,
            requested_mode="command",
        )
    return {
        "ok": False,
        "error": f"Unknown patterns action: {action}",
        "reason_code": "patterns.unknown_action",
    }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_patterns": (
        tool_praxis_patterns,
        {
            "kind": "analytics",
            "description": (
                "Inspect and materialize durable platform patterns: recurring failure shapes "
                "clustered from friction events, bugs, and receipts. Patterns sit between "
                "raw evidence and bug tickets so repeated platform pain becomes one "
                "queryable authority object with evidence links and promotion rules.\n\n"
                "USE WHEN: a failure appears repeatedly, bugs look noisy, friction patterns "
                "should become durable, or you need to ask what keeps becoming possible.\n\n"
                "EXAMPLES:\n"
                "  List patterns:       praxis_patterns(action='list')\n"
                "  Find candidates:     praxis_patterns(action='candidates', sources=['friction','bugs','receipts'])\n"
                "  Hydrate candidates:  praxis_patterns(action='candidates', include_hydration=True)\n"
                "  Materialize:         praxis_patterns(action='materialize', threshold=3)\n"
                "  Pattern evidence:    praxis_patterns(action='evidence', pattern_ref='PATTERN-ABC123')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'list', 'candidates', 'evidence', or 'materialize'.",
                        "enum": ["list", "candidates", "evidence", "materialize"],
                        "default": "list",
                    },
                    "pattern_ref": {
                        "type": "string",
                        "description": "Pattern ref for action=evidence.",
                    },
                    "pattern_kind": {
                        "type": "string",
                        "description": "Filter list by kind.",
                        "enum": [
                            "architecture_smell",
                            "runtime_failure_pattern",
                            "operator_friction",
                            "missing_authority",
                            "weak_observability",
                        ],
                    },
                    "status": {
                        "type": "string",
                        "description": "Filter list by lifecycle status, or materialize with this status.",
                        "enum": [
                            "observing",
                            "confirmed",
                            "intervention_planned",
                            "mitigated",
                            "rejected",
                        ],
                    },
                    "sources": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Candidate sources: friction, bugs, receipts.",
                        "default": ["friction", "bugs", "receipts"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows or candidates to return.",
                        "default": 20,
                    },
                    "threshold": {
                        "type": "integer",
                        "description": "Evidence count needed for promotion_candidate=true.",
                        "default": 3,
                    },
                    "since_hours": {
                        "type": "number",
                        "description": "Only scan candidate evidence from the last N hours.",
                    },
                    "include_test": {
                        "type": "boolean",
                        "description": "Include test friction events when deriving candidates.",
                        "default": False,
                    },
                    "include_evidence": {
                        "type": "boolean",
                        "description": "Attach evidence links in list responses.",
                        "default": False,
                    },
                    "include_hydration": {
                        "type": "boolean",
                        "description": "Attach semantic binding suggestions, retrieval provenance, typed gaps, and primitive next-action options to pattern candidate/list responses.",
                        "default": False,
                    },
                    "candidate_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Materialize only these candidate pattern keys.",
                    },
                    "promotion_only": {
                        "type": "boolean",
                        "description": "When materializing, skip candidates below threshold.",
                        "default": True,
                    },
                    "created_by": {
                        "type": "string",
                        "description": "Actor label for materialized evidence links.",
                    },
                },
                "required": ["action"],
                "x-action-requirements": {
                    "evidence": {"required": ["pattern_ref"]}
                },
            },
            "type_contract": {
                "list": {"consumes": [], "produces": ["praxis.pattern.record_list"]},
                "candidates": {"consumes": [], "produces": ["praxis.pattern.candidate_list"]},
                "evidence": {"consumes": ["praxis.pattern.record"], "produces": ["praxis.pattern.evidence_list"]},
                "materialize": {"consumes": ["praxis.pattern.candidate_list"], "produces": ["praxis.pattern.record_list"]},
            },
            "cli": {
                "surface": "evidence",
                "tier": "stable",
                "recommended_alias": None,
                "when_to_use": "Inspect or materialize recurring platform failure patterns before opening more bug tickets.",
                "when_not_to_use": "Do not use it for one-off defects that already have a concrete fix path; use praxis_bugs instead.",
                "risks": {
                    "default": "read",
                    "actions": {
                        "list": "read",
                        "candidates": "read",
                        "evidence": "read",
                        "materialize": "write",
                    },
                },
                "examples": [
                    {"title": "List durable patterns", "input": {"action": "list"}},
                    {
                        "title": "Find promotion candidates",
                        "input": {
                            "action": "candidates",
                            "sources": ["friction", "bugs", "receipts"],
                            "threshold": 3,
                        },
                    },
                    {
                        "title": "Hydrate candidate actions",
                        "input": {
                            "action": "candidates",
                            "sources": ["friction"],
                            "threshold": 3,
                            "include_hydration": True,
                        },
                    },
                    {
                        "title": "Materialize candidates",
                        "input": {"action": "materialize", "threshold": 3},
                    },
                ],
            },
        },
    ),
}
