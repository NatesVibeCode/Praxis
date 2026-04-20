"""Tools: praxis_data_dictionary_stewardship — steward authority over known objects."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_stewardship import (
    DataDictionaryStewardshipError,
    clear_operator_steward,
    describe_stewards,
    find_by_steward,
    set_operator_steward,
    stewardship_summary,
)
from ..subsystems import _subs


def _conn() -> Any:
    return _subs.get_pg_conn()


def _str(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return default


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None
    return None


def tool_praxis_data_dictionary_stewardship(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Browse and edit stewardship (owners / approvers / contacts / publishers / consumers)."""
    action = _str(params.get("action"), default="summary").lower()
    try:
        if action == "summary":
            return {"action": "summary", **stewardship_summary(_conn())}

        if action == "describe":
            return {
                "action": "describe",
                **describe_stewards(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_opt_str(params.get("field_path")),
                    include_layers=bool(params.get("include_layers", False)),
                ),
            }

        if action == "by_steward":
            return {
                "action": "by_steward",
                **find_by_steward(
                    _conn(),
                    steward_id=_str(params.get("steward_id")),
                    steward_kind=_opt_str(params.get("steward_kind")),
                ),
            }

        if action == "set":
            return {
                "action": "set",
                **set_operator_steward(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    steward_kind=_str(params.get("steward_kind")),
                    steward_id=_str(params.get("steward_id")),
                    steward_type=_str(params.get("steward_type"), default="person"),
                    confidence=float(params.get("confidence", 1.0)),
                    metadata=params.get("metadata"),
                ),
            }

        if action == "clear":
            return {
                "action": "clear",
                **clear_operator_steward(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    steward_kind=_str(params.get("steward_kind")),
                    steward_id=_str(params.get("steward_id")),
                ),
            }

        if action == "reproject":
            from memory.data_dictionary_stewardship_projector import (
                DataDictionaryStewardshipProjector,
            )

            projector = DataDictionaryStewardshipProjector(_subs.get_pg_conn())
            result = projector.run()
            return {
                "action": "reproject",
                "ok": getattr(result, "ok", True),
                "duration_ms": getattr(result, "duration_ms", None),
                "error": getattr(result, "error", None),
            }

        return {"error": f"unknown action: {action}"}
    except DataDictionaryStewardshipError as exc:
        return {"error": str(exc), "status_code": exc.status_code}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_stewardship": (
        tool_praxis_data_dictionary_stewardship,
        {
            "description": (
                "Stewardship authority for data dictionary objects. Auto-projected "
                "from audit-column names, namespace prefix → service owner, and "
                "known projector modules. Operator stewards take precedence.\n\n"
                "ACTIONS:\n"
                "  summary     — steward counts by source and by role.\n"
                "  describe    — effective stewards for an object (optionally a field).\n"
                "  by_steward  — reverse lookup: what does a principal steward?\n"
                "  set         — upsert an operator-layer steward.\n"
                "  clear       — drop an operator-layer steward.\n"
                "  reproject   — run the stewardship projector now.\n\n"
                "RESERVED steward_kind values: owner, approver, contact, publisher, "
                "consumer. steward_type: person|team|agent|role|service."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "summary", "describe", "by_steward",
                            "set", "clear", "reproject",
                        ],
                        "default": "summary",
                    },
                    "object_kind": {"type": "string"},
                    "field_path": {"type": "string"},
                    "steward_kind": {"type": "string"},
                    "steward_id": {"type": "string"},
                    "steward_type": {"type": "string"},
                    "include_layers": {"type": "boolean", "default": False},
                    "confidence": {"type": "number"},
                    "metadata": {"type": "object"},
                },
                "required": ["action"],
            },
            "cli": {
                "surface": "general",
                "tier": "advanced",
                "recommended_alias": None,
                "examples": [
                    {
                        "description": "Steward counts across the graph.",
                        "input": {"action": "summary"},
                    },
                    {
                        "description": "Effective stewards for a table.",
                        "input": {
                            "action": "describe",
                            "object_kind": "table:workflow_runs",
                        },
                    },
                    {
                        "description": "What does the heartbeat_runner publish?",
                        "input": {
                            "action": "by_steward",
                            "steward_id": "heartbeat_runner",
                            "steward_kind": "owner",
                        },
                    },
                ],
                "when_to_use": (
                    "Identify owners / approvers / publishers for data assets, or "
                    "override heuristic stewardship with operator authority."
                ),
                "when_not_to_use": (
                    "Not for assigning work — use the bugs and roadmap tools for "
                    "that. Stewardship is a labeling authority for data governance."
                ),
            },
        },
    ),
}
