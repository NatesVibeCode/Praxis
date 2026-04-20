"""Tools: praxis_data_dictionary — unified data dictionary authority surface."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary import (
    DataDictionaryBoundaryError,
    clear_operator_override,
    describe_object,
    list_object_kinds,
    set_operator_override,
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


def tool_praxis_data_dictionary(params: dict[str, Any]) -> dict[str, Any]:
    """Query, describe, and edit the unified data dictionary."""
    action = _str(params.get("action"), default="list").lower()
    try:
        if action == "list":
            rows = list_object_kinds(
                _conn(), category=_str(params.get("category")) or None
            )
            return {"action": "list", "count": len(rows), "objects": rows}

        if action == "describe":
            return {
                "action": "describe",
                **describe_object(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    include_layers=bool(params.get("include_layers", False)),
                ),
            }

        if action == "set_override":
            result = set_operator_override(
                _conn(),
                object_kind=_str(params.get("object_kind")),
                field_path=_str(params.get("field_path")),
                field_kind=params.get("field_kind"),
                label=params.get("label"),
                description=params.get("description"),
                required=params.get("required"),
                default_value=params.get("default_value"),
                valid_values=params.get("valid_values"),
                examples=params.get("examples"),
                deprecation_notes=params.get("deprecation_notes"),
                display_order=params.get("display_order"),
                metadata=params.get("metadata"),
            )
            return {"action": "set_override", **result}

        if action == "clear_override":
            result = clear_operator_override(
                _conn(),
                object_kind=_str(params.get("object_kind")),
                field_path=_str(params.get("field_path")),
            )
            return {"action": "clear_override", **result}

        if action == "reproject":
            from memory.data_dictionary_projector import DataDictionaryProjector

            projector = DataDictionaryProjector(_subs.get_pg_conn())
            result = projector.run()
            return {
                "action": "reproject",
                "ok": getattr(result, "ok", True),
                "duration_ms": getattr(result, "duration_ms", None),
                "error": getattr(result, "error", None),
            }

        return {"error": f"unknown action: {action}"}
    except DataDictionaryBoundaryError as exc:
        return {"error": str(exc), "status_code": exc.status_code}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary": (
        tool_praxis_data_dictionary,
        {
            "description": (
                "Unified data dictionary authority. Auto-projects field descriptors for "
                "every injected object (tables, object_types, integrations, datasets, "
                "ingest payloads, operator decisions, receipts, MCP tools). Operator "
                "overrides win over projected rows.\n\n"
                "ACTIONS:\n"
                "  list           — catalog of object kinds, optionally filtered by `category`.\n"
                "  describe       — merged field list for one `object_kind` (pass `include_layers` to see per-source rows).\n"
                "  set_override   — write/update the operator layer for (object_kind, field_path).\n"
                "  clear_override — drop the operator row; projector/inferred layers are unaffected.\n"
                "  reproject      — run the projector now (normally scheduled by heartbeat).\n\n"
                "CATEGORIES: table, object_type, integration, dataset, ingest, decision, receipt, tool, object."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list",
                            "describe",
                            "set_override",
                            "clear_override",
                            "reproject",
                        ],
                        "default": "list",
                    },
                    "object_kind": {"type": "string"},
                    "field_path": {"type": "string"},
                    "category": {"type": "string"},
                    "include_layers": {"type": "boolean", "default": False},
                    "field_kind": {"type": "string"},
                    "label": {"type": "string"},
                    "description": {"type": "string"},
                    "required": {"type": "boolean"},
                    "default_value": {},
                    "valid_values": {"type": "array"},
                    "examples": {"type": "array"},
                    "deprecation_notes": {"type": "string"},
                    "display_order": {"type": "integer"},
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
                        "description": "List available data dictionary object kinds.",
                        "input": {"action": "list"},
                    },
                    {
                        "description": "Describe one object kind with merged field descriptors.",
                        "input": {
                            "action": "describe",
                            "object_kind": "workflow_runs",
                        },
                    },
                ],
                "when_to_use": (
                    "Browse or edit field descriptors for any injected object kind."
                ),
                "when_not_to_use": (
                    "Don't use for per-column SQL schema checks — those are covered by "
                    "praxis_query 'schema for <table>'."
                ),
            },
        },
    ),
}
