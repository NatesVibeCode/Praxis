"""Tools: praxis_data_dictionary_lineage — directed lineage over known objects."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_lineage import (
    DataDictionaryLineageError,
    clear_operator_edge,
    describe_edges,
    lineage_summary,
    set_operator_edge,
    walk_impact,
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


def tool_praxis_data_dictionary_lineage(params: dict[str, Any]) -> dict[str, Any]:
    """Browse and edit the data-dictionary lineage graph."""
    action = _str(params.get("action"), default="summary").lower()
    try:
        if action == "summary":
            return {"action": "summary", **lineage_summary(_conn())}

        if action == "describe":
            return {
                "action": "describe",
                **describe_edges(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    direction=_str(params.get("direction"), default="both"),
                    edge_kind=_str(params.get("edge_kind")) or None,
                    include_layers=bool(params.get("include_layers", False)),
                ),
            }

        if action == "impact":
            return {
                "action": "impact",
                **walk_impact(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    direction=_str(params.get("direction"), default="downstream"),
                    max_depth=int(params.get("max_depth", 5) or 5),
                    edge_kind=_str(params.get("edge_kind")) or None,
                ),
            }

        if action == "set_edge":
            return {
                "action": "set_edge",
                **set_operator_edge(
                    _conn(),
                    src_object_kind=_str(params.get("src_object_kind")),
                    src_field_path=_str(params.get("src_field_path")),
                    dst_object_kind=_str(params.get("dst_object_kind")),
                    dst_field_path=_str(params.get("dst_field_path")),
                    edge_kind=_str(params.get("edge_kind")),
                    confidence=float(params.get("confidence", 1.0)),
                    metadata=params.get("metadata"),
                ),
            }

        if action == "clear_edge":
            return {
                "action": "clear_edge",
                **clear_operator_edge(
                    _conn(),
                    src_object_kind=_str(params.get("src_object_kind")),
                    src_field_path=_str(params.get("src_field_path")),
                    dst_object_kind=_str(params.get("dst_object_kind")),
                    dst_field_path=_str(params.get("dst_field_path")),
                    edge_kind=_str(params.get("edge_kind")),
                ),
            }

        if action == "reproject":
            from memory.data_dictionary_lineage_projector import (
                DataDictionaryLineageProjector,
            )

            projector = DataDictionaryLineageProjector(_subs.get_pg_conn())
            result = projector.run()
            return {
                "action": "reproject",
                "ok": getattr(result, "ok", True),
                "duration_ms": getattr(result, "duration_ms", None),
                "error": getattr(result, "error", None),
            }

        return {"error": f"unknown action: {action}"}
    except DataDictionaryLineageError as exc:
        return {"error": str(exc), "status_code": exc.status_code}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_lineage": (
        tool_praxis_data_dictionary_lineage,
        {
            "description": (
                "Directed lineage graph over data dictionary objects. Auto-projected "
                "from Postgres FK constraints, view dependencies, dataset_promotions, "
                "integration manifests, and MCP tool input schemas. Operator-authored "
                "edges take precedence.\n\n"
                "ACTIONS:\n"
                "  summary     — edge counts by source.\n"
                "  describe    — one-hop neighborhood for an `object_kind`.\n"
                "  impact      — walk reachable graph up to `max_depth` (default 5).\n"
                "  set_edge    — upsert an operator-layer edge.\n"
                "  clear_edge  — drop an operator-layer edge.\n"
                "  reproject   — run the lineage projector now (normally scheduled).\n\n"
                "EDGE KINDS: references, derives_from, projects_to, ingests_from, "
                "produces, consumes, promotes_to, same_as."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "summary", "describe", "impact",
                            "set_edge", "clear_edge", "reproject",
                        ],
                        "default": "summary",
                    },
                    "object_kind": {"type": "string"},
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream", "both"],
                        "default": "both",
                    },
                    "edge_kind": {"type": "string"},
                    "max_depth": {"type": "integer", "minimum": 1, "maximum": 10},
                    "include_layers": {"type": "boolean", "default": False},
                    "src_object_kind": {"type": "string"},
                    "src_field_path": {"type": "string"},
                    "dst_object_kind": {"type": "string"},
                    "dst_field_path": {"type": "string"},
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
                        "description": "Edge counts across the graph.",
                        "input": {"action": "summary"},
                    },
                    {
                        "description": "One-hop neighborhood for a table.",
                        "input": {
                            "action": "describe",
                            "object_kind": "table:workflow_runs",
                        },
                    },
                    {
                        "description": "Impact walk (what depends on this table).",
                        "input": {
                            "action": "impact",
                            "object_kind": "table:workflow_runs",
                            "direction": "downstream",
                            "max_depth": 3,
                        },
                    },
                ],
                "when_to_use": (
                    "Trace which objects depend on or derive from a given object_kind."
                ),
                "when_not_to_use": (
                    "Not a field-level descriptor browser — use praxis_data_dictionary "
                    "for field-level reads and operator overrides."
                ),
            },
        },
    ),
}
