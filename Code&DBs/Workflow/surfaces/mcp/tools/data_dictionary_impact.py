"""Tool: praxis_data_dictionary_impact — cross-axis blast-radius report."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_impact import (
    DataDictionaryImpactError,
    impact_analysis,
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


def tool_praxis_data_dictionary_impact(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Walk the lineage of an object and aggregate governance across the radius."""
    try:
        payload = impact_analysis(
            _conn(),
            object_kind=_str(params.get("object_kind")),
            direction=_str(params.get("direction"), default="downstream"),
            max_depth=int(params.get("max_depth", 5)),
            edge_kind=_opt_str(params.get("edge_kind")),
        )
        return {"action": "impact", **payload}
    except DataDictionaryImpactError as exc:
        return {"error": str(exc), "status_code": exc.status_code}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_impact": (
        tool_praxis_data_dictionary_impact,
        {
            "description": (
                "Cross-axis impact analysis for a data-dictionary object. Walks "
                "lineage in the given direction, then for every reached node "
                "reports effective tags, stewards, quality rules, and latest "
                "run status. Returns aggregate rollups (PII field count, "
                "failing-rule count, distinct owners + publishers) across the "
                "blast radius.\n\n"
                "USE THIS when answering 'if I change table:X, what else is "
                "affected and what governance policies apply?'"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "object_kind": {"type": "string"},
                    "direction": {
                        "type": "string",
                        "enum": ["downstream", "upstream"],
                        "default": "downstream",
                    },
                    "max_depth": {"type": "integer", "default": 5},
                    "edge_kind": {"type": "string"},
                },
                "required": ["object_kind"],
            },
            "cli": {
                "surface": "general",
                "tier": "advanced",
                "recommended_alias": None,
                "examples": [
                    {
                        "description": "Downstream blast radius for a core table.",
                        "input": {
                            "object_kind": "table:workflow_runs",
                            "direction": "downstream",
                            "max_depth": 3,
                        },
                    },
                    {
                        "description": "Upstream producers for a derived dataset.",
                        "input": {
                            "object_kind": "dataset:slm/review",
                            "direction": "upstream",
                        },
                    },
                ],
                "when_to_use": (
                    "Governance / change-safety review: surface who owns what, "
                    "which nodes carry PII, which quality rules are currently "
                    "failing, before making a schema change."
                ),
                "when_not_to_use": (
                    "Don't use for simple field-level reads — praxis_data_dictionary "
                    "is faster. Don't use for pure lineage walks — the existing "
                    "data-dictionary lineage tool returns just the graph."
                ),
            },
        },
    ),
}
