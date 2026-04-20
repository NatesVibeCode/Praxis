"""Tools: praxis_data_dictionary_classifications — tag authority over known objects."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_classifications import (
    DataDictionaryClassificationError,
    classification_summary,
    clear_operator_classification,
    describe_classifications,
    find_by_tag,
    set_operator_classification,
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


def tool_praxis_data_dictionary_classifications(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Browse and edit classification tags (PII / sensitive / owner / etc)."""
    action = _str(params.get("action"), default="summary").lower()
    try:
        if action == "summary":
            return {"action": "summary", **classification_summary(_conn())}

        if action == "describe":
            return {
                "action": "describe",
                **describe_classifications(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_opt_str(params.get("field_path")),
                    include_layers=bool(params.get("include_layers", False)),
                ),
            }

        if action == "by_tag":
            return {
                "action": "by_tag",
                **find_by_tag(
                    _conn(),
                    tag_key=_str(params.get("tag_key")),
                    tag_value=_opt_str(params.get("tag_value")),
                ),
            }

        if action == "set":
            return {
                "action": "set",
                **set_operator_classification(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    tag_key=_str(params.get("tag_key")),
                    tag_value=_str(params.get("tag_value")),
                    confidence=float(params.get("confidence", 1.0)),
                    metadata=params.get("metadata"),
                ),
            }

        if action == "clear":
            return {
                "action": "clear",
                **clear_operator_classification(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    tag_key=_str(params.get("tag_key")),
                ),
            }

        if action == "reproject":
            from memory.data_dictionary_classifications_projector import (
                DataDictionaryClassificationsProjector,
            )

            projector = DataDictionaryClassificationsProjector(_subs.get_pg_conn())
            result = projector.run()
            return {
                "action": "reproject",
                "ok": getattr(result, "ok", True),
                "duration_ms": getattr(result, "duration_ms", None),
                "error": getattr(result, "error", None),
            }

        return {"error": f"unknown action: {action}"}
    except DataDictionaryClassificationError as exc:
        return {"error": str(exc), "status_code": exc.status_code}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_classifications": (
        tool_praxis_data_dictionary_classifications,
        {
            "description": (
                "Classification / tag authority for data dictionary objects. Auto-"
                "projected from name heuristics (PII detectors, credential tokens, "
                "owner columns) and structural type hints. Operator tags take "
                "precedence.\n\n"
                "ACTIONS:\n"
                "  summary    — tag counts by source.\n"
                "  describe   — effective tags for an object (optionally a field).\n"
                "  by_tag     — list every field with a given tag (compliance).\n"
                "  set        — upsert an operator-layer tag.\n"
                "  clear      — drop an operator-layer tag.\n"
                "  reproject  — run the classification projector now.\n\n"
                "RESERVED tag_keys: pii, sensitive, retention, owner_domain, "
                "structured_shape."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "summary", "describe", "by_tag",
                            "set", "clear", "reproject",
                        ],
                        "default": "summary",
                    },
                    "object_kind": {"type": "string"},
                    "field_path": {"type": "string"},
                    "tag_key": {"type": "string"},
                    "tag_value": {"type": "string"},
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
                        "description": "Tag counts across the graph.",
                        "input": {"action": "summary"},
                    },
                    {
                        "description": "Effective tags for a table.",
                        "input": {
                            "action": "describe",
                            "object_kind": "table:workflow_runs",
                        },
                    },
                    {
                        "description": "Compliance report: every PII email field.",
                        "input": {
                            "action": "by_tag",
                            "tag_key": "pii",
                            "tag_value": "email",
                        },
                    },
                ],
                "when_to_use": (
                    "Identify which fields carry PII / credentials / ownership "
                    "labels, or override heuristic tags with operator authority."
                ),
                "when_not_to_use": (
                    "Not a field descriptor browser — use praxis_data_dictionary "
                    "for field-level reads and operator overrides."
                ),
            },
        },
    ),
}
