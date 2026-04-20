"""Tools: praxis_data_dictionary_quality — data-quality rules + runs."""

from __future__ import annotations

from typing import Any

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


def tool_praxis_data_dictionary_quality(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Declarative data-quality rules + their runs."""
    action = _str(params.get("action"), default="summary").lower()
    try:
        if action == "summary":
            return {"action": "summary", **quality_summary(_conn())}

        if action == "list_rules":
            return {
                "action": "list_rules",
                **describe_rules(
                    _conn(),
                    object_kind=_opt_str(params.get("object_kind")),
                    field_path=_opt_str(params.get("field_path")),
                    include_layers=bool(params.get("include_layers", False)),
                ),
            }

        if action == "list_runs":
            return {
                "action": "list_runs",
                **latest_runs(
                    _conn(),
                    object_kind=_opt_str(params.get("object_kind")),
                    status=_opt_str(params.get("status")),
                    limit=int(params.get("limit", 100) or 100),
                ),
            }

        if action == "run_history":
            return {
                "action": "run_history",
                **run_history(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    rule_kind=_str(params.get("rule_kind")),
                    limit=int(params.get("limit", 50) or 50),
                ),
            }

        if action == "set":
            return {
                "action": "set",
                **set_operator_rule(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    rule_kind=_str(params.get("rule_kind")),
                    expression=params.get("expression") or {},
                    severity=_str(params.get("severity"), default="warning"),
                    description=_str(params.get("description")),
                    enabled=bool(params.get("enabled", True)),
                    metadata=params.get("metadata"),
                ),
            }

        if action == "clear":
            return {
                "action": "clear",
                **clear_operator_rule(
                    _conn(),
                    object_kind=_str(params.get("object_kind")),
                    field_path=_str(params.get("field_path")),
                    rule_kind=_str(params.get("rule_kind")),
                ),
            }

        if action == "evaluate":
            return {
                "action": "evaluate",
                **evaluate_all(
                    _conn(),
                    object_kind=_opt_str(params.get("object_kind")),
                ),
            }

        if action == "reproject":
            from memory.data_dictionary_quality_projector import (
                DataDictionaryQualityProjector,
            )

            projector = DataDictionaryQualityProjector(_subs.get_pg_conn())
            result = projector.run()
            return {
                "action": "reproject",
                "ok": getattr(result, "ok", True),
                "duration_ms": getattr(result, "duration_ms", None),
                "error": getattr(result, "error", None),
            }

        return {"error": f"unknown action: {action}"}
    except DataDictionaryQualityError as exc:
        return {"error": str(exc), "status_code": exc.status_code}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_quality": (
        tool_praxis_data_dictionary_quality,
        {
            "description": (
                "Declarative data-quality rules + their runs. Auto-projected from "
                "Postgres schema (NOT NULL, UNIQUE, FK referential checks) with "
                "operator overrides.\n\n"
                "ACTIONS:\n"
                "  summary       — rule + run counts by source / status.\n"
                "  list_rules    — effective rules, optionally filtered.\n"
                "  list_runs     — latest run per rule.\n"
                "  run_history   — last N runs for one rule.\n"
                "  set           — upsert operator-layer rule.\n"
                "  clear         — drop operator-layer rule.\n"
                "  evaluate      — run evaluator now (inserts runs).\n"
                "  reproject     — run projector now.\n\n"
                "RULE KINDS: not_null, unique, regex_match, enum, range, "
                "row_count_min, row_count_max, referential, custom_sql."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "summary", "list_rules", "list_runs", "run_history",
                            "set", "clear", "evaluate", "reproject",
                        ],
                        "default": "summary",
                    },
                    "object_kind": {"type": "string"},
                    "field_path": {"type": "string"},
                    "rule_kind": {"type": "string"},
                    "expression": {"type": "object"},
                    "severity": {
                        "type": "string",
                        "enum": ["info", "warning", "error", "critical"],
                    },
                    "description": {"type": "string"},
                    "enabled": {"type": "boolean"},
                    "status": {"type": "string", "enum": ["pass", "fail", "error"]},
                    "include_layers": {"type": "boolean", "default": False},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
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
                        "description": "Rules + runs summary.",
                        "input": {"action": "summary"},
                    },
                    {
                        "description": "Failing rules across the dictionary.",
                        "input": {"action": "list_runs", "status": "fail"},
                    },
                    {
                        "description": "Run evaluator for one table.",
                        "input": {"action": "evaluate", "object_kind": "table:bugs"},
                    },
                ],
                "when_to_use": (
                    "Add a declarative check to a field and track pass / fail "
                    "over time."
                ),
                "when_not_to_use": (
                    "Not a generic SQL runner — use praxis_query for ad-hoc "
                    "data inspection."
                ),
            },
        },
    ),
}
