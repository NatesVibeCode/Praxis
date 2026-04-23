"""Tool: praxis_data_dictionary_wiring_audit — hard-path + unwired lint."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_wiring_audit import (
    audit_code_orphan_tables,
    audit_hard_paths,
    audit_trend,
    audit_unreferenced_decisions,
    run_full_audit,
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


def tool_praxis_data_dictionary_wiring_audit(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Scan for things that will break on VPS migration, and for unwired
    authority rows nothing references.

    Actions:
      - all:          run every audit and return aggregated findings
      - hard_paths:   classified hardcoded paths / localhost / ports
      - decisions:    only unreferenced operator decisions
      - orphans:      only code-orphan tables (dict entries nothing uses)
    """
    action = _str(params.get("action"), default="all").lower()

    try:
        if action == "all":
            return {"action": "all", **run_full_audit(_conn())}

        if action == "hard_paths":
            findings = audit_hard_paths()
            by_classification: dict[str, int] = {}
            by_surface: dict[str, int] = {}
            for finding in findings:
                classification = str(
                    finding.details.get("classification") or "unclassified"
                )
                surface = str(finding.details.get("surface") or "unknown")
                by_classification[classification] = (
                    by_classification.get(classification, 0) + 1
                )
                by_surface[surface] = by_surface.get(surface, 0) + 1
            return {
                "action": "hard_paths",
                "total": len(findings),
                "by_classification": by_classification,
                "by_surface": by_surface,
                "actionable_total": by_classification.get("live_authority_bug", 0),
                "findings": [f.to_payload() for f in findings],
            }

        if action == "decisions":
            findings = audit_unreferenced_decisions(_conn())
            return {
                "action": "decisions",
                "total": len(findings),
                "findings": [f.to_payload() for f in findings],
            }

        if action == "orphans":
            findings = audit_code_orphan_tables(_conn())
            return {
                "action": "orphans",
                "total": len(findings),
                "findings": [f.to_payload() for f in findings],
            }

        if action == "trend":
            limit = int(params.get("limit", 50))
            return {"action": "trend", **audit_trend(_conn(), limit=limit)}

        return {"error": f"unknown action: {action}", "status_code": 400}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_wiring_audit": (
        tool_praxis_data_dictionary_wiring_audit,
        {
            "description": (
                "Wiring + hard-path audit over Praxis. Reports two "
                "classes of issue that bloat attention and/or break on "
                "VPS migration: (1) hardcoded paths / localhost / "
                "ports in source, docs, skills, MCP metadata, CLI "
                "surfaces, and queue specs, classified by authority "
                "status; (2) unwired authority rows — "
                "operator decisions nothing cites, and data-dictionary "
                "tables zero code references. No automatic bug filing; "
                "the output is a report the operator reviews."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["all", "hard_paths", "decisions", "orphans", "trend"],
                        "default": "all",
                    },
                },
            },
            "cli": {
                "surface": "general",
                "tier": "advanced",
                "recommended_alias": None,
                "examples": [
                    {
                        "description": "Full audit — everything at once.",
                        "input": {"action": "all"},
                    },
                    {
                        "description": "VPS-migration readiness: hardcoded paths only.",
                        "input": {"action": "hard_paths"},
                    },
                    {
                        "description": "Decisions filed but never cited.",
                        "input": {"action": "decisions"},
                    },
                    {
                        "description": "Tables in the dictionary with no code refs.",
                        "input": {"action": "orphans"},
                    },
                ],
                "when_to_use": (
                    "Before VPS migration, or any time the platform "
                    "feels noisy — the report separates 'attention debt' "
                    "(unwired authority) from 'deployment debt' "
                    "(hardcoded paths)."
                ),
                "when_not_to_use": (
                    "Don't use to fix things — this is read-only lint. "
                    "For fixes, the findings point you at file:line "
                    "locations to edit or authority rows to retire."
                ),
            },
        },
    ),
}
