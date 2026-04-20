"""Tool: praxis_data_dictionary_drift — schema-drift detector."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_drift import (
    detect_drift,
    diff_snapshots,
    drift_history,
    impact_of_diff,
    take_snapshot,
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


def tool_praxis_data_dictionary_drift(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Inspect, diff, and react to schema drift in the data dictionary.

    Actions:
      - latest    (default): diff the two most recent snapshots and
                  report cross-axis impact. Read-only.
      - snapshot: take a fresh snapshot now and diff it against the
                  prior snapshot. Returns {snapshot, diff, impact}.
      - history:  list recent snapshots with metadata.
      - diff:     explicit diff between two snapshot ids. Requires
                  `from` and `to`.
    """
    action = _str(params.get("action"), default="latest").lower()

    try:
        if action == "latest":
            return {"action": "latest", **detect_drift(_conn(), snapshot_first=False)}

        if action == "snapshot":
            return {"action": "snapshot", **detect_drift(_conn(), snapshot_first=True)}

        if action == "history":
            limit = int(params.get("limit", 50))
            return {"action": "history", **drift_history(_conn(), limit=limit)}

        if action == "diff":
            old_id = _str(params.get("from"))
            new_id = _str(params.get("to"))
            if not old_id or not new_id:
                return {"error": "from and to are required for diff", "status_code": 400}
            conn = _conn()
            diff = diff_snapshots(conn, old_id=old_id, new_id=new_id)
            return {
                "action": "diff",
                "diff": diff.to_payload(),
                "impact": [i.to_payload() for i in impact_of_diff(conn, diff)],
            }

        return {"error": f"unknown action: {action}", "status_code": 400}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_drift": (
        tool_praxis_data_dictionary_drift,
        {
            "description": (
                "Schema-drift detector for the data dictionary. Snapshots "
                "the field inventory each heartbeat, diffs successive "
                "snapshots, and reports cross-axis impact (PII dropped, "
                "downstream consumers affected, quality rules orphaned, "
                "stewards to notify). High-severity drift (P0/P1) auto-"
                "files dedupe-keyed governance bugs.\n\n"
                "USE THIS to answer 'did anything in the schema move "
                "since the last cycle, and who needs to know?'"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["latest", "snapshot", "history", "diff"],
                        "default": "latest",
                    },
                    "from":   {"type": "string", "description": "snapshot id (diff)"},
                    "to":     {"type": "string", "description": "snapshot id (diff)"},
                    "limit":  {"type": "integer", "default": 50},
                },
            },
            "cli": {
                "surface": "general",
                "tier": "advanced",
                "recommended_alias": None,
                "examples": [
                    {
                        "description": "What changed in the schema lately?",
                        "input": {"action": "latest"},
                    },
                    {
                        "description": "Force a fresh snapshot + diff now.",
                        "input": {"action": "snapshot"},
                    },
                    {
                        "description": "List recent snapshot ids.",
                        "input": {"action": "history", "limit": 10},
                    },
                ],
                "when_to_use": (
                    "Before / after a migration; investigating whether a "
                    "field deletion broke downstream consumers; auditing "
                    "schema-change cadence."
                ),
                "when_not_to_use": (
                    "Don't use to inspect current schema (use "
                    "praxis_data_dictionary). Don't use to find the "
                    "blast radius of a *single* known object — use "
                    "praxis_data_dictionary_impact instead."
                ),
            },
        },
    ),
}
