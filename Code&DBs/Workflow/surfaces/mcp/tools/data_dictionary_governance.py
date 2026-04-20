"""Tool: praxis_data_dictionary_governance — policy compliance scan."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_governance import (
    compute_scorecard,
    run_governance_scan,
)
from runtime.data_dictionary_governance_clustering import (
    suggest_cluster_fixes,
)
from runtime.data_dictionary_governance_remediation import (
    suggest_all_remediations,
)
from ..subsystems import _subs


def _conn() -> Any:
    return _subs.get_pg_conn()


def _discover_fn(query: str, limit: int) -> list[dict[str, Any]]:
    """Wrap the codebase indexer for remediation.DiscoverFn.

    Returns [] on any failure so remediation is safe even when the index
    is unavailable.
    """
    try:
        indexer = _subs.get_module_indexer()
    except Exception:
        return []
    try:
        rows = indexer.search(query=query, limit=limit, threshold=0.3) or []
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "name": r.get("name", ""),
            "kind": r.get("kind", ""),
            "path": r.get("module_path", ""),
            "similarity": round(float(r.get("cosine_similarity") or 0), 2),
        })
    return out


def _str(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return default


def tool_praxis_data_dictionary_governance(
    params: dict[str, Any],
) -> dict[str, Any]:
    """Run the governance compliance scan.

    Actions:
      - scan      (default): dry-run, returns violations without filing bugs
      - enforce:  file bugs for any new violations (dedup-keyed on
                  `decision_ref`); bugs are auto-assigned to the nearest
                  upstream owner and severity-weighted by blast radius.
      - scorecard: single-number compliance health + per-axis coverage pcts
      - remediate: for every open violation, emit a two-path plan
                   (immediate one-click fix + permanent structural backstop)
      - cluster:   group violations by shared root cause and emit one
                   bulk fix per cluster; reduces N bugs → M root causes
    """
    action = _str(params.get("action"), default="scan").lower()

    try:
        if action == "cluster":
            payload = suggest_cluster_fixes(_conn())
            return {"action": "cluster", **payload}

        if action == "remediate":
            payload = suggest_all_remediations(_conn(), discover=_discover_fn)
            return {"action": "remediate", **payload}

        if action == "enforce":
            from runtime.bug_tracker import BugTracker

            conn = _conn()
            tracker = BugTracker(conn)
            payload = run_governance_scan(conn, tracker=tracker, dry_run=False)
            return {"action": "enforce", **payload}

        if action == "scan":
            payload = run_governance_scan(_conn(), tracker=None, dry_run=True)
            return {"action": "scan", **payload}

        if action == "scorecard":
            payload = compute_scorecard(_conn())
            return {"action": "scorecard", **payload}

        return {"error": f"unknown action: {action}", "status_code": 400}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_data_dictionary_governance": (
        tool_praxis_data_dictionary_governance,
        {
            "description": (
                "Cross-axis governance compliance scan over the data "
                "dictionary. Checks three policies: (1) objects carrying a "
                "`pii` tag without an owner steward, (2) objects carrying a "
                "`sensitive` tag without an owner, (3) enabled rules with "
                "severity='error' whose latest run is fail/error. "
                "`scan` returns violations only; `enforce` additionally "
                "files dedupe-keyed bugs (decision_ref is "
                "`governance.<policy>.<object_kind>[.<rule_kind>]`)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "scan", "enforce", "scorecard",
                            "remediate", "cluster",
                        ],
                        "default": "scan",
                    },
                },
            },
            "cli": {
                "surface": "general",
                "tier": "advanced",
                "recommended_alias": None,
                "examples": [
                    {
                        "description": "Dry-run scan — list violations.",
                        "input": {"action": "scan"},
                    },
                    {
                        "description": "Enforce — file bugs for new violations.",
                        "input": {"action": "enforce"},
                    },
                    {
                        "description": "Scorecard — single health metric + axis coverage.",
                        "input": {"action": "scorecard"},
                    },
                    {
                        "description": (
                            "Remediate — per-violation immediate fix + "
                            "permanent backstop plan."
                        ),
                        "input": {"action": "remediate"},
                    },
                    {
                        "description": (
                            "Cluster — group violations by shared root "
                            "cause and emit one bulk-fix per cluster."
                        ),
                        "input": {"action": "cluster"},
                    },
                ],
                "when_to_use": (
                    "Governance review: before a release, or when "
                    "investigating a data-governance complaint, run a dry "
                    "scan to see which PII/sensitive objects lack owners "
                    "and which error-severity rules are failing."
                ),
                "when_not_to_use": (
                    "Don't use as a substitute for the data-dictionary "
                    "write tools (set_operator_classification / "
                    "set_operator_steward) — this only reports, it does not "
                    "fix the underlying governance gaps."
                ),
            },
        },
    ),
}
