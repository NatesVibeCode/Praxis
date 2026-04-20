"""Tool: praxis_data_dictionary_governance — policy compliance scan."""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_governance import (
    compute_scorecard,
    run_governance_scan,
)
from runtime.data_dictionary_governance_change_feed import (
    drain_change_feed,
    peek_pending,
)
from runtime.data_dictionary_governance_clustering import (
    suggest_cluster_fixes,
)
from runtime.data_dictionary_governance_remediation import (
    suggest_all_remediations,
)
from storage.postgres.data_dictionary_governance_scans_repository import (
    fetch_scan_by_id,
    fetch_scans_for_bug,
    list_scans,
)
from ..subsystems import _subs


def _conn() -> Any:
    return _subs.get_pg_conn()


def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _serialize_scan_summary(row: dict[str, Any]) -> dict[str, Any]:
    """List-view projection of a scan row — metadata only, no violations blob."""
    return {
        "scan_id": row.get("scan_id"),
        "scanned_at": _iso(row.get("scanned_at")),
        "triggered_by": row.get("triggered_by"),
        "dry_run": row.get("dry_run"),
        "total_violations": row.get("total_violations"),
        "bugs_filed": row.get("bugs_filed"),
        "bugs_skipped": row.get("bugs_skipped"),
        "bugs_errored": row.get("bugs_errored"),
        "by_policy": row.get("by_policy") or {},
    }


def _serialize_scan_full(row: dict[str, Any]) -> dict[str, Any]:
    """Full scan detail including the violations snapshot + filed bug ids."""
    import json

    def _jsonb(v):
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return v
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v

    return {
        **_serialize_scan_summary(row),
        "violations": _jsonb(row.get("violations")) or [],
        "filed_bug_ids": list(row.get("filed_bug_ids") or []),
        "metadata": _jsonb(row.get("metadata")) or {},
    }


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
      - scans:     list recent governance scan audit records
      - scan_detail: fetch one scan by scan_id (full violations snapshot)
      - scans_for_bug: reverse lookup — which scans filed this bug_id
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

        if action == "scans":
            limit = int(params.get("limit", 25))
            triggered = params.get("triggered_by") or None
            scans = list_scans(_conn(), limit=limit, triggered_by=triggered)
            return {
                "action": "scans",
                "count": len(scans),
                "scans": [_serialize_scan_summary(s) for s in scans],
            }

        if action == "scan_detail":
            scan_id = params.get("scan_id", "").strip()
            if not scan_id:
                return {"error": "scan_id is required", "status_code": 400}
            row = fetch_scan_by_id(_conn(), scan_id)
            if not row:
                return {"error": f"scan not found: {scan_id}", "status_code": 404}
            return {"action": "scan_detail", "scan": _serialize_scan_full(row)}

        if action == "pending":
            limit = int(params.get("limit", 20))
            return {"action": "pending", **peek_pending(_conn(), limit=limit)}

        if action == "drain":
            from runtime.bug_tracker import BugTracker

            dry = bool(params.get("dry_run", False))
            limit = int(params.get("limit", 100))
            conn = _conn()
            tracker = None if dry else BugTracker(conn)
            payload = drain_change_feed(
                conn, tracker=tracker, limit=limit, triggered_by="operator_mcp",
            )
            return {"action": "drain", **payload}

        if action == "scans_for_bug":
            bug_id = params.get("bug_id", "").strip()
            if not bug_id:
                return {"error": "bug_id is required", "status_code": 400}
            scans = fetch_scans_for_bug(_conn(), bug_id)
            return {
                "action": "scans_for_bug",
                "bug_id": bug_id,
                "count": len(scans),
                "scans": [_serialize_scan_summary(s) for s in scans],
            }

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
                            "scans", "scan_detail", "scans_for_bug",
                            "pending", "drain",
                        ],
                        "default": "scan",
                    },
                    "scan_id": {"type": "string"},
                    "bug_id":  {"type": "string"},
                    "triggered_by": {"type": "string"},
                    "limit":   {"type": "integer", "default": 25},
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
