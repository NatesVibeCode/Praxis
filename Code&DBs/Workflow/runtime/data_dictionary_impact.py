"""Cross-axis impact analysis for the data dictionary.

Given a root `object_kind`, walk the lineage graph and for every
reachable node aggregate the other three governance axes:
classifications (tags), quality rules + latest runs, and stewardship.

Intended reader: an operator asking "if I change table:X, what's the
blast radius and what policies apply to that set?" The answer is a
single JSON payload combining:

* every reachable node (with traversal depth)
* per-node effective tags, stewards, and effective rules
* per-node latest rule-run status
* aggregate rollups: counts of PII / sensitive fields, failing rules,
  distinct owners + publishers across the blast radius

This module is read-only and composes the four existing axes — it
owns no table of its own.
"""

from __future__ import annotations

from typing import Any

from runtime.data_dictionary_classifications import describe_classifications
from runtime.data_dictionary_lineage import walk_impact
from runtime.data_dictionary_quality import describe_rules, latest_runs
from runtime.data_dictionary_stewardship import describe_stewards


class DataDictionaryImpactError(RuntimeError):
    """Raised when impact analysis cannot execute (bad input)."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


# --- per-node collection -------------------------------------------------


def _tag_pairs(effective: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Strip classifications down to the readable (key, value, source) fields."""
    return [
        {
            "tag_key": r.get("tag_key"),
            "tag_value": r.get("tag_value"),
            "field_path": r.get("field_path"),
            "source": r.get("effective_source"),
        }
        for r in effective
    ]


def _steward_refs(effective: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "steward_kind": r.get("steward_kind"),
            "steward_id": r.get("steward_id"),
            "steward_type": r.get("steward_type"),
            "source": r.get("effective_source"),
        }
        for r in effective
    ]


def _rule_summaries(effective: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "rule_kind": r.get("rule_kind"),
            "field_path": r.get("field_path"),
            "severity": r.get("severity"),
            "source": r.get("effective_source"),
        }
        for r in effective
    ]


def _run_statuses(runs: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in runs:
        status = str(r.get("status") or "")
        if status:
            out[status] = out.get(status, 0) + 1
    return out


def _collect_node(conn: Any, object_kind: str) -> dict[str, Any]:
    # Classifications
    try:
        cls_payload = describe_classifications(
            conn, object_kind=object_kind, field_path=None, include_layers=False,
        )
        tags = _tag_pairs(cls_payload.get("effective", []) or [])
    except Exception as exc:  # boundary error → empty list, note in payload
        tags = []
        tags_error = str(exc)
    else:
        tags_error = None

    # Stewardship
    try:
        stw_payload = describe_stewards(
            conn, object_kind=object_kind, field_path=None, include_layers=False,
        )
        stewards = _steward_refs(stw_payload.get("effective", []) or [])
    except Exception as exc:
        stewards = []
        stewards_error = str(exc)
    else:
        stewards_error = None

    # Quality rules
    try:
        q_payload = describe_rules(
            conn, object_kind=object_kind, field_path=None, include_layers=False,
        )
        rules = _rule_summaries(q_payload.get("effective", []) or [])
    except Exception as exc:
        rules = []
        rules_error = str(exc)
    else:
        rules_error = None

    # Latest run statuses
    try:
        runs = latest_runs(conn, object_kind=object_kind, status=None, limit=500)
        run_status = _run_statuses(runs.get("runs", []) or [])
    except Exception:
        run_status = {}

    node: dict[str, Any] = {
        "object_kind": object_kind,
        "tags": tags,
        "stewards": stewards,
        "rules": rules,
        "run_status": run_status,
    }
    errors = {k: v for k, v in {
        "tags_error": tags_error,
        "stewards_error": stewards_error,
        "rules_error": rules_error,
    }.items() if v}
    if errors:
        node["errors"] = errors
    return node


# --- aggregate rollups ---------------------------------------------------


_SENSITIVE_TAG_KEYS = {"pii", "sensitive"}


def _aggregate(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    pii_fields = 0
    sensitive_fields = 0
    rule_count = 0
    failing_runs = 0
    erroring_runs = 0
    owners: set[tuple[str, str]] = set()
    publishers: set[tuple[str, str]] = set()

    for n in nodes:
        for tag in n.get("tags", []):
            key = tag.get("tag_key")
            if key == "pii":
                pii_fields += 1
            if key in _SENSITIVE_TAG_KEYS:
                sensitive_fields += 1
        for steward in n.get("stewards", []):
            key = (str(steward.get("steward_kind") or ""),
                   str(steward.get("steward_id") or ""))
            if not key[0] or not key[1]:
                continue
            if key[0] == "owner":
                owners.add(key)
            elif key[0] == "publisher":
                publishers.add(key)
        rule_count += len(n.get("rules", []) or [])
        rs = n.get("run_status") or {}
        failing_runs += int(rs.get("fail", 0))
        erroring_runs += int(rs.get("error", 0))

    return {
        "total_nodes": len(nodes),
        "pii_fields": pii_fields,
        "sensitive_fields": sensitive_fields,
        "rule_count": rule_count,
        "failing_runs": failing_runs,
        "erroring_runs": erroring_runs,
        "distinct_owners": sorted({f"{k}:{v}" for k, v in owners}),
        "distinct_publishers": sorted({f"{k}:{v}" for k, v in publishers}),
    }


# --- public entry point --------------------------------------------------


def impact_analysis(
    conn: Any,
    *,
    object_kind: str,
    direction: str = "downstream",
    max_depth: int = 5,
    edge_kind: str | None = None,
) -> dict[str, Any]:
    """Full cross-axis impact report for a root asset.

    direction: "downstream" follows produces/derives-from edges forward;
               "upstream" follows them in reverse.
    max_depth: lineage walk depth (clamped to [1, 10]).
    edge_kind: optional filter on lineage edge kind.
    """
    root = _text(object_kind)
    if not root:
        raise DataDictionaryImpactError("object_kind is required")
    if direction not in ("upstream", "downstream"):
        raise DataDictionaryImpactError(
            "direction must be 'upstream' or 'downstream'"
        )

    walk = walk_impact(
        conn,
        object_kind=root,
        direction=direction,
        max_depth=max_depth,
        edge_kind=edge_kind,
    )

    reached = list(walk.get("nodes") or [])
    # `walk_impact` returns the root in `nodes`; keep it — it's always
    # part of the blast radius.
    if root not in reached:
        reached.insert(0, root)

    nodes: list[dict[str, Any]] = []
    for object_kind_ in reached:
        nodes.append(_collect_node(conn, object_kind_))

    return {
        "root": root,
        "direction": direction,
        "max_depth": walk.get("max_depth", max_depth),
        "edges": walk.get("edges", []),
        "nodes": nodes,
        "aggregate": _aggregate(nodes),
    }


__all__ = [
    "DataDictionaryImpactError",
    "impact_analysis",
]
