"""Governance compliance scan over the data-dictionary axes.

Cross-axis policy check that reads the three governance views
(classifications / stewardship / quality rules + runs) and surfaces
objects that breach a small set of hard-coded rules:

* `pii_without_owner`      — any object with a `pii` tag but no `owner`
  stewardship.
* `sensitive_without_owner` — same for `sensitive`.
* `error_rule_failing`      — any object with an *enabled* rule whose
  severity is `error` and whose latest run in
  `data_dictionary_quality_runs` was `fail` or `error`.

Violations are filed as bugs using a stable `decision_ref`
(`governance.<policy>.<object_kind>[.<rule_kind>]`). The same decision
ref is used as a dedupe key: if an OPEN or IN_PROGRESS bug already
exists with that ref, we skip. Closed / fixed bugs are allowed to
re-open — that's the signal that a regression happened.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.bug_tracker import BugCategory, BugSeverity, BugTracker
from runtime.primitive_contracts import bug_open_status_values


_FILED_BY = "governance_compliance_heartbeat"
_SOURCE_KIND = "governance"
_CATEGORY = BugCategory.ARCHITECTURE
_PRIMARY_CONSUMER = "llm"
_REVIEW_PLANE = "none_inline"
_ESCALATION_PLANE = "canonical_bug_or_operator_decision"
_ESCALATION_MODEL = "automated_resolve_else_escalate_when_unresolvable"

# Severity is now impact-weighted:
#   downstream_count >= 10  → P1 (high blast radius)
#   otherwise               → P2
_P1_THRESHOLD = 10


# ---------------------------------------------------------------------------
# Violation dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GovernanceViolation:
    policy: str
    object_kind: str
    rule_kind: str = ""  # only populated for rule-scoped violations
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def decision_ref(self) -> str:
        if self.rule_kind:
            return f"governance.{self.policy}.{self.object_kind}.{self.rule_kind}"
        return f"governance.{self.policy}.{self.object_kind}"

    @property
    def review_dedupe_key(self) -> str:
        subject = self.object_kind
        issue_type = self.policy if not self.rule_kind else f"{self.policy}:{self.rule_kind}"
        return f"data_dictionary::{subject}::{issue_type}"

    def to_bug_title(self) -> str:
        if self.policy == "pii_without_owner":
            return f"Governance: PII object {self.object_kind} has no owner"
        if self.policy == "sensitive_without_owner":
            return f"Governance: sensitive object {self.object_kind} has no owner"
        if self.policy == "error_rule_failing":
            return (
                f"Governance: error-severity rule {self.rule_kind} on "
                f"{self.object_kind} is failing"
            )
        return f"Governance: {self.policy} on {self.object_kind}"

    def to_bug_description(self) -> str:
        parts = [
            f"policy:        {self.policy}",
            f"object_kind:   {self.object_kind}",
        ]
        if self.rule_kind:
            parts.append(f"rule_kind:     {self.rule_kind}")
        for k, v in sorted(self.details.items()):
            parts.append(f"{k:14s} {v}")
        parts.append("")
        parts.append("Filed automatically by the governance compliance heartbeat.")
        parts.append(
            "The same decision_ref is used to dedupe open bugs — once this "
            "bug is closed, a future scan will re-open only if the "
            "violation recurs."
        )
        return "\n".join(parts)

    def to_payload(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "object_kind": self.object_kind,
            "rule_kind": self.rule_kind,
            "details": dict(self.details),
            "decision_ref": self.decision_ref,
            "review_dedupe_key": self.review_dedupe_key,
            "primary_consumer": _PRIMARY_CONSUMER,
            "review_plane": _REVIEW_PLANE,
            "escalation_plane": _ESCALATION_PLANE,
            "escalation_model": _ESCALATION_MODEL,
        }


# ---------------------------------------------------------------------------
# Scan SQL
# ---------------------------------------------------------------------------

_SQL_UNOWNED_TAGS = """
-- Only operator-layer pii/sensitive tags trip governance. Auto-detected
-- heuristic tags are informational — if a human hasn't explicitly flagged
-- the asset, don't file bugs about it.
SELECT DISTINCT c.object_kind, c.tag_key
FROM data_dictionary_classifications_effective c
LEFT JOIN data_dictionary_stewardship_effective s
  ON s.object_kind = c.object_kind
 AND s.steward_kind = 'owner'
WHERE c.tag_key IN ('pii', 'sensitive')
  AND c.effective_source = 'operator'
  AND s.object_kind IS NULL
ORDER BY c.object_kind, c.tag_key
"""

_SQL_FAILING_ERROR_RULES = """
WITH latest AS (
    SELECT DISTINCT ON (object_kind, field_path, rule_kind)
        object_kind, field_path, rule_kind, status, started_at
    FROM data_dictionary_quality_runs
    ORDER BY object_kind, field_path, rule_kind, started_at DESC
)
SELECT r.object_kind, r.rule_kind, r.field_path, l.status, l.started_at
FROM data_dictionary_quality_rules_effective r
JOIN latest l
  ON l.object_kind = r.object_kind
 AND l.field_path = r.field_path
 AND l.rule_kind = r.rule_kind
WHERE r.severity = 'error'
  AND r.enabled = TRUE
  AND l.status IN ('fail', 'error')
ORDER BY r.object_kind, r.rule_kind
"""


def scan_violations(conn: Any) -> list[GovernanceViolation]:
    """Run every policy check and return a flat list of violations."""
    out: list[GovernanceViolation] = []

    for row in conn.execute(_SQL_UNOWNED_TAGS) or []:
        tag = str(row.get("tag_key") or "").strip()
        obj = str(row.get("object_kind") or "").strip()
        if not tag or not obj:
            continue
        policy = "pii_without_owner" if tag == "pii" else "sensitive_without_owner"
        out.append(GovernanceViolation(
            policy=policy,
            object_kind=obj,
            details={"tag_key": tag},
        ))

    for row in conn.execute(_SQL_FAILING_ERROR_RULES) or []:
        obj = str(row.get("object_kind") or "").strip()
        rule = str(row.get("rule_kind") or "").strip()
        if not obj or not rule:
            continue
        out.append(GovernanceViolation(
            policy="error_rule_failing",
            object_kind=obj,
            rule_kind=rule,
            details={
                "field_path": str(row.get("field_path") or ""),
                "status": str(row.get("status") or ""),
                "last_run_at": str(row.get("started_at") or ""),
            },
        ))

    return out


# ---------------------------------------------------------------------------
# Bug filing
# ---------------------------------------------------------------------------

_OPEN_STATUSES = bug_open_status_values()


def _downstream_count(conn: Any, object_kind: str) -> int:
    """Best-effort downstream blast-radius count (excludes the root)."""
    try:
        from runtime.data_dictionary_lineage import walk_impact

        walk = walk_impact(
            conn,
            object_kind=object_kind,
            direction="downstream",
            max_depth=3,
        )
        nodes = [n for n in (walk.get("nodes") or []) if n != object_kind]
        return len(nodes)
    except Exception:
        return 0


def _nearest_upstream_owner(conn: Any, object_kind: str) -> str | None:
    """Find the nearest owner across the lineage neighborhood.

    Searches in order:
      1. The object itself (direct owner).
      2. Upstream producers — tables this object was derived from.
      3. Downstream references — tables this object FK-points to.

    Case 3 covers the FK-linkability scenario: a table that only exists
    as a join-partner to some owned authority table should inherit that
    authority's owner, because that's the team likely responsible for
    the data semantics.
    """
    try:
        from runtime.data_dictionary_lineage import walk_impact
        from runtime.data_dictionary_stewardship import describe_stewards
    except Exception:
        return None

    def _first_owner(kind: str) -> str | None:
        try:
            payload = describe_stewards(
                conn, object_kind=kind, field_path=None, include_layers=False,
            )
        except Exception:
            return None
        for row in payload.get("effective") or []:
            if str(row.get("steward_kind") or "") == "owner":
                owner_id = str(row.get("steward_id") or "").strip()
                if owner_id:
                    return owner_id
        return None

    # 1) The object itself.
    direct = _first_owner(object_kind)
    if direct:
        return direct

    # 2) Upstream producers, then 3) downstream references — first match wins.
    for direction in ("upstream", "downstream"):
        try:
            walk = walk_impact(
                conn,
                object_kind=object_kind,
                direction=direction,
                max_depth=3,
            )
        except Exception:
            continue
        for node in walk.get("nodes") or []:
            if node == object_kind:
                continue
            owner = _first_owner(node)
            if owner:
                return owner
    return None


def _severity_for(downstream_count: int) -> BugSeverity:
    return BugSeverity.P1 if downstream_count >= _P1_THRESHOLD else BugSeverity.P2


def _existing_open_bug_id(conn: Any, decision_ref: str) -> str | None:
    rows = conn.execute(
        "SELECT bug_id FROM bugs WHERE decision_ref = $1 AND status = ANY($2)"
        " ORDER BY opened_at DESC LIMIT 1",
        decision_ref,
        list(_OPEN_STATUSES),
    )
    if rows:
        return str(rows[0]["bug_id"])
    return None


def file_violation_bugs(
    conn: Any,
    tracker: BugTracker,
    violations: list[GovernanceViolation],
) -> dict[str, Any]:
    """File bugs for every violation that does not already have an open bug.

    Returns {"filed": [...], "skipped": [...], "errors": [...]}.
    """
    filed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for v in violations:
        ref = v.decision_ref
        try:
            existing = _existing_open_bug_id(conn, ref)
        except Exception as exc:  # DB error on dedup check — record, continue
            errors.append({"decision_ref": ref, "error": f"dedup: {exc}"})
            continue

        if existing:
            skipped.append({"decision_ref": ref, "bug_id": existing})
            continue

        # Impact weighting: compute blast radius for severity upgrade and
        # upstream owner walk for auto-assignment.
        downstream = _downstream_count(conn, v.object_kind)
        severity = _severity_for(downstream)
        assignee = _nearest_upstream_owner(conn, v.object_kind)

        # Enrich violation details so the bug description is self-contained.
        enriched = GovernanceViolation(
            policy=v.policy,
            object_kind=v.object_kind,
            rule_kind=v.rule_kind,
            details={
                **v.details,
                "downstream_count": downstream,
                "upstream_owner": assignee or "",
            },
        )
        # Embed the immediate-remediation suggestion directly in the bug
        # so operators see the fix without having to re-query the tool.
        try:
            from runtime.data_dictionary_governance_remediation import (
                inline_immediate_summary,
            )
            remediation_text = inline_immediate_summary(conn, enriched)
        except Exception:
            remediation_text = ""

        description = enriched.to_bug_description()
        if remediation_text:
            description = description + "\n\n" + remediation_text

        try:
            bug, _ = tracker.file_bug(
                title=enriched.to_bug_title(),
                severity=severity,
                category=_CATEGORY,
                description=description,
                filed_by=_FILED_BY,
                source_kind=_SOURCE_KIND,
                decision_ref=ref,
                tags=("governance", v.policy),
            )
            if assignee:
                try:
                    tracker.assign(bug.bug_id, assignee)
                except Exception:
                    # Assignment is best-effort; filing already succeeded.
                    pass
            filed.append({
                "decision_ref": ref,
                "bug_id": bug.bug_id,
                "severity": severity.value,
                "downstream_count": downstream,
                "assigned_to": assignee,
            })
        except Exception as exc:  # filing failure — record, continue
            errors.append({"decision_ref": ref, "error": f"file: {exc}"})

    return {"filed": filed, "skipped": skipped, "errors": errors}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_governance_scan(
    conn: Any,
    tracker: BugTracker | None = None,
    *,
    dry_run: bool = False,
    triggered_by: str = "heartbeat",
    record_scan: bool = True,
) -> dict[str, Any]:
    """Scan for violations and optionally file bugs.

    When `dry_run=True` (or no tracker is provided) the function returns
    the violations it found without filing anything. When `dry_run=False`
    and a tracker is supplied, every unique (policy, object, rule)
    without an already-open bug gets a new bug filed.

    When `record_scan=True` (default) the scan is persisted as an
    immutable audit row in `data_dictionary_governance_scans`, and every
    newly-filed bug is linked back to that scan via `bug_evidence_links`
    (evidence_kind='governance_scan'). Callers can set `record_scan=False`
    for read-only previews that shouldn't leave an audit trail.
    """
    violations = scan_violations(conn)

    by_policy: dict[str, int] = {}
    for v in violations:
        by_policy[v.policy] = by_policy.get(v.policy, 0) + 1

    summary: dict[str, Any] = {
        "total_violations": len(violations),
        "by_policy": by_policy,
        "violations": [v.to_payload() for v in violations],
    }

    if dry_run or tracker is None:
        summary["dry_run"] = True
        if record_scan:
            _maybe_record_scan(
                conn,
                triggered_by=triggered_by,
                dry_run=True,
                total_violations=len(violations),
                by_policy=by_policy,
                violations=summary["violations"],
                filed_bugs=[], bugs_skipped=0, bugs_errored=0,
                summary_out=summary,
            )
        return summary

    result = file_violation_bugs(conn, tracker, violations)
    summary["dry_run"] = False
    summary["filed_bugs"] = result["filed"]
    summary["skipped_existing"] = result["skipped"]
    summary["filing_errors"] = result["errors"]

    if record_scan:
        _maybe_record_scan(
            conn,
            triggered_by=triggered_by,
            dry_run=False,
            total_violations=len(violations),
            by_policy=by_policy,
            violations=summary["violations"],
            filed_bugs=result["filed"],
            bugs_skipped=len(result["skipped"]),
            bugs_errored=len(result["errors"]),
            summary_out=summary,
        )
    return summary


def _maybe_record_scan(
    conn: Any,
    *,
    triggered_by: str,
    dry_run: bool,
    total_violations: int,
    by_policy: dict[str, int],
    violations: list[dict[str, Any]],
    filed_bugs: list[dict[str, Any]],
    bugs_skipped: int,
    bugs_errored: int,
    summary_out: dict[str, Any],
) -> None:
    """Persist an audit row + link every newly-filed bug to this scan.

    Failure here is non-fatal: audit loss must not crash governance. We
    tuck the scan_id into `summary_out` when the write succeeds.
    """
    try:
        from storage.postgres.data_dictionary_governance_scans_repository import (
            insert_scan, link_bug_to_scan,
        )

        scan = insert_scan(
            conn,
            triggered_by=triggered_by,
            dry_run=dry_run,
            total_violations=total_violations,
            bugs_filed=len(filed_bugs),
            bugs_skipped=bugs_skipped,
            bugs_errored=bugs_errored,
            by_policy=by_policy,
            violations=violations,
            filed_bug_ids=[b["bug_id"] for b in filed_bugs if b.get("bug_id")],
            metadata={},
        )
        summary_out["scan_id"] = scan["scan_id"]

        # Back-link every NEW bug to this scan — so any bug viewer can see
        # "discovered by <scan_id>" in the evidence list.
        for b in filed_bugs:
            bug_id = b.get("bug_id")
            if not bug_id:
                continue
            try:
                link_bug_to_scan(
                    conn, bug_id=bug_id, scan_id=scan["scan_id"],
                    role="discovered_by",
                )
            except Exception:
                pass
    except Exception:
        # Audit is best-effort.
        pass


# ---------------------------------------------------------------------------
# Scorecard — single-number compliance health
# ---------------------------------------------------------------------------

_SCORECARD_WEIGHTS = {
    "owned_pct": 0.25,       # sensitive/pii assets with an owner
    "classified_pct": 0.15,  # objects with any classification
    "rule_coverage_pct": 0.25,  # objects with at least one enabled rule
    "bug_inverse": 0.15,     # 1.0 minus fraction of objects with an open governance bug
    "wiring_pct": 0.20,      # 1.0 minus normalized hard-path + unwired-authority load
}


# Tuning constants for the wiring sub-score. Hard-paths are per-file
# findings; 50+ hits counts as fully saturated pain. Unreferenced
# decisions: 100+ is saturated. Code-orphans: 30+ saturated.
_WIRING_SATURATION = {
    "hard_paths": 50,
    "unreferenced_decisions": 100,
    "code_orphans": 30,
}


def _latest_wiring_snapshot(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT hard_path_total,
               absolute_user_paths,
               hardcoded_localhost,
               hardcoded_ports,
               unreferenced_decisions,
               code_orphan_tables
        FROM data_dictionary_wiring_audit_snapshots
        ORDER BY taken_at DESC
        LIMIT 1
        """
    )
    if not rows:
        return {
            "hard_path_total": 0, "absolute_user_paths": 0,
            "hardcoded_localhost": 0, "hardcoded_ports": 0,
            "unreferenced_decisions": 0, "code_orphan_tables": 0,
        }
    row = dict(rows[0])
    return {k: int(row.get(k) or 0) for k in (
        "hard_path_total", "absolute_user_paths",
        "hardcoded_localhost", "hardcoded_ports",
        "unreferenced_decisions", "code_orphan_tables",
    )}


def _wiring_pct(snapshot: dict[str, int]) -> float:
    """Turn the three wiring counts into a 0..1 health metric.

    Each count is compared to its saturation level and clamped. The
    pct is the inverse average of the three normalized loads.
    """
    sat = _WIRING_SATURATION
    hp_load   = min(1.0, snapshot["hard_path_total"] / max(1, sat["hard_paths"]))
    ud_load   = min(1.0, snapshot["unreferenced_decisions"] / max(1, sat["unreferenced_decisions"]))
    orph_load = min(1.0, snapshot["code_orphan_tables"] / max(1, sat["code_orphans"]))
    avg_load = (hp_load + ud_load + orph_load) / 3.0
    return max(0.0, 1.0 - avg_load)


def _pct(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return max(0.0, min(1.0, n / d))


def compute_scorecard(conn: Any) -> dict[str, Any]:
    """Single-number governance health across the four axes.

    Returns:
        total_objects, objects_with_owner, objects_with_classification,
        objects_with_rule, open_governance_bugs, metrics (pcts),
        compliance_score (weighted average 0..1).
    """
    total_objects = 0
    rows = conn.execute("SELECT COUNT(*)::int AS c FROM data_dictionary_objects")
    if rows:
        total_objects = int(rows[0].get("c") or 0)

    owner_rows = conn.execute(
        "SELECT COUNT(DISTINCT object_kind)::int AS c "
        "FROM data_dictionary_stewardship_effective WHERE steward_kind = 'owner'"
    )
    objects_with_owner = int(owner_rows[0]["c"]) if owner_rows else 0

    cls_rows = conn.execute(
        "SELECT COUNT(DISTINCT object_kind)::int AS c "
        "FROM data_dictionary_classifications_effective"
    )
    objects_with_classification = int(cls_rows[0]["c"]) if cls_rows else 0

    rule_rows = conn.execute(
        "SELECT COUNT(DISTINCT object_kind)::int AS c "
        "FROM data_dictionary_quality_rules_effective WHERE enabled = TRUE"
    )
    objects_with_rule = int(rule_rows[0]["c"]) if rule_rows else 0

    # Sensitive/PII coverage — a PII asset without an owner hurts the
    # owned-pct metric disproportionately (governance is about exposure).
    sensitive_rows = conn.execute(
        "SELECT COUNT(DISTINCT object_kind)::int AS c "
        "FROM data_dictionary_classifications_effective "
        "WHERE tag_key IN ('pii', 'sensitive') "
        "  AND effective_source = 'operator'"
    )
    sensitive_objects = int(sensitive_rows[0]["c"]) if sensitive_rows else 0

    unowned_sensitive_rows = conn.execute(
        "SELECT COUNT(DISTINCT c.object_kind)::int AS c "
        "FROM data_dictionary_classifications_effective c "
        "LEFT JOIN data_dictionary_stewardship_effective s "
        "  ON s.object_kind = c.object_kind AND s.steward_kind = 'owner' "
        "WHERE c.tag_key IN ('pii', 'sensitive') "
        "  AND c.effective_source = 'operator' "
        "  AND s.object_kind IS NULL"
    )
    unowned_sensitive = int(unowned_sensitive_rows[0]["c"]) if unowned_sensitive_rows else 0

    open_bug_rows = conn.execute(
        "SELECT COUNT(*)::int AS c FROM bugs "
        "WHERE decision_ref LIKE 'governance.%' AND status = ANY($1)",
        list(_OPEN_STATUSES),
    )
    open_governance_bugs = int(open_bug_rows[0]["c"]) if open_bug_rows else 0

    bug_by_policy_rows = conn.execute(
        "SELECT split_part(decision_ref, '.', 2) AS policy, COUNT(*)::int AS c "
        "FROM bugs WHERE decision_ref LIKE 'governance.%' AND status = ANY($1) "
        "GROUP BY 1",
        list(_OPEN_STATUSES),
    )
    by_policy: dict[str, int] = {}
    for r in bug_by_policy_rows or []:
        policy = str(r.get("policy") or "").strip()
        if policy:
            by_policy[policy] = int(r.get("c") or 0)

    # Metrics
    owned_pct = (
        _pct(sensitive_objects - unowned_sensitive, sensitive_objects)
        if sensitive_objects > 0 else 1.0
    )
    classified_pct = _pct(objects_with_classification, total_objects)
    rule_coverage_pct = _pct(objects_with_rule, total_objects)
    bug_inverse = 1.0 - _pct(open_governance_bugs, max(total_objects, 1))

    # Wiring sub-score reads the most recent wiring-audit snapshot.
    # The projector writes one every heartbeat; this makes the scorecard
    # react to VPS-migration readiness without a separate API call.
    wiring_snapshot = _latest_wiring_snapshot(conn)
    wiring_pct = _wiring_pct(wiring_snapshot)

    compliance_score = round(
        owned_pct * _SCORECARD_WEIGHTS["owned_pct"]
        + classified_pct * _SCORECARD_WEIGHTS["classified_pct"]
        + rule_coverage_pct * _SCORECARD_WEIGHTS["rule_coverage_pct"]
        + bug_inverse * _SCORECARD_WEIGHTS["bug_inverse"]
        + wiring_pct * _SCORECARD_WEIGHTS["wiring_pct"],
        4,
    )

    # Cluster leverage — how many distinct root causes the open bugs
    # collapse to. Surfaces as "N bugs / M clusters" so operators see
    # whether one big fix can retire a chunk of the backlog.
    try:
        from runtime.data_dictionary_governance_clustering import cluster_violations

        vs = scan_violations(conn)
        clusters = cluster_violations(conn, vs)
        cluster_count = len(clusters)
        bulk_fix_count = sum(1 for c in clusters if c.cluster_fix)
    except Exception:
        cluster_count = 0
        bulk_fix_count = 0

    # Letter grade for quick eyeballing.
    if compliance_score >= 0.9:
        grade = "A"
    elif compliance_score >= 0.8:
        grade = "B"
    elif compliance_score >= 0.7:
        grade = "C"
    elif compliance_score >= 0.6:
        grade = "D"
    else:
        grade = "F"

    return {
        "total_objects": total_objects,
        "objects_with_owner": objects_with_owner,
        "objects_with_classification": objects_with_classification,
        "objects_with_rule": objects_with_rule,
        "sensitive_objects": sensitive_objects,
        "unowned_sensitive": unowned_sensitive,
        "open_governance_bugs": open_governance_bugs,
        "open_governance_bugs_by_policy": by_policy,
        "cluster_count": cluster_count,
        "bulk_fixes_available": bulk_fix_count,
        "wiring_audit": wiring_snapshot,
        "metrics": {
            "owned_pct": round(owned_pct, 4),
            "classified_pct": round(classified_pct, 4),
            "rule_coverage_pct": round(rule_coverage_pct, 4),
            "bug_inverse": round(bug_inverse, 4),
            "wiring_pct": round(wiring_pct, 4),
        },
        "weights": dict(_SCORECARD_WEIGHTS),
        "compliance_score": compliance_score,
        "grade": grade,
    }


__all__ = [
    "GovernanceViolation",
    "compute_scorecard",
    "file_violation_bugs",
    "run_governance_scan",
    "scan_violations",
]
