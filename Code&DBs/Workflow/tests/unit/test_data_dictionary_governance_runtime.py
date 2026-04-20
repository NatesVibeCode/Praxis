"""Unit tests for `runtime.data_dictionary_governance`.

The scanner is SQL-driven, so tests stub `conn.execute` with a
dict-keyed responder that returns fake rows for whichever query is
being run. Bug filing is stubbed through a fake `BugTracker` that
captures calls.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime.bug_tracker import BugSeverity, BugCategory
from runtime import data_dictionary_governance as governance
from runtime.data_dictionary_governance import (
    GovernanceViolation,
    compute_scorecard,
    file_violation_bugs,
    run_governance_scan,
    scan_violations,
)


class _FakeConn:
    def __init__(self) -> None:
        self.unowned_rows: list[dict[str, Any]] = []
        self.failing_rule_rows: list[dict[str, Any]] = []
        self.existing_open: dict[str, str] = {}  # decision_ref -> bug_id

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        if "data_dictionary_classifications_effective" in sql:
            return list(self.unowned_rows)
        if "data_dictionary_quality_rules_effective" in sql:
            return list(self.failing_rule_rows)
        if "FROM bugs WHERE decision_ref" in sql:
            ref = args[0]
            if ref in self.existing_open:
                return [{"bug_id": self.existing_open[ref]}]
            return []
        return []


class _FakeBug:
    def __init__(self, bug_id: str) -> None:
        self.bug_id = bug_id


class _FakeTracker:
    def __init__(self) -> None:
        self.filed: list[dict[str, Any]] = []
        self.assigned: list[tuple[str, str]] = []
        self._counter = 0

    def file_bug(self, **kw: Any) -> tuple[Any, list[Any]]:
        self._counter += 1
        bug_id = f"BUG-{self._counter:04X}"
        self.filed.append({"bug_id": bug_id, **kw})
        return _FakeBug(bug_id), []

    def assign(self, bug_id: str, assigned_to: str) -> None:
        self.assigned.append((bug_id, assigned_to))


@pytest.fixture
def _no_impact(monkeypatch):
    """Zero out impact/owner lookup so file_violation_bugs tests stay pure."""
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 0)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: None)


# --- GovernanceViolation dataclass --------------------------------------


def test_decision_ref_without_rule_kind() -> None:
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    assert v.decision_ref == "governance.pii_without_owner.table:users"


def test_decision_ref_with_rule_kind() -> None:
    v = GovernanceViolation(
        policy="error_rule_failing",
        object_kind="table:bugs",
        rule_kind="not_null",
    )
    assert v.decision_ref == "governance.error_rule_failing.table:bugs.not_null"


def test_bug_title_mentions_policy_and_object() -> None:
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    assert "PII" in v.to_bug_title()
    assert "table:users" in v.to_bug_title()


def test_to_payload_is_flat_dict() -> None:
    v = GovernanceViolation(
        policy="sensitive_without_owner",
        object_kind="table:hr",
        details={"tag_key": "sensitive"},
    )
    p = v.to_payload()
    assert p["policy"] == "sensitive_without_owner"
    assert p["object_kind"] == "table:hr"
    assert p["rule_kind"] == ""
    assert p["details"] == {"tag_key": "sensitive"}
    assert p["decision_ref"].startswith("governance.sensitive_without_owner.")


# --- scan_violations ----------------------------------------------------


def test_scan_emits_pii_and_sensitive_from_unowned_rows() -> None:
    conn = _FakeConn()
    conn.unowned_rows = [
        {"object_kind": "table:users", "tag_key": "pii"},
        {"object_kind": "table:hr", "tag_key": "sensitive"},
    ]
    violations = scan_violations(conn)
    policies = {v.policy for v in violations}
    assert policies == {"pii_without_owner", "sensitive_without_owner"}


def test_scan_emits_failing_error_rules() -> None:
    conn = _FakeConn()
    conn.failing_rule_rows = [
        {
            "object_kind": "table:bugs",
            "rule_kind": "not_null",
            "field_path": "title",
            "status": "fail",
            "started_at": "2026-04-19T00:00:00Z",
        }
    ]
    violations = scan_violations(conn)
    assert len(violations) == 1
    v = violations[0]
    assert v.policy == "error_rule_failing"
    assert v.object_kind == "table:bugs"
    assert v.rule_kind == "not_null"
    assert v.details["field_path"] == "title"
    assert v.details["status"] == "fail"


def test_scan_skips_rows_with_missing_object_kind() -> None:
    conn = _FakeConn()
    conn.unowned_rows = [
        {"object_kind": "", "tag_key": "pii"},
        {"object_kind": "table:ok", "tag_key": "pii"},
    ]
    conn.failing_rule_rows = [
        {"object_kind": "", "rule_kind": "x"},
        {"object_kind": "table:ok", "rule_kind": ""},
    ]
    violations = scan_violations(conn)
    assert len(violations) == 1
    assert violations[0].object_kind == "table:ok"


# --- file_violation_bugs ------------------------------------------------


def test_file_violation_bugs_files_new_violation(_no_impact) -> None:
    conn = _FakeConn()
    tracker = _FakeTracker()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    result = file_violation_bugs(conn, tracker, [v])
    assert len(result["filed"]) == 1
    assert result["skipped"] == []
    filed = tracker.filed[0]
    assert filed["decision_ref"] == v.decision_ref
    assert filed["severity"] == BugSeverity.P2  # no blast radius → default
    assert filed["category"] == BugCategory.ARCHITECTURE
    assert "governance" in filed["tags"]
    assert filed["filed_by"] == "governance_compliance_heartbeat"
    # With no upstream owner, nothing should be assigned.
    assert tracker.assigned == []


def test_file_violation_bugs_dedupes_against_open_bug(_no_impact) -> None:
    conn = _FakeConn()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    conn.existing_open[v.decision_ref] = "BUG-ABCD"
    tracker = _FakeTracker()
    result = file_violation_bugs(conn, tracker, [v])
    assert result["filed"] == []
    assert result["skipped"] == [{"decision_ref": v.decision_ref, "bug_id": "BUG-ABCD"}]
    assert tracker.filed == []


def test_file_violation_bugs_isolates_dedup_errors(monkeypatch) -> None:
    class _BoomConn:
        def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM bugs WHERE decision_ref" in sql:
                raise RuntimeError("connection lost")
            return []

    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    tracker = _FakeTracker()
    result = file_violation_bugs(_BoomConn(), tracker, [v])
    assert result["filed"] == []
    assert result["errors"][0]["decision_ref"] == v.decision_ref
    assert "connection lost" in result["errors"][0]["error"]


def test_file_violation_bugs_isolates_filing_errors(_no_impact) -> None:
    conn = _FakeConn()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")

    class _FailingTracker(_FakeTracker):
        def file_bug(self, **kw: Any):
            raise RuntimeError("db write failed")

    result = file_violation_bugs(conn, _FailingTracker(), [v])
    assert result["filed"] == []
    assert "db write failed" in result["errors"][0]["error"]


# --- Impact weighting + auto-assignment ---------------------------------


def test_high_blast_radius_upgrades_severity_to_p1(monkeypatch) -> None:
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 42)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: None)
    tracker = _FakeTracker()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:hub")
    file_violation_bugs(_FakeConn(), tracker, [v])
    assert tracker.filed[0]["severity"] == BugSeverity.P1


def test_low_blast_radius_stays_p2(monkeypatch) -> None:
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 2)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: None)
    tracker = _FakeTracker()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:leaf")
    file_violation_bugs(_FakeConn(), tracker, [v])
    assert tracker.filed[0]["severity"] == BugSeverity.P2


def test_auto_assignment_uses_upstream_owner(monkeypatch) -> None:
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 0)
    monkeypatch.setattr(
        governance, "_nearest_upstream_owner",
        lambda conn, k: "alice@example.com",
    )
    tracker = _FakeTracker()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    file_violation_bugs(_FakeConn(), tracker, [v])
    assert len(tracker.assigned) == 1
    bug_id, assignee = tracker.assigned[0]
    assert bug_id == tracker.filed[0]["bug_id"]
    assert assignee == "alice@example.com"


def test_assignment_failure_does_not_rollback_filing(monkeypatch) -> None:
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 0)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: "bob")

    class _NoAssignTracker(_FakeTracker):
        def assign(self, bug_id: str, assigned_to: str) -> None:
            raise RuntimeError("assign failed")

    tracker = _NoAssignTracker()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:x")
    result = file_violation_bugs(_FakeConn(), tracker, [v])
    # Filing succeeded; assignment failure is swallowed.
    assert len(result["filed"]) == 1
    assert result["errors"] == []


def test_enriched_description_includes_blast_radius(monkeypatch) -> None:
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 17)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: "team-data")
    tracker = _FakeTracker()
    v = GovernanceViolation(
        policy="pii_without_owner",
        object_kind="table:users",
        details={"tag_key": "pii"},
    )
    file_violation_bugs(_FakeConn(), tracker, [v])
    desc = tracker.filed[0]["description"]
    assert "downstream_count" in desc
    assert "17" in desc
    assert "upstream_owner" in desc


def test_filed_record_exposes_severity_and_assignee(monkeypatch) -> None:
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 12)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: "team-x")
    tracker = _FakeTracker()
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:x")
    result = file_violation_bugs(_FakeConn(), tracker, [v])
    rec = result["filed"][0]
    assert rec["severity"] == "P1"
    assert rec["downstream_count"] == 12
    assert rec["assigned_to"] == "team-x"


# --- run_governance_scan ------------------------------------------------


def test_dry_run_returns_violations_without_filing() -> None:
    conn = _FakeConn()
    conn.unowned_rows = [
        {"object_kind": "table:users", "tag_key": "pii"},
    ]
    tracker = _FakeTracker()
    result = run_governance_scan(conn, tracker, dry_run=True)
    assert result["dry_run"] is True
    assert result["total_violations"] == 1
    assert result["by_policy"] == {"pii_without_owner": 1}
    assert "filed_bugs" not in result
    assert tracker.filed == []


def test_enforce_files_bugs_and_counts_them(_no_impact) -> None:
    conn = _FakeConn()
    conn.unowned_rows = [
        {"object_kind": "table:users", "tag_key": "pii"},
        {"object_kind": "table:hr", "tag_key": "sensitive"},
    ]
    conn.failing_rule_rows = [
        {"object_kind": "table:bugs", "rule_kind": "not_null",
         "field_path": "title", "status": "fail", "started_at": "now"},
    ]
    tracker = _FakeTracker()
    result = run_governance_scan(conn, tracker, dry_run=False)
    assert result["dry_run"] is False
    assert result["total_violations"] == 3
    assert len(result["filed_bugs"]) == 3
    assert result["by_policy"] == {
        "pii_without_owner": 1,
        "sensitive_without_owner": 1,
        "error_rule_failing": 1,
    }


def test_enforce_preserves_skip_and_error_breakdown(_no_impact) -> None:
    conn = _FakeConn()
    v_obj = "table:users"
    conn.unowned_rows = [{"object_kind": v_obj, "tag_key": "pii"}]
    conn.existing_open[f"governance.pii_without_owner.{v_obj}"] = "BUG-EXISTING"
    tracker = _FakeTracker()
    result = run_governance_scan(conn, tracker, dry_run=False)
    assert result["filed_bugs"] == []
    assert result["skipped_existing"][0]["bug_id"] == "BUG-EXISTING"
    assert result["filing_errors"] == []


def test_none_tracker_forces_dry_run() -> None:
    conn = _FakeConn()
    conn.unowned_rows = [{"object_kind": "table:x", "tag_key": "pii"}]
    result = run_governance_scan(conn, tracker=None, dry_run=False)
    assert result["dry_run"] is True
    assert result["total_violations"] == 1


# --- Scorecard ----------------------------------------------------------


class _ScorecardConn:
    """Respond to every scorecard query with canned counts."""

    def __init__(self, **counts: int) -> None:
        self.counts = counts
        self.by_policy: list[dict[str, Any]] = []
        # Cluster-query output: returned by scan_violations() invoked from
        # the scorecard's cluster-count block. Default to empty.
        self.violation_rows_unowned: list[dict[str, Any]] = []
        self.violation_rows_failing: list[dict[str, Any]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        # Order matters: more-specific SQL shapes must match before the
        # substring fallbacks for the same table.
        if "FROM data_dictionary_objects" in sql:
            return [{"c": self.counts.get("total_objects", 0)}]
        if "LEFT JOIN data_dictionary_stewardship_effective" in sql:
            # This query is used by BOTH the scorecard's unowned_sensitive
            # count AND scan_violations(). scan_violations expects rows
            # with object_kind + tag_key, scorecard expects rows with "c".
            # We disambiguate by return shape based on the SELECT list.
            if "COUNT(DISTINCT" in sql:
                return [{"c": self.counts.get("unowned_sensitive", 0)}]
            return list(self.violation_rows_unowned)
        if "FROM data_dictionary_stewardship_effective" in sql:
            return [{"c": self.counts.get("objects_with_owner", 0)}]
        if "tag_key IN ('pii', 'sensitive')" in sql and "COUNT" in sql:
            return [{"c": self.counts.get("sensitive_objects", 0)}]
        if "FROM data_dictionary_classifications_effective" in sql:
            return [{"c": self.counts.get("objects_with_classification", 0)}]
        if ("FROM data_dictionary_quality_rules_effective" in sql
                and "COUNT" in sql):
            return [{"c": self.counts.get("objects_with_rule", 0)}]
        if "data_dictionary_quality_rules_effective" in sql:
            # scan_violations failing-rules query.
            return list(self.violation_rows_failing)
        if "FROM bugs" in sql and "GROUP BY" in sql:
            return list(self.by_policy)
        if "FROM bugs" in sql:
            return [{"c": self.counts.get("open_governance_bugs", 0)}]
        return []


def test_scorecard_perfect_score_when_everything_owned_and_no_bugs() -> None:
    conn = _ScorecardConn(
        total_objects=100,
        objects_with_owner=100,
        objects_with_classification=100,
        objects_with_rule=100,
        sensitive_objects=10,
        unowned_sensitive=0,
        open_governance_bugs=0,
    )
    s = compute_scorecard(conn)
    assert s["compliance_score"] == 1.0
    assert s["grade"] == "A"
    assert s["metrics"]["owned_pct"] == 1.0


def test_scorecard_penalizes_unowned_sensitive() -> None:
    conn = _ScorecardConn(
        total_objects=100,
        objects_with_owner=50,
        objects_with_classification=50,
        objects_with_rule=50,
        sensitive_objects=10,
        unowned_sensitive=5,  # 50% coverage of sensitive
        open_governance_bugs=0,
    )
    s = compute_scorecard(conn)
    assert s["metrics"]["owned_pct"] == 0.5
    assert s["compliance_score"] < 1.0


def test_scorecard_handles_zero_objects() -> None:
    conn = _ScorecardConn(total_objects=0, sensitive_objects=0)
    s = compute_scorecard(conn)
    # No objects → classification/rule pcts are 0 but owned_pct defaults to
    # perfect (nothing sensitive to worry about) and bug_inverse to 1.0.
    assert s["total_objects"] == 0
    assert s["metrics"]["owned_pct"] == 1.0
    assert s["metrics"]["classified_pct"] == 0.0
    assert 0.0 < s["compliance_score"] <= 1.0


def test_scorecard_buckets_bugs_by_policy() -> None:
    conn = _ScorecardConn(total_objects=10, open_governance_bugs=3)
    conn.by_policy = [
        {"policy": "pii_without_owner", "c": 2},
        {"policy": "error_rule_failing", "c": 1},
    ]
    s = compute_scorecard(conn)
    assert s["open_governance_bugs_by_policy"] == {
        "pii_without_owner": 2,
        "error_rule_failing": 1,
    }


def test_scorecard_grade_boundaries() -> None:
    # 0.70*0.30 + 0.75*0.20 + 0.75*0.30 + 1.0*0.20 = 0.785  → grade C
    conn = _ScorecardConn(
        total_objects=100,
        objects_with_owner=75,
        objects_with_classification=75,
        objects_with_rule=75,
        sensitive_objects=10,
        unowned_sensitive=3,   # 70% owned of sensitive
        open_governance_bugs=0,
    )
    s = compute_scorecard(conn)
    assert s["grade"] in {"B", "C"}
    assert 0.7 <= s["compliance_score"] <= 0.85
