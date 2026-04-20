"""Unit tests for governance-scan audit receipt wiring.

Verifies that `run_governance_scan`:
  * writes a row to data_dictionary_governance_scans on every invocation
    that has `record_scan=True` (default)
  * back-links every newly-filed bug to the scan via bug_evidence_links
  * records dry_run and triggered_by correctly
  * swallows audit-write failures without poisoning the scan result
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime.data_dictionary_governance import (
    GovernanceViolation,
    run_governance_scan,
)
from runtime import data_dictionary_governance as governance


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.executed.append((sql, args))
        if "data_dictionary_classifications_effective" in sql:
            return []
        if "data_dictionary_quality_rules_effective" in sql:
            return []
        if "FROM bugs WHERE decision_ref" in sql:
            return []  # no pre-existing open bug
        return []


class _FakeBug:
    def __init__(self, bug_id: str) -> None:
        self.bug_id = bug_id


class _FakeTracker:
    def __init__(self) -> None:
        self.filed: list[dict[str, Any]] = []
        self.counter = 0

    def file_bug(self, **kw: Any) -> tuple[Any, list[Any]]:
        self.counter += 1
        bug_id = f"BUG-{self.counter:04X}"
        self.filed.append({"bug_id": bug_id, **kw})
        return _FakeBug(bug_id), []

    def assign(self, bug_id: str, assigned_to: str) -> None:
        pass


@pytest.fixture
def _audit_spy(monkeypatch) -> dict[str, Any]:
    """Intercept the repository writes and record arguments."""
    captured: dict[str, Any] = {"inserts": [], "links": []}

    def _insert(conn, **kw):
        captured["inserts"].append(kw)
        return {"scan_id": f"SCAN-{len(captured['inserts'])}", "scanned_at": "T"}

    def _link(conn, *, bug_id, scan_id, role="discovered_by"):
        captured["links"].append({"bug_id": bug_id, "scan_id": scan_id, "role": role})

    import storage.postgres.data_dictionary_governance_scans_repository as repo
    monkeypatch.setattr(repo, "insert_scan", _insert)
    monkeypatch.setattr(repo, "link_bug_to_scan", _link)
    return captured


@pytest.fixture
def _no_impact(monkeypatch):
    """Zero out impact-analysis helpers so the scan path is pure."""
    monkeypatch.setattr(governance, "_downstream_count", lambda conn, k: 0)
    monkeypatch.setattr(governance, "_nearest_upstream_owner", lambda conn, k: None)


# ---------------------------------------------------------------------------
# Dry-run scans record an audit row with dry_run=True
# ---------------------------------------------------------------------------

def test_dry_run_records_audit_row(_audit_spy, monkeypatch) -> None:
    monkeypatch.setattr(
        governance, "scan_violations",
        lambda conn: [
            GovernanceViolation(policy="pii_without_owner", object_kind="table:x"),
        ],
    )
    result = run_governance_scan(_FakeConn(), tracker=None, dry_run=True)
    assert result["dry_run"] is True
    assert len(_audit_spy["inserts"]) == 1
    insert = _audit_spy["inserts"][0]
    assert insert["dry_run"] is True
    assert insert["total_violations"] == 1
    assert insert["bugs_filed"] == 0
    assert insert["filed_bug_ids"] == []
    # No bug links for a dry run.
    assert _audit_spy["links"] == []


def test_audit_captures_triggered_by(_audit_spy, monkeypatch) -> None:
    monkeypatch.setattr(governance, "scan_violations", lambda conn: [])
    run_governance_scan(
        _FakeConn(), tracker=None, dry_run=True, triggered_by="operator_http",
    )
    assert _audit_spy["inserts"][0]["triggered_by"] == "operator_http"


# ---------------------------------------------------------------------------
# Enforce path records + back-links every newly filed bug
# ---------------------------------------------------------------------------

def test_enforce_links_every_filed_bug_to_scan(_audit_spy, _no_impact, monkeypatch) -> None:
    monkeypatch.setattr(
        governance, "scan_violations",
        lambda conn: [
            GovernanceViolation(policy="pii_without_owner", object_kind="table:a"),
            GovernanceViolation(policy="pii_without_owner", object_kind="table:b"),
        ],
    )
    tracker = _FakeTracker()
    result = run_governance_scan(_FakeConn(), tracker=tracker, dry_run=False)
    assert len(result["filed_bugs"]) == 2
    # Scan recorded once.
    assert len(_audit_spy["inserts"]) == 1
    insert = _audit_spy["inserts"][0]
    assert insert["dry_run"] is False
    assert insert["bugs_filed"] == 2
    assert sorted(insert["filed_bug_ids"]) == ["BUG-0001", "BUG-0002"]
    # One link per filed bug.
    assert len(_audit_spy["links"]) == 2
    roles = {l["role"] for l in _audit_spy["links"]}
    assert roles == {"discovered_by"}


def test_scan_id_exposed_on_returned_summary(_audit_spy, _no_impact, monkeypatch) -> None:
    monkeypatch.setattr(governance, "scan_violations", lambda conn: [])
    result = run_governance_scan(_FakeConn(), tracker=_FakeTracker(), dry_run=False)
    assert result["scan_id"] == "SCAN-1"


# ---------------------------------------------------------------------------
# record_scan=False explicitly disables the audit row
# ---------------------------------------------------------------------------

def test_record_scan_false_skips_audit(_audit_spy, monkeypatch) -> None:
    monkeypatch.setattr(governance, "scan_violations", lambda conn: [])
    run_governance_scan(
        _FakeConn(), tracker=None, dry_run=True, record_scan=False,
    )
    assert _audit_spy["inserts"] == []


# ---------------------------------------------------------------------------
# Audit-write failures do not break the scan
# ---------------------------------------------------------------------------

def test_audit_write_failure_is_swallowed(monkeypatch) -> None:
    import storage.postgres.data_dictionary_governance_scans_repository as repo

    def _boom(conn, **kw):
        raise RuntimeError("audit db down")

    monkeypatch.setattr(repo, "insert_scan", _boom)
    monkeypatch.setattr(governance, "scan_violations", lambda conn: [])
    # Must not raise.
    result = run_governance_scan(_FakeConn(), tracker=None, dry_run=True)
    assert result["total_violations"] == 0
    assert "scan_id" not in result


def test_link_failures_are_swallowed(_audit_spy, _no_impact, monkeypatch) -> None:
    import storage.postgres.data_dictionary_governance_scans_repository as repo

    def _bad_link(conn, **kw):
        raise RuntimeError("link dead")

    monkeypatch.setattr(repo, "link_bug_to_scan", _bad_link)
    monkeypatch.setattr(
        governance, "scan_violations",
        lambda conn: [GovernanceViolation(policy="pii_without_owner",
                                          object_kind="table:a")],
    )
    result = run_governance_scan(_FakeConn(), tracker=_FakeTracker(), dry_run=False)
    # Scan still recorded, bug still filed — linking failure is best-effort.
    assert result["scan_id"] == "SCAN-1"
    assert len(result["filed_bugs"]) == 1
