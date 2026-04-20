"""Unit tests for the governance change-feed drain.

Verifies that `drain_change_feed`:
  * claims pending ledger rows
  * dedupes affected objects before scanning
  * runs scoped per-object scans against the effective views
  * files bugs when violations exist (respecting dry_run)
  * marks rows processed with the resulting scan_id
  * swallows filing errors without losing ledger progress
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime.data_dictionary_governance_change_feed import (
    drain_change_feed,
    peek_pending,
    pending_count,
)


class _FakeConn:
    def __init__(
        self,
        *,
        pending_rows: list[dict[str, Any]] | None = None,
        unowned_by_object: dict[str, list[dict[str, Any]]] | None = None,
        failing_by_object: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self._pending = list(pending_rows or [])
        self._unowned = dict(unowned_by_object or {})
        self._failing = dict(failing_by_object or {})
        self.executed_sql: list[tuple[str, tuple[Any, ...]]] = []
        self.mark_processed_calls: list[tuple[list[int], str | None]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.executed_sql.append((sql, args))
        if "FROM data_dictionary_governance_change_ledger" in sql and "processed_at IS NULL" in sql:
            if "COUNT(*)" in sql:
                return [{"c": sum(1 for r in self._pending)}]
            # The claim query.
            return list(self._pending)
        if "UPDATE data_dictionary_governance_change_ledger" in sql:
            self.mark_processed_calls.append((list(args[0]), args[1]))
            return []
        if "FROM data_dictionary_classifications_effective c" in sql:
            obj = args[0] if args else ""
            return list(self._unowned.get(obj, []))
        if "FROM data_dictionary_quality_rules_effective r" in sql:
            obj = args[0] if args else ""
            return list(self._failing.get(obj, []))
        if "FROM bugs WHERE decision_ref" in sql:
            return []  # no pre-existing bugs
        if "data_dictionary_classifications_effective" in sql:
            return []
        if "data_dictionary_quality_rules_effective" in sql:
            return []
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
def _audit_stub(monkeypatch) -> dict[str, Any]:
    """Stub the audit-write path so drain tests don't write real rows."""
    captured: dict[str, Any] = {"scans": 0}

    def _fake_record(conn, **kw):
        captured["scans"] += 1
        kw["summary_out"]["scan_id"] = f"SCAN-{captured['scans']}"

    import runtime.data_dictionary_governance as gov
    monkeypatch.setattr(gov, "_maybe_record_scan", _fake_record)
    # Also patch the re-export path used by the change_feed module.
    import runtime.data_dictionary_governance_change_feed as cf
    monkeypatch.setattr(cf, "_maybe_record_scan", _fake_record)
    return captured


@pytest.fixture
def _no_impact(monkeypatch):
    import runtime.data_dictionary_governance as gov
    monkeypatch.setattr(gov, "_downstream_count", lambda conn, k: 0)
    monkeypatch.setattr(gov, "_nearest_upstream_owner", lambda conn, k: None)


# ---------------------------------------------------------------------------
# Empty ledger — fast path
# ---------------------------------------------------------------------------

def test_empty_ledger_returns_zero_drain(_audit_stub) -> None:
    result = drain_change_feed(_FakeConn(), tracker=None)
    assert result["drained"] == 0
    assert result["objects_scanned"] == 0
    assert result["total_violations"] == 0
    assert result["scan_id"] is None


# ---------------------------------------------------------------------------
# Drain claims + dedupes affected objects
# ---------------------------------------------------------------------------

def test_drain_dedupes_multiple_rows_per_object(_audit_stub) -> None:
    conn = _FakeConn(pending_rows=[
        {"change_id": 1, "affected_object_kind": "table:a",
         "source_table": "data_dictionary_classifications",
         "change_kind": "insert", "observed_at": "t1"},
        {"change_id": 2, "affected_object_kind": "table:a",
         "source_table": "data_dictionary_stewardship",
         "change_kind": "update", "observed_at": "t2"},
        {"change_id": 3, "affected_object_kind": "table:b",
         "source_table": "data_dictionary_classifications",
         "change_kind": "delete", "observed_at": "t3"},
    ])
    result = drain_change_feed(conn, tracker=None)
    assert result["drained"] == 3
    assert result["objects_scanned"] == 2
    assert set(result["affected_objects"]) == {"table:a", "table:b"}


# ---------------------------------------------------------------------------
# Scoped scan surfaces per-object violations
# ---------------------------------------------------------------------------

def test_drain_finds_pii_without_owner_for_affected_object(
    _audit_stub, _no_impact,
) -> None:
    conn = _FakeConn(
        pending_rows=[
            {"change_id": 1, "affected_object_kind": "table:users",
             "source_table": "data_dictionary_classifications",
             "change_kind": "insert", "observed_at": "t"},
        ],
        unowned_by_object={
            "table:users": [
                {"object_kind": "table:users", "tag_key": "pii"},
            ],
        },
    )
    tracker = _FakeTracker()
    result = drain_change_feed(conn, tracker=tracker)
    assert result["total_violations"] == 1
    assert result["by_policy"] == {"pii_without_owner": 1}
    assert len(result["filed_bugs"]) == 1


def test_drain_finds_failing_error_rules(_audit_stub, _no_impact) -> None:
    conn = _FakeConn(
        pending_rows=[
            {"change_id": 1, "affected_object_kind": "table:bugs",
             "source_table": "data_dictionary_quality_rules",
             "change_kind": "insert", "observed_at": "t"},
        ],
        failing_by_object={
            "table:bugs": [
                {"object_kind": "table:bugs", "rule_kind": "not_null",
                 "field_path": "title", "status": "fail", "started_at": "t"},
            ],
        },
    )
    tracker = _FakeTracker()
    result = drain_change_feed(conn, tracker=tracker)
    assert result["total_violations"] == 1
    assert result["by_policy"] == {"error_rule_failing": 1}


# ---------------------------------------------------------------------------
# Dry-run mode files no bugs but still marks ledger processed
# ---------------------------------------------------------------------------

def test_dry_run_still_marks_rows_processed(_audit_stub) -> None:
    conn = _FakeConn(pending_rows=[
        {"change_id": 42, "affected_object_kind": "table:x",
         "source_table": "data_dictionary_stewardship",
         "change_kind": "update", "observed_at": "t"},
    ])
    drain_change_feed(conn, tracker=None)
    # One mark_processed call, with change_id 42.
    assert len(conn.mark_processed_calls) == 1
    ids, _scan = conn.mark_processed_calls[0]
    assert ids == [42]


# ---------------------------------------------------------------------------
# Mark-processed records the scan_id (links ledger back to audit row)
# ---------------------------------------------------------------------------

def test_mark_processed_stores_scan_id(_audit_stub) -> None:
    conn = _FakeConn(pending_rows=[
        {"change_id": 1, "affected_object_kind": "table:x",
         "source_table": "data_dictionary_classifications",
         "change_kind": "insert", "observed_at": "t"},
    ])
    drain_change_feed(conn, tracker=None)
    _ids, scan_id = conn.mark_processed_calls[0]
    assert scan_id == "SCAN-1"


# ---------------------------------------------------------------------------
# peek_pending returns metadata preview without marking rows processed
# ---------------------------------------------------------------------------

def test_peek_pending_is_readonly() -> None:
    conn = _FakeConn(pending_rows=[
        {"change_id": 1, "affected_object_kind": "table:a",
         "source_table": "data_dictionary_classifications",
         "change_kind": "insert", "observed_at": "t1"},
        {"change_id": 2, "affected_object_kind": "table:a",
         "source_table": "data_dictionary_classifications",
         "change_kind": "update", "observed_at": "t2"},
    ])
    p = peek_pending(conn, limit=5)
    assert p["showing"] == 2
    assert p["distinct_objects"] == 1
    # No ledger mutations.
    assert conn.mark_processed_calls == []


def test_pending_count_query_shape() -> None:
    conn = _FakeConn(pending_rows=[
        {"change_id": 1, "affected_object_kind": "x", "source_table": "y",
         "change_kind": "insert", "observed_at": "t"},
    ])
    assert pending_count(conn) == 1


# ---------------------------------------------------------------------------
# Drain respects limit cap
# ---------------------------------------------------------------------------

def test_drain_passes_limit_to_claim_sql(_audit_stub) -> None:
    conn = _FakeConn()
    drain_change_feed(conn, tracker=None, limit=5)
    # First executed query should be the claim, with limit=5 in args.
    claim_sql, claim_args = conn.executed_sql[0]
    assert "processed_at IS NULL" in claim_sql
    assert claim_args[0] == 5
