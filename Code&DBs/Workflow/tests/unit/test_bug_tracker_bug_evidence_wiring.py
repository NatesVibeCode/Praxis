"""Wiring checks for runtime.bug_tracker -> runtime.bug_evidence."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest


_mod_path = Path(__file__).resolve().parents[2] / "runtime" / "bug_tracker.py"
_spec = importlib.util.spec_from_file_location("bug_tracker_wiring", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["bug_tracker_wiring"] = _mod
_spec.loader.exec_module(_mod)

Bug = _mod.Bug
BugCategory = _mod.BugCategory
BugSeverity = _mod.BugSeverity
BugStatus = _mod.BugStatus
BugTracker = _mod.BugTracker


def _sample_bug() -> Bug:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    return Bug(
        bug_id="BUG-TEST",
        bug_key="bug_test",
        title="Test bug",
        severity=BugSeverity.P1,
        status=BugStatus.OPEN,
        priority="P1",
        category=BugCategory.RUNTIME,
        description="Test bug description",
        summary="Test bug summary",
        filed_at=now,
        updated_at=now,
        resolved_at=None,
        created_at=now,
        filed_by="tester",
        assigned_to=None,
        tags=("failure_code:timeout_exceeded", "node_id:node-a"),
        source_kind="dispatch",
        discovered_in_run_id=None,
        discovered_in_receipt_id=None,
        owner_ref=None,
        source_issue_id=None,
        decision_ref="",
        resolution_summary=None,
    )


def test_build_failure_signature_delegates_to_bug_evidence(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_build_failure_signature(**kwargs):
        captured.update(kwargs)
        return {"fingerprint": "wired"}

    monkeypatch.setattr(_mod._bug_evidence, "build_failure_signature", _fake_build_failure_signature)

    result = _mod.build_failure_signature(
        failure_code="timeout_exceeded",
        job_label="job-a",
        node_id="node-a",
        failure_category="runtime_failed",
        agent="agent-a",
        provider_slug="openai",
        model_slug="gpt-5.4",
        source_kind="dispatch",
    )

    assert result == {"fingerprint": "wired"}
    assert captured["failure_code"] == "timeout_exceeded"
    assert captured["node_id"] == "node-a"
    assert captured["source_kind"] == "dispatch"


def test_bug_tracker_helpers_delegate_to_bug_evidence(monkeypatch) -> None:
    tracker = BugTracker(conn=object())
    bug = _sample_bug()

    monkeypatch.setattr(_mod._bug_evidence, "build_observability_gaps", lambda **kwargs: ("wired",))
    monkeypatch.setattr(_mod._bug_evidence, "compare_write_sets", lambda conn, latest_receipt: {"wire": True})

    gaps = tracker._build_observability_gaps(
        bug=bug,
        evidence_links=[],
        latest_receipt=None,
        fix_validation_count=0,
    )
    write_diff = tracker._compare_write_sets(None)

    assert gaps == ("wired",)
    assert write_diff == {"wire": True}


def test_compare_write_sets_surfaces_query_failure() -> None:
    class _BrokenConn:
        def fetchrow(self, *args, **kwargs):
            raise RuntimeError("receipt lane offline")

    result = _mod._bug_evidence.compare_write_sets(
        _BrokenConn(),
        {
            "receipt_id": "receipt-1",
            "workflow_id": "workflow-1",
            "node_id": "node-1",
            "write_paths": ["src/a.py"],
        },
    )

    assert result["error"] == {
        "scope": "write_set_diff",
        "reason_code": "write_set_diff.query_failed",
        "error_type": "RuntimeError",
        "error_message": "receipt lane offline",
    }
    assert result["note"] == "baseline receipt lookup failed"


def test_build_blast_radius_surfaces_query_failure() -> None:
    class _BrokenConn:
        def fetchrow(self, *args, **kwargs):
            raise RuntimeError("blast radius lane offline")

    result = _mod._bug_evidence.build_blast_radius(
        _BrokenConn(),
        failure_code="timeout_exceeded",
        node_id="node-a",
    )

    assert result["error"] == {
        "scope": "blast_radius",
        "reason_code": "blast_radius.query_failed",
        "error_type": "RuntimeError",
        "error_message": "blast radius lane offline",
    }
    assert result["occurrence_count"] == 0


def test_link_evidence_writes_through_bug_evidence_repository(monkeypatch) -> None:
    tracker = BugTracker(conn=object())
    bug = _sample_bug()
    captured: dict[str, object] = {}

    class _FakeRepository:
        def upsert_bug_evidence_link(self, **kwargs):
            captured.update(kwargs)
            return {
                "bug_evidence_link_id": "bug_evidence_link:test",
                "bug_id": kwargs["bug_id"],
                "evidence_kind": kwargs["evidence_kind"],
                "evidence_ref": kwargs["evidence_ref"],
                "evidence_role": kwargs["evidence_role"],
                "created_at": datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc),
                "created_by": kwargs["created_by"],
                "notes": kwargs["notes"],
            }

    monkeypatch.setattr(tracker, "get", lambda bug_id: bug if bug_id == bug.bug_id else None)
    monkeypatch.setattr(tracker, "_validate_evidence_reference", lambda **_kwargs: None)
    monkeypatch.setattr(_mod, "_bug_evidence_repository", lambda conn: _FakeRepository())

    result = tracker.link_evidence(
        bug.bug_id,
        evidence_kind="receipt",
        evidence_ref="receipt-123",
        evidence_role="observed_in",
        created_by="tester",
        notes="wired through repository",
    )

    assert result is not None
    assert captured["bug_id"] == bug.bug_id
    assert captured["evidence_kind"] == "receipt"
    assert captured["evidence_ref"] == "receipt-123"
    assert captured["evidence_role"] == "observed_in"
    assert captured["created_by"] == "tester"
    assert result["notes"] == "wired through repository"


def test_governance_scan_evidence_reference_validates_against_scan_table() -> None:
    class _Conn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def fetchval(self, query: str, *params: object) -> int:
            self.calls.append((query, params))
            return 1

    conn = _Conn()
    tracker = BugTracker(conn=conn)

    tracker._validate_evidence_reference(
        evidence_kind=_mod.EVIDENCE_KIND_GOVERNANCE_SCAN,
        evidence_ref="governance-scan-123",
    )

    assert len(conn.calls) == 1
    assert "FROM data_dictionary_governance_scans" in conn.calls[0][0]
    assert conn.calls[0][1] == ("governance-scan-123",)


def test_governance_scan_evidence_reference_rejects_unknown_scan() -> None:
    class _Conn:
        def fetchval(self, query: str, *params: object) -> int:
            return 0

    tracker = BugTracker(conn=_Conn())

    with pytest.raises(ValueError, match="unknown governance_scan reference"):
        tracker._validate_evidence_reference(
            evidence_kind=_mod.EVIDENCE_KIND_GOVERNANCE_SCAN,
            evidence_ref="missing-scan",
        )


def test_resolve_fixed_requires_passed_validates_fix_verification(monkeypatch) -> None:
    tracker = BugTracker(conn=object())
    bug = _sample_bug()

    monkeypatch.setattr(tracker, "get", lambda bug_id: bug if bug_id == bug.bug_id else None)
    monkeypatch.setattr(
        tracker,
        "_passed_validates_fix_evidence_with_error",
        lambda bug_id: ([], None),
    )

    with pytest.raises(ValueError, match="passed validates_fix verification evidence"):
        tracker.resolve(bug.bug_id, BugStatus.FIXED)


def test_resolve_fixed_surfaces_validates_fix_query_failure(monkeypatch) -> None:
    tracker = BugTracker(conn=object())
    bug = _sample_bug()

    monkeypatch.setattr(tracker, "get", lambda bug_id: bug if bug_id == bug.bug_id else None)
    monkeypatch.setattr(
        tracker,
        "_passed_validates_fix_evidence_with_error",
        lambda bug_id: ([], "bug_evidence_links.query_failed:RuntimeError: proof lane offline"),
    )

    with pytest.raises(
        ValueError,
        match="could not load validates_fix evidence.*proof lane offline",
    ):
        tracker.resolve(bug.bug_id, BugStatus.FIXED)


def test_resolve_fixed_updates_bug_when_passed_verification_exists(monkeypatch) -> None:
    class _RecordingConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *params: object):
            self.calls.append((query, params))
            return [{"bug_id": "BUG-TEST"}]

    conn = _RecordingConn()
    tracker = BugTracker(conn=conn)
    bug = _sample_bug()
    resolved_at = datetime(2026, 4, 16, 18, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(tracker, "get", lambda bug_id: bug if bug_id == bug.bug_id else None)
    monkeypatch.setattr(
        tracker,
        "_passed_validates_fix_evidence_with_error",
        lambda bug_id: ([{"evidence_ref": "verification-run-1"}], None),
    )
    monkeypatch.setattr(tracker, "_now", lambda: resolved_at)
    monkeypatch.setattr(
        tracker,
        "_row_to_bug",
        lambda row: replace(
            bug,
            status=BugStatus.FIXED,
            resolved_at=resolved_at,
            updated_at=resolved_at,
        ),
    )

    resolved = tracker.resolve(bug.bug_id, BugStatus.FIXED)

    assert resolved is not None
    assert resolved.status == BugStatus.FIXED
    assert resolved.resolved_at == resolved_at
    assert conn.calls[0][1][0] == "FIXED"
    assert conn.calls[0][1][3] == bug.bug_id
