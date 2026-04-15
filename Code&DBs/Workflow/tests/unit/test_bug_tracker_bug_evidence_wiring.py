"""Wiring checks for runtime.bug_tracker -> runtime.bug_evidence."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path


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
