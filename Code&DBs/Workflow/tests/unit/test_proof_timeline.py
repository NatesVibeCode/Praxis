from __future__ import annotations

from datetime import datetime, timezone

from runtime.proof_timeline import (
    PROOF_TIMELINE_AUTHORITY,
    bug_proof_timeline,
    historical_fix_evidence,
    passed_validates_fix_entries,
)


def test_bug_proof_timeline_merges_bug_links_with_discovery_provenance() -> None:
    now = datetime(2026, 4, 25, 1, 30, tzinfo=timezone.utc)

    def _query_rows(query: str, *params: object):
        assert "FROM bug_evidence_links AS bel" in query
        assert params == ("BUG-PROOF",)
        return [
            {
                "subject_kind": "bug",
                "subject_ref": "BUG-PROOF",
                "proof_kind": "verification_run",
                "proof_ref": "verification_run:passed",
                "evidence_role": "validates_fix",
                "source_table": "bug_evidence_links",
                "source_ref": "bug_evidence_link:1",
                "source_created_by": "tester",
                "notes": "proves the fix",
                "recorded_at": now,
                "proof_status": "passed",
                "proof_recorded_at": now,
                "verification_run_id": "verification_run:passed",
                "verifier_ref": "verifier.job.python.pytest_file",
                "target_kind": "path",
                "target_ref": "tests/unit/test_proof_timeline.py",
                "decision_ref": "decision.test",
                "duration_ms": 42,
                "inputs": {"path": "tests/unit/test_proof_timeline.py"},
                "outputs": {"ok": True},
                "artifacts": {},
                "decision_refs": [],
            },
            {
                "subject_kind": "bug",
                "subject_ref": "BUG-PROOF",
                "proof_kind": "receipt",
                "proof_ref": "receipt:observed",
                "evidence_role": "observed_in",
                "source_table": "bugs",
                "source_ref": "BUG-PROOF",
                "source_created_by": "tester",
                "recorded_at": now,
                "proof_status": "failed",
                "proof_recorded_at": now,
                "run_id": "run:observed",
                "receipt_id": "receipt:observed",
                "workflow_id": "workflow:test",
                "request_id": "request:test",
                "inputs": {},
                "outputs": {},
                "artifacts": {},
                "decision_refs": ["decision.receipt"],
            },
        ], None

    timeline, error = bug_proof_timeline(
        bug_id="BUG-PROOF",
        query_rows_fn=_query_rows,
    )

    assert error is None
    assert [entry["proof_ref"] for entry in timeline] == [
        "receipt:observed",
        "verification_run:passed",
    ]
    assert timeline[1]["authority"] == PROOF_TIMELINE_AUTHORITY
    assert timeline[1]["proof_passed"] is True
    assert timeline[1]["metadata"]["inputs"] == {
        "path": "tests/unit/test_proof_timeline.py"
    }
    assert timeline[0]["run_id"] == "run:observed"
    assert timeline[0]["metadata"]["decision_refs"] == ["decision.receipt"]


def test_passed_validates_fix_entries_filters_to_passed_verification_runs() -> None:
    entries = [
        {
            "proof_kind": "verification_run",
            "proof_ref": "verification_run:passed",
            "evidence_role": "validates_fix",
            "proof_passed": True,
        },
        {
            "proof_kind": "verification_run",
            "proof_ref": "verification_run:failed",
            "evidence_role": "validates_fix",
            "proof_passed": False,
        },
        {
            "proof_kind": "receipt",
            "proof_ref": "receipt:passed-looking",
            "evidence_role": "validates_fix",
            "proof_passed": True,
        },
    ]

    assert passed_validates_fix_entries(entries) == [entries[0]]


def test_historical_fix_evidence_uses_the_shared_timeline_projection() -> None:
    now = datetime(2026, 4, 25, 1, 45, tzinfo=timezone.utc)

    def _query_rows(query: str, *params: object):
        del query
        assert params == ("BUG-FIXED",)
        return [
            {
                "subject_kind": "bug",
                "subject_ref": "BUG-FIXED",
                "proof_kind": "verification_run",
                "proof_ref": "verification_run:passed",
                "evidence_role": "validates_fix",
                "source_table": "bug_evidence_links",
                "source_ref": "bug_evidence_link:1",
                "recorded_at": now,
                "proof_status": "passed",
                "proof_recorded_at": now,
                "verification_run_id": "verification_run:passed",
                "verifier_ref": "verifier.job.python.pytest_file",
                "target_kind": "path",
                "target_ref": "tests/unit/test_proof_timeline.py",
                "decision_ref": "decision.test",
                "duration_ms": 42,
                "inputs": {},
                "outputs": {},
                "artifacts": {},
                "decision_refs": [],
            },
            {
                "subject_kind": "bug",
                "subject_ref": "BUG-FIXED",
                "proof_kind": "healing_run",
                "proof_ref": "healing_run:attempt",
                "evidence_role": "attempted_fix",
                "source_table": "bug_evidence_links",
                "source_ref": "bug_evidence_link:2",
                "recorded_at": now,
                "proof_status": "succeeded",
                "proof_recorded_at": now,
                "healing_run_id": "healing_run:attempt",
                "healer_ref": "healer.test",
                "verifier_ref": "verifier.test",
                "target_kind": "path",
                "target_ref": "runtime/proof_timeline.py",
                "decision_ref": "decision.heal",
                "duration_ms": 12,
                "inputs": {},
                "outputs": {},
                "artifacts": {},
                "decision_refs": [],
            },
        ], None

    summary = historical_fix_evidence(
        bug_id="BUG-FIXED",
        query_rows_fn=_query_rows,
    )

    assert summary["fix_verified"] is True
    assert summary["linked_validation_count"] == 1
    assert summary["verified_validation_count"] == 1
    assert summary["last_validation"]["verification_run_id"] == "verification_run:passed"
    assert summary["attempted_fix_count"] == 1
    assert summary["last_attempted_fix"]["healing_run_id"] == "healing_run:attempt"
    assert summary["errors"] == ()


def test_bug_proof_timeline_surfaces_query_failures() -> None:
    def _query_rows(_query: str, *_params: object):
        return [], "RuntimeError: proof tables unavailable"

    timeline, error = bug_proof_timeline(
        bug_id="BUG-BROKEN",
        query_rows_fn=_query_rows,
    )

    assert timeline == []
    assert error == "proof_timeline.query_failed:RuntimeError: proof tables unavailable"
