"""Focused regression for semantic-neighbor lookup failure handling."""

from __future__ import annotations

from datetime import datetime, timezone

from runtime.bug_tracker import Bug, BugCategory, BugSeverity, BugStatus, BugTracker


def _sample_bug() -> Bug:
    return Bug(
        bug_id="BUG-SEMANTIC",
        bug_key="bug_semantic",
        title="Broken semantic lookup",
        severity=BugSeverity.P2,
        status=BugStatus.OPEN,
        priority="P2",
        category=BugCategory.RUNTIME,
        description="Semantic lookup should report query failures explicitly.",
        summary="Semantic lookup should report query failures explicitly.",
        filed_at=datetime(2026, 4, 16, 5, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 16, 5, 0, tzinfo=timezone.utc),
        resolved_at=None,
        created_at=datetime(2026, 4, 16, 5, 0, tzinfo=timezone.utc),
        filed_by="qa",
        assigned_to=None,
        tags=("cluster:test", "area:leases"),
        source_kind="test",
        discovered_in_run_id=None,
        discovered_in_receipt_id=None,
        owner_ref=None,
        source_issue_id=None,
        decision_ref="",
        resolution_summary=None,
    )


def test_failure_packet_surfaces_semantic_lookup_failures(monkeypatch) -> None:
    bug = _sample_bug()
    tracker = BugTracker(conn=object())

    class _BrokenVectorStore:
        def search_vector(self, *args, **kwargs):
            raise RuntimeError("vector lane offline")

        def prepare(self, *args, **kwargs):
            raise RuntimeError("text lane offline")

    class _BrokenConn:
        def fetchrow(self, *args, **kwargs):
            raise RuntimeError("embedding lane offline")

        def execute(self, *args, **kwargs):
            raise RuntimeError("tag lane offline")

    monkeypatch.setattr(tracker, "get", lambda bug_id: bug if bug_id == bug.bug_id else None)
    monkeypatch.setattr(tracker, "list_evidence", lambda bug_id: [])
    monkeypatch.setattr(
        tracker,
        "backfill_replay_provenance",
        lambda bug_id: {
            "bug_id": bug_id,
            "linked_refs": (),
            "linked_count": 0,
            "reason_code": "bug.replay_backfill.none",
        },
    )
    monkeypatch.setattr(tracker, "_find_signature_receipts", lambda **_kwargs: ([], None))
    monkeypatch.setattr(tracker, "_load_verification_rows", lambda *args, **kwargs: ({}, None))
    monkeypatch.setattr(
        tracker,
        "_build_historical_fixes",
        lambda **kwargs: {"count": 0, "items": (), "reason_code": "bug.historical_fixes.none", "errors": ()},
    )
    monkeypatch.setattr(
        tracker,
        "_compare_write_sets",
        lambda latest_receipt: {
            "baseline_receipt_id": None,
            "added_paths": (),
            "removed_paths": (),
            "unchanged_paths": (),
            "current_write_count": 0,
            "baseline_write_count": 0,
            "note": "no receipt evidence available",
            "error": None,
        },
    )
    monkeypatch.setattr(
        tracker,
        "_build_blast_radius",
        lambda *, failure_code, node_id: {
            "window": "7 days",
            "occurrence_count": 0,
            "distinct_runs": 0,
            "distinct_workflows": 0,
            "distinct_nodes": 0,
            "distinct_requests": 0,
            "distinct_agents": 0,
            "error": None,
        },
    )
    monkeypatch.setattr(
        tracker,
        "_replay_action",
        lambda **_kwargs: {
            "available": False,
            "automatic": False,
            "reason_code": "bug.replay_not_ready",
            "tool": "praxis_bugs",
            "arguments": {"action": "replay", "bug_id": bug.bug_id},
        },
    )
    monkeypatch.setattr(tracker, "_vector_store", _BrokenVectorStore())
    monkeypatch.setattr(tracker, "_conn", _BrokenConn())

    packet = tracker.failure_packet(bug.bug_id)
    assert packet is not None
    assert packet["observability_state"] == "degraded"
    assert "semantic_neighbors.query_failed:embedding.query_failed:RuntimeError: embedding lane offline" in packet["errors"]

    semantic = packet["semantic_neighbors"]
    assert semantic["reason_code"] == "bug.semantic_neighbors.query_failed"
    assert "embedding.query_failed:RuntimeError: embedding lane offline" in semantic["errors"]
    assert "tags.query_failed:RuntimeError: tag lane offline" in semantic["errors"]


def test_failure_packet_surfaces_helper_query_failures(monkeypatch) -> None:
    bug = _sample_bug()
    tracker = BugTracker(conn=object())

    monkeypatch.setattr(tracker, "get", lambda bug_id: bug if bug_id == bug.bug_id else None)
    monkeypatch.setattr(tracker, "list_evidence", lambda bug_id: [])
    monkeypatch.setattr(
        tracker,
        "backfill_replay_provenance",
        lambda bug_id: {
            "bug_id": bug_id,
            "linked_refs": (),
            "linked_count": 0,
            "reason_code": "bug.replay_backfill.none",
        },
    )
    monkeypatch.setattr(tracker, "_find_signature_receipts", lambda **_kwargs: ([], None))
    monkeypatch.setattr(tracker, "_load_verification_rows", lambda *args, **kwargs: ({}, None))
    monkeypatch.setattr(
        tracker,
        "_build_historical_fixes",
        lambda **kwargs: {"count": 0, "items": (), "reason_code": "bug.historical_fixes.none", "errors": ()},
    )
    monkeypatch.setattr(
        tracker,
        "_compare_write_sets",
        lambda latest_receipt: {
            "baseline_receipt_id": None,
            "added_paths": (),
            "removed_paths": (),
            "unchanged_paths": (),
            "current_write_count": 0,
            "baseline_write_count": 0,
            "note": "baseline receipt lookup failed",
            "error": "RuntimeError: receipt lane offline",
        },
    )
    monkeypatch.setattr(
        tracker,
        "_build_blast_radius",
        lambda *, failure_code, node_id: {
            "window": "7 days",
            "occurrence_count": 0,
            "distinct_runs": 0,
            "distinct_workflows": 0,
            "distinct_nodes": 0,
            "distinct_requests": 0,
            "distinct_agents": 0,
            "error": "RuntimeError: blast radius lane offline",
        },
    )
    monkeypatch.setattr(
        tracker,
        "_semantic_neighbor_bundle",
        lambda bug: {
            "reason_code": "bug.semantic_neighbors.none",
            "items": (),
            "note": "No semantic neighbors available.",
            "sources_tried": (),
            "errors": (),
        },
    )
    monkeypatch.setattr(
        tracker,
        "_replay_action",
        lambda **_kwargs: {
            "available": False,
            "automatic": False,
            "reason_code": "bug.replay_not_ready",
            "tool": "praxis_bugs",
            "arguments": {"action": "replay", "bug_id": bug.bug_id},
        },
    )

    packet = tracker.failure_packet(bug.bug_id)
    assert packet is not None
    assert packet["observability_state"] == "degraded"
    assert "write_set_diff.query_failed:RuntimeError: receipt lane offline" in packet["errors"]
    assert "blast_radius.query_failed:RuntimeError: blast radius lane offline" in packet["errors"]
