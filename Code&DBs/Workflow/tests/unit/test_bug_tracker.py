"""Tests for runtime.bug_tracker."""

import importlib.util
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
from observability.read_models import (
    ProjectionCompleteness,
    ProjectionWatermark,
    ReplayReadModel,
)

# Direct import to avoid runtime/__init__.py pulling unrelated modules.
_mod_path = Path(__file__).resolve().parents[2] / "runtime" / "bug_tracker.py"
_spec = importlib.util.spec_from_file_location("bug_tracker", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["bug_tracker"] = _mod
_spec.loader.exec_module(_mod)

Bug = _mod.Bug
BugCategory = _mod.BugCategory
BugSeverity = _mod.BugSeverity
BugStats = _mod.BugStats
BugStatus = _mod.BugStatus
BugTracker = _mod.BugTracker


import uuid as _uuid

_TEST_PREFIX = _uuid.uuid4().hex[:8]


@pytest.fixture
def tracker():
    from _pg_test_conn import get_isolated_conn

    conn = get_isolated_conn()
    try:
        yield BugTracker(conn)
    finally:
        conn.close()


@pytest.fixture
def sample_bug(tracker: BugTracker) -> Bug:
    pfx = _uuid.uuid4().hex[:8]
    bug, _ = tracker.file_bug(
        title=f"Widget crashes on load [{pfx}]",
        severity=BugSeverity.P1,
        category=BugCategory.RUNTIME,
        description=f"The widget throws a TypeError when loading with empty config. [{pfx}]",
        filed_by="alice",
        tags=("ui", "crash"),
    )
    return bug


# ── file / get lifecycle ───────────────────────────────────────────────


class TestFileAndGet:
    def test_file_returns_bug(self, sample_bug: Bug):
        assert sample_bug.bug_id.startswith("BUG-")
        assert sample_bug.bug_key.startswith("bug_")
        assert "Widget crashes on load" in sample_bug.title
        assert sample_bug.severity == BugSeverity.P1
        assert sample_bug.status == BugStatus.OPEN
        assert sample_bug.priority == "P1"
        assert sample_bug.category == BugCategory.RUNTIME
        assert sample_bug.summary.startswith("The widget throws")
        assert sample_bug.filed_by == "alice"
        assert sample_bug.tags == ("ui", "crash")
        assert sample_bug.resolved_at is None
        assert sample_bug.assigned_to is None
        assert isinstance(sample_bug.created_at, datetime)

    def test_file_preserves_provenance_metadata(self, tracker: BugTracker):
        bug, similar_bugs = tracker.file_bug(
            title="Metadata rich bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="The full context should survive the file path.",
            filed_by="workflow_api",
            source_kind="manual",
            decision_ref="decision:bugs:metadata",
            owner_ref="owner-789",
            tags=("api", "metadata"),
        )
        assert similar_bugs == []
        assert bug.source_kind == "manual"
        assert bug.decision_ref == "decision:bugs:metadata"
        assert bug.owner_ref == "owner-789"
        assert bug.tags == ("api", "metadata")

    def test_link_evidence_rejects_unknown_role(self, tracker: BugTracker, sample_bug: Bug):
        with pytest.raises(ValueError, match="evidence_role"):
            tracker.link_evidence(
                sample_bug.bug_id,
                evidence_kind="receipt",
                evidence_ref="receipt-123",
                evidence_role="typo_role",
            )

    def test_get_round_trip(self, tracker: BugTracker, sample_bug: Bug):
        fetched = tracker.get(sample_bug.bug_id)
        assert fetched is not None
        assert fetched.bug_id == sample_bug.bug_id
        assert fetched.title == sample_bug.title

    def test_get_missing_returns_none(self, tracker: BugTracker):
        assert tracker.get("BUG-NONEXISTENT") is None


# ── status transitions ─────────────────────────────────────────────────


class TestStatusTransitions:
    def test_update_status(self, tracker: BugTracker, sample_bug: Bug):
        updated = tracker.update_status(sample_bug.bug_id, BugStatus.IN_PROGRESS)
        assert updated is not None
        assert updated.status == BugStatus.IN_PROGRESS

    def test_update_missing_returns_none(self, tracker: BugTracker):
        assert tracker.update_status("BUG-NOPE", BugStatus.OPEN) is None

    def test_assign(self, tracker: BugTracker, sample_bug: Bug):
        assigned = tracker.assign(sample_bug.bug_id, "bob")
        assert assigned is not None
        assert assigned.assigned_to == "bob"


# ── resolve ────────────────────────────────────────────────────────────


class TestResolve:
    def test_resolve_sets_resolved_at(self, tracker: BugTracker, sample_bug: Bug):
        resolved = tracker.resolve(sample_bug.bug_id, BugStatus.FIXED)
        assert resolved is not None
        assert resolved.status == BugStatus.FIXED
        assert resolved.resolved_at is not None
        assert isinstance(resolved.resolved_at, datetime)

    def test_resolve_wont_fix(self, tracker: BugTracker, sample_bug: Bug):
        resolved = tracker.resolve(sample_bug.bug_id, BugStatus.WONT_FIX)
        assert resolved is not None
        assert resolved.status == BugStatus.WONT_FIX
        assert resolved.resolved_at is not None

    def test_resolve_deferred(self, tracker: BugTracker, sample_bug: Bug):
        resolved = tracker.resolve(sample_bug.bug_id, BugStatus.DEFERRED)
        assert resolved is not None
        assert resolved.status == BugStatus.DEFERRED

    def test_resolve_rejects_invalid_status(self, tracker: BugTracker, sample_bug: Bug):
        with pytest.raises(ValueError, match="resolve"):
            tracker.resolve(sample_bug.bug_id, BugStatus.OPEN)

        with pytest.raises(ValueError, match="resolve"):
            tracker.resolve(sample_bug.bug_id, BugStatus.IN_PROGRESS)


# ── list with filters ──────────────────────────────────────────────────


class TestListBugs:
    def _seed(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        tracker.file_bug(f"Bug A [{pfx}]", BugSeverity.P0, BugCategory.SCOPE, "desc a", "alice")
        tracker.file_bug(f"Bug B [{pfx}]", BugSeverity.P1, BugCategory.RUNTIME, "desc b", "bob")
        b3, _ = tracker.file_bug(f"Bug C [{pfx}]", BugSeverity.P2, BugCategory.TEST, "desc c", "carol")
        tracker.resolve(b3.bug_id, BugStatus.FIXED)
        return pfx

    def test_list_all(self, tracker: BugTracker):
        before = len(tracker.list_bugs(limit=10000))
        self._seed(tracker)
        after = len(tracker.list_bugs(limit=10000))
        assert after - before == 3

    def test_filter_by_status(self, tracker: BugTracker):
        open_before = len(tracker.list_bugs(status=BugStatus.OPEN, limit=10000))
        fixed_before = len(tracker.list_bugs(status=BugStatus.FIXED, limit=10000))
        self._seed(tracker)
        open_after = len(tracker.list_bugs(status=BugStatus.OPEN, limit=10000))
        fixed_after = len(tracker.list_bugs(status=BugStatus.FIXED, limit=10000))
        assert open_after - open_before == 2
        assert fixed_after - fixed_before == 1

    def test_filter_by_severity(self, tracker: BugTracker):
        p0_before = len(tracker.list_bugs(severity=BugSeverity.P0, limit=10000))
        pfx = self._seed(tracker)
        p0_after = tracker.list_bugs(severity=BugSeverity.P0, limit=10000)
        assert len(p0_after) - p0_before == 1
        assert any(f"Bug A [{pfx}]" in b.title for b in p0_after)

    def test_filter_by_category(self, tracker: BugTracker):
        rt_before = len(tracker.list_bugs(category=BugCategory.RUNTIME, limit=10000))
        pfx = self._seed(tracker)
        rt_after = tracker.list_bugs(category=BugCategory.RUNTIME, limit=10000)
        assert len(rt_after) - rt_before == 1
        assert any(f"Bug B [{pfx}]" in b.title for b in rt_after)

    def test_filter_open_only(self, tracker: BugTracker):
        before = len(tracker.list_bugs(open_only=True, limit=10000))
        self._seed(tracker)
        after = len(tracker.list_bugs(open_only=True, limit=10000))
        assert after - before == 2

    def test_legacy_status_semantics_are_normalized(self, tracker: BugTracker):
        legacy_open, _ = tracker.file_bug(
            "Legacy open casing",
            BugSeverity.P1,
            BugCategory.RUNTIME,
            "Lowercase open should still behave as open",
            "dave",
        )
        legacy_resolved, _ = tracker.file_bug(
            "Legacy resolved alias",
            BugSeverity.P1,
            BugCategory.RUNTIME,
            "Resolved alias should still behave as fixed",
            "erin",
        )
        legacy_other_category, _ = tracker.file_bug(
            "Legacy category casing",
            BugSeverity.P2,
            BugCategory.RUNTIME,
            "Lowercase category should still normalize",
            "frank",
        )
        tracker._conn.execute("UPDATE bugs SET status = $1 WHERE bug_id = $2", "open", legacy_open.bug_id)
        tracker._conn.execute(
            "UPDATE bugs SET status = $1, category = $2 WHERE bug_id = $3",
            "resolved",
            "other",
            legacy_resolved.bug_id,
        )
        tracker._conn.execute("UPDATE bugs SET category = $1 WHERE bug_id = $2", "other", legacy_other_category.bug_id)

        open_rows = tracker.list_bugs(status=BugStatus.OPEN, limit=10000)
        fixed_rows = tracker.list_bugs(status=BugStatus.FIXED, limit=10000)
        open_only = tracker.list_bugs(open_only=True, limit=10000)

        assert any(bug.bug_id == legacy_open.bug_id for bug in open_rows)
        assert any(bug.bug_id == legacy_open.bug_id for bug in open_only)
        assert any(bug.bug_id == legacy_resolved.bug_id for bug in fixed_rows)
        assert all(bug.bug_id != legacy_resolved.bug_id for bug in open_only)

        legacy_other = tracker.get(legacy_other_category.bug_id)
        assert legacy_other is not None
        assert legacy_other.category == BugCategory.OTHER

    def test_filter_by_legacy_severity_alias(self, tracker: BugTracker):
        _, _ = tracker.file_bug(
            "Legacy severity alias",
            BugSeverity.P2,
            BugCategory.SCOPE,
            "This row uses legacy alias severity values in persistence.",
            "ivy",
        )
        bug_row = tracker._conn.fetchrow(
            "SELECT bug_id FROM bugs WHERE title = $1", "Legacy severity alias"
        )
        assert bug_row is not None
        bug_id = bug_row["bug_id"]

        tracker._conn.execute(
            "UPDATE bugs SET severity = $1, priority = $2 WHERE bug_id = $3",
            "medium",
            "p2",
            bug_id,
        )

        p2_bugs = tracker.list_bugs(severity=BugSeverity.P2, limit=10000)
        assert any(bug.bug_id == bug_id for bug in p2_bugs)

    def test_filter_by_title_like(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        tracker.file_bug(
            f"Legacy shim cleanup [{pfx}]",
            BugSeverity.P2,
            BugCategory.WIRING,
            "Legacy shim should be removable.",
            "dave",
        )
        found = tracker.list_bugs(title_like="Legacy shim", limit=10000)
        assert any("Legacy shim cleanup" in bug.title for bug in found)

    def test_filter_tags_and_exclude_tags(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        legacy, _ = tracker.file_bug(
            f"Legacy shim bug [{pfx}]",
            BugSeverity.P3,
            BugCategory.OTHER,
            "legacy shim shim logic",
            "dave",
            tags=("legacy_shim",),
        )
        stable, _ = tracker.file_bug(
            f"Stable task [{pfx}]",
            BugSeverity.P2,
            BugCategory.RUNTIME,
            "healthy baseline path",
            "erin",
            tags=("stable",),
        )

        by_tag = tracker.list_bugs(tags=("legacy_shim",), limit=10000)
        assert any(item.bug_id == legacy.bug_id for item in by_tag)
        without_legacy = tracker.list_bugs(exclude_tags=("legacy_shim",), limit=10000)
        assert any(item.bug_id == stable.bug_id for item in without_legacy)
        assert all("legacy_shim" not in item.tags for item in without_legacy)

    def test_limit(self, tracker: BugTracker):
        self._seed(tracker)
        bugs = tracker.list_bugs(limit=2)
        assert len(bugs) == 2


# ── FTS search ─────────────────────────────────────────────────────────


class TestSearch:
    def test_search_by_title(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        tracker.file_bug(f"Memory leak in parser [{pfx}]", BugSeverity.P1, BugCategory.RUNTIME, "OOM after 1h", "alice")
        tracker.file_bug(f"Typo in docs [{pfx}]", BugSeverity.P3, BugCategory.OTHER, "Wrong spelling", "bob")

        results = tracker.search(pfx)
        titles = [r.title for r in results]
        assert any("Memory leak" in t for t in titles)
        assert any("Typo" in t for t in titles)

    def test_search_by_description(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        tracker.file_bug(f"Crash [{pfx}]", BugSeverity.P0, BugCategory.RUNTIME, f"segfault in allocator {pfx}", "alice")
        results = tracker.search(pfx)
        assert len(results) >= 1
        assert any("Crash" in r.title for r in results)

    def test_search_no_results(self, tracker: BugTracker):
        results = tracker.search("nonexistent_term_xyz_99999")
        assert len(results) == 0


# ── stats ──────────────────────────────────────────────────────────────


class TestStats:
    def test_stats_counts(self, tracker: BugTracker):
        st_before = tracker.stats()
        p0_before = st_before.by_severity.get("P0", 0)
        p1_before = st_before.by_severity.get("P1", 0)
        scope_before = st_before.by_category.get("SCOPE", 0)
        open_before = st_before.open_count
        total_before = st_before.total

        tracker.file_bug("A", BugSeverity.P0, BugCategory.SCOPE, "d", "a")
        tracker.file_bug("B", BugSeverity.P1, BugCategory.RUNTIME, "d", "b")
        b3, _ = tracker.file_bug("C", BugSeverity.P0, BugCategory.SCOPE, "d", "c")
        tracker.update_status(b3.bug_id, BugStatus.IN_PROGRESS)

        st = tracker.stats()
        assert isinstance(st, BugStats)
        assert st.total - total_before == 3
        assert st.by_severity.get("P0", 0) - p0_before == 2
        assert st.by_severity.get("P1", 0) - p1_before == 1
        assert st.by_category.get("SCOPE", 0) - scope_before == 2
        assert st.open_count - open_before == 3  # 2 OPEN + 1 IN_PROGRESS

    def test_stats_shape(self, tracker: BugTracker):
        st = tracker.stats()
        assert isinstance(st, BugStats)
        assert isinstance(st.total, int)
        assert isinstance(st.open_count, int)
        assert st.observability_state in {"complete", "degraded"}

    def test_stats_degrade_instead_of_zeroing_on_query_failure(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        original = tracker._query_scalar_with_error

        def _query_scalar_with_error(query: str, *params: object):
            if "bug_evidence_links AS bel" in query or "JOIN verification_runs" in query:
                return None, "RuntimeError: forced stats failure"
            return original(query, *params)

        monkeypatch.setattr(tracker, "_query_scalar_with_error", _query_scalar_with_error)

        stats = tracker.stats()
        assert stats.observability_state == "degraded"
        assert stats.packet_ready_count is None
        assert stats.replay_ready_count is None
        assert stats.replay_blocked_count is None
        assert stats.fix_verified_count is None
        assert stats.underlinked_count is None
        assert any("query_failed" in error for error in stats.errors)


# ── MTTR ───────────────────────────────────────────────────────────────


class TestMTTR:
    def test_mttr_calculated(self, tracker: BugTracker):
        b1, _ = tracker.file_bug("MTTR-A", BugSeverity.P1, BugCategory.RUNTIME, "d", "a")
        b2, _ = tracker.file_bug("MTTR-B", BugSeverity.P2, BugCategory.TEST, "d", "b")

        # Resolve both immediately -- MTTR should be very small but not None
        tracker.resolve(b1.bug_id, BugStatus.FIXED)
        tracker.resolve(b2.bug_id, BugStatus.WONT_FIX)

        st = tracker.stats()
        # With production data there may already be resolved bugs
        assert st.mttr_hours is not None
        assert st.mttr_hours >= 0.0

    def test_mttr_only_resolved(self, tracker: BugTracker):
        tracker.file_bug("MTTR-Open", BugSeverity.P1, BugCategory.RUNTIME, "d", "a")
        b2, _ = tracker.file_bug("MTTR-Fixed", BugSeverity.P2, BugCategory.TEST, "d", "b")
        tracker.resolve(b2.bug_id, BugStatus.FIXED)

        st = tracker.stats()
        assert st.mttr_hours is not None
        assert st.mttr_hours >= 0.0


class TestEvidencePackets:
    def test_failure_packet_builds_replay_context_and_write_diff(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        bug, _ = tracker.file_bug(
            title="Packet rich bug",
            severity=BugSeverity.P1,
            category=BugCategory.RUNTIME,
            description="Need a replayable failure packet.",
            filed_by="alice",
            decision_ref="decision.packet.bug",
            tags=("failure_code:timeout_exceeded", "job_label:job-a"),
        )
        tracker._conn.execute(
            """
            INSERT INTO verification_runs (
                verification_run_id,
                verifier_ref,
                target_kind,
                target_ref,
                status,
                inputs,
                outputs,
                decision_ref,
                attempted_at,
                duration_ms
            ) VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, '{}'::jsonb, $6, $7, $8)
            """,
            "verification-run-1",
            "verifier.platform.schema_authority",
            "run",
            "run-failed",
            "passed",
            "decision.alpha",
            datetime(2026, 4, 10, 10, 5, tzinfo=timezone.utc),
            50,
        )
        monkeypatch.setattr(tracker, "_validate_evidence_reference", lambda **_kwargs: None)
        tracker.link_evidence(
            bug.bug_id,
            evidence_kind="receipt",
            evidence_ref="receipt-failed",
            evidence_role="observed_in",
            created_by="test",
        )
        tracker.link_evidence(
            bug.bug_id,
            evidence_kind="run",
            evidence_ref="run-failed",
            evidence_role="observed_in",
            created_by="test",
        )
        tracker.link_evidence(
            bug.bug_id,
            evidence_kind="verification_run",
            evidence_ref="verification-run-1",
            evidence_role="validates_fix",
            created_by="test",
        )

        failed_row = {
            "receipt_id": "receipt-failed",
            "workflow_id": "workflow.alpha",
            "run_id": "run-failed",
            "request_id": "request.alpha",
            "node_id": "job-a",
            "attempt_no": 1,
            "started_at": datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 4, 10, 10, 1, tzinfo=timezone.utc),
            "executor_type": "openai/gpt-5.4",
            "status": "failed",
            "inputs": {
                "agent_slug": "openai/gpt-5.4",
                "workspace_ref": "workspace.alpha",
                "runtime_profile_ref": "runtime_profile.alpha",
            },
            "outputs": {
                "author_model": "openai/gpt-5.4",
                "duration_ms": 1200,
                "verification_status": "failed",
                "failure_classification": {"category": "runtime_failed"},
                "git_provenance": {
                    "available": True,
                    "repo_snapshot_ref": "repo_snapshot.alpha",
                },
                "write_manifest": {
                    "results": [{"file_path": "src/new.py"}],
                },
                "verified_paths": ["src/new.py"],
            },
            "artifacts": {},
            "failure_code": "timeout_exceeded",
            "decision_refs": [{"decision_id": "decision.alpha"}],
        }
        baseline_row = {
            **failed_row,
            "receipt_id": "receipt-success",
            "status": "succeeded",
            "failure_code": "",
            "outputs": {
                **failed_row["outputs"],
                "verification_status": "passed",
                "write_manifest": {
                    "results": [{"file_path": "src/old.py"}],
                },
                "verified_paths": ["src/old.py"],
            },
        }
        original_query_rows_with_error = tracker._query_rows_with_error
        original_fetchrow = tracker._conn.fetchrow

        def _query_rows_with_error(query: str, *params: object):
            if "FROM receipts" in query:
                return [failed_row], None
            return original_query_rows_with_error(query, *params)

        def _fetchrow(query: str, *params: object):
            normalized = " ".join(query.split())
            if "COUNT(*) AS occurrence_count" in normalized:
                return {
                    "occurrence_count": 4,
                    "distinct_runs": 3,
                    "distinct_workflows": 2,
                    "distinct_nodes": 1,
                    "distinct_requests": 3,
                    "distinct_agents": 1,
                }
            if "status = 'succeeded'" in normalized:
                return baseline_row
            return original_fetchrow(query, *params)

        monkeypatch.setattr(tracker, "_query_rows_with_error", _query_rows_with_error)
        monkeypatch.setattr(tracker._conn, "fetchrow", _fetchrow)
        monkeypatch.setattr(
            tracker,
            "_load_verification_rows",
            lambda table_name, id_field, refs: ({
                "verification-run-1": {
                    "verification_run_id": "verification-run-1",
                    "verifier_ref": "verifier.alpha",
                    "status": "passed",
                    "attempted_at": datetime(2026, 4, 10, 10, 5, tzinfo=timezone.utc),
                    "target_kind": "run",
                    "target_ref": "run-failed",
                    "inputs": {},
                    "outputs": {},
                    "decision_ref": "decision.alpha",
                    "duration_ms": 50,
                }
            } if table_name == "verification_runs" else {}, None),
        )

        packet = tracker.failure_packet(bug.bug_id)
        assert packet is not None
        assert packet["signature"]["failure_code"] == "timeout_exceeded"
        assert packet["replay_context"]["ready"] is True
        assert packet["replay_context"]["repo_snapshot_ref"] == "repo_snapshot.alpha"
        assert packet["latest_receipt"]["payload_redacted"] is True
        assert "inputs" not in packet["latest_receipt"]
        assert packet["minimal_repro"]["payload_redacted"] is True
        assert packet["minimal_repro"]["input_keys"] == ("agent_slug", "runtime_profile_ref", "workspace_ref")
        assert packet["write_set_diff"]["added_paths"] == ("src/new.py",)
        assert packet["write_set_diff"]["removed_paths"] == ("src/old.py",)
        assert packet["lifecycle"]["fix_validation_count"] == 1
        assert packet["lifecycle"]["verified_validation_count"] == 1
        assert packet["fix_verification"]["fix_verified"] is True
        assert packet["blast_radius"]["occurrence_count"] == 4
        assert packet["trace"]["decision_refs"] == (
            "decision.packet.bug",
            {"decision_id": "decision.alpha"},
            "decision.alpha",
        )
        assert packet["observability_gaps"] == ()
        assert packet["agent_actions"]["replay"]["available"] is True
        assert packet["agent_actions"]["replay"]["arguments"] == {
            "action": "replay",
            "bug_id": bug.bug_id,
        }

    def test_failure_packet_surfaces_missing_observability(self, tracker: BugTracker):
        bug, _ = tracker.file_bug(
            title="No evidence bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="No receipt context attached yet.",
            filed_by="alice",
        )
        packet = tracker.failure_packet(bug.bug_id)
        assert packet is not None
        assert "bug.evidence_links.missing" in packet["observability_gaps"]
        assert "receipt.missing" in packet["observability_gaps"]

    def test_failure_packet_keeps_fallback_receipts_non_replayable(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        bug, _ = tracker.file_bug(
            title="Fallback-only bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="No explicit evidence, only signature matches.",
            filed_by="alice",
            tags=("failure_code:timeout_exceeded", "node_id:job-a"),
        )
        monkeypatch.setattr(
            tracker,
            "_find_signature_receipts",
            lambda **_kwargs: (
                [
                    {
                        "receipt_id": "receipt-fallback",
                        "workflow_id": "workflow.alpha",
                        "run_id": "run-fallback",
                        "request_id": "request.alpha",
                        "node_id": "job-a",
                        "status": "failed",
                        "failure_code": "timeout_exceeded",
                        "timestamp": datetime(2026, 4, 10, 10, 1, tzinfo=timezone.utc),
                        "started_at": None,
                        "finished_at": None,
                        "executor_type": "openai/gpt-5.4",
                        "agent": "openai/gpt-5.4",
                        "provider_slug": "openai",
                        "model_slug": "gpt-5.4",
                        "latency_ms": 1200,
                        "verification_status": "failed",
                        "failure_category": "runtime_failed",
                        "inputs": {"workspace_ref": "workspace.alpha"},
                        "outputs": {},
                        "artifacts": {},
                        "decision_refs": (),
                        "git_provenance": {"repo_snapshot_ref": "repo.alpha"},
                        "write_paths": (),
                        "verified_paths": (),
                    }
                ],
                None,
            ),
        )

        packet = tracker.failure_packet(bug.bug_id)
        assert packet is not None
        assert packet["replay_context"]["ready"] is False
        assert packet["replay_context"]["source"] == "fallback"
        assert packet["latest_receipt"] is None
        assert packet["fallback_receipts"][0]["receipt_id"] == "receipt-fallback"
        assert "receipt.inferred_only" in packet["observability_gaps"]
        assert packet["agent_actions"]["replay"]["available"] is False
        assert packet["agent_actions"]["replay"]["reason_code"] == "bug.replay_inferred_only"

    def test_failure_packet_handles_missing_attempted_at_rows(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        bug, _ = tracker.file_bug(
            title="Partial verification bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Verification rows can be partial.",
            filed_by="alice",
        )
        monkeypatch.setattr(tracker, "_validate_evidence_reference", lambda **_kwargs: None)
        tracker.link_evidence(
            bug.bug_id,
            evidence_kind="verification_run",
            evidence_ref="verification-run-a",
            evidence_role="validates_fix",
            created_by="test",
        )
        tracker.link_evidence(
            bug.bug_id,
            evidence_kind="verification_run",
            evidence_ref="verification-run-b",
            evidence_role="validates_fix",
            created_by="test",
        )
        monkeypatch.setattr(
            tracker,
            "_load_verification_rows",
            lambda table_name, id_field, refs: ({
                "verification-run-a": {
                    "verification_run_id": "verification-run-a",
                    "status": "failed",
                    "attempted_at": None,
                    "decision_ref": "",
                },
                "verification-run-b": {
                    "verification_run_id": "verification-run-b",
                    "status": "passed",
                    "attempted_at": datetime(2026, 4, 10, 10, 5, tzinfo=timezone.utc),
                    "decision_ref": "decision.partial",
                },
            }, None),
        )

        packet = tracker.failure_packet(bug.bug_id)
        assert packet is not None
        assert packet["fix_verification"]["fix_verified"] is True
        assert packet["fix_verification"]["last_validation"]["verification_run_id"] == "verification-run-b"

    def test_stats_report_packet_and_fix_counts(self, tracker: BugTracker):
        packet_bug, _ = tracker.file_bug(
            title="Linked bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Observed in one run.",
            filed_by="alice",
        )
        tracker._conn.execute(
            """
            INSERT INTO verification_runs (
                verification_run_id,
                verifier_ref,
                target_kind,
                target_ref,
                status,
                inputs,
                outputs,
                decision_ref,
                attempted_at,
                duration_ms
            ) VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, '{}'::jsonb, $6, NOW(), $7)
            """,
            "verification-run-1",
            "verifier.platform.schema_authority",
            "run",
            "run-1",
            "passed",
            "decision.stats",
            10,
        )
        tracker._validate_evidence_reference = lambda **_kwargs: None
        tracker.link_evidence(
            packet_bug.bug_id,
            evidence_kind="run",
            evidence_ref="run-1",
            evidence_role="observed_in",
            created_by="test",
        )
        tracker.link_evidence(
            packet_bug.bug_id,
            evidence_kind="verification_run",
            evidence_ref="verification-run-1",
            evidence_role="validates_fix",
            created_by="test",
        )
        tracker.file_bug(
            title="Underlinked bug",
            severity=BugSeverity.P3,
            category=BugCategory.OTHER,
            description="No evidence yet.",
            filed_by="bob",
        )

        stats = tracker.stats()
        assert stats.packet_ready_count >= 1
        assert stats.replay_ready_count >= 1
        assert stats.fix_verified_count >= 1
        assert stats.underlinked_count >= 1

    def test_replay_bug_returns_replay_view_when_packet_ready(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        bug, _ = tracker.file_bug(
            title="Replayable bug",
            severity=BugSeverity.P1,
            category=BugCategory.RUNTIME,
            description="Ready for replay.",
            filed_by="alice",
        )
        monkeypatch.setattr(
            tracker,
            "failure_packet",
            lambda bug_id, receipt_limit=5: {
                "bug": bug,
                "signature": {"fingerprint": "fp-alpha"},
                "observability_state": "complete",
                "observability_gaps": (),
                "replay_context": {
                    "ready": True,
                    "source": "evidence",
                    "run_id": "run-alpha",
                    "receipt_id": "receipt-alpha",
                },
                "agent_actions": {
                    "replay": {
                        "available": True,
                        "automatic": True,
                        "reason_code": "bug.replay_ready",
                        "tool": "praxis_bugs",
                        "arguments": {"action": "replay", "bug_id": bug_id},
                    }
                },
            },
        )
        monkeypatch.setattr(
            tracker,
            "_replay_run_view",
            lambda run_id: ReplayReadModel(
                run_id=run_id,
                request_id="request-alpha",
                completeness=ProjectionCompleteness(is_complete=True),
                watermark=ProjectionWatermark(evidence_seq=8),
                dependency_order=("node_0", "node_1"),
                node_outcomes=("node_0:succeeded", "node_1:failed"),
                admitted_definition_ref="workflow_definition.alpha.v1",
                terminal_reason="runtime.workflow_failed",
            ),
        )

        replay = tracker.replay_bug(bug.bug_id)
        assert replay is not None
        assert replay["ready"] is True
        assert replay["reason_code"] == "bug.replay_loaded"
        assert replay["packet_ready"] is True
        assert replay["replay"].run_id == "run-alpha"
        assert replay["tooling"]["replay"]["arguments"] == {
            "action": "replay",
            "bug_id": bug.bug_id,
        }

    def test_replay_bug_stays_blocked_for_inferred_only_packets(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        bug, _ = tracker.file_bug(
            title="Underlinked replay bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Packet is not authoritative enough to replay.",
            filed_by="alice",
        )
        monkeypatch.setattr(
            tracker,
            "failure_packet",
            lambda bug_id, receipt_limit=5: {
                "bug": bug,
                "signature": {"fingerprint": "fp-fallback"},
                "observability_state": "degraded",
                "observability_gaps": ("receipt.inferred_only",),
                "replay_context": {
                    "ready": False,
                    "source": "fallback",
                    "run_id": "run-fallback",
                    "receipt_id": None,
                },
                "agent_actions": {
                    "replay": {
                        "available": False,
                        "automatic": False,
                        "reason_code": "bug.replay_inferred_only",
                        "tool": "praxis_bugs",
                        "arguments": {"action": "replay", "bug_id": bug_id},
                    }
                },
            },
        )

        replay = tracker.replay_bug(bug.bug_id)
        assert replay is not None
        assert replay["ready"] is False
        assert replay["reason_code"] == "bug.replay_inferred_only"
        assert replay["replay"] is None

    def test_backfill_replay_provenance_links_discovery_fields_once(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setattr(tracker, "_validate_evidence_reference", lambda **_kwargs: None)
        bug, _ = tracker.file_bug(
            title="Discovery-backed bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Discovery fields should backfill observed links.",
            filed_by="alice",
        )
        bug_with_provenance = replace(
            bug,
            discovered_in_run_id="run-123",
            discovered_in_receipt_id="receipt-123",
        )
        monkeypatch.setattr(tracker, "get", lambda bug_id: bug_with_provenance if bug_id == bug.bug_id else None)

        first = tracker.backfill_replay_provenance(bug.bug_id)
        second = tracker.backfill_replay_provenance(bug.bug_id)

        assert first is not None
        assert first["linked_count"] == 2
        assert second is not None
        assert second["linked_count"] == 0
        links = tracker.list_evidence(bug.bug_id)
        assert {
            (link["evidence_kind"], link["evidence_ref"])
            for link in links
            if link["evidence_role"] == "observed_in"
        } == {("run", "run-123"), ("receipt", "receipt-123")}

    def test_backfill_replay_provenance_uses_unique_signature_match(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setattr(tracker, "_validate_evidence_reference", lambda **_kwargs: None)
        bug, _ = tracker.file_bug(
            title="Signature-backed bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Unique signature match should backfill observed links.",
            filed_by="alice",
            tags=("failure_code:timeout_exceeded", "node_id:job-a"),
        )
        monkeypatch.setattr(
            tracker,
            "_find_signature_receipts",
            lambda **_kwargs: (
                [
                    {
                        "receipt_id": "receipt-signature",
                        "run_id": "run-signature",
                        "node_id": "job-a",
                        "failure_code": "timeout_exceeded",
                    }
                ],
                None,
            ),
        )

        result = tracker.backfill_replay_provenance(bug.bug_id)

        assert result is not None
        assert result["linked_count"] == 2
        assert result["reason_code"] == "bug.replay_backfill.signature_match"
        links = tracker.list_evidence(bug.bug_id)
        assert {
            (link["evidence_kind"], link["evidence_ref"])
            for link in links
            if link["evidence_role"] == "observed_in"
        } == {("run", "run-signature"), ("receipt", "receipt-signature")}

    def test_bulk_backfill_replay_provenance_summarizes_results(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        bug_ready, _ = tracker.file_bug(
            title="Ready bulk bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Ready for replay after backfill.",
            filed_by="alice",
        )
        bug_blocked, _ = tracker.file_bug(
            title="Blocked bulk bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Still blocked after backfill.",
            filed_by="alice",
        )

        def _backfill(bug_id: str):
            if bug_id == bug_ready.bug_id:
                return {
                    "bug_id": bug_id,
                    "linked_count": 2,
                    "linked_refs": (
                        {"evidence_kind": "run", "evidence_ref": "run-123"},
                        {"evidence_kind": "receipt", "evidence_ref": "receipt-123"},
                    ),
                    "reason_code": "bug.replay_backfill.authoritative_fields",
                }
            return {
                "bug_id": bug_id,
                "linked_count": 0,
                "linked_refs": (),
                "reason_code": "bug.replay_backfill.no_unique_match",
            }

        def _hint(bug_id: str, *, receipt_limit: int = 1):
            assert receipt_limit == 1
            if bug_id == bug_ready.bug_id:
                return {
                    "available": True,
                    "reason_code": "bug.replay_ready",
                    "run_id": "run-123",
                    "receipt_id": "receipt-123",
                    "automatic": True,
                }
            return {
                "available": False,
                "reason_code": "bug.replay_missing_run_context",
                "run_id": None,
                "receipt_id": None,
                "automatic": False,
            }

        monkeypatch.setattr(tracker, "backfill_replay_provenance", _backfill)
        monkeypatch.setattr(tracker, "replay_hint", _hint)

        result = tracker.bulk_backfill_replay_provenance(limit=2)

        assert result["scanned_count"] == 2
        assert result["backfilled_count"] == 1
        assert result["linked_count"] == 2
        assert result["replay_ready_count"] == 1
        assert result["replay_blocked_count"] == 1
        assert result["bugs"][0]["replay_reason_code"] in {
            "bug.replay_ready",
            "bug.replay_missing_run_context",
        }

    def test_failure_packet_includes_historical_fixed_bugs(
        self,
        tracker: BugTracker,
        monkeypatch: pytest.MonkeyPatch,
    ):
        current_bug, _ = tracker.file_bug(
            title="Current timeout bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Current failing bug.",
            filed_by="alice",
            tags=("failure_code:timeout_exceeded", "node_id:job-a"),
        )
        fixed_bug, _ = tracker.file_bug(
            title="Older timeout bug",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Previously fixed version of the same failure.",
            filed_by="bob",
            tags=("failure_code:timeout_exceeded", "node_id:job-a"),
        )
        tracker.resolve(fixed_bug.bug_id, BugStatus.FIXED)
        monkeypatch.setattr(
            tracker,
            "backfill_replay_provenance",
            lambda bug_id: {
                "bug_id": bug_id,
                "linked_count": 0,
                "linked_refs": (),
                "reason_code": "bug.replay_backfill.no_unique_match",
            },
        )

        original_list_evidence = tracker.list_evidence

        def _list_evidence(bug_id: str):
            if bug_id == fixed_bug.bug_id:
                return [
                    {
                        "evidence_kind": "verification_run",
                        "evidence_ref": "verification-run-1",
                        "evidence_role": "validates_fix",
                        "created_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
                    }
                ]
            return original_list_evidence(bug_id)

        monkeypatch.setattr(tracker, "list_evidence", _list_evidence)

        def _load_verification_rows(table_name: str, id_field: str, refs: tuple[str, ...]):
            if table_name == "verification_runs" and "verification-run-1" in refs:
                return (
                    {
                        "verification-run-1": {
                            "verification_run_id": "verification-run-1",
                            "status": "passed",
                            "decision_ref": "decision:fix:validated",
                            "attempted_at": datetime(2026, 4, 10, 1, tzinfo=timezone.utc),
                            "inputs": {},
                            "outputs": {},
                            "verifier_ref": "verify.worker",
                            "target_kind": "bug",
                            "target_ref": fixed_bug.bug_id,
                            "duration_ms": 1200,
                        }
                    },
                    None,
                )
            return {}, None

        monkeypatch.setattr(tracker, "_load_verification_rows", _load_verification_rows)

        packet = tracker.failure_packet(current_bug.bug_id)

        assert packet is not None
        assert packet["historical_fixes"]["count"] == 1
        fix = packet["historical_fixes"]["items"][0]
        assert fix["bug_id"] == fixed_bug.bug_id
        assert "failure_code" in fix["shared_signature_fields"]
        assert "node_id" in fix["shared_signature_fields"]
        assert fix["fix_verification"]["fix_verified"] is True
        assert fix["fix_verification"]["verified_validation_count"] == 1


class TestSemanticNeighborCluster:
    def test_failure_packet_surfaces_tag_cluster_siblings(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        tag = f"cluster:{pfx}"
        alpha, _ = tracker.file_bug(
            title=f"Alpha lease flake [{pfx}]",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Lease renew sometimes drops.",
            filed_by="qa",
            tags=(tag, "area:leases"),
        )
        beta, _ = tracker.file_bug(
            title=f"Beta lease flake [{pfx}]",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="Similar renew window.",
            filed_by="qa",
            tags=(tag, "area:leases"),
        )
        packet = tracker.failure_packet(alpha.bug_id)
        assert packet is not None
        sn = packet.get("semantic_neighbors")
        assert isinstance(sn, dict)
        neighbor_ids = {item["bug_id"] for item in sn.get("items") or ()}
        assert beta.bug_id in neighbor_ids
        assert alpha.bug_id not in neighbor_ids
        assert sn.get("note")
        assert sn.get("reason_code") == "bug.semantic_neighbors.found"


class TestResumeContext:
    def test_file_bug_accepts_initial_resume_context(self, tracker: BugTracker):
        pfx = _uuid.uuid4().hex[:8]
        bug, _ = tracker.file_bug(
            title=f"Resume handoff [{pfx}]",
            severity=BugSeverity.P2,
            category=BugCategory.RUNTIME,
            description="desc",
            filed_by="tester",
            resume_context={
                "hypothesis": "race in lease",
                "next_steps": ["check TTL", "trace holder"],
            },
        )
        assert bug.resume_context.get("hypothesis") == "race in lease"
        loaded = tracker.get(bug.bug_id)
        assert loaded is not None
        assert loaded.resume_context["next_steps"] == ["check TTL", "trace holder"]

    def test_merge_resume_context_shallow_merges(self, tracker: BugTracker, sample_bug: Bug):
        first = tracker.merge_resume_context(
            sample_bug.bug_id, {"verified": "repro on main"}
        )
        assert first is not None
        assert first.resume_context.get("verified") == "repro on main"
        second = tracker.merge_resume_context(
            sample_bug.bug_id, {"hypothesis": "stale cache"}
        )
        assert second is not None
        assert second.resume_context["verified"] == "repro on main"
        assert second.resume_context["hypothesis"] == "stale cache"

    def test_failure_packet_includes_resume_context(
        self, tracker: BugTracker, sample_bug: Bug
    ):
        tracker.merge_resume_context(sample_bug.bug_id, {"hypothesis": "timeout budget"})
        packet = tracker.failure_packet(sample_bug.bug_id)
        assert packet is not None
        assert packet["resume_context"].get("hypothesis") == "timeout budget"
