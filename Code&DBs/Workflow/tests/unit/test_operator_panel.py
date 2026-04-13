"""Tests for runtime.operator_panel."""

from datetime import datetime, timezone

import sys
import importlib.util
from pathlib import Path

# Direct-import operator_panel without triggering runtime/__init__.py
# which has Python 3.10+ syntax incompatible with 3.9
_mod_path = str(Path(__file__).resolve().parents[2] / "runtime" / "operator_panel.py")
_spec = importlib.util.spec_from_file_location("operator_panel", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["operator_panel"] = _mod  # register so dataclasses can resolve
_spec.loader.exec_module(_mod)

LaneCue = _mod.LaneCue
OperatorPanel = _mod.OperatorPanel
OperatorSnapshot = _mod.OperatorSnapshot


class TestSnapshot:
    """snapshot() captures all registered state."""

    def test_snapshot_reflects_registered_state(self):
        panel = OperatorPanel()
        panel.register_posture("build")
        panel.register_circuit_breakers({"openai": True, "anthropic": False})
        panel.register_lease_count(3)
        panel.register_job_counts(pending=5, running=2)
        panel.register_pass_rate(0.75)
        panel.register_failure_codes({"TIMEOUT": 2, "OOM": 1})
        panel.register_loop_warnings(1)
        panel.register_write_conflicts(2)
        panel.register_governance_blocks(4)

        snap = panel.snapshot()

        assert isinstance(snap, OperatorSnapshot)
        assert snap.posture == "build"
        assert snap.circuit_breaker_open == ("openai",)
        assert snap.active_leases == 3
        assert snap.pending_jobs == 5
        assert snap.running_jobs == 2
        assert snap.recent_pass_rate == 0.75
        assert snap.recent_failure_codes == {"TIMEOUT": 2, "OOM": 1}
        assert snap.recent_failure_categories == {}
        assert snap.recent_lineage_depth == 0
        assert snap.last_run_id is None
        assert snap.last_failure_category is None
        assert snap.last_activity_at is None
        assert snap.loop_warnings == 1
        assert snap.write_conflicts == 2
        assert snap.governance_blocks == 4
        assert isinstance(snap.timestamp, datetime)

    def test_snapshot_is_frozen(self):
        panel = OperatorPanel()
        snap = panel.snapshot()
        try:
            snap.posture = "nope"  # type: ignore[misc]
            assert False, "expected FrozenInstanceError"
        except AttributeError:
            pass


class TestRecommendLaneObserve:
    """recommend_lane returns observe when circuit breakers open or pass rate low."""

    def test_observe_when_circuit_breakers_open(self):
        panel = OperatorPanel()
        panel.register_circuit_breakers({"openai": True})
        panel.register_pass_rate(0.95)

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "observe"
        assert cue.confidence >= 0.6
        assert cue.degraded_cause is not None
        assert "circuit breaker" in cue.degraded_cause.lower()

    def test_observe_confidence_scales_with_breaker_count(self):
        panel = OperatorPanel()
        panel.register_circuit_breakers({"a": True, "b": True, "c": True})

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "observe"
        assert cue.confidence >= 0.8

    def test_observe_when_pass_rate_low(self):
        panel = OperatorPanel()
        panel.register_pass_rate(0.3)

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "observe"
        assert cue.degraded_cause is not None
        assert "pass rate" in cue.degraded_cause.lower()


class TestRecommendLaneBuild:
    """recommend_lane returns build when healthy."""

    def test_build_when_healthy(self):
        panel = OperatorPanel()
        panel.register_pass_rate(0.95)
        panel.register_circuit_breakers({})
        panel.register_loop_warnings(0)
        panel.register_write_conflicts(0)

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "build"
        assert cue.confidence >= 0.8
        assert cue.degraded_cause is None


class TestRecommendLaneOperate:
    """recommend_lane returns operate as safe default or when warnings present."""

    def test_operate_with_loop_warnings(self):
        panel = OperatorPanel()
        panel.register_pass_rate(0.9)
        panel.register_loop_warnings(2)

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "operate"
        assert cue.degraded_cause is not None
        assert "loop" in cue.degraded_cause.lower()

    def test_operate_with_write_conflicts(self):
        panel = OperatorPanel()
        panel.register_pass_rate(0.9)
        panel.register_write_conflicts(1)

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "operate"
        assert cue.degraded_cause is not None
        assert "write conflict" in cue.degraded_cause.lower()

    def test_operate_as_safe_default(self):
        """Pass rate between 0.5 and 0.8 with no warnings -> operate."""
        panel = OperatorPanel()
        panel.register_pass_rate(0.65)

        cue = panel.recommend_lane()

        assert cue.recommended_posture == "operate"
        assert cue.degraded_cause is None


class TestDegradedCause:
    """degraded_cause is populated when problems detected."""

    def test_degraded_cause_none_when_healthy(self):
        panel = OperatorPanel()
        panel.register_pass_rate(0.95)
        assert panel.recommend_lane().degraded_cause is None

    def test_degraded_cause_set_on_breakers(self):
        panel = OperatorPanel()
        panel.register_circuit_breakers({"openai": True})
        cue = panel.recommend_lane()
        assert cue.degraded_cause is not None

    def test_degraded_cause_set_on_low_pass_rate(self):
        panel = OperatorPanel()
        panel.register_pass_rate(0.2)
        cue = panel.recommend_lane()
        assert cue.degraded_cause is not None


class TestEmptyState:
    """Empty state (no registrations) returns sensible defaults."""

    def test_empty_panel_snapshot(self):
        panel = OperatorPanel()
        snap = panel.snapshot()

        # No explicit posture registered — auto-syncs from lane recommendation.
        # Default pass_rate=1.0 with no degradation signals → "build"
        assert snap.posture == "build"
        assert snap.circuit_breaker_open == ()
        assert snap.active_leases == 0
        assert snap.pending_jobs == 0
        assert snap.running_jobs == 0
        assert snap.recent_pass_rate == 1.0
        assert snap.recent_failure_codes == {}
        assert snap.recent_failure_categories == {}
        assert snap.recent_lineage_depth == 0
        assert snap.last_run_id is None
        assert snap.last_failure_category is None
        assert snap.last_activity_at is None
        assert snap.loop_warnings == 0
        assert snap.write_conflicts == 0
        assert snap.governance_blocks == 0

    def test_empty_panel_recommends_build(self):
        """Default pass_rate is 1.0 with no warnings -> build."""
        panel = OperatorPanel()
        cue = panel.recommend_lane()

        assert cue.recommended_posture == "build"
        assert cue.degraded_cause is None

    def test_lane_cue_is_frozen(self):
        panel = OperatorPanel()
        cue = panel.recommend_lane()
        assert isinstance(cue, LaneCue)
        try:
            cue.confidence = 0.0  # type: ignore[misc]
            assert False, "expected FrozenInstanceError"
        except AttributeError:
            pass
