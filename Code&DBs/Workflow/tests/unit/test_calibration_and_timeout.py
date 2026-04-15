"""Tests for calibration engine and dynamic timeout calculator."""

import importlib.util
import json
import os
import sys
import tempfile

_RUNTIME_DIR = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, "runtime"
)


def _load_module(name: str):
    path = os.path.join(_RUNTIME_DIR, f"{name}.py")
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_cal = _load_module("calibration")
_dto = _load_module("dynamic_timeout")

CalibratedParam = _cal.CalibratedParam
CalibrationEngine = _cal.CalibrationEngine
CalibrationOutcome = _cal.CalibrationOutcome

ComplexityTier = _dto.ComplexityTier
DynamicTimeoutCalculator = _dto.DynamicTimeoutCalculator
TimeoutConfig = _dto.TimeoutConfig
complexity_tier_from_name = _dto.complexity_tier_from_name
max_complexity_tier = _dto.max_complexity_tier
calculate_timeout_seconds = _dto.calculate_timeout_seconds


# ── Calibration Engine ──────────────────────────────────────────────


class TestCalibratedParamFrozen:
    def test_frozen(self):
        p = CalibratedParam("x", 0.5, 0.0, 1.0, 0.0, None)
        try:
            p.current = 0.9  # type: ignore[misc]
            assert False, "Should have raised"
        except AttributeError:
            pass


class TestCalibrationOutcomeFrozen:
    def test_frozen(self):
        from datetime import datetime, timezone

        o = CalibrationOutcome("x", 0.6, True, datetime.now(timezone.utc))
        try:
            o.actual_outcome = False  # type: ignore[misc]
            assert False, "Should have raised"
        except AttributeError:
            pass


class TestCalibrationEngineInit:
    def test_initial_values(self):
        engine = CalibrationEngine({"threshold": (0.5, 0.0, 1.0)})
        p = engine.get("threshold")
        assert p.current == 0.5
        assert p.min_val == 0.0
        assert p.max_val == 1.0
        assert p.last_nudge == 0.0
        assert p.prediction_accuracy is None

    def test_all_params(self):
        engine = CalibrationEngine({"a": (0.3, 0.0, 1.0), "b": (10.0, 1.0, 100.0)})
        params = engine.all_params()
        assert set(params.keys()) == {"a", "b"}


class TestCalibrationEngineRecord:
    def test_record_unknown_param_raises(self):
        engine = CalibrationEngine({"x": (0.5, 0.0, 1.0)})
        try:
            engine.record_outcome("missing", 0.6, True)
            assert False, "Should raise KeyError"
        except KeyError:
            pass

    def test_record_stores_outcomes(self):
        engine = CalibrationEngine({"x": (0.5, 0.0, 1.0)})
        engine.record_outcome("x", 0.6, True)
        engine.record_outcome("x", 0.4, False)
        # After calibration, accuracy should reflect recorded data
        result = engine.calibrate("x")
        assert result.prediction_accuracy is not None


class TestCalibrationEngineCalibrateDown:
    def test_nudge_down_when_too_strict(self):
        engine = CalibrationEngine({"t": (0.5, 0.0, 1.0)})
        # All decisions above threshold fail -> accuracy = 0
        for _ in range(10):
            engine.record_outcome("t", 0.6, False)
        result = engine.calibrate("t")
        assert result.prediction_accuracy < 0.5
        assert result.current < 0.5
        assert result.last_nudge < 0


class TestCalibrationEngineCalibrateUp:
    def test_nudge_up_when_too_lenient(self):
        engine = CalibrationEngine({"t": (0.5, 0.0, 1.0)})
        # All decisions above threshold succeed -> accuracy = 1.0
        for _ in range(10):
            engine.record_outcome("t", 0.6, True)
        result = engine.calibrate("t")
        assert result.prediction_accuracy > 0.8
        assert result.current > 0.5
        assert result.last_nudge > 0


class TestCalibrationEngineClamp:
    def test_clamp_to_max(self):
        engine = CalibrationEngine({"t": (0.99, 0.0, 1.0)})
        for _ in range(10):
            engine.record_outcome("t", 1.0, True)
        result = engine.calibrate("t", perturbation_pct=0.5)
        assert result.current <= 1.0

    def test_clamp_to_min(self):
        engine = CalibrationEngine({"t": (0.01, 0.0, 1.0)})
        for _ in range(10):
            engine.record_outcome("t", 0.02, False)
        result = engine.calibrate("t", perturbation_pct=0.5)
        assert result.current >= 0.0


class TestCalibrationEngineNoOutcomes:
    def test_calibrate_with_no_data_returns_unchanged(self):
        engine = CalibrationEngine({"t": (0.5, 0.0, 1.0)})
        result = engine.calibrate("t")
        assert result.current == 0.5
        assert result.prediction_accuracy is None


class TestCalibrationEnginePersistence:
    def test_save_and_load(self):
        engine = CalibrationEngine({"t": (0.5, 0.0, 1.0)})
        engine.record_outcome("t", 0.7, True)
        engine.record_outcome("t", 0.3, False)
        engine.calibrate("t")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            engine.save(path)
            engine2 = CalibrationEngine({"t": (0.5, 0.0, 1.0)})
            engine2.load(path)
            assert engine2.get("t").current == engine.get("t").current
            assert engine2.get("t").prediction_accuracy == engine.get("t").prediction_accuracy
        finally:
            os.unlink(path)


# ── Dynamic Timeout ─────────────────────────────────────────────────


class TestComplexityTier:
    def test_multipliers(self):
        assert ComplexityTier.TRIVIAL.multiplier == 0.5
        assert ComplexityTier.STANDARD.multiplier == 1.0
        assert ComplexityTier.COMPLEX.multiplier == 2.0
        assert ComplexityTier.FRONTIER.multiplier == 3.0


class TestTimeoutConfigFrozen:
    def test_frozen(self):
        cfg = TimeoutConfig(300, ComplexityTier.STANDARD, None, None)
        try:
            cfg.base_seconds = 100  # type: ignore[misc]
            assert False, "Should have raised"
        except AttributeError:
            pass


class TestDynamicTimeoutBasic:
    def test_default_with_complexity(self):
        calc = DynamicTimeoutCalculator(default_timeout=300)
        assert calc.calculate("job_a", ComplexityTier.STANDARD) == 300
        assert calc.calculate("job_a", ComplexityTier.TRIVIAL) == 150
        assert calc.calculate("job_a", ComplexityTier.COMPLEX) == 600
        assert calc.calculate("job_a", ComplexityTier.FRONTIER) == 900


class TestDynamicTimeoutHistorical:
    def test_p95_overrides_base(self):
        calc = DynamicTimeoutCalculator(default_timeout=300)
        # Record 20 durations: 100..119
        for i in range(20):
            calc.record_duration("build", 100.0 + i, ComplexityTier.STANDARD)
        # p95 should be ~118, * 1.5 = ~177
        result = calc.calculate("build", ComplexityTier.STANDARD)
        assert result >= 150
        assert result <= 200


class TestDynamicTimeoutStageBlend:
    def test_stage_default_blends(self):
        calc = DynamicTimeoutCalculator(default_timeout=300)
        calc.stage_defaults["build"] = 600
        # base = 300 * 1.0 = 300, blend with 600 -> (300+600)/2 = 450
        result = calc.calculate("new_job", ComplexityTier.STANDARD, stage="build")
        assert result == 450


class TestDynamicTimeoutClamp:
    def test_clamp_min(self):
        calc = DynamicTimeoutCalculator(default_timeout=10, min_timeout=60)
        result = calc.calculate("tiny", ComplexityTier.TRIVIAL)
        # 10 * 0.5 = 5, clamped to 60
        assert result == 60

    def test_clamp_max(self):
        calc = DynamicTimeoutCalculator(default_timeout=1000, max_timeout=1800)
        result = calc.calculate("huge", ComplexityTier.FRONTIER)
        # 1000 * 3.0 = 3000, clamped to 1800
        assert result == 1800


class TestDynamicTimeoutHelpers:
    def test_complexity_name_mapping(self):
        assert complexity_tier_from_name("low") == ComplexityTier.TRIVIAL
        assert complexity_tier_from_name("moderate") == ComplexityTier.STANDARD
        assert complexity_tier_from_name("medium") == ComplexityTier.STANDARD
        assert complexity_tier_from_name("high") == ComplexityTier.COMPLEX
        assert complexity_tier_from_name("frontier") == ComplexityTier.FRONTIER
        assert complexity_tier_from_name("unknown") == ComplexityTier.STANDARD

    def test_max_complexity_tier(self):
        assert max_complexity_tier(["low", "moderate", "high"]) == ComplexityTier.COMPLEX

    def test_calculate_timeout_seconds_uses_history(self):
        result = calculate_timeout_seconds(
            "spec-a",
            "moderate",
            default_timeout=300,
            historical_p95_seconds=118.0,
        )
        assert result == 177


class TestDynamicTimeoutRecordAndP95:
    def test_single_record(self):
        calc = DynamicTimeoutCalculator(default_timeout=300)
        calc.record_duration("j", 200.0, ComplexityTier.STANDARD)
        # p95 of [200] = 200, * 1.5 = 300
        result = calc.calculate("j", ComplexityTier.STANDARD)
        assert result == 300
