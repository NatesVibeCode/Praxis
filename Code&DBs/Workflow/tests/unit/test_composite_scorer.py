"""Tests for runtime.composite_scorer."""

from __future__ import annotations

import math

import importlib.util as _ilu
import pathlib
import sys

import pytest

_WORKFLOW_ROOT = str(pathlib.Path(__file__).resolve().parents[2])

# Import the module directly to avoid runtime/__init__.py pulling in
# unrelated code that may not be compatible with this Python version.
_spec = _ilu.spec_from_file_location(
    "composite_scorer",
    f"{_WORKFLOW_ROOT}/runtime/composite_scorer.py",
)
_mod = _ilu.module_from_spec(_spec)
sys.modules["composite_scorer"] = _mod
_spec.loader.exec_module(_mod)

CompositeScorer = _mod.CompositeScorer
ScaleFn = _mod.ScaleFn
ScoreResult = _mod.ScoreResult
ScoringFactor = _mod.ScoringFactor
_apply_scale = _mod._apply_scale


# ---------------------------------------------------------------------------
# Weight validation
# ---------------------------------------------------------------------------

class TestWeightValidation:
    def test_weights_sum_to_one(self):
        scorer = CompositeScorer([("a", 0.6, ScaleFn.LINEAR, False),
                                   ("b", 0.4, ScaleFn.LINEAR, False)])
        assert scorer is not None

    def test_weights_within_epsilon(self):
        # 0.005 off should still be accepted (epsilon = 0.01)
        scorer = CompositeScorer([("a", 0.505, ScaleFn.LINEAR, False),
                                   ("b", 0.5, ScaleFn.LINEAR, False)])
        assert scorer is not None

    def test_weights_too_low(self):
        with pytest.raises(ValueError, match="Weights must sum to 1.0"):
            CompositeScorer([("a", 0.3, ScaleFn.LINEAR, False),
                             ("b", 0.3, ScaleFn.LINEAR, False)])

    def test_weights_too_high(self):
        with pytest.raises(ValueError, match="Weights must sum to 1.0"):
            CompositeScorer([("a", 0.7, ScaleFn.LINEAR, False),
                             ("b", 0.7, ScaleFn.LINEAR, False)])

    def test_empty_factors_rejected(self):
        with pytest.raises(ValueError, match="At least one"):
            CompositeScorer([])

    def test_duplicate_name_rejected(self):
        with pytest.raises(ValueError, match="Duplicate"):
            CompositeScorer([("a", 0.5, ScaleFn.LINEAR, False),
                             ("a", 0.5, ScaleFn.LINEAR, False)])


# ---------------------------------------------------------------------------
# Scale functions
# ---------------------------------------------------------------------------

class TestScaleFunctions:
    def test_linear_passthrough(self):
        assert _apply_scale(ScaleFn.LINEAR, 0.0) == 0.0
        assert _apply_scale(ScaleFn.LINEAR, 0.5) == 0.5
        assert _apply_scale(ScaleFn.LINEAR, 1.0) == 1.0

    def test_linear_clamps(self):
        assert _apply_scale(ScaleFn.LINEAR, -0.5) == 0.0
        assert _apply_scale(ScaleFn.LINEAR, 1.5) == 1.0

    def test_sigmoid_centre(self):
        # At 0.5 the sigmoid should return exactly 0.5
        assert _apply_scale(ScaleFn.SIGMOID, 0.5) == pytest.approx(0.5)

    def test_sigmoid_monotonic(self):
        vals = [_apply_scale(ScaleFn.SIGMOID, x / 10) for x in range(11)]
        for i in range(len(vals) - 1):
            assert vals[i] <= vals[i + 1]

    def test_sigmoid_bounds(self):
        assert 0.0 < _apply_scale(ScaleFn.SIGMOID, 0.0) < 0.1
        assert 0.9 < _apply_scale(ScaleFn.SIGMOID, 1.0) < 1.0

    def test_bucket_low(self):
        assert _apply_scale(ScaleFn.BUCKET, 0.0) == 0.0
        assert _apply_scale(ScaleFn.BUCKET, 0.32) == 0.0

    def test_bucket_mid(self):
        assert _apply_scale(ScaleFn.BUCKET, 0.33) == 0.5
        assert _apply_scale(ScaleFn.BUCKET, 0.65) == 0.5

    def test_bucket_high(self):
        assert _apply_scale(ScaleFn.BUCKET, 0.66) == 1.0
        assert _apply_scale(ScaleFn.BUCKET, 1.0) == 1.0

    def test_logarithmic_endpoints(self):
        assert _apply_scale(ScaleFn.LOGARITHMIC, 0.0) == pytest.approx(0.0)
        assert _apply_scale(ScaleFn.LOGARITHMIC, 1.0) == pytest.approx(1.0)

    def test_logarithmic_compression(self):
        # Log scale compresses high values -- mid-input should map above 0.5
        mid = _apply_scale(ScaleFn.LOGARITHMIC, 0.5)
        assert mid > 0.5


# ---------------------------------------------------------------------------
# Required factor enforcement
# ---------------------------------------------------------------------------

class TestRequiredFactors:
    def test_missing_required_raises(self):
        scorer = CompositeScorer([
            ("trust", 0.5, ScaleFn.LINEAR, True),
            ("cost", 0.5, ScaleFn.LINEAR, False),
        ])
        with pytest.raises(ValueError, match="Required factor 'trust'"):
            scorer.score(cost=0.8)

    def test_all_required_provided(self):
        scorer = CompositeScorer([
            ("trust", 0.5, ScaleFn.LINEAR, True),
            ("cost", 0.5, ScaleFn.LINEAR, True),
        ])
        result = scorer.score(trust=0.9, cost=0.7)
        assert isinstance(result, ScoreResult)
        assert result.missing_factors == ()


# ---------------------------------------------------------------------------
# Completeness tracking
# ---------------------------------------------------------------------------

class TestCompleteness:
    def test_full_completeness(self):
        scorer = CompositeScorer([
            ("a", 0.5, ScaleFn.LINEAR, False),
            ("b", 0.5, ScaleFn.LINEAR, False),
        ])
        result = scorer.score(a=0.5, b=0.5)
        assert result.completeness_ratio == pytest.approx(1.0)
        assert result.missing_factors == ()

    def test_partial_completeness(self):
        scorer = CompositeScorer([
            ("a", 0.5, ScaleFn.LINEAR, False),
            ("b", 0.5, ScaleFn.LINEAR, False),
        ])
        result = scorer.score(a=0.5)
        assert result.completeness_ratio == pytest.approx(0.5)
        assert result.missing_factors == ("b",)

    def test_no_factors_provided(self):
        scorer = CompositeScorer([
            ("a", 0.5, ScaleFn.LINEAR, False),
            ("b", 0.5, ScaleFn.LINEAR, False),
        ])
        result = scorer.score()
        assert result.completeness_ratio == pytest.approx(0.0)
        assert set(result.missing_factors) == {"a", "b"}


# ---------------------------------------------------------------------------
# Realistic dispatch scoring
# ---------------------------------------------------------------------------

class TestDispatchScoring:
    @pytest.fixture()
    def dispatch_scorer(self):
        return CompositeScorer([
            ("trust", 0.35, ScaleFn.SIGMOID, True),
            ("cost", 0.25, ScaleFn.LINEAR, True),
            ("latency", 0.20, ScaleFn.LOGARITHMIC, False),
            ("pass_rate", 0.20, ScaleFn.BUCKET, True),
        ])

    def test_high_quality_agent(self, dispatch_scorer):
        result = dispatch_scorer.score(
            trust=0.95, cost=0.85, latency=0.90, pass_rate=0.80,
        )
        assert result.total_score > 0.7
        assert result.completeness_ratio == 1.0
        assert len(result.factors) == 4

    def test_low_quality_agent(self, dispatch_scorer):
        result = dispatch_scorer.score(
            trust=0.1, cost=0.1, latency=0.1, pass_rate=0.1,
        )
        assert result.total_score < 0.3

    def test_missing_optional_latency(self, dispatch_scorer):
        result = dispatch_scorer.score(trust=0.8, cost=0.7, pass_rate=0.7)
        assert result.completeness_ratio == pytest.approx(0.75)
        assert "latency" in result.missing_factors
        # Score should still be computed from the 3 provided factors
        assert result.total_score > 0.0

    def test_per_factor_breakdown(self, dispatch_scorer):
        result = dispatch_scorer.score(
            trust=0.5, cost=0.5, latency=0.5, pass_rate=0.5,
        )
        by_name = {f.name: f for f in result.factors}
        # Sigmoid at 0.5 should yield exactly 0.5
        assert by_name["trust"].scaled_value == pytest.approx(0.5)
        # Linear at 0.5 should yield 0.5
        assert by_name["cost"].scaled_value == pytest.approx(0.5)
        # Bucket at 0.5 should yield 0.5
        assert by_name["pass_rate"].scaled_value == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_all_zeros(self):
        scorer = CompositeScorer([
            ("a", 0.5, ScaleFn.LINEAR, False),
            ("b", 0.5, ScaleFn.LINEAR, False),
        ])
        result = scorer.score(a=0.0, b=0.0)
        assert result.total_score == pytest.approx(0.0)

    def test_all_ones(self):
        scorer = CompositeScorer([
            ("a", 0.5, ScaleFn.LINEAR, False),
            ("b", 0.5, ScaleFn.LINEAR, False),
        ])
        result = scorer.score(a=1.0, b=1.0)
        assert result.total_score == pytest.approx(1.0)

    def test_missing_optional_contributes_zero(self):
        scorer = CompositeScorer([
            ("a", 0.5, ScaleFn.LINEAR, False),
            ("b", 0.5, ScaleFn.LINEAR, False),
        ])
        full = scorer.score(a=1.0, b=1.0)
        partial = scorer.score(a=1.0)
        assert partial.total_score < full.total_score

    def test_score_result_is_frozen(self):
        scorer = CompositeScorer([("x", 1.0, ScaleFn.LINEAR, False)])
        result = scorer.score(x=0.5)
        with pytest.raises(AttributeError):
            result.total_score = 0.99  # type: ignore[misc]

    def test_scoring_factor_is_frozen(self):
        f = ScoringFactor("t", 0.5, ScaleFn.LINEAR, True, 0.5, 0.5)
        with pytest.raises(AttributeError):
            f.raw_value = 0.9  # type: ignore[misc]
