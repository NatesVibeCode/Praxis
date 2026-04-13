"""Tests for consensus scoring and focus-aware scoring modules."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from memory.consensus import (
    ConsensusEngine,
    ConsensusResult,
    GraphSignal,
    Signal,
    SignalType,
    TemporalSignal,
    TextSignal,
    VocabSignal,
)
from memory.focus_scoring import DecayProfile, FocusBoost, FocusScorer


# ========================================================================
# Module 1 — Consensus
# ========================================================================

class TestTextSignal:
    def test_identical_strings(self):
        assert TextSignal.score("alice", "alice") == 1.0

    def test_similar_strings(self):
        s = TextSignal.score("martha", "marhta")
        assert 0.9 < s <= 1.0

    def test_completely_different(self):
        s = TextSignal.score("abc", "xyz")
        assert s < 0.5

    def test_empty_string(self):
        assert TextSignal.score("", "hello") == 0.0
        assert TextSignal.score("", "") == 1.0


class TestGraphSignal:
    def test_full_overlap(self):
        assert GraphSignal.score(5, 5) == 1.0

    def test_no_overlap(self):
        assert GraphSignal.score(0, 10) == 0.0

    def test_partial_overlap(self):
        assert GraphSignal.score(3, 12) == 0.25

    def test_empty_neighborhood(self):
        assert GraphSignal.score(0, 0) == 0.0


class TestTemporalSignal:
    def test_recent_single_occurrence(self):
        s = TemporalSignal.score(1, 0.0)
        assert s == 1.0

    def test_decay_at_half_life(self):
        s = TemporalSignal.score(1, 14.0, half_life=14.0)
        assert abs(s - 0.5) < 1e-9

    def test_high_count_capped(self):
        s = TemporalSignal.score(100, 0.0)
        assert s == 1.0  # capped at 1.0


class TestVocabSignal:
    def test_identical_text(self):
        s = VocabSignal.score("hello world", "hello world")
        assert s == pytest.approx(1.0, abs=1e-9)

    def test_disjoint_text(self):
        s = VocabSignal.score("cat dog", "red blue")
        assert s == 0.0

    def test_partial_overlap(self):
        s = VocabSignal.score("machine learning model", "deep learning model")
        assert 0.0 < s < 1.0

    def test_empty_text(self):
        assert VocabSignal.score("", "hello") == 0.0


class TestConsensusEngine:
    def test_no_signals_returns_zero(self):
        engine = ConsensusEngine()
        result = engine.evaluate("a", "b", [])
        assert result.combined_score == 0.0
        assert result.is_match is False

    def test_single_perfect_signal(self):
        engine = ConsensusEngine()
        sig = Signal(SignalType.TEXT, score=1.0, weight=0.3, source="text")
        result = engine.evaluate("a", "b", [sig])
        assert result.combined_score == pytest.approx(0.3)

    def test_noisy_or_combination(self):
        engine = ConsensusEngine()
        signals = [
            Signal(SignalType.TEXT, score=1.0, weight=0.3, source="text"),
            Signal(SignalType.GRAPH, score=1.0, weight=0.25, source="graph"),
        ]
        result = engine.evaluate("a", "b", signals)
        expected = 1.0 - (1.0 - 0.3) * (1.0 - 0.25)
        assert result.combined_score == pytest.approx(expected)

    def test_match_threshold(self):
        engine = ConsensusEngine(match_threshold=0.4)
        sig = Signal(SignalType.TEXT, score=1.0, weight=0.3, source="text")
        result = engine.evaluate("a", "b", [sig])
        assert result.combined_score == pytest.approx(0.3)
        assert result.is_match is False

        engine2 = ConsensusEngine(match_threshold=0.2)
        result2 = engine2.evaluate("a", "b", [sig])
        assert result2.is_match is True

    def test_custom_weights(self):
        engine = ConsensusEngine(weights={SignalType.TEXT: 0.9})
        sig = Signal(SignalType.TEXT, score=1.0, weight=0.0, source="text")
        result = engine.evaluate("a", "b", [sig])
        assert result.combined_score == pytest.approx(0.9)

    def test_result_fields(self):
        engine = ConsensusEngine()
        sig = Signal(SignalType.TEXT, score=0.8, weight=0.3, source="test")
        result = engine.evaluate("id1", "id2", [sig])
        assert result.entity_id_a == "id1"
        assert result.entity_id_b == "id2"
        assert isinstance(result.signals, tuple)
        assert len(result.signals) == 1


# ========================================================================
# Module 2 — Focus Scoring
# ========================================================================

class TestFocusScorer:
    NOW = datetime(2026, 4, 4, tzinfo=timezone.utc)

    def test_never_decay_constraint(self):
        scorer = FocusScorer()
        old = self.NOW - timedelta(days=365)
        s = scorer.score("e1", "constraint", 1.0, old, now=self.NOW)
        assert s == 1.0

    def test_task_decays(self):
        scorer = FocusScorer()
        ten_days_ago = self.NOW - timedelta(days=10)
        s = scorer.score("e1", "task", 1.0, ten_days_ago, now=self.NOW)
        assert abs(s - 0.5) < 1e-9  # half_life=10

    def test_focus_boost_applied(self):
        boost = FocusBoost(entity_id="e1", boost_factor=0.5, reason="active")
        scorer = FocusScorer(active_focus=[boost])
        s = scorer.score("e1", "constraint", 1.0, self.NOW, now=self.NOW)
        assert s == pytest.approx(1.5)

    def test_no_boost_for_other_entity(self):
        boost = FocusBoost(entity_id="e1", boost_factor=0.5, reason="active")
        scorer = FocusScorer(active_focus=[boost])
        s = scorer.score("e2", "constraint", 1.0, self.NOW, now=self.NOW)
        assert s == 1.0

    def test_batch_score_sorted_descending(self):
        scorer = FocusScorer()
        entities = [
            {"entity_id": "a", "entity_type": "constraint", "base_score": 0.5, "updated_at": self.NOW, "now": self.NOW},
            {"entity_id": "b", "entity_type": "constraint", "base_score": 0.9, "updated_at": self.NOW, "now": self.NOW},
            {"entity_id": "c", "entity_type": "constraint", "base_score": 0.7, "updated_at": self.NOW, "now": self.NOW},
        ]
        results = scorer.batch_score(entities)
        scores = [s for _, s in results]
        assert scores == sorted(scores, reverse=True)
        assert results[0][0] == "b"

    def test_set_focus_replaces(self):
        scorer = FocusScorer()
        scorer.set_focus([FocusBoost("e1", 2.0, "hot")])
        s = scorer.score("e1", "constraint", 1.0, self.NOW, now=self.NOW)
        assert s == pytest.approx(3.0)
        scorer.set_focus([])
        s2 = scorer.score("e1", "constraint", 1.0, self.NOW, now=self.NOW)
        assert s2 == 1.0

    def test_unknown_entity_type_defaults(self):
        scorer = FocusScorer()
        ten_days_ago = self.NOW - timedelta(days=30)
        s = scorer.score("e1", "unknown_type", 1.0, ten_days_ago, now=self.NOW)
        # default half_life 30 => decay ~0.5
        assert abs(s - 0.5) < 1e-9

    def test_default_profiles_loaded(self):
        scorer = FocusScorer()
        assert "pattern" in scorer._profiles
        assert scorer._profiles["pattern"].never_decay is True
        assert scorer._profiles["task"].half_life_days == 10.0
