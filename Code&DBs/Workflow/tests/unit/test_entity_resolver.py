"""Tests for memory.entity_resolver — match strategies, chain, combiner, resolver."""

import pytest

from memory.entity_resolver import (
    EntityResolver,
    MatchChain,
    MatchResult,
    MatchStep,
    MatchStrategy,
    NoisyOrCombiner,
    _levenshtein_distance,
    _jaro_winkler_score,
    _token_set_ratio,
    _metaphone_encode,
    _metaphone_score,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
CANDIDATES = [
    ("1", "Acme Corporation"),
    ("2", "Globex Industries"),
    ("3", "Initech"),
    ("4", "Umbrella Corp"),
]


# ---------------------------------------------------------------------------
# EXACT match
# ---------------------------------------------------------------------------

class TestExactMatch:
    def test_case_insensitive_exact(self):
        chain = MatchChain([MatchStep(MatchStrategy.EXACT, 1.0, 1.0)])
        results = chain.match("acme corporation", CANDIDATES)
        assert len(results) == 1
        assert results[0].entity_id == "1"
        assert results[0].score == 1.0

    def test_exact_no_match(self):
        chain = MatchChain([MatchStep(MatchStrategy.EXACT, 1.0, 1.0)])
        results = chain.match("nonexistent", CANDIDATES)
        assert results == []


# ---------------------------------------------------------------------------
# LEVENSHTEIN
# ---------------------------------------------------------------------------

class TestLevenshtein:
    def test_known_edit_distance(self):
        # "kitten" -> "sitting" = 3 edits
        dist = _levenshtein_distance("kitten", "sitting")
        assert dist == 3

    def test_scoring(self):
        chain = MatchChain([MatchStep(MatchStrategy.LEVENSHTEIN, 0.5, 1.0)])
        results = chain.match("Initek", CANDIDATES)  # 1 edit from "Initech"
        initech = [r for r in results if r.entity_id == "3"]
        assert len(initech) == 1
        # "initek" vs "initech" -> distance 2, max_len 7 -> score ~0.71
        assert initech[0].score > 0.6

    def test_identical_strings(self):
        dist = _levenshtein_distance("hello", "hello")
        assert dist == 0

    def test_empty_string(self):
        dist = _levenshtein_distance("", "abc")
        assert dist == 3


# ---------------------------------------------------------------------------
# JARO_WINKLER
# ---------------------------------------------------------------------------

class TestJaroWinkler:
    def test_identical(self):
        score = _jaro_winkler_score("Martha", "Martha")
        assert score == pytest.approx(1.0)

    def test_known_similarity(self):
        # Classic example: MARTHA vs MARHTA
        score = _jaro_winkler_score("MARTHA", "MARHTA")
        assert score > 0.95  # Known to be ~0.961

    def test_completely_different(self):
        score = _jaro_winkler_score("abc", "xyz")
        assert score < 0.5

    def test_empty_strings(self):
        score = _jaro_winkler_score("", "")
        assert score == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# TOKEN_SET_RATIO
# ---------------------------------------------------------------------------

class TestTokenSetRatio:
    def test_identical_sets(self):
        score = _token_set_ratio("hello world", "world hello")
        assert score == pytest.approx(1.0)

    def test_partial_overlap(self):
        # {"acme", "inc"} & {"acme", "corporation"} = {"acme"}
        # union = {"acme", "inc", "corporation"} -> 1/3
        score = _token_set_ratio("Acme Inc", "Acme Corporation")
        assert score == pytest.approx(1 / 3)

    def test_no_overlap(self):
        score = _token_set_ratio("alpha beta", "gamma delta")
        assert score == pytest.approx(0.0)

    def test_empty(self):
        score = _token_set_ratio("", "")
        assert score == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# METAPHONE
# ---------------------------------------------------------------------------

class TestMetaphone:
    def test_phonetically_similar_names(self):
        # Smith and Smyth should encode similarly
        enc_smith = _metaphone_encode("Smith")
        enc_smyth = _metaphone_encode("Smyth")
        assert enc_smith == enc_smyth

    def test_phonetically_different(self):
        enc_a = _metaphone_encode("Robert")
        enc_b = _metaphone_encode("William")
        assert enc_a != enc_b

    def test_score_similar_names(self):
        score = _metaphone_score("Steven", "Stephen")
        assert score > 0.5

    def test_score_different_names(self):
        score = _metaphone_score("Alice", "Robert")
        assert score < 0.5


# ---------------------------------------------------------------------------
# MatchChain early exit / fallthrough
# ---------------------------------------------------------------------------

class TestMatchChainBehavior:
    def test_early_exit_on_exact(self):
        """Exact match should stop the chain -- only EXACT strategy used."""
        chain = MatchChain([
            MatchStep(MatchStrategy.EXACT, 1.0, 1.0),
            MatchStep(MatchStrategy.LEVENSHTEIN, 0.5, 0.8),
        ])
        results = chain.match("Initech", CANDIDATES)
        # Should have exactly one result (the exact match) and strategy EXACT
        exact_hits = [r for r in results if r.entity_id == "3"]
        assert len(exact_hits) == 1
        assert exact_hits[0].strategy == MatchStrategy.EXACT
        assert exact_hits[0].score == pytest.approx(1.0)

    def test_fallthrough_to_fuzzy(self):
        """No exact match -> chain continues to fuzzy strategies."""
        chain = MatchChain([
            MatchStep(MatchStrategy.EXACT, 1.0, 1.0),
            MatchStep(MatchStrategy.LEVENSHTEIN, 0.5, 0.8),
        ])
        results = chain.match("Initek", CANDIDATES)  # close to Initech
        assert len(results) > 0
        initech = [r for r in results if r.entity_id == "3"]
        assert len(initech) == 1
        assert initech[0].score > 0.4


# ---------------------------------------------------------------------------
# NoisyOrCombiner
# ---------------------------------------------------------------------------

class TestNoisyOrCombiner:
    def test_single_score(self):
        c = NoisyOrCombiner()
        # 1 - (1 - 0.8*1.0) = 0.8
        assert c.combine([(0.8, 1.0)]) == pytest.approx(0.8)

    def test_multiple_scores(self):
        c = NoisyOrCombiner()
        # 1 - (1-0.5*1.0)*(1-0.6*0.8) = 1 - 0.5*0.52 = 1 - 0.26 = 0.74
        result = c.combine([(0.5, 1.0), (0.6, 0.8)])
        assert result == pytest.approx(0.74)

    def test_empty(self):
        c = NoisyOrCombiner()
        assert c.combine([]) == pytest.approx(0.0)

    def test_perfect_scores(self):
        c = NoisyOrCombiner()
        # 1 - (1-1.0)*(1-1.0) = 1 - 0 = 1.0
        assert c.combine([(1.0, 1.0), (1.0, 1.0)]) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# EntityResolver (default chain)
# ---------------------------------------------------------------------------

class TestEntityResolver:
    def test_default_chain_exact(self):
        resolver = EntityResolver()
        results = resolver.resolve("Globex Industries", CANDIDATES)
        assert results[0].entity_id == "2"
        assert results[0].score == pytest.approx(1.0)

    def test_default_chain_fuzzy(self):
        resolver = EntityResolver()
        results = resolver.resolve("Umbrela Corp", CANDIDATES)  # typo
        umbrella = [r for r in results if r.entity_id == "4"]
        assert len(umbrella) == 1
        assert umbrella[0].score > 0.5

    def test_resolve_best_returns_top(self):
        resolver = EntityResolver()
        best = resolver.resolve_best("Initech", CANDIDATES)
        assert best is not None
        assert best.entity_id == "3"

    def test_resolve_best_none_below_threshold(self):
        resolver = EntityResolver()
        best = resolver.resolve_best("zzzzzzzzzzz", CANDIDATES, min_threshold=0.5)
        assert best is None

    def test_empty_candidates(self):
        resolver = EntityResolver()
        assert resolver.resolve("anything", []) == []
        assert resolver.resolve_best("anything", []) is None
