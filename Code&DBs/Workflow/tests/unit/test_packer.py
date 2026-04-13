"""Tests for memory.packer — context packing with dedup, diversity, and budgets."""

import pytest

from memory.packer import (
    BudgetAllocator,
    ContextPacker,
    ContextSection,
    MMRDiversifier,
    PackedContext,
    SimHash,
    estimate_tokens,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section(name: str, content: str, priority: float = 0.5, tokens: int = 100, source: str = "test") -> ContextSection:
    return ContextSection(name=name, content=content, priority=priority, token_estimate=tokens, source=source)


# ---------------------------------------------------------------------------
# SimHash tests
# ---------------------------------------------------------------------------

class TestSimHash:
    def test_identical_texts_same_hash(self):
        text = "the quick brown fox jumps over the lazy dog"
        assert SimHash.compute(text) == SimHash.compute(text)

    def test_different_texts_different_hash(self):
        a = "the quick brown fox jumps over the lazy dog"
        b = "completely unrelated content about quantum physics experiments"
        assert SimHash.compute(a) != SimHash.compute(b)

    def test_near_duplicate_detection(self):
        a = "the quick brown fox jumps over the lazy dog"
        b = "the quick brown fox leaps over the lazy dog"  # one word changed
        ha, hb = SimHash.compute(a), SimHash.compute(b)
        # Near-duplicates should have small hamming distance
        assert SimHash.hamming_distance(ha, hb) < 20

    def test_hamming_distance_identical(self):
        assert SimHash.hamming_distance(0b1010, 0b1010) == 0

    def test_hamming_distance_all_differ(self):
        assert SimHash.hamming_distance(0b0000, 0b1111) == 4

    def test_is_near_duplicate_true(self):
        assert SimHash.is_near_duplicate(0b1010, 0b1011, threshold=3) is True

    def test_is_near_duplicate_false(self):
        assert SimHash.is_near_duplicate(0b0000, 0b1111, threshold=2) is False

    def test_empty_text(self):
        assert SimHash.compute("") == 0

    def test_single_word(self):
        h = SimHash.compute("hello")
        assert isinstance(h, int)


# ---------------------------------------------------------------------------
# MMR diversifier tests
# ---------------------------------------------------------------------------

class TestMMRDiversifier:
    def test_empty_candidates(self):
        assert MMRDiversifier.select([]) == []

    def test_selects_up_to_limit(self):
        candidates = [_section(f"s{i}", f"content {i}", priority=0.5) for i in range(20)]
        result = MMRDiversifier.select(candidates, limit=5)
        assert len(result) == 5

    def test_diverse_selection_not_just_top_priority(self):
        """MMR should promote diversity, not just pick the top-N by priority."""
        # Three high-priority sections with identical content, one lower-priority but unique
        similar_content = "database schema migration upgrade path for production"
        unique_content = "front-end accessibility audit results for mobile users"

        candidates = [
            _section("db1", similar_content, priority=0.9, tokens=100),
            _section("db2", similar_content + " version two", priority=0.85, tokens=100),
            _section("db3", similar_content + " version three", priority=0.8, tokens=100),
            _section("a11y", unique_content, priority=0.6, tokens=100),
        ]

        result = MMRDiversifier.select(candidates, lambda_param=0.5, limit=3)
        result_names = [s.name for s in result]

        # The unique section should appear even though it has lower priority
        assert "a11y" in result_names

    def test_single_candidate(self):
        candidates = [_section("only", "solo content", priority=0.9)]
        result = MMRDiversifier.select(candidates, limit=5)
        assert len(result) == 1
        assert result[0].name == "only"


# ---------------------------------------------------------------------------
# ContextPacker tests
# ---------------------------------------------------------------------------

class TestContextPacker:
    def test_empty_input(self):
        packer = ContextPacker(token_budget=8000)
        result = packer.pack([])
        assert result.sections == ()
        assert result.total_tokens == 0
        assert result.budget_used_ratio == 0.0
        assert result.dropped_sections == ()

    def test_respects_token_budget(self):
        packer = ContextPacker(token_budget=250)
        sections = [
            _section("a", "alpha content here", priority=0.9, tokens=100),
            _section("b", "beta content different", priority=0.8, tokens=100),
            _section("c", "gamma content unique", priority=0.7, tokens=100),
        ]
        result = packer.pack(sections)
        assert result.total_tokens <= 250

    def test_dropped_sections_tracked(self):
        packer = ContextPacker(token_budget=150)
        sections = [
            _section("fits", "this fits in budget", priority=0.9, tokens=100),
            _section("dropped", "this will not fit in the remaining budget", priority=0.3, tokens=100),
        ]
        result = packer.pack(sections)
        assert len(result.dropped_sections) > 0

    def test_attention_last_positioning(self):
        """Constraints/risks/decisions should appear at the end."""
        packer = ContextPacker(token_budget=10000)
        sections = [
            _section("constraints", "must not exceed rate limits", priority=0.9, tokens=100),
            _section("decisions", "chose postgres over mongo", priority=0.8, tokens=100),
            _section("overview", "project overview and goals", priority=0.7, tokens=100),
            _section("risks", "downtime risk during migration", priority=0.85, tokens=100),
        ]
        result = packer.pack(sections)
        names = [s.name for s in result.sections]

        # Non-attention-last sections come first
        attention_last_names = {"constraints", "decisions", "risks"}
        first_attention_last_idx = None
        last_regular_idx = None

        for i, name in enumerate(names):
            if name in attention_last_names:
                if first_attention_last_idx is None:
                    first_attention_last_idx = i
            else:
                last_regular_idx = i

        if first_attention_last_idx is not None and last_regular_idx is not None:
            assert last_regular_idx < first_attention_last_idx, (
                f"Regular section at {last_regular_idx} appears after attention-last at {first_attention_last_idx}"
            )

    def test_near_duplicate_removal(self):
        """Near-duplicate sections should be deduplicated."""
        packer = ContextPacker(token_budget=10000)
        content = "the quick brown fox jumps over the lazy dog in the park"
        sections = [
            _section("original", content, priority=0.9, tokens=100),
            _section("duplicate", content, priority=0.5, tokens=100),
        ]
        result = packer.pack(sections)
        # Only one should survive dedup
        assert len(result.sections) == 1
        # The higher-priority one should be kept
        assert result.sections[0].name == "original"

    def test_budget_used_ratio(self):
        packer = ContextPacker(token_budget=1000)
        sections = [_section("a", "content alpha", priority=0.9, tokens=500)]
        result = packer.pack(sections)
        assert result.budget_used_ratio == pytest.approx(0.5)

    def test_all_sections_fit(self):
        packer = ContextPacker(token_budget=10000)
        sections = [
            _section("a", "alpha unique content here", priority=0.9, tokens=100),
            _section("b", "beta different content there", priority=0.8, tokens=100),
        ]
        result = packer.pack(sections)
        assert len(result.dropped_sections) == 0
        assert len(result.sections) == 2


# ---------------------------------------------------------------------------
# BudgetAllocator tests
# ---------------------------------------------------------------------------

class TestBudgetAllocator:
    def test_proportional_allocation(self):
        names = ["code", "docs", "tests"]
        weights = {"code": 3.0, "docs": 1.0, "tests": 1.0}
        alloc = BudgetAllocator.allocate(names, weights, total_budget=5000)

        assert alloc["code"] == 3000
        assert alloc["docs"] == 1000
        assert alloc["tests"] == 1000

    def test_empty_names(self):
        assert BudgetAllocator.allocate([], {}, 1000) == {}

    def test_zero_weights_equal_split(self):
        names = ["a", "b"]
        alloc = BudgetAllocator.allocate(names, {"a": 0, "b": 0}, total_budget=1000)
        assert alloc["a"] == 500
        assert alloc["b"] == 500

    def test_single_section_gets_full_budget(self):
        alloc = BudgetAllocator.allocate(["only"], {"only": 1.0}, total_budget=8000)
        assert alloc["only"] == 8000

    def test_missing_weight_treated_as_zero(self):
        names = ["has_weight", "no_weight"]
        weights = {"has_weight": 1.0}
        alloc = BudgetAllocator.allocate(names, weights, total_budget=1000)
        assert alloc["has_weight"] == 1000
        assert alloc["no_weight"] == 0


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------

class TestEstimateTokens:
    def test_basic_estimate(self):
        # 40 chars -> ~10 tokens
        assert estimate_tokens("a" * 40) == 10

    def test_empty_string_returns_one(self):
        assert estimate_tokens("") == 1


# ---------------------------------------------------------------------------
# Frozen dataclass checks
# ---------------------------------------------------------------------------

class TestDataclasses:
    def test_context_section_frozen(self):
        s = _section("x", "y")
        with pytest.raises(AttributeError):
            s.name = "z"  # type: ignore[misc]

    def test_packed_context_frozen(self):
        pc = PackedContext(sections=(), total_tokens=0, budget_used_ratio=0.0, dropped_sections=())
        with pytest.raises(AttributeError):
            pc.total_tokens = 5  # type: ignore[misc]
