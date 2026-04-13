"""Context packing with deduplication, diversity selection, and budget management."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ContextSection:
    """A single section of context to be packed into a prompt."""

    name: str
    content: str
    priority: float  # 0-1
    token_estimate: int
    source: str


@dataclass(frozen=True)
class PackedContext:
    """Result of packing sections into a token budget."""

    sections: tuple[ContextSection, ...]
    total_tokens: int
    budget_used_ratio: float
    dropped_sections: tuple[str, ...]


# ---------------------------------------------------------------------------
# SimHash -- locality-sensitive fingerprinting
# ---------------------------------------------------------------------------

class SimHash:
    """SimHash fingerprinting for near-duplicate detection."""

    @staticmethod
    def compute(text: str, num_bits: int = 64) -> int:
        """Compute a simhash fingerprint using word-level shingles."""
        words = text.lower().split()
        if not words:
            return 0

        # Build shingles (bigrams; fall back to unigrams for single words)
        if len(words) == 1:
            shingles = [words[0]]
        else:
            shingles = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]

        vector = [0] * num_bits

        for shingle in shingles:
            h = int(hashlib.md5(shingle.encode("utf-8")).hexdigest(), 16)
            for i in range(num_bits):
                if h & (1 << i):
                    vector[i] += 1
                else:
                    vector[i] -= 1

        fingerprint = 0
        for i in range(num_bits):
            if vector[i] > 0:
                fingerprint |= 1 << i
        return fingerprint

    @staticmethod
    def hamming_distance(a: int, b: int) -> int:
        """Count differing bits between two hashes."""
        return bin(a ^ b).count("1")

    @staticmethod
    def is_near_duplicate(a: int, b: int, threshold: int = 3) -> bool:
        """Return True when hashes are within *threshold* bits of each other."""
        return SimHash.hamming_distance(a, b) <= threshold


# ---------------------------------------------------------------------------
# MMR diversifier
# ---------------------------------------------------------------------------

def _jaccard_similarity(text_a: str, text_b: str) -> float:
    """Word-set Jaccard similarity between two strings."""
    set_a = set(text_a.lower().split())
    set_b = set(text_b.lower().split())
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


class MMRDiversifier:
    """Maximal Marginal Relevance selection for context sections."""

    @staticmethod
    def select(
        candidates: list[ContextSection],
        lambda_param: float = 0.7,
        limit: int = 10,
    ) -> list[ContextSection]:
        """Select up to *limit* sections balancing relevance and diversity.

        Score = lambda * priority  -  (1 - lambda) * max_sim_to_selected
        """
        if not candidates:
            return []

        remaining = list(candidates)
        selected: list[ContextSection] = []

        while remaining and len(selected) < limit:
            best_score = -float("inf")
            best_idx = 0

            for idx, cand in enumerate(remaining):
                relevance = cand.priority

                if selected:
                    max_sim = max(
                        _jaccard_similarity(cand.content, s.content)
                        for s in selected
                    )
                else:
                    max_sim = 0.0

                score = lambda_param * relevance - (1 - lambda_param) * max_sim
                if score > best_score:
                    best_score = score
                    best_idx = idx

            selected.append(remaining.pop(best_idx))

        return selected


# ---------------------------------------------------------------------------
# Budget allocator
# ---------------------------------------------------------------------------

class BudgetAllocator:
    """Proportional token-budget allocation across named sections."""

    @staticmethod
    def allocate(
        section_names: list[str],
        weights: dict[str, float],
        total_budget: int,
    ) -> dict[str, int]:
        """Distribute *total_budget* tokens proportionally by weight."""
        if not section_names:
            return {}

        total_weight = sum(weights.get(n, 0.0) for n in section_names)
        if total_weight == 0:
            per_section = total_budget // len(section_names)
            return {n: per_section for n in section_names}

        result: dict[str, int] = {}
        for name in section_names:
            w = weights.get(name, 0.0)
            result[name] = int((w / total_weight) * total_budget)
        return result


# ---------------------------------------------------------------------------
# Attention-last category helpers
# ---------------------------------------------------------------------------

_ATTENTION_LAST_KEYWORDS = {"constraint", "constraints", "risk", "risks", "decision", "decisions"}


def _is_attention_last(section: ContextSection) -> bool:
    """Sections whose name contains constraint/risk/decision go last."""
    name_lower = section.name.lower()
    return any(kw in name_lower for kw in _ATTENTION_LAST_KEYWORDS)


# ---------------------------------------------------------------------------
# Token estimation helper
# ---------------------------------------------------------------------------

def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# Context packer
# ---------------------------------------------------------------------------

class ContextPacker:
    """Pack context sections into a token budget with dedup, diversity, and ordering."""

    def __init__(self, token_budget: int = 8000) -> None:
        self.token_budget = token_budget

    def pack(self, sections: list[ContextSection]) -> PackedContext:
        if not sections:
            return PackedContext(
                sections=(),
                total_tokens=0,
                budget_used_ratio=0.0,
                dropped_sections=(),
            )

        # 1. Deduplicate near-duplicates via SimHash
        deduped = self._deduplicate(sections)

        # 2. Apply MMR for diversity
        diverse = MMRDiversifier.select(deduped, lambda_param=0.7, limit=len(deduped))

        # 3. Greedy knapsack by priority/token ratio
        filled, dropped = self._knapsack_fill(diverse)

        # 4. Attention-last positioning (constraints/risks/decisions at end)
        ordered = self._order_attention_last(filled)

        total = sum(s.token_estimate for s in ordered)
        ratio = total / self.token_budget if self.token_budget else 0.0

        return PackedContext(
            sections=tuple(ordered),
            total_tokens=total,
            budget_used_ratio=ratio,
            dropped_sections=tuple(dropped),
        )

    # -- internal helpers ---------------------------------------------------

    def _deduplicate(self, sections: list[ContextSection]) -> list[ContextSection]:
        """Remove near-duplicate sections, keeping the higher-priority one."""
        kept: list[ContextSection] = []
        hashes: list[int] = []

        # Process in priority order so we keep the best version
        for sec in sorted(sections, key=lambda s: s.priority, reverse=True):
            h = SimHash.compute(sec.content)
            if any(SimHash.is_near_duplicate(h, existing) for existing in hashes):
                continue
            kept.append(sec)
            hashes.append(h)

        return kept

    def _knapsack_fill(self, sections: list[ContextSection]) -> tuple[list[ContextSection], list[str]]:
        """Greedy fill by priority-to-token ratio."""
        ranked = sorted(
            sections,
            key=lambda s: (s.priority / max(s.token_estimate, 1)),
            reverse=True,
        )
        filled: list[ContextSection] = []
        dropped: list[str] = []
        remaining_budget = self.token_budget

        for sec in ranked:
            if sec.token_estimate <= remaining_budget:
                filled.append(sec)
                remaining_budget -= sec.token_estimate
            else:
                dropped.append(sec.name)

        return filled, dropped

    @staticmethod
    def _order_attention_last(sections: list[ContextSection]) -> list[ContextSection]:
        """Put constraint/risk/decision sections at the end."""
        front = [s for s in sections if not _is_attention_last(s)]
        back = [s for s in sections if _is_attention_last(s)]
        return front + back
