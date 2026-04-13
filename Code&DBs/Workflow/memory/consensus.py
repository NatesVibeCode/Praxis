"""Multi-Signal Consensus Scoring for entity resolution."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Sequence


class SignalType(Enum):
    TEXT = auto()
    GRAPH = auto()
    TEMPORAL = auto()
    VOCAB = auto()
    BOTTOM_UP = auto()


@dataclass(frozen=True)
class Signal:
    signal_type: SignalType
    score: float  # 0-1
    weight: float  # 0-1
    source: str


@dataclass(frozen=True)
class ConsensusResult:
    entity_id_a: str
    entity_id_b: str
    combined_score: float
    signals: tuple[Signal, ...]
    is_match: bool


# ---------------------------------------------------------------------------
# Signal scorers
# ---------------------------------------------------------------------------

class TextSignal:
    """Jaro-Winkler similarity between two name strings."""

    @staticmethod
    def score(name_a: str, name_b: str) -> float:
        s1 = name_a
        s2 = name_b
        if s1 == s2:
            return 1.0
        len1, len2 = len(s1), len(s2)
        if len1 == 0 or len2 == 0:
            return 0.0

        match_distance = max(len1, len2) // 2 - 1
        if match_distance < 0:
            match_distance = 0

        s1_matches = [False] * len1
        s2_matches = [False] * len2

        matches = 0
        transpositions = 0

        for i in range(len1):
            start = max(0, i - match_distance)
            end = min(i + match_distance + 1, len2)
            for j in range(start, end):
                if s2_matches[j] or s1[i] != s2[j]:
                    continue
                s1_matches[i] = True
                s2_matches[j] = True
                matches += 1
                break

        if matches == 0:
            return 0.0

        k = 0
        for i in range(len1):
            if not s1_matches[i]:
                continue
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1

        jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3.0

        # Winkler adjustment
        prefix = 0
        for i in range(min(4, len1, len2)):
            if s1[i] == s2[i]:
                prefix += 1
            else:
                break

        return jaro + prefix * 0.1 * (1.0 - jaro)


class GraphSignal:
    """Jaccard coefficient over shared graph neighbors."""

    @staticmethod
    def score(shared_neighbors: int, total_neighbors: int) -> float:
        if total_neighbors == 0:
            return 0.0
        return shared_neighbors / total_neighbors


class TemporalSignal:
    """Co-occurrence count with exponential recency decay."""

    @staticmethod
    def score(co_occurrence_count: int, recency_days: float, half_life: float = 14.0) -> float:
        decay = math.exp(-math.log(2) * recency_days / half_life)
        return min(1.0, co_occurrence_count * decay)


class VocabSignal:
    """TF-IDF cosine similarity on word tokens (from scratch)."""

    @staticmethod
    def score(text_a: str, text_b: str) -> float:
        tokens_a = text_a.lower().split()
        tokens_b = text_b.lower().split()
        if not tokens_a or not tokens_b:
            return 0.0

        # Build corpus (two documents)
        docs = [tokens_a, tokens_b]
        # Document frequency
        all_terms = set(tokens_a) | set(tokens_b)
        doc_freq: dict[str, int] = {}
        for term in all_terms:
            doc_freq[term] = sum(1 for d in docs if term in d)

        num_docs = 2

        def tfidf_vector(tokens: list[str]) -> dict[str, float]:
            tf: dict[str, float] = {}
            for t in tokens:
                tf[t] = tf.get(t, 0) + 1
            for t in tf:
                tf[t] /= len(tokens)  # normalized TF
            vec: dict[str, float] = {}
            for t in tf:
                idf = math.log(1.0 + num_docs / doc_freq[t])
                vec[t] = tf[t] * idf
            return vec

        vec_a = tfidf_vector(tokens_a)
        vec_b = tfidf_vector(tokens_b)

        # Cosine similarity
        dot = sum(vec_a.get(t, 0.0) * vec_b.get(t, 0.0) for t in all_terms)
        norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
        norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return dot / (norm_a * norm_b)


# ---------------------------------------------------------------------------
# Consensus engine
# ---------------------------------------------------------------------------

_DEFAULT_WEIGHTS: dict[SignalType, float] = {
    SignalType.TEXT: 0.3,
    SignalType.GRAPH: 0.25,
    SignalType.TEMPORAL: 0.2,
    SignalType.VOCAB: 0.15,
    SignalType.BOTTOM_UP: 0.1,
}


class ConsensusEngine:
    """Noisy-OR combination of multi-signal scores."""

    def __init__(
        self,
        match_threshold: float = 0.6,
        weights: dict[SignalType, float] | None = None,
    ) -> None:
        self.match_threshold = match_threshold
        self.weights = dict(weights) if weights else dict(_DEFAULT_WEIGHTS)

    def evaluate(
        self,
        entity_a_id: str,
        entity_b_id: str,
        signals: Sequence[Signal],
    ) -> ConsensusResult:
        if not signals:
            return ConsensusResult(
                entity_id_a=entity_a_id,
                entity_id_b=entity_b_id,
                combined_score=0.0,
                signals=tuple(signals),
                is_match=False,
            )

        # Noisy-OR: 1 - product(1 - score_i * weight_i)
        product = 1.0
        for sig in signals:
            w = self.weights.get(sig.signal_type, sig.weight)
            product *= 1.0 - sig.score * w

        combined = 1.0 - product
        return ConsensusResult(
            entity_id_a=entity_a_id,
            entity_id_b=entity_b_id,
            combined_score=combined,
            signals=tuple(signals),
            is_match=combined >= self.match_threshold,
        )
