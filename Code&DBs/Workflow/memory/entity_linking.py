from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class MentionDetection:
    entity_id: str
    entity_name: str
    mention_text: str
    position: int
    confidence: float


@dataclass(frozen=True)
class CoOccurrence:
    entity_a_id: str
    entity_b_id: str
    count: int
    pmi_score: float
    contexts: tuple[str, ...]


class EntityLinker:
    """Detect and link entity mentions in text via fuzzy matching."""

    def __init__(self, known_entities: list[tuple[str, str]]) -> None:
        # Store as (entity_id, entity_name) pairs.
        self._entities: list[tuple[str, str]] = list(known_entities)

    def detect_mentions(self, text: str) -> list[MentionDetection]:
        """Fuzzy-match entity names in *text*.

        Strategy:
        1. Word-boundary match (higher confidence).
        2. Case-insensitive substring match (lower confidence).
        Results are sorted by position, then descending confidence.
        """
        mentions: list[MentionDetection] = []
        text_lower = text.lower()

        for entity_id, entity_name in self._entities:
            name_lower = entity_name.lower()
            # Word-boundary pattern: \\b around the literal name.
            wb_pattern = re.compile(
                r"\b" + re.escape(name_lower) + r"\b", re.IGNORECASE
            )
            seen_positions: set[int] = set()

            for m in wb_pattern.finditer(text):
                mentions.append(
                    MentionDetection(
                        entity_id=entity_id,
                        entity_name=entity_name,
                        mention_text=m.group(),
                        position=m.start(),
                        confidence=0.9,
                    )
                )
                seen_positions.add(m.start())

            # Substring fallback: find occurrences not already covered.
            start = 0
            while True:
                idx = text_lower.find(name_lower, start)
                if idx == -1:
                    break
                if idx not in seen_positions:
                    mentions.append(
                        MentionDetection(
                            entity_id=entity_id,
                            entity_name=entity_name,
                            mention_text=text[idx : idx + len(entity_name)],
                            position=idx,
                            confidence=0.6,
                        )
                    )
                start = idx + 1

        mentions.sort(key=lambda m: (m.position, -m.confidence))
        return mentions

    def link_mentions(self, text: str) -> list[tuple[str, str, str]]:
        """Return ``(entity_id, entity_name, matched_text)`` tuples.

        De-duplicates by (entity_id, position), keeping the highest-confidence
        match at each position.
        """
        detections = self.detect_mentions(text)
        best: dict[tuple[str, int], MentionDetection] = {}
        for det in detections:
            key = (det.entity_id, det.position)
            if key not in best or det.confidence > best[key].confidence:
                best[key] = det
        return [
            (d.entity_id, d.entity_name, d.mention_text)
            for d in sorted(best.values(), key=lambda d: d.position)
        ]


class CoOccurrenceDiscovery:
    """Track entity co-occurrence across documents and compute PMI."""

    def __init__(self) -> None:
        self._pair_counts: defaultdict[tuple[str, str], int] = defaultdict(int)
        self._entity_counts: defaultdict[str, int] = defaultdict(int)
        self._pair_contexts: defaultdict[tuple[str, str], list[str]] = defaultdict(list)
        self._doc_count: int = 0

    def record(self, text: str, linker: EntityLinker) -> None:
        """Detect mentions in *text* and record co-occurring entity pairs."""
        self._doc_count += 1
        detections = linker.detect_mentions(text)
        # Unique entity ids in this document.
        entity_ids = sorted({d.entity_id for d in detections})

        for eid in entity_ids:
            self._entity_counts[eid] += 1

        for i, a in enumerate(entity_ids):
            for b in entity_ids[i + 1 :]:
                pair = (a, b)
                self._pair_counts[pair] += 1
                # Keep a short context snippet.
                snippet = text[:120] if len(text) > 120 else text
                self._pair_contexts[pair].append(snippet)

    def compute_pmi(self, total_documents: int) -> list[CoOccurrence]:
        """Compute pointwise mutual information for all observed pairs.

        PMI = log2(P(a,b) / (P(a) * P(b)))
        """
        results: list[CoOccurrence] = []
        n = total_documents if total_documents > 0 else 1
        for (a, b), count in self._pair_counts.items():
            p_ab = count / n
            p_a = self._entity_counts[a] / n
            p_b = self._entity_counts[b] / n
            denom = p_a * p_b
            if denom > 0 and p_ab > 0:
                pmi = math.log2(p_ab / denom)
            else:
                pmi = 0.0
            results.append(
                CoOccurrence(
                    entity_a_id=a,
                    entity_b_id=b,
                    count=count,
                    pmi_score=pmi,
                    contexts=tuple(self._pair_contexts[(a, b)]),
                )
            )
        results.sort(key=lambda c: c.pmi_score, reverse=True)
        return results

    def top_pairs(self, limit: int = 20) -> list[CoOccurrence]:
        """Return top co-occurring pairs sorted by PMI descending."""
        n = self._doc_count if self._doc_count > 0 else 1
        return self.compute_pmi(n)[:limit]
