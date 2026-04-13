"""Entity resolver with pluggable match strategies and chain-based resolution."""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Enums & data classes
# ---------------------------------------------------------------------------

class MatchStrategy(enum.Enum):
    EXACT = "exact"
    LEVENSHTEIN = "levenshtein"
    JARO_WINKLER = "jaro_winkler"
    TOKEN_SET_RATIO = "token_set_ratio"
    METAPHONE = "metaphone"


@dataclass(frozen=True)
class MatchResult:
    entity_id: str
    entity_name: str
    score: float  # 0-1
    strategy: MatchStrategy


@dataclass(frozen=True)
class MatchStep:
    strategy: MatchStrategy
    threshold: float
    weight: float


# ---------------------------------------------------------------------------
# Strategy implementations (pure Python, no external deps)
# ---------------------------------------------------------------------------

def _exact_score(a: str, b: str) -> float:
    return 1.0 if a.lower() == b.lower() else 0.0


def _levenshtein_distance(a: str, b: str) -> int:
    """Classic DP edit distance."""
    la, lb = len(a), len(b)
    if la == 0:
        return lb
    if lb == 0:
        return la
    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        curr = [i] + [0] * lb
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            curr[j] = min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost)
        prev = curr
    return prev[lb]


def _levenshtein_score(a: str, b: str) -> float:
    al, bl = a.lower(), b.lower()
    max_len = max(len(al), len(bl))
    if max_len == 0:
        return 1.0
    return 1.0 - (_levenshtein_distance(al, bl) / max_len)


def _jaro_similarity(a: str, b: str) -> float:
    al, bl = a.lower(), b.lower()
    la, lb = len(al), len(bl)
    if la == 0 and lb == 0:
        return 1.0
    if la == 0 or lb == 0:
        return 0.0

    match_window = max(la, lb) // 2 - 1
    if match_window < 0:
        match_window = 0

    a_matched = [False] * la
    b_matched = [False] * lb
    matches = 0
    transpositions = 0

    for i in range(la):
        start = max(0, i - match_window)
        end = min(lb, i + match_window + 1)
        for j in range(start, end):
            if b_matched[j] or al[i] != bl[j]:
                continue
            a_matched[i] = True
            b_matched[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(la):
        if not a_matched[i]:
            continue
        while not b_matched[k]:
            k += 1
        if al[i] != bl[k]:
            transpositions += 1
        k += 1

    jaro = (matches / la + matches / lb + (matches - transpositions / 2) / matches) / 3
    return jaro


def _jaro_winkler_score(a: str, b: str, prefix_weight: float = 0.1) -> float:
    jaro = _jaro_similarity(a, b)
    al, bl = a.lower(), b.lower()
    prefix_len = 0
    for i in range(min(len(al), len(bl), 4)):
        if al[i] == bl[i]:
            prefix_len += 1
        else:
            break
    return jaro + prefix_len * prefix_weight * (1 - jaro)


def _token_set_ratio(a: str, b: str) -> float:
    """Jaccard similarity on word token sets."""
    sa = set(a.lower().split())
    sb = set(b.lower().split())
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    intersection = sa & sb
    union = sa | sb
    return len(intersection) / len(union)


def _metaphone_encode(word: str) -> str:
    """Basic metaphone encoding (simplified double-metaphone)."""
    w = "".join(c for c in word.upper() if c.isalpha())
    if not w:
        return ""

    # Drop duplicate adjacent letters
    deduped = [w[0]]
    for c in w[1:]:
        if c != deduped[-1]:
            deduped.append(c)
    w = "".join(deduped)

    # Handle initial clusters
    if w[:2] in ("AE", "GN", "KN", "PN", "WR"):
        w = w[1:]
    elif w[0] == "X":
        w = "S" + w[1:]

    result: list[str] = []
    i = 0
    length = len(w)

    while i < length and len(result) < 6:
        c = w[i]

        # Vowels only kept at start
        if c in "AEIOU":
            if i == 0:
                result.append(c)
            i += 1
            continue

        two = w[i:i + 2] if i + 1 < length else ""
        three = w[i:i + 3] if i + 2 < length else ""

        if c == "B":
            # Silent B after M at end
            if i == length - 1 and i > 0 and w[i - 1] == "M":
                i += 1
                continue
            result.append("B")
            i += 1
        elif c == "C":
            if two in ("CI", "CE", "CY"):
                result.append("S")
                i += 2
            elif two == "CH":
                result.append("X")
                i += 2
            else:
                result.append("K")
                i += 1
        elif c == "D":
            if two in ("DG",) and i + 2 < length and w[i + 2] in "IEY":
                result.append("J")
                i += 3
            else:
                result.append("T")
                i += 1
        elif c == "F":
            result.append("F")
            i += 1
        elif c == "G":
            if i + 1 < length and w[i + 1] == "H":
                if i + 2 < length and w[i + 2] not in "AEIOU":
                    # GH not before vowel -> silent
                    i += 2
                    continue
                else:
                    result.append("K")
                    i += 2
            elif two in ("GI", "GE", "GY"):
                result.append("J")
                i += 2
            else:
                result.append("K")
                i += 1
        elif c == "H":
            if i + 1 < length and w[i + 1] in "AEIOU":
                if i == 0 or w[i - 1] not in "AEIOU":
                    result.append("H")
            i += 1
        elif c == "J":
            result.append("J")
            i += 1
        elif c == "K":
            if i > 0 and w[i - 1] == "C":
                i += 1
                continue
            result.append("K")
            i += 1
        elif c == "L":
            result.append("L")
            i += 1
        elif c == "M":
            result.append("M")
            i += 1
        elif c == "N":
            result.append("N")
            i += 1
        elif c == "P":
            if two == "PH":
                result.append("F")
                i += 2
            else:
                result.append("P")
                i += 1
        elif c == "Q":
            result.append("K")
            i += 1
        elif c == "R":
            result.append("R")
            i += 1
        elif c == "S":
            if two == "SH" or three == "SIO" or three == "SIA":
                result.append("X")
                i += 2
            else:
                result.append("S")
                i += 1
        elif c == "T":
            if two == "TH":
                result.append("0")  # theta
                i += 2
            elif three in ("TIA", "TIO"):
                result.append("X")
                i += 3
            else:
                result.append("T")
                i += 1
        elif c == "V":
            result.append("F")
            i += 1
        elif c == "W":
            if i + 1 < length and w[i + 1] in "AEIOU":
                result.append("W")
            i += 1
        elif c == "X":
            result.append("K")
            result.append("S")
            i += 1
        elif c == "Y":
            if i + 1 < length and w[i + 1] in "AEIOU":
                result.append("Y")
            i += 1
        elif c == "Z":
            result.append("S")
            i += 1
        else:
            i += 1

    return "".join(result)


def _metaphone_score(a: str, b: str) -> float:
    """Compare metaphone encodings of each word, return best average match."""
    words_a = a.lower().split()
    words_b = b.lower().split()
    if not words_a or not words_b:
        return 0.0

    enc_a = [_metaphone_encode(w) for w in words_a]
    enc_b = [_metaphone_encode(w) for w in words_b]

    # For each word in a, find best match in b
    total = 0.0
    for ea in enc_a:
        best = 0.0
        for eb in enc_b:
            if ea and eb and ea == eb:
                best = 1.0
                break
            # Partial: shared prefix ratio
            if ea and eb:
                common = 0
                for ca, cb in zip(ea, eb):
                    if ca == cb:
                        common += 1
                    else:
                        break
                ratio = common / max(len(ea), len(eb))
                if ratio > best:
                    best = ratio
        total += best

    return total / len(enc_a)


# Map strategy enum to scoring function
_SCORERS = {
    MatchStrategy.EXACT: _exact_score,
    MatchStrategy.LEVENSHTEIN: _levenshtein_score,
    MatchStrategy.JARO_WINKLER: _jaro_winkler_score,
    MatchStrategy.TOKEN_SET_RATIO: _token_set_ratio,
    MatchStrategy.METAPHONE: _metaphone_score,
}


# ---------------------------------------------------------------------------
# MatchChain
# ---------------------------------------------------------------------------

class MatchChain:
    """Ordered chain of match strategies with early-exit semantics."""

    def __init__(self, steps: list[MatchStep]) -> None:
        self.steps = steps

    def match(self, query: str, candidates: list[tuple[str, str]]) -> list[MatchResult]:
        """Run each step in order. Early exit if any step exceeds its threshold.

        candidates: list of (entity_id, entity_name) tuples.
        Returns matches sorted by combined score descending.
        """
        if not candidates:
            return []

        # Accumulate per-candidate: list of (score, weight) for NoisyOr
        accum: dict[str, list[tuple[float, float]]] = {eid: [] for eid, _ in candidates}
        names: dict[str, str] = {eid: name for eid, name in candidates}
        best_strategy: dict[str, MatchStrategy] = {}

        combiner = NoisyOrCombiner()

        for step in self.steps:
            scorer = _SCORERS[step.strategy]
            has_above_threshold = False

            for eid, name in candidates:
                score = scorer(query, name)
                if score > 0:
                    accum[eid].append((score, step.weight))
                    if eid not in best_strategy or score > accum[eid][-2][0] if len(accum[eid]) > 1 else True:
                        best_strategy[eid] = step.strategy
                if score >= step.threshold:
                    has_above_threshold = True

            if has_above_threshold:
                break

        results: list[MatchResult] = []
        for eid, scores in accum.items():
            if not scores:
                continue
            combined = combiner.combine(scores)
            # Pick strategy of highest individual score
            best_strat = best_strategy.get(eid, self.steps[0].strategy)
            results.append(MatchResult(
                entity_id=eid,
                entity_name=names[eid],
                score=combined,
                strategy=best_strat,
            ))

        results.sort(key=lambda r: r.score, reverse=True)
        return results


# ---------------------------------------------------------------------------
# NoisyOrCombiner
# ---------------------------------------------------------------------------

class NoisyOrCombiner:
    """Combines multiple (score, weight) pairs via Noisy-OR."""

    def combine(self, scores: list[tuple[float, float]]) -> float:
        """1 - product(1 - score * weight) for each (score, weight) pair."""
        if not scores:
            return 0.0
        product = 1.0
        for score, weight in scores:
            product *= (1.0 - score * weight)
        return 1.0 - product


# ---------------------------------------------------------------------------
# EntityResolver
# ---------------------------------------------------------------------------

_DEFAULT_CHAIN = MatchChain([
    MatchStep(strategy=MatchStrategy.EXACT, threshold=1.0, weight=1.0),
    MatchStep(strategy=MatchStrategy.JARO_WINKLER, threshold=0.85, weight=0.8),
    MatchStep(strategy=MatchStrategy.TOKEN_SET_RATIO, threshold=0.7, weight=0.6),
    MatchStep(strategy=MatchStrategy.LEVENSHTEIN, threshold=0.6, weight=0.5),
])


class EntityResolver:
    """Resolves query strings to known entities using a configurable match chain."""

    def __init__(self, chain: MatchChain | None = None) -> None:
        self.chain = chain or _DEFAULT_CHAIN

    def resolve(self, query: str, candidates: list[tuple[str, str]]) -> list[MatchResult]:
        """Return all matches from the chain, sorted by score descending."""
        return self.chain.match(query, candidates)

    def resolve_best(self, query: str, candidates: list[tuple[str, str]], min_threshold: float = 0.3) -> MatchResult | None:
        """Return the top match, or None if below minimum threshold."""
        results = self.resolve(query, candidates)
        if results and results[0].score >= min_threshold:
            return results[0]
        return None
