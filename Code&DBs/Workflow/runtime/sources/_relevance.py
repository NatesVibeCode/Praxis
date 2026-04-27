"""Shared relevance scoring for non-semantic search source plugins.

Sources without a real similarity score (BugTracker, receipt_store,
file enumeration, git log) used to hardcode ``score = 1.0`` for every
hit, which crowded out semantic-scored sources in the federated rank.
Position-decay was a Band-Aid (BUG-6E719C54). Token-overlap is the
right answer: a hit's score is the fraction of query tokens that
appear in its text payload.

Cross-source comparable to cosine similarity from code/knowledge —
a 1.00 here means "every query token appeared in this row's text",
matching the semantic 1.00 meaning of "perfectly aligned vector".
"""
from __future__ import annotations


_MIN_TOKEN_LEN = 3


def query_tokens(query: str) -> list[str]:
    """Lowercase tokens of length >= 3 from the query.

    Filters short tokens that match too generously (``a``, ``of``,
    ``in``) — they'd inflate scores for unrelated rows.
    """
    return [t for t in query.lower().split() if len(t) >= _MIN_TOKEN_LEN]


def token_overlap_score(tokens: list[str], text: str) -> float:
    """Score a row by what fraction of query tokens appear in its text.

    Returns 0.0 when no tokens match — caller can drop the row entirely
    so the source doesn't pollute federated rank with tangential hits.
    Returns 0.5 (neutral) when the query has no matchable tokens at all,
    so a structured-query call (e.g. ``query="."`` with a ``scope.paths``
    filter) doesn't get rejected — the path glob is the actual filter.
    """
    if not tokens:
        return 0.5
    if not text:
        return 0.0
    haystack = text.lower()
    matched = sum(1 for t in tokens if t in haystack)
    if matched == 0:
        return 0.0
    return round(matched / len(tokens), 4)


__all__ = ["query_tokens", "token_overlap_score"]
