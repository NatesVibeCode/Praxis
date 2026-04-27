"""Unit tests for runtime/sources/_relevance.py and the source plugins
that use it. Closes BUG-6E719C54.
"""
from __future__ import annotations

from runtime.sources._relevance import query_tokens, token_overlap_score


def test_query_tokens_drops_short_tokens():
    assert query_tokens("a in of provider routing") == ["provider", "routing"]


def test_query_tokens_lowercases():
    assert query_tokens("Provider ROUTING CLI") == ["provider", "routing", "cli"]


def test_token_overlap_score_perfect_match():
    tokens = ["provider", "routing", "cli"]
    assert token_overlap_score(tokens, "Provider routing CLI default") == 1.0


def test_token_overlap_score_partial_match():
    tokens = ["provider", "routing", "cli", "default", "api"]
    score = token_overlap_score(tokens, "Provider routing for the gateway")
    # 2 of 5 tokens match → 0.4
    assert score == 0.4


def test_token_overlap_score_zero_match():
    tokens = ["provider", "routing"]
    assert token_overlap_score(tokens, "totally unrelated content") == 0.0


def test_token_overlap_score_no_tokens_returns_neutral():
    # Query with no matchable tokens (all stopwords / short) → neutral 0.5
    assert token_overlap_score([], "anything") == 0.5


def test_token_overlap_score_empty_text():
    assert token_overlap_score(["x"], "") == 0.0


def test_bugs_source_uses_token_overlap_helper():
    """The bugs source must import and call token_overlap_score, not hardcode 1.0."""
    from runtime.sources import bugs_source

    src = bugs_source.__file__
    text = open(src, encoding="utf-8").read()
    assert "from runtime.sources._relevance import" in text
    assert "token_overlap_score" in text
    # Sanity-check the old hardcoded path is gone
    assert '"score": 1.0,' not in text


def test_receipts_source_uses_token_overlap_helper():
    from runtime.sources import receipts_source

    text = open(receipts_source.__file__, encoding="utf-8").read()
    assert "from runtime.sources._relevance import" in text
    assert "token_overlap_score" in text
    assert '"score": 1.0,' not in text


def test_git_source_uses_token_overlap_helper():
    from runtime.sources import git_source

    text = open(git_source.__file__, encoding="utf-8").read()
    assert "from runtime.sources._relevance import" in text
    assert "token_overlap_score" in text


def test_files_source_uses_token_overlap_helper():
    from runtime.sources import files_source

    text = open(files_source.__file__, encoding="utf-8").read()
    assert "from runtime.sources._relevance import" in text
    assert "token_overlap_score" in text


def test_db_read_source_intentionally_keeps_constant_score():
    """db_read rows are exact predicate matches; score=1.0 is correct there."""
    from runtime.sources import db_read_source

    text = open(db_read_source.__file__, encoding="utf-8").read()
    # No relevance helper import — this is intentional
    assert "token_overlap_score" not in text
    assert '"score": 1.0,' in text
