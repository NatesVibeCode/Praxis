"""Unit tests for the search envelope parser."""
from __future__ import annotations

import pytest

from surfaces.mcp.tools._search_envelope import (
    MODE_AUTO,
    MODE_EXACT,
    MODE_REGEX,
    MODE_SEMANTIC,
    SHAPE_CONTEXT,
    SHAPE_FULL,
    SHAPE_MATCH,
    SOURCE_CODE,
    SearchEnvelopeError,
    build_response,
    parse_envelope,
    resolve_mode,
)


def test_parse_envelope_minimum():
    env = parse_envelope({"query": "retry logic"})
    assert env.query == "retry logic"
    assert env.mode == MODE_AUTO
    assert env.sources == (SOURCE_CODE,)
    assert env.shape == SHAPE_CONTEXT
    assert env.context_lines == 5
    assert env.limit == 20
    assert env.explain is False


def test_parse_envelope_full():
    env = parse_envelope(
        {
            "query": "/class.*Authority/",
            "mode": "regex",
            "sources": ["code", "knowledge"],
            "scope": {
                "paths": ["runtime/**/*.py"],
                "exclude_paths": ["tests/**"],
                "since_iso": "2026-04-01T00:00:00",
                "exclude_terms": ["legacy"],
                "type_slug": "workflow_run",
                "entity_kind": "class",
            },
            "shape": "match",
            "context_lines": 0,
            "limit": 50,
            "explain": True,
        }
    )
    assert env.mode == MODE_REGEX
    assert env.sources == ("code", "knowledge")
    assert env.scope.paths == ("runtime/**/*.py",)
    assert env.scope.exclude_paths == ("tests/**",)
    assert env.scope.since_iso == "2026-04-01T00:00:00"
    assert env.scope.type_slug == "workflow_run"
    assert env.scope.entity_kind == "class"
    assert env.scope.exclude_terms == ("legacy",)
    assert env.shape == SHAPE_MATCH
    assert env.limit == 50
    assert env.explain is True


def test_parse_envelope_rejects_empty_query():
    with pytest.raises(SearchEnvelopeError):
        parse_envelope({"query": ""})


def test_parse_envelope_rejects_unknown_mode():
    with pytest.raises(SearchEnvelopeError):
        parse_envelope({"query": "x", "mode": "fuzzy"})


def test_parse_envelope_rejects_unknown_shape():
    with pytest.raises(SearchEnvelopeError):
        parse_envelope({"query": "x", "shape": "snippet"})


def test_parse_envelope_clamps_limit_and_context():
    env = parse_envelope({"query": "x", "limit": 99999, "context_lines": 99999})
    assert env.limit <= 200
    assert env.context_lines <= 200


def test_resolve_mode_auto_regex():
    env = parse_envelope({"query": "/class.*Authority/"})
    assert resolve_mode(env) == MODE_REGEX


def test_resolve_mode_auto_exact_quoted():
    env = parse_envelope({"query": '"subprocess.run"'})
    assert resolve_mode(env) == MODE_EXACT


def test_resolve_mode_auto_semantic_default():
    env = parse_envelope({"query": "retry logic with backoff"})
    assert resolve_mode(env) == MODE_SEMANTIC


def test_build_response_shape():
    env = parse_envelope({"query": "x"})
    payload = build_response(
        envelope=env,
        results=[{"source": "code", "score": 0.9}],
        sources_status={"code": "ok"},
        freshness={"code": {"total_indexed": 12}},
    )
    assert payload["ok"] is True
    assert payload["count"] == 1
    assert payload["_meta"]["sources_queried"] == ["code"]
    assert payload["_meta"]["source_status"] == {"code": "ok"}
    assert payload["_meta"]["index_freshness_per_source"]["code"]["total_indexed"] == 12
    assert payload["_meta"]["mode_resolved"] in {
        MODE_SEMANTIC,
        MODE_EXACT,
        MODE_REGEX,
    }
