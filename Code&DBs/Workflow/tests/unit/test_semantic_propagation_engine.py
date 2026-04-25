"""Unit tests for runtime.semantic_propagation_engine."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from runtime.semantic_propagation_engine import (
    SemanticPropagationError,
    fire_causal_propagations,
)


class _FakeConn:
    """Minimal async conn stand-in that serves predicate rows + records emits."""

    def __init__(self, predicates: list[dict[str, Any]]) -> None:
        self._predicates = predicates
        self.cache_invalidations: list[dict[str, Any]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "FROM semantic_predicate_catalog" not in normalized:
            return []
        target_event = args[0]
        return [
            dict(p)
            for p in self._predicates
            if (p.get("propagation_policy") or {}).get("on_event") == target_event
        ]


def _causal_predicate(
    *,
    slug: str,
    on_event: str,
    fires: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "predicate_slug": slug,
        "predicate_kind": "causal",
        "propagation_policy": {"on_event": on_event, "fires": fires},
        "decision_ref": "decision.test",
    }


def test_fire_causal_propagations_invokes_cache_invalidate(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def _fake_emit(
        conn,
        *,
        cache_kind,
        cache_key,
        reason,
        invalidated_by,
        decision_ref=None,
    ):
        captured["cache_kind"] = cache_kind
        captured["cache_key"] = cache_key
        captured["reason"] = reason
        captured["invalidated_by"] = invalidated_by
        return 1234

    import runtime.cache_invalidation as cache_invalidation

    monkeypatch.setattr(cache_invalidation, "aemit_cache_invalidation", _fake_emit)

    conn = _FakeConn(
        [
            _causal_predicate(
                slug="dataset_promotion.invalidates_curated_projection_cache",
                on_event="dataset_promotion_recorded",
                fires=[
                    {
                        "action": "cache_invalidate",
                        "cache_kind_ref": "CACHE_KIND_DATASET_CURATED_PROJECTION",
                        "cache_key_template": "{specialist_target}:{dataset_family}:{split_tag|none}",
                        "reason_template": "promotion {promotion_id}",
                    }
                ],
            )
        ]
    )

    result = asyncio.run(
        fire_causal_propagations(
            conn,
            event_type="dataset_promotion_recorded",
            event_payload={
                "promotion_id": "prom_abc",
                "specialist_target": "slm/review",
                "dataset_family": "sft",
                "split_tag": None,
            },
            emitted_by="test.caller",
        )
    )

    assert result["predicate_count"] == 1
    assert result["skipped"] == []
    assert len(result["fired"]) == 1
    assert captured["cache_kind"] == "dataset_curated_projection"
    assert captured["cache_key"] == "slm/review:sft:none"
    assert captured["reason"] == "promotion prom_abc"
    assert captured["invalidated_by"] == "test.caller"


def test_fire_causal_propagations_skips_unknown_action_kinds() -> None:
    conn = _FakeConn(
        [
            _causal_predicate(
                slug="hypothetical.future_action",
                on_event="some_event",
                fires=[{"action": "supersede"}, {"action": "cache_invalidate", "cache_kind": "k"}],
            )
        ]
    )

    async def _go():
        return await fire_causal_propagations(
            conn,
            event_type="some_event",
            event_payload={},
            emitted_by="test",
        )

    # The cache_invalidate path needs aemit_cache_invalidation; patch it locally.
    import runtime.cache_invalidation as cache_invalidation

    async def _fake_emit(*args, **kwargs):
        return 1

    cache_invalidation.aemit_cache_invalidation = _fake_emit  # type: ignore[assignment]
    try:
        result = asyncio.run(_go())
    finally:
        # Restore — defensive, the module reload would also work.
        import importlib
        importlib.reload(cache_invalidation)

    assert result["predicate_count"] == 1
    assert len(result["fired"]) == 1
    assert len(result["skipped"]) == 1
    assert result["skipped"][0]["reason"] == "no_handler"
    assert result["skipped"][0]["action"] == "supersede"


def test_fire_causal_propagations_returns_empty_when_no_predicates_match() -> None:
    conn = _FakeConn([])

    result = asyncio.run(
        fire_causal_propagations(
            conn,
            event_type="not_in_catalog",
            event_payload={},
            emitted_by="test",
        )
    )

    assert result == {"fired": [], "skipped": [], "predicate_count": 0}


def test_render_template_supports_fallback_for_missing_optional_field() -> None:
    from runtime.semantic_propagation_engine import _render_template

    rendered = _render_template(
        "{specialist_target}:{dataset_family}:{split_tag|none}",
        {"specialist_target": "slm/review", "dataset_family": "sft"},
    )
    assert rendered == "slm/review:sft:none"


def test_resolve_cache_kind_accepts_both_constant_name_and_literal() -> None:
    from runtime.semantic_propagation_engine import _resolve_cache_kind

    assert _resolve_cache_kind("CACHE_KIND_DATASET_CURATED_PROJECTION") == "dataset_curated_projection"
    assert _resolve_cache_kind("dataset_curated_projection") == "dataset_curated_projection"
    with pytest.raises(SemanticPropagationError):
        _resolve_cache_kind("")


def test_propagation_policy_decoded_when_returned_as_jsonb_string() -> None:
    """Rows from asyncpg may arrive with JSONB serialized as str.  The engine
    should decode it before extracting on_event/fires."""

    async def _go():
        class _StringConn:
            async def fetch(self, sql: str, *args: Any):
                return [
                    {
                        "predicate_slug": "x",
                        "predicate_kind": "causal",
                        "propagation_policy": json.dumps(
                            {
                                "on_event": "dataset_promotion_recorded",
                                "fires": [
                                    {
                                        "action": "cache_invalidate",
                                        "cache_kind": "k",
                                        "cache_key_template": "static",
                                    }
                                ],
                            }
                        ),
                        "decision_ref": "decision.test",
                    }
                ]

        captured: dict[str, Any] = {}

        async def _fake_emit(conn, *, cache_kind, cache_key, reason, invalidated_by, decision_ref=None):
            captured["cache_key"] = cache_key
            return 1

        import runtime.cache_invalidation as cache_invalidation
        cache_invalidation.aemit_cache_invalidation = _fake_emit  # type: ignore[assignment]
        try:
            return await fire_causal_propagations(
                _StringConn(),
                event_type="dataset_promotion_recorded",
                event_payload={},
                emitted_by="t",
            ), captured
        finally:
            import importlib
            importlib.reload(cache_invalidation)

    result, captured = asyncio.run(_go())
    assert result["predicate_count"] == 1
    assert captured["cache_key"] == "static"
