from __future__ import annotations

from typing import Any

import runtime.interpretive_context as ctx
from runtime.interpretive_context import (
    InterpretiveContextCandidate,
    attach_interpretive_context_to_items,
    build_interpretive_context,
    build_tool_interpretive_context,
    discover_result_candidates,
    tool_catalog_item_candidates,
    tool_name_candidates,
)


def test_discover_candidates_map_tool_name_and_path_without_duplicates() -> None:
    item = {
        "name": "tool_praxis_data_dictionary",
        "path": "surfaces/mcp/tools/data_dictionary.py",
    }

    candidates = discover_result_candidates(item)

    assert [c.object_kind for c in candidates] == ["tool:praxis_data_dictionary"]
    assert candidates[0].reason == "discover_result.name"


def test_tool_candidates_map_catalog_names() -> None:
    assert tool_name_candidates(
        "praxis_query",
        reason="tool_catalog.describe",
    ) == [
        InterpretiveContextCandidate(
            "tool:praxis_query",
            "tool_catalog.describe",
        )
    ]
    assert tool_name_candidates(
        "tool_praxis_query",
        reason="discover_result.name",
    ) == [
        InterpretiveContextCandidate(
            "tool:praxis_query",
            "discover_result.name",
        )
    ]
    assert tool_name_candidates("query", reason="alias") == []


def test_tool_catalog_item_candidates_use_tool_name() -> None:
    assert tool_catalog_item_candidates({"name": "praxis_bugs"}) == [
        InterpretiveContextCandidate("tool:praxis_bugs", "tool_catalog.item")
    ]


def test_build_interpretive_context_is_bounded_and_non_blocking(monkeypatch) -> None:
    def _describe(conn: Any, *, object_kind: str, include_layers: bool) -> dict[str, Any]:
        assert include_layers is False
        if object_kind == "tool:missing":
            raise RuntimeError("dictionary unavailable")
        return {
            "object": {
                "object_kind": object_kind,
                "category": "tool",
                "label": "praxis_data_dictionary",
                "summary": "A long but useful summary.",
            },
            "fields": [
                {
                    "field_path": "action",
                    "field_kind": "enum",
                    "description": "The operation to perform.",
                    "required": True,
                    "effective_source": "operator",
                    "valid_values": ["list", "describe", "set_override"],
                },
                {
                    "field_path": "object_kind",
                    "field_kind": "text",
                    "description": "Target object kind.",
                    "required": False,
                    "effective_source": "auto",
                },
            ],
            "entries_by_source": {"auto": 2, "operator": 1},
        }

    monkeypatch.setattr(ctx, "describe_object", _describe)

    payload = build_interpretive_context(
        object(),
        candidates=[
            InterpretiveContextCandidate("tool:missing", "test"),
            InterpretiveContextCandidate("tool:praxis_data_dictionary", "test"),
        ],
        max_objects=1,
        max_fields_per_object=1,
    )

    assert payload["authority_mode"] == "interpretive"
    assert payload["enforcement"] == "non_blocking"
    assert payload["primary_consumer"] == "llm"
    assert payload["review_plane"] == "none_inline"
    assert payload["escalation_plane"] == "canonical_bug_or_operator_decision"
    assert payload["escalation"] == "automated_resolve_else_escalate_when_unresolvable"
    assert payload["sources"] == ["data_dictionary_effective"]
    assert payload["payload_limits"] == {
        "max_objects": 1,
        "max_fields_per_object": 1,
    }
    item = payload["items"][0]
    assert item["object_kind"] == "tool:praxis_data_dictionary"
    assert item["fields"] == [
        {
            "field_path": "action",
            "field_kind": "enum",
            "description": "The operation to perform.",
            "required": True,
            "effective_source": "operator",
            "valid_values": ["list", "describe", "set_override"],
        }
    ]
    assert item["omitted_fields"] == 1


def test_build_tool_interpretive_context_uses_single_object(monkeypatch) -> None:
    def _describe(conn: Any, *, object_kind: str, include_layers: bool) -> dict[str, Any]:
        return {
            "object": {
                "object_kind": object_kind,
                "category": "tool",
                "label": "query",
            },
            "fields": [{"field_path": "question", "field_kind": "text"}],
            "entries_by_source": {"operator": 1},
        }

    monkeypatch.setattr(ctx, "describe_object", _describe)

    payload = build_tool_interpretive_context(
        object(),
        tool_name="praxis_query",
        reason="tool_catalog.describe",
    )

    assert payload["items"][0]["object_kind"] == "tool:praxis_query"
    assert payload["items"][0]["attached_because"] == "tool_catalog.describe"


def test_attach_interpretive_context_limits_attached_items(monkeypatch) -> None:
    monkeypatch.setattr(
        ctx,
        "describe_object",
        lambda conn, *, object_kind, include_layers: {
            "object": {"object_kind": object_kind, "category": "tool"},
            "fields": [],
            "entries_by_source": {},
        },
    )

    items = attach_interpretive_context_to_items(
        object(),
        [
            {"name": "tool_praxis_data_dictionary"},
            {"name": "tool_praxis_query"},
        ],
        candidate_fn=discover_result_candidates,
        max_context_items=1,
    )

    assert "interpretive_context" in items[0]
    assert "interpretive_context" not in items[1]
