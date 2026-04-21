from __future__ import annotations

from typing import Any

import runtime.interpretive_context as context_mod
from surfaces.mcp.tools import discover as discover_mod


class _FakeIndexer:
    def search(self, *, query: str, limit: int, kind: str | None, threshold: float):
        return [
            {
                "name": "tool_praxis_data_dictionary",
                "kind": "function",
                "module_path": "surfaces/mcp/tools/data_dictionary.py",
                "cosine_similarity": 0.82,
                "docstring_preview": "Query, describe, and edit the unified data dictionary.",
                "signature": "tool_praxis_data_dictionary(params)",
            },
        ]


def test_discover_attaches_bounded_interpretive_context(monkeypatch) -> None:
    conn = object()
    monkeypatch.setattr(discover_mod._subs, "get_module_indexer", lambda: _FakeIndexer())
    monkeypatch.setattr(discover_mod._subs, "get_pg_conn", lambda: conn)

    def _describe(conn_arg: Any, *, object_kind: str, include_layers: bool) -> dict[str, Any]:
        assert conn_arg is conn
        assert object_kind == "tool:praxis_data_dictionary"
        assert include_layers is False
        return {
            "object": {
                "object_kind": object_kind,
                "category": "tool",
                "label": "praxis_data_dictionary",
                "summary": "Unified data dictionary authority.",
            },
            "fields": [
                {
                    "field_path": "action",
                    "field_kind": "enum",
                    "description": "Action to perform.",
                    "required": True,
                    "effective_source": "auto",
                },
                {
                    "field_path": "object_kind",
                    "field_kind": "text",
                    "description": "Target object kind.",
                    "required": False,
                    "effective_source": "auto",
                },
            ],
            "entries_by_source": {"auto": 2},
        }

    monkeypatch.setattr(context_mod, "describe_object", _describe)

    result = discover_mod.tool_praxis_discover({
        "query": "data dictionary tool",
        "max_context_fields": 1,
    })

    attached = result["results"][0]["interpretive_context"]
    assert attached["authority_mode"] == "interpretive"
    assert attached["primary_consumer"] == "llm"
    assert attached["review_plane"] == "none_inline"
    assert attached["escalation_plane"] == "canonical_bug_or_operator_decision"
    assert attached["items"][0]["object_kind"] == "tool:praxis_data_dictionary"
    assert attached["items"][0]["fields"][0]["field_path"] == "action"
    assert attached["items"][0]["omitted_fields"] == 1


def test_discover_context_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setattr(discover_mod._subs, "get_module_indexer", lambda: _FakeIndexer())

    def _unexpected_conn() -> object:
        raise AssertionError("context lookup should not run")

    monkeypatch.setattr(discover_mod._subs, "get_pg_conn", _unexpected_conn)

    result = discover_mod.tool_praxis_discover({
        "query": "data dictionary tool",
        "include_interpretive_context": False,
    })

    assert "interpretive_context" not in result["results"][0]
