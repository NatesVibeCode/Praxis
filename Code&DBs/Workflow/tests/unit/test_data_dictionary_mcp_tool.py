"""Unit tests for the praxis_data_dictionary MCP tool.

Exercises the action dispatch table (`list`, `describe`, `set_override`,
`clear_override`, `reproject`) and its error translation.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from runtime.data_dictionary import DataDictionaryBoundaryError
from surfaces.mcp.tools import data_dictionary as tool_mod
from surfaces.mcp.tools.data_dictionary import (
    TOOLS,
    tool_praxis_data_dictionary,
)


@pytest.fixture(autouse=True)
def _patch_subs(monkeypatch) -> None:
    """Replace the singleton `_subs` with a stub conn so the tool works offline."""
    fake_subs = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(tool_mod, "_subs", fake_subs)


def test_default_action_is_list(monkeypatch) -> None:
    monkeypatch.setattr(
        tool_mod,
        "list_object_kinds",
        lambda conn, category=None: [{"object_kind": "table:a"}],
    )
    result = tool_praxis_data_dictionary({})
    assert result["action"] == "list"
    assert result["count"] == 1


def test_list_action_forwards_category(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_list(conn, category=None):
        captured["category"] = category
        return []

    monkeypatch.setattr(tool_mod, "list_object_kinds", fake_list)
    tool_praxis_data_dictionary({"action": "list", "category": "tool"})
    assert captured["category"] == "tool"


def test_describe_action_merges_result(monkeypatch) -> None:
    monkeypatch.setattr(
        tool_mod,
        "describe_object",
        lambda conn, object_kind, include_layers: {
            "object": {"object_kind": object_kind},
            "fields": [],
            "entries_by_source": {},
        },
    )
    result = tool_praxis_data_dictionary(
        {"action": "describe", "object_kind": "table:orders"}
    )
    assert result["action"] == "describe"
    assert result["object"]["object_kind"] == "table:orders"


def test_set_override_action_wires_all_fields(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_set(conn, **kw):
        captured.update(kw)
        return {"object_kind": kw["object_kind"], "field_path": kw["field_path"]}

    monkeypatch.setattr(tool_mod, "set_operator_override", fake_set)
    result = tool_praxis_data_dictionary(
        {
            "action": "set_override",
            "object_kind": "table:orders",
            "field_path": "status",
            "field_kind": "enum",
            "label": "Status",
            "description": "lifecycle",
            "required": True,
            "default_value": "open",
            "valid_values": ["open", "closed"],
            "examples": ["open"],
            "deprecation_notes": None,
            "display_order": 20,
            "metadata": {"owner": "orders"},
        }
    )
    assert result["action"] == "set_override"
    assert captured["field_kind"] == "enum"
    assert captured["valid_values"] == ["open", "closed"]
    assert captured["required"] is True


def test_clear_override_action_delegates(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_clear(conn, **kw):
        captured.update(kw)
        return {"object_kind": kw["object_kind"], "field_path": kw["field_path"], "removed": True}

    monkeypatch.setattr(tool_mod, "clear_operator_override", fake_clear)
    result = tool_praxis_data_dictionary(
        {"action": "clear_override", "object_kind": "x", "field_path": "a"}
    )
    assert result["action"] == "clear_override"
    assert captured == {"object_kind": "x", "field_path": "a"}
    assert result["removed"] is True


def test_reproject_action_invokes_projector(monkeypatch) -> None:
    monkeypatch.setattr(
        tool_mod,
        "refresh_data_dictionary_authority",
        lambda conn: {
            "ok": True,
            "duration_ms": 7.0,
            "error": None,
            "modules": [{"name": "data_dictionary_projector", "ok": True}],
        },
    )

    result = tool_praxis_data_dictionary({"action": "reproject"})
    assert result == {
        "action": "reproject",
        "ok": True,
        "duration_ms": 7.0,
        "error": None,
        "modules": [{"name": "data_dictionary_projector", "ok": True}],
    }


def test_unknown_action_returns_error() -> None:
    result = tool_praxis_data_dictionary({"action": "whatever"})
    assert "error" in result
    assert "unknown action" in result["error"]


def test_boundary_error_is_serialized_with_status(monkeypatch) -> None:
    def boom(conn, category=None):
        raise DataDictionaryBoundaryError("nope", status_code=404)

    monkeypatch.setattr(tool_mod, "list_object_kinds", boom)
    result = tool_praxis_data_dictionary({"action": "list"})
    assert result == {"error": "nope", "status_code": 404}


def test_unexpected_exception_is_captured_as_error(monkeypatch) -> None:
    def boom(conn, category=None):
        raise RuntimeError("db is on fire")

    monkeypatch.setattr(tool_mod, "list_object_kinds", boom)
    result = tool_praxis_data_dictionary({"action": "list"})
    assert result == {"error": "db is on fire"}


# --- TOOLS registry entry ------------------------------------------------


def test_tools_registry_exposes_praxis_data_dictionary() -> None:
    assert "praxis_data_dictionary" in TOOLS
    impl, definition = TOOLS["praxis_data_dictionary"]
    assert impl is tool_praxis_data_dictionary
    schema = definition["inputSchema"]
    assert schema["required"] == ["action"]
    assert set(schema["properties"]["action"]["enum"]) == {
        "list", "describe", "set_override", "clear_override", "reproject",
    }
