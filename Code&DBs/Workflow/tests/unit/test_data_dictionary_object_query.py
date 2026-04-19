"""Unit tests for `handle_query_data_dictionary_object`.

This is the object-kind-oriented query path (new unified registry). The legacy
table-only path is covered by `test_data_dictionary_query.py`.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from runtime.operations.queries import data_dictionary as query_mod
from runtime.operations.queries.data_dictionary import (
    QueryDataDictionaryObject,
    handle_query_data_dictionary_object,
)
from runtime.data_dictionary import DataDictionaryBoundaryError


def _subs(conn: Any = None) -> Any:
    return SimpleNamespace(get_pg_conn=lambda: conn or object())


def test_catalog_scope_returned_when_no_object_kind(monkeypatch) -> None:
    monkeypatch.setattr(
        query_mod,
        "list_object_kinds",
        lambda conn, category=None: [
            {"object_kind": "table:a", "category": "table"},
            {"object_kind": "table:b", "category": "table"},
        ],
    )
    result = handle_query_data_dictionary_object(
        QueryDataDictionaryObject(), _subs()
    )
    assert result["scope"] == "catalog"
    assert result["count"] == 2
    assert result["routed_to"] == "data_dictionary_object"
    assert len(result["objects"]) == 2
    assert "generated_at" in result


def test_catalog_scope_forwards_category_filter(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_list(conn, category=None):
        captured["category"] = category
        return []

    monkeypatch.setattr(query_mod, "list_object_kinds", fake_list)
    result = handle_query_data_dictionary_object(
        QueryDataDictionaryObject(category="tool"), _subs()
    )
    assert captured["category"] == "tool"
    assert result["category"] == "tool"
    assert result["count"] == 0


def test_object_scope_returns_describe_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        query_mod,
        "describe_object",
        lambda conn, object_kind, include_layers: {
            "object": {"object_kind": object_kind, "category": "table"},
            "fields": [{"field_path": "id", "field_kind": "text"}],
            "entries_by_source": {"auto": 1},
        },
    )
    result = handle_query_data_dictionary_object(
        QueryDataDictionaryObject(object_kind="table:orders"), _subs()
    )
    assert result["scope"] == "object"
    assert result["object_kind"] == "table:orders"
    assert result["object"]["object_kind"] == "table:orders"
    assert result["fields"][0]["field_path"] == "id"


def test_object_scope_forwards_include_layers_flag(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_describe(conn, object_kind, include_layers):
        captured["include_layers"] = include_layers
        return {"object": {"object_kind": object_kind}, "fields": [], "layers": []}

    monkeypatch.setattr(query_mod, "describe_object", fake_describe)
    handle_query_data_dictionary_object(
        QueryDataDictionaryObject(object_kind="x", include_layers=True), _subs()
    )
    assert captured["include_layers"] is True


def test_boundary_error_surfaces_as_error_scope(monkeypatch) -> None:
    def raise_boundary(conn, object_kind, include_layers):
        raise DataDictionaryBoundaryError("unknown", status_code=404)

    monkeypatch.setattr(query_mod, "describe_object", raise_boundary)
    result = handle_query_data_dictionary_object(
        QueryDataDictionaryObject(object_kind="ghost"), _subs()
    )
    assert result["scope"] == "error"
    assert result["error"] == "unknown"
    assert result["status_code"] == 404


def test_resolve_conn_errors_when_no_connection_available() -> None:
    # subsystems with neither get_pg_conn nor _postgres_env
    subs = SimpleNamespace()
    with pytest.raises(RuntimeError, match="No Postgres connection"):
        handle_query_data_dictionary_object(
            QueryDataDictionaryObject(), subs
        )


def test_resolve_conn_falls_back_to_postgres_env(monkeypatch) -> None:
    captured_conn = object()
    subs = SimpleNamespace(_postgres_env=lambda: {"pg_conn": captured_conn})

    captured: dict[str, Any] = {}

    def fake_list(conn, category=None):
        captured["conn"] = conn
        return []

    monkeypatch.setattr(query_mod, "list_object_kinds", fake_list)
    handle_query_data_dictionary_object(QueryDataDictionaryObject(), subs)
    assert captured["conn"] is captured_conn


def test_whitespace_only_object_kind_routes_to_catalog(monkeypatch) -> None:
    monkeypatch.setattr(query_mod, "list_object_kinds", lambda conn, category=None: [])
    result = handle_query_data_dictionary_object(
        QueryDataDictionaryObject(object_kind="   "), _subs()
    )
    assert result["scope"] == "catalog"
