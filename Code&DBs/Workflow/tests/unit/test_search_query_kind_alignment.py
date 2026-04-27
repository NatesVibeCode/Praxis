"""Verifier test for BUG-36B00C79 — search query operations must use
``operation_kind='query'`` consistently across:
  - operation_catalog_registry.operation_kind
  - authority_object_registry.object_kind
  - data_dictionary_objects.category

Migration 279 widens the authority_object_registry.object_kind check to
allow 'query' and updates existing search.* rows. Migration 270's
data_dictionary_objects.category constraint already includes 'query'.
"""
from __future__ import annotations

import pytest


def _live_pg_conn():
    try:
        from surfaces.mcp.subsystems import _subs
        return _subs.get_pg_conn()
    except Exception as exc:
        pytest.skip(f"live pg conn unavailable: {exc}")


def test_authority_object_registry_constraint_accepts_query():
    conn = _live_pg_conn()
    rows = conn.execute(
        "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
        "WHERE conname = 'authority_object_registry_object_kind_check'"
    )
    rows = list(rows)
    assert rows, "constraint missing"
    definition = (
        rows[0].get("def") if hasattr(rows[0], "get") else rows[0]["def"]
    )
    assert "'query'" in definition, (
        f"authority_object_registry.object_kind constraint must include 'query'; "
        f"got: {definition}"
    )


def test_data_dictionary_objects_constraint_accepts_query():
    conn = _live_pg_conn()
    rows = list(
        conn.execute(
            "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
            "WHERE conname = 'data_dictionary_objects_category_check'"
        )
    )
    assert rows, "constraint missing"
    definition = (
        rows[0].get("def") if hasattr(rows[0], "get") else rows[0]["def"]
    )
    assert "'query'" in definition


def test_search_op_rows_aligned_to_query_kind():
    """Every search.* row in authority_object_registry uses object_kind='query'."""
    conn = _live_pg_conn()
    rows = list(
        conn.execute(
            "SELECT object_kind, COUNT(*) AS n FROM authority_object_registry "
            "WHERE object_ref LIKE 'operation.search.%' GROUP BY object_kind"
        )
    )
    by_kind = {
        (r.get("object_kind") if hasattr(r, "get") else r["object_kind"]): int(
            r.get("n") if hasattr(r, "get") else r["n"]
        )
        for r in rows
    }
    assert by_kind.get("query", 0) > 0, (
        f"search.* rows must use object_kind='query'; got {by_kind}"
    )
    assert "command" not in by_kind, (
        f"search.* rows must not regress to object_kind='command'; got {by_kind}"
    )


def test_search_op_rows_aligned_to_query_category():
    """Every search.* row in data_dictionary_objects uses category='query'."""
    conn = _live_pg_conn()
    rows = list(
        conn.execute(
            "SELECT category, COUNT(*) AS n FROM data_dictionary_objects "
            "WHERE object_kind LIKE 'operation.search.%' GROUP BY category"
        )
    )
    by_cat = {
        (r.get("category") if hasattr(r, "get") else r["category"]): int(
            r.get("n") if hasattr(r, "get") else r["n"]
        )
        for r in rows
    }
    assert by_cat.get("query", 0) > 0, by_cat
    assert "command" not in by_cat, by_cat


def test_operation_catalog_registry_search_ops_are_query_kind():
    """Sanity check: operation_catalog_registry already used 'query' all along."""
    conn = _live_pg_conn()
    rows = list(
        conn.execute(
            "SELECT operation_kind, COUNT(*) AS n FROM operation_catalog_registry "
            "WHERE operation_name LIKE 'search.%' GROUP BY operation_kind"
        )
    )
    by_kind = {
        (r.get("operation_kind") if hasattr(r, "get") else r["operation_kind"]): int(
            r.get("n") if hasattr(r, "get") else r["n"]
        )
        for r in rows
    }
    assert by_kind.get("query", 0) >= 10, by_kind
