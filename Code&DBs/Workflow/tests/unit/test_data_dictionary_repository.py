"""Unit tests for the Postgres data-dictionary repository.

These verify the validation rules, JSON-encoding behaviour, and the SQL the
repository sends for each write. A fake connection captures SQL + args and
returns canned rows, so tests don't need a live Postgres.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from storage.postgres import data_dictionary_repository as repo
from storage.postgres.validators import PostgresWriteError


class _FakeConn:
    """Record SQL + args, return queued rows from a lookup table."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_returns: list[Any] = []
        self.execute_returns: list[Any] = []

    def _norm(self, sql: str) -> str:
        return " ".join(sql.split()).strip()

    def execute(self, sql: str, *args: Any) -> Any:
        self.calls.append((self._norm(sql), args))
        if self.execute_returns:
            return self.execute_returns.pop(0)
        return []

    def fetchrow(self, sql: str, *args: Any) -> Any:
        self.calls.append((self._norm(sql), args))
        if self.fetchrow_returns:
            return self.fetchrow_returns.pop(0)
        return None


# --- _encode_jsonb -------------------------------------------------------


def test_encode_jsonb_passes_through_valid_json_strings() -> None:
    assert repo._encode_jsonb('{"a":1}') == '{"a":1}'


def test_encode_jsonb_serializes_dict_and_list() -> None:
    assert json.loads(repo._encode_jsonb({"a": 1})) == {"a": 1}
    assert json.loads(repo._encode_jsonb([1, 2])) == [1, 2]


def test_encode_jsonb_defaults_for_none_and_invalid() -> None:
    assert repo._encode_jsonb(None) == "{}"
    assert repo._encode_jsonb(None, default="null") == "null"
    assert repo._encode_jsonb("not-json") == "{}"
    assert repo._encode_jsonb("") == "{}"


# --- upsert_object -------------------------------------------------------


def test_upsert_object_rejects_empty_kind() -> None:
    with pytest.raises(PostgresWriteError, match="object_kind"):
        repo.upsert_object(_FakeConn(), object_kind="   ")


def test_upsert_object_rejects_unknown_category() -> None:
    with pytest.raises(PostgresWriteError, match="category"):
        repo.upsert_object(_FakeConn(), object_kind="x", category="not_real")


def test_upsert_object_issues_insert_on_conflict_with_jsonb(monkeypatch) -> None:
    conn = _FakeConn()
    conn.fetchrow_returns = [
        {"object_kind": "t:x", "label": "X", "category": "table"}
    ]
    result = repo.upsert_object(
        conn,
        object_kind=" t:x ",
        label="X",
        category="table",
        summary="y",
        origin_ref={"a": 1},
        metadata={"b": 2},
    )
    assert result["object_kind"] == "t:x"
    sql, args = conn.calls[0]
    assert "INSERT INTO data_dictionary_objects" in sql
    assert "ON CONFLICT (object_kind) DO UPDATE" in sql
    # jsonb encoded
    assert json.loads(args[4]) == {"a": 1}
    assert json.loads(args[5]) == {"b": 2}
    # text was stripped
    assert args[0] == "t:x"


# --- list_objects / get_object -------------------------------------------


def test_list_objects_with_category_filters_and_preserves_order() -> None:
    conn = _FakeConn()
    conn.execute_returns = [[{"object_kind": "a", "category": "table"}]]
    rows = repo.list_objects(conn, category="table")
    sql, args = conn.calls[0]
    assert args == ("table",)
    assert "WHERE category = $1" in sql
    assert rows[0]["object_kind"] == "a"


def test_list_objects_without_category_orders_by_category_and_kind() -> None:
    conn = _FakeConn()
    conn.execute_returns = [[]]
    repo.list_objects(conn)
    sql, _ = conn.calls[0]
    assert "ORDER BY category, object_kind" in sql


def test_get_object_returns_none_when_missing() -> None:
    conn = _FakeConn()
    conn.fetchrow_returns = [None]
    assert repo.get_object(conn, object_kind="ghost") is None


# --- delete_object -------------------------------------------------------


def test_delete_object_returns_true_when_deleted() -> None:
    conn = _FakeConn()
    conn.fetchrow_returns = [{"object_kind": "gone"}]
    assert repo.delete_object(conn, object_kind="gone") is True


def test_delete_object_returns_false_when_noop() -> None:
    conn = _FakeConn()
    conn.fetchrow_returns = [None]
    assert repo.delete_object(conn, object_kind="never-there") is False


# --- upsert_entry --------------------------------------------------------


def test_upsert_entry_rejects_invalid_source() -> None:
    with pytest.raises(PostgresWriteError, match="source"):
        repo.upsert_entry(
            _FakeConn(),
            object_kind="x",
            field_path="a",
            source="bogus",
            field_kind="text",
        )


def test_upsert_entry_rejects_invalid_field_kind() -> None:
    with pytest.raises(PostgresWriteError, match="field_kind"):
        repo.upsert_entry(
            _FakeConn(),
            object_kind="x",
            field_path="a",
            source="auto",
            field_kind="varchar",  # not a canonical kind
        )


def test_upsert_entry_encodes_defaults_as_null_and_lists_as_empty_arrays() -> None:
    conn = _FakeConn()
    conn.fetchrow_returns = [{"field_path": "a"}]
    repo.upsert_entry(
        conn,
        object_kind="x",
        field_path="a",
        source="auto",
        field_kind="text",
    )
    sql, args = conn.calls[0]
    assert "INSERT INTO data_dictionary_entries" in sql
    # default_value null, valid_values / examples []
    assert args[7] == "null"
    assert args[8] == "[]"
    assert args[9] == "[]"


# --- replace_auto_entries -----------------------------------------------


def test_replace_auto_entries_refuses_operator_source() -> None:
    with pytest.raises(PostgresWriteError, match="operator"):
        repo.replace_auto_entries(
            _FakeConn(), object_kind="x", source="operator", entries=[]
        )


def test_replace_auto_entries_wipes_when_empty_list() -> None:
    conn = _FakeConn()
    repo.replace_auto_entries(
        conn, object_kind="x", source="auto", entries=[]
    )
    # First call is the unconditional delete
    sql, args = conn.calls[0]
    assert "DELETE FROM data_dictionary_entries" in sql
    assert "object_kind = $1 AND source = $2" in sql
    assert "ANY" not in sql
    assert args == ("x", "auto")


def test_replace_auto_entries_retains_listed_paths_and_upserts_each() -> None:
    conn = _FakeConn()
    # after the initial DELETE, upsert_entry fetchrows will return canned rows
    conn.fetchrow_returns = [
        {"field_path": "a"},
        {"field_path": "b"},
    ]
    written = repo.replace_auto_entries(
        conn,
        object_kind="x",
        source="auto",
        entries=[
            {"field_path": "a", "field_kind": "text"},
            {"field_path": "b", "field_kind": "number"},
        ],
    )
    assert written == 2
    delete_sql, delete_args = conn.calls[0]
    assert "NOT (field_path = ANY($3::text[]))" in delete_sql
    assert delete_args[2] == ["a", "b"]
    # two upserts afterwards
    assert len(conn.calls) == 3  # delete + 2 upserts


# --- delete_entry --------------------------------------------------------


def test_delete_entry_returns_bool_and_validates_source() -> None:
    conn = _FakeConn()
    conn.fetchrow_returns = [{"field_path": "a"}]
    assert repo.delete_entry(conn, object_kind="x", field_path="a", source="operator") is True

    with pytest.raises(PostgresWriteError, match="source"):
        repo.delete_entry(_FakeConn(), object_kind="x", field_path="a", source="nope")


# --- list_entries / list_effective_entries -------------------------------


def test_list_entries_with_source_uses_simple_order() -> None:
    conn = _FakeConn()
    conn.execute_returns = [[]]
    repo.list_entries(conn, object_kind="x", source="auto")
    sql, args = conn.calls[0]
    assert args == ("x", "auto")
    assert "ORDER BY display_order, field_path" in sql


def test_list_entries_without_source_orders_by_precedence() -> None:
    conn = _FakeConn()
    conn.execute_returns = [[]]
    repo.list_entries(conn, object_kind="x")
    sql, args = conn.calls[0]
    assert args == ("x",)
    assert "CASE source WHEN 'operator' THEN 0" in sql


def test_list_effective_entries_reads_from_merged_view() -> None:
    conn = _FakeConn()
    conn.execute_returns = [[{"field_path": "a", "effective_source": "operator"}]]
    rows = repo.list_effective_entries(conn, object_kind="x")
    sql, _ = conn.calls[0]
    assert "FROM data_dictionary_effective" in sql
    assert rows[0]["effective_source"] == "operator"


def test_count_entries_by_source_returns_int_map() -> None:
    conn = _FakeConn()
    conn.execute_returns = [[
        {"source": "auto", "n": 3},
        {"source": "operator", "n": 1},
    ]]
    counts = repo.count_entries_by_source(conn, object_kind="x")
    assert counts == {"auto": 3, "operator": 1}
