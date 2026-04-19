"""Unit tests for the runtime data-dictionary authority module.

Covers `runtime/data_dictionary.py` — the boundary layer between projectors /
operators and the storage repository. Storage calls are patched so these tests
focus on normalization, merge-with-current behaviour, and error translation.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary as authority
from runtime.data_dictionary import (
    DataDictionaryBoundaryError,
    apply_projection,
    clear_operator_override,
    describe_object,
    list_object_kinds,
    normalize_field_kind,
    set_operator_override,
)
from storage.postgres.validators import PostgresWriteError


# --- normalize_field_kind ------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("text", "text"),
        ("TEXT", "text"),
        ("string", "text"),
        ("str", "text"),
        ("varchar", "text"),
        ("int", "number"),
        ("integer", "number"),
        ("float", "number"),
        ("decimal", "number"),
        ("bool", "boolean"),
        ("dict", "object"),
        ("map", "object"),
        ("jsonb", "json"),
        ("list", "array"),
        ("tuple", "array"),
        ("timestamp", "datetime"),
        ("timestamptz", "datetime"),
        ("ref", "reference"),
        ("fk", "reference"),
        ("enum", "enum"),
        (None, "text"),
        ("", "text"),
        ("mystery_kind", "text"),  # unknown → fallback to text
    ],
)
def test_normalize_field_kind_handles_aliases_and_unknowns(value: Any, expected: str) -> None:
    assert normalize_field_kind(value) == expected


# --- apply_projection ----------------------------------------------------


def test_apply_projection_requires_object_kind() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match="object_kind is required"):
        apply_projection(
            conn=object(), object_kind="   ", category="table", entries=[]
        )


def test_apply_projection_rejects_unknown_category() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match="category must be one of"):
        apply_projection(
            conn=object(), object_kind="x", category="nonsense", entries=[]
        )


def test_apply_projection_rejects_operator_source() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match="auto/inferred"):
        apply_projection(
            conn=object(),
            object_kind="x",
            category="table",
            entries=[],
            source="operator",
        )


def test_apply_projection_requires_field_path_on_every_entry() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match=r"entries\[1\]\.field_path"):
        apply_projection(
            conn=object(),
            object_kind="x",
            category="table",
            entries=[{"field_path": "a"}, {"field_path": "  "}],
        )


def test_apply_projection_normalizes_entries_and_forwards_to_repository(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_upsert_object(conn, **kwargs):
        captured["object"] = kwargs
        return kwargs

    def fake_replace(conn, **kwargs):
        captured["replace"] = kwargs
        return len(kwargs["entries"])

    monkeypatch.setattr(authority, "upsert_object", fake_upsert_object)
    monkeypatch.setattr(authority, "replace_auto_entries", fake_replace)

    result = apply_projection(
        conn=object(),
        object_kind="table:orders",
        category="table",
        entries=[
            {"field_path": "id", "field_kind": "uuid", "required": True},
            {"field_path": "status", "field_kind": "string", "valid_values": ["open"]},
        ],
        source="auto",
        label="Orders",
        summary="Orders table",
        origin_ref={"projector": "t"},
    )

    assert result == {
        "object_kind": "table:orders",
        "source": "auto",
        "entries_written": 2,
    }
    assert captured["object"]["object_kind"] == "table:orders"
    assert captured["object"]["category"] == "table"
    assert captured["object"]["label"] == "Orders"
    assert captured["replace"]["source"] == "auto"
    # uuid is not in allowed kinds → normalized to text; string → text
    entries = captured["replace"]["entries"]
    assert entries[0]["field_kind"] == "text"
    assert entries[0]["required"] is True
    assert entries[0]["display_order"] == 10
    assert entries[1]["field_kind"] == "text"
    assert entries[1]["valid_values"] == ["open"]
    assert entries[1]["display_order"] == 20


def test_apply_projection_defaults_label_to_object_kind(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(authority, "upsert_object", lambda conn, **k: captured.update(k) or k)
    monkeypatch.setattr(authority, "replace_auto_entries", lambda conn, **k: 0)
    apply_projection(
        conn=object(),
        object_kind="ingest:x",
        category="ingest",
        entries=[],
    )
    assert captured["label"] == "ingest:x"


def test_apply_projection_translates_storage_error(monkeypatch) -> None:
    def raise_storage(conn, **kw):
        raise PostgresWriteError(
            "data_dictionary.invalid_submission", "bad"
        )

    monkeypatch.setattr(authority, "upsert_object", raise_storage)
    monkeypatch.setattr(authority, "replace_auto_entries", lambda *a, **k: 0)

    with pytest.raises(DataDictionaryBoundaryError) as excinfo:
        apply_projection(
            conn=object(),
            object_kind="x",
            category="table",
            entries=[{"field_path": "a"}],
        )
    assert excinfo.value.status_code == 400


def test_apply_projection_translates_non_submission_errors_as_500(monkeypatch) -> None:
    def raise_storage(conn, **kw):
        raise PostgresWriteError("data_dictionary.other", "boom")

    monkeypatch.setattr(authority, "upsert_object", raise_storage)
    monkeypatch.setattr(authority, "replace_auto_entries", lambda *a, **k: 0)

    with pytest.raises(DataDictionaryBoundaryError) as excinfo:
        apply_projection(
            conn=object(),
            object_kind="x",
            category="table",
            entries=[{"field_path": "a"}],
        )
    assert excinfo.value.status_code == 500


# --- operator overrides --------------------------------------------------


def test_set_operator_override_requires_identifiers() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match="object_kind is required"):
        set_operator_override(conn=object(), object_kind="", field_path="x")
    with pytest.raises(DataDictionaryBoundaryError, match="field_path is required"):
        set_operator_override(conn=object(), object_kind="x", field_path="   ")


def test_set_operator_override_merges_with_current_effective_row(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        authority,
        "list_effective_entries",
        lambda conn, object_kind: [
            {
                "field_path": "status",
                "field_kind": "enum",
                "label": "Status",
                "description": "old description",
                "required": True,
                "default_value": "open",
                "valid_values": ["open", "closed"],
                "examples": ["open"],
                "deprecation_notes": "",
                "display_order": 20,
            }
        ],
    )
    monkeypatch.setattr(
        authority,
        "get_object",
        lambda conn, object_kind: {"object_kind": object_kind, "category": "table"},
    )
    monkeypatch.setattr(
        authority,
        "upsert_object",
        lambda conn, **k: captured.setdefault("object", k),
    )

    def fake_upsert_entry(conn, **kw):
        captured["entry"] = kw
        return kw

    monkeypatch.setattr(authority, "upsert_entry", fake_upsert_entry)

    result = set_operator_override(
        conn=object(),
        object_kind="table:orders",
        field_path="status",
        description="operator-supplied description",
    )

    # description is overridden; other fields pulled from current effective row.
    entry = captured["entry"]
    assert entry["source"] == "operator"
    assert entry["description"] == "operator-supplied description"
    assert entry["field_kind"] == "enum"  # carried over
    assert entry["valid_values"] == ["open", "closed"]
    assert entry["required"] is True
    assert entry["default_value"] == "open"
    assert entry["display_order"] == 20
    assert entry["origin_ref"] == {"source": "operator"}
    assert captured["object"]["category"] == "table"
    assert result["object_kind"] == "table:orders"
    assert result["field_path"] == "status"


def test_set_operator_override_handles_absent_effective_row(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(authority, "list_effective_entries", lambda *a, **k: [])
    monkeypatch.setattr(authority, "get_object", lambda *a, **k: None)
    monkeypatch.setattr(authority, "upsert_object", lambda conn, **k: None)
    monkeypatch.setattr(
        authority,
        "upsert_entry",
        lambda conn, **kw: captured.setdefault("entry", kw) or kw,
    )

    set_operator_override(
        conn=object(),
        object_kind="brand:new",
        field_path="x",
        label="X",
        field_kind="number",
    )
    entry = captured["entry"]
    assert entry["label"] == "X"
    assert entry["field_kind"] == "number"
    # defaults when no current row
    assert entry["required"] is False
    assert entry["display_order"] == 100


def test_clear_operator_override_requires_identifiers() -> None:
    with pytest.raises(DataDictionaryBoundaryError):
        clear_operator_override(conn=object(), object_kind="", field_path="a")
    with pytest.raises(DataDictionaryBoundaryError):
        clear_operator_override(conn=object(), object_kind="a", field_path="")


def test_clear_operator_override_delegates_to_delete_entry(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_delete(conn, **kw):
        captured.update(kw)
        return True

    monkeypatch.setattr(authority, "delete_entry", fake_delete)
    result = clear_operator_override(
        conn=object(),
        object_kind="t:x",
        field_path="a",
    )
    assert result == {"object_kind": "t:x", "field_path": "a", "removed": True}
    assert captured["source"] == "operator"


# --- read API ------------------------------------------------------------


def test_list_object_kinds_rejects_unknown_category() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match="category must be one of"):
        list_object_kinds(conn=object(), category="nonsense")


def test_list_object_kinds_attaches_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        authority,
        "list_objects",
        lambda conn, category=None: [
            {"object_kind": "table:a", "label": "A", "category": "table"},
            {"object_kind": "table:b", "label": "B", "category": "table"},
        ],
    )

    def fake_counts(conn, object_kind):
        return {"auto": 3} if object_kind == "table:a" else {"auto": 1, "operator": 1}

    monkeypatch.setattr(authority, "count_entries_by_source", fake_counts)
    rows = list_object_kinds(conn=object())
    assert rows[0]["entries_by_source"] == {"auto": 3}
    assert rows[1]["entries_by_source"] == {"auto": 1, "operator": 1}


def test_describe_object_requires_kind() -> None:
    with pytest.raises(DataDictionaryBoundaryError, match="object_kind is required"):
        describe_object(conn=object(), object_kind="")


def test_describe_object_404_when_unknown(monkeypatch) -> None:
    monkeypatch.setattr(authority, "get_object", lambda conn, object_kind: None)
    with pytest.raises(DataDictionaryBoundaryError) as excinfo:
        describe_object(conn=object(), object_kind="ghost")
    assert excinfo.value.status_code == 404


def test_describe_object_merges_header_fields_and_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        authority,
        "get_object",
        lambda conn, object_kind: {"object_kind": object_kind, "category": "table"},
    )
    monkeypatch.setattr(
        authority,
        "list_effective_entries",
        lambda conn, object_kind: [{"field_path": "a", "field_kind": "text"}],
    )
    monkeypatch.setattr(
        authority,
        "count_entries_by_source",
        lambda conn, object_kind: {"auto": 1},
    )
    monkeypatch.setattr(
        authority,
        "list_entries",
        lambda conn, object_kind: [
            {"field_path": "a", "field_kind": "text", "source": "auto"},
        ],
    )

    # Without layers
    result = describe_object(conn=object(), object_kind="table:a")
    assert "layers" not in result
    assert result["fields"] == [{"field_path": "a", "field_kind": "text"}]
    assert result["entries_by_source"] == {"auto": 1}

    # With layers
    result = describe_object(conn=object(), object_kind="table:a", include_layers=True)
    assert result["layers"][0]["source"] == "auto"
