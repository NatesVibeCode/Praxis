"""Unit tests for the data dictionary projector.

The projector sweeps every injection site (Postgres tables, object_types,
integrations, dataset families, ingest kinds, operator decision kinds,
receipts, MCP tools) and calls `apply_projection` once per object_kind. These
tests stub out each data source and assert the projector calls
`apply_projection` with the expected shape.
"""
from __future__ import annotations

from typing import Any

import pytest

from memory import data_dictionary_projector as projector
from memory.data_dictionary_projector import (
    DataDictionaryProjector,
    _json_schema_to_field_kind,
    _sql_to_field_kind,
)
from runtime.integration_manifest import ManifestLoadReport


# --- helpers ----------------------------------------------------------------


@pytest.mark.parametrize(
    "sql_type,expected",
    [
        ("integer", "number"),
        ("bigint", "number"),
        ("smallint", "number"),
        ("numeric", "number"),
        ("real", "number"),
        ("double precision", "number"),
        ("boolean", "boolean"),
        ("date", "date"),
        ("timestamp", "datetime"),
        ("timestamp with time zone", "datetime"),
        ("jsonb", "json"),
        ("json", "json"),
        ("uuid", "text"),
        ("text", "text"),
        ("character varying", "text"),
        ("ARRAY", "array"),
        ("unknown_sql_type", "text"),  # fallback
        (None, "text"),
    ],
)
def test_sql_to_field_kind_maps_all_known_types(sql_type, expected) -> None:
    assert _sql_to_field_kind(sql_type) == expected


@pytest.mark.parametrize(
    "prop,expected",
    [
        ({"type": "string"}, "text"),
        ({"type": "integer"}, "number"),
        ({"type": "number"}, "number"),
        ({"type": "boolean"}, "boolean"),
        ({"type": "array"}, "array"),
        ({"type": "object"}, "object"),
        ({"type": "string", "enum": ["a", "b"]}, "enum"),
        ({"enum": ["x"]}, "enum"),
        ({}, "text"),
        ({"type": "anything_else"}, "text"),
    ],
)
def test_json_schema_to_field_kind(prop, expected) -> None:
    assert _json_schema_to_field_kind(prop) == expected


# --- projector plumbing -----------------------------------------------------


class _FakeConn:
    def __init__(self, responses: dict[str, list[dict[str, Any]]]) -> None:
        self._responses = responses

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        for key, rows in self._responses.items():
            if key in sql:
                return rows
        return []


def test_project_tables_emits_one_apply_projection_per_table(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        projector,
        "apply_projection",
        lambda conn, **kw: calls.append(kw),
    )

    conn = _FakeConn({
        "information_schema.columns": [
            {
                "table_name": "orders",
                "column_name": "id",
                "data_type": "uuid",
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 1,
                "column_description": None,
            },
            {
                "table_name": "orders",
                "column_name": "status",
                "data_type": "text",
                "is_nullable": "YES",
                "column_default": "'open'",
                "ordinal_position": 2,
                "column_description": "row status",
            },
        ],
        "pg_constraint": [],
        "pg_class": [{"table_name": "orders", "table_comment": "Orders"}],
    })

    DataDictionaryProjector(conn)._project_tables()

    assert len(calls) == 1
    call = calls[0]
    assert call["object_kind"] == "table:orders"
    assert call["category"] == "table"
    assert call["source"] == "auto"
    assert call["summary"] == "Orders"

    entries = call["entries"]
    assert [e["field_path"] for e in entries] == ["id", "status"]
    assert entries[0]["required"] is True  # NOT NULL
    assert entries[0]["field_kind"] == "text"  # uuid → text per map
    assert entries[1]["required"] is False  # is_nullable="YES"
    assert entries[1]["description"] == "row status"
    assert entries[1]["display_order"] == 2


def test_project_tables_picks_up_check_constraint_valid_values(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        projector,
        "apply_projection",
        lambda conn, **kw: calls.append(kw),
    )

    conn = _FakeConn({
        "information_schema.columns": [
            {
                "table_name": "orders",
                "column_name": "status",
                "data_type": "text",
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 1,
                "column_description": None,
            }
        ],
        "pg_constraint": [
            {
                "table_name": "orders",
                "check_def": "CHECK ((status = ANY (ARRAY['open'::text, 'closed'::text])))",
            }
        ],
        "pg_class": [],
    })

    DataDictionaryProjector(conn)._project_tables()

    entries = calls[0]["entries"]
    assert entries[0]["valid_values"] == ["open", "closed"]


def test_project_object_types_forwards_field_registry_rows(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        projector,
        "apply_projection",
        lambda conn, **kw: calls.append(kw),
    )

    class _Conn:
        def __init__(self) -> None:
            self._calls = 0

        def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
            self._calls += 1
            if "FROM object_types" in sql:
                return [{
                    "type_id": "project",
                    "name": "Project",
                    "description": "A project",
                    "icon": "folder",
                }]
            if "FROM object_field_registry" in sql:
                return [
                    {
                        "field_name": "title",
                        "label": "Title",
                        "field_kind": "text",
                        "description": "Human title.",
                        "required": True,
                        "default_value": None,
                        "options": None,
                        "display_order": 10,
                    }
                ]
            return []

    DataDictionaryProjector(_Conn())._project_object_types()

    assert len(calls) == 1
    assert calls[0]["object_kind"] == "object_type:project"
    assert calls[0]["category"] == "object_type"
    assert calls[0]["entries"][0]["field_path"] == "title"
    assert calls[0]["entries"][0]["label"] == "Title"
    assert calls[0]["entries"][0]["required"] is True


def test_project_decision_kinds_emits_fixed_schema_per_kind(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        projector,
        "apply_projection",
        lambda conn, **kw: calls.append(kw),
    )

    conn = _FakeConn({
        "FROM operator_decisions": [
            {"decision_kind": "architecture-policy"},
            {"decision_kind": "dataset-promotion"},
        ]
    })

    DataDictionaryProjector(conn)._project_decision_kinds()

    assert {c["object_kind"] for c in calls} == {
        "decision:architecture-policy",
        "decision:dataset-promotion",
    }
    # All decision kinds share the same 8-field schema
    for call in calls:
        paths = [e["field_path"] for e in call["entries"]]
        assert "decision_key" in paths
        assert "rationale" in paths
        assert "effective_from" in paths
        assert "effective_to" in paths
        assert call["category"] == "decision"


def test_project_ingest_kinds_pulls_enum_from_ingest_module(monkeypatch) -> None:
    import enum
    import sys

    class FakeIngestKind(str, enum.Enum):
        FACT = "fact"
        TASK = "task"

    # Provide a fake memory.ingest module
    fake_module = type(sys)("memory.ingest")
    fake_module.IngestKind = FakeIngestKind
    monkeypatch.setitem(sys.modules, "memory.ingest", fake_module)

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        projector,
        "apply_projection",
        lambda conn, **kw: calls.append(kw),
    )

    DataDictionaryProjector(object())._project_ingest_kinds()

    assert len(calls) == 1
    assert calls[0]["object_kind"] == "ingest:IngestPayload"
    kind_entry = next(e for e in calls[0]["entries"] if e["field_path"] == "kind")
    assert kind_entry["valid_values"] == ["fact", "task"]


def test_project_receipt_kinds_skips_when_table_missing() -> None:
    class _RaisingConn:
        def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
            raise RuntimeError("table missing")

    # Should swallow and return without raising.
    DataDictionaryProjector(_RaisingConn())._project_receipt_kinds()


def test_project_integration_manifests_fails_closed_on_manifest_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.integration_manifest.load_manifest_report",
        lambda: ManifestLoadReport(manifests=(), errors=("bad.toml: TOMLDecodeError: boom",)),
    )

    with pytest.raises(RuntimeError, match="malformed manifest"):
        DataDictionaryProjector(object())._project_integration_manifests()


def test_run_collects_per_step_errors_and_reports_them(monkeypatch) -> None:
    """Each projector step is isolated; a failing step shouldn't abort others."""

    class _BoomConn:
        def execute(self, *a, **k):
            raise RuntimeError("db down")

    # Make every step raise via a busted conn.
    proj = DataDictionaryProjector(_BoomConn())
    result = proj.run()
    assert result.ok is False
    # Error field names each failing step
    assert "tables" in result.error
    assert "object_types" in result.error
