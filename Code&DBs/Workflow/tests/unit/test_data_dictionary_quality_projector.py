"""Unit tests for the data dictionary quality projector.

The projector walks Postgres schema catalogs (pg_attribute for NOT NULL,
pg_index for UNIQUE, pg_constraint for FKs) and emits declarative check
rules. These tests stub the catalog rows and assert the expected rules
are emitted with the right severity / origin / expression.
"""
from __future__ import annotations

from typing import Any

from memory import data_dictionary_quality_projector as projector
from memory.data_dictionary_quality_projector import (
    DataDictionaryQualityProjector,
    _known_tables,
)


class _FakeConn:
    def __init__(self, responses: dict[str, list[dict[str, Any]]]) -> None:
        self._responses = responses

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        for key, rows in self._responses.items():
            if key in sql:
                return rows
        return []


def _install_catcher(monkeypatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _apply(conn, **kw):
        calls.append(kw)
        return {
            "projector": kw.get("projector_tag"),
            "rules_written": len(kw.get("rules", [])),
        }

    monkeypatch.setattr(projector, "apply_projected_rules", _apply)
    return calls


# --- _known_tables -------------------------------------------------------


def test_known_tables_filters_by_category() -> None:
    conn = _FakeConn({
        "FROM data_dictionary_objects": [
            {"object_kind": "table:bugs"},
            {"object_kind": "table:receipts"},
        ],
    })
    assert _known_tables(conn) == {"table:bugs", "table:receipts"}


# --- NOT NULL projection -------------------------------------------------


def test_project_not_null_emits_rule_per_column(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn({
        "FROM pg_attribute": [
            {"table_name": "bugs", "column_name": "id"},
            {"table_name": "bugs", "column_name": "title"},
            # Column on an unknown table must be skipped.
            {"table_name": "ghost_table", "column_name": "x"},
        ],
    })
    known = {"table:bugs"}
    DataDictionaryQualityProjector(conn)._project_not_null(known)
    assert len(calls) == 1
    assert calls[0]["projector_tag"] == "quality_not_null_from_pg_attribute"
    emitted = calls[0]["rules"]
    assert len(emitted) == 2
    assert {e["field_path"] for e in emitted} == {"id", "title"}
    assert all(e["rule_kind"] == "not_null" for e in emitted)
    assert all(e["severity"] == "error" for e in emitted)


# --- UNIQUE projection ---------------------------------------------------


def test_project_unique_emits_single_column_rules(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn({
        "FROM pg_index": [
            {"table_name": "users", "column_name": "email"},
            {"table_name": "users", "column_name": "id"},
        ],
    })
    known = {"table:users"}
    DataDictionaryQualityProjector(conn)._project_unique(known)
    assert len(calls) == 1
    emitted = calls[0]["rules"]
    assert len(emitted) == 2
    assert all(e["rule_kind"] == "unique" for e in emitted)


# --- FK referential projection ------------------------------------------


def test_project_referential_emits_fk_rules(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn({
        "FROM pg_constraint": [
            {
                "src_table": "workflow_runs",
                "dst_table": "workflow_specs",
                "src_column": "spec_id",
                "dst_column": "spec_id",
            },
            # Unknown src must be skipped.
            {
                "src_table": "ghost_table",
                "dst_table": "workflow_specs",
                "src_column": "x",
                "dst_column": "y",
            },
        ],
    })
    known = {"table:workflow_runs"}
    DataDictionaryQualityProjector(conn)._project_referential(known)
    assert len(calls) == 1
    emitted = calls[0]["rules"]
    assert len(emitted) == 1
    rule = emitted[0]
    assert rule["rule_kind"] == "referential"
    assert rule["expression"]["references"] == {
        "table": "workflow_specs", "column": "spec_id",
    }


# --- run() aggregates all steps -----------------------------------------


def test_run_reports_errors_when_step_raises(monkeypatch) -> None:
    monkeypatch.setattr(
        DataDictionaryQualityProjector,
        "_project_not_null",
        lambda self, known: (_ for _ in ()).throw(RuntimeError("boom nn")),
    )
    monkeypatch.setattr(
        DataDictionaryQualityProjector,
        "_project_unique",
        lambda self, known: None,
    )
    monkeypatch.setattr(
        DataDictionaryQualityProjector,
        "_project_referential",
        lambda self, known: None,
    )
    conn = _FakeConn({"FROM data_dictionary_objects": []})
    result = DataDictionaryQualityProjector(conn).run()
    assert result.ok is False
    assert "not_null" in (result.error or "")
