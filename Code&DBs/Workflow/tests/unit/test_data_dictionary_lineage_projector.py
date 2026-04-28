"""Unit tests for the data dictionary lineage projector.

The projector walks every known source of object-to-object relationships
(FK constraints, view dependencies, dataset_promotions, integration
manifests, MCP tool input schemas) and calls
``apply_projected_edges`` once per projector step. These tests stub each
source and assert the projector emits the expected edges.
"""
from __future__ import annotations

from typing import Any

import pytest

from memory import data_dictionary_lineage_projector as projector
from memory.data_dictionary_lineage_projector import (
    DataDictionaryLineageProjector,
    _collect_string_values,
    _dedupe_edges,
    _known_by_category,
)
from runtime.integration_manifest import ManifestLoadReport


# --- helpers ----------------------------------------------------------------


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
        return {"projector": kw.get("projector_tag"), "edges_written": len(kw.get("edges", []))}

    monkeypatch.setattr(projector, "apply_projected_edges", _apply)
    return calls


# --- _collect_string_values -------------------------------------------------


def test_collect_string_values_flattens_nested_json() -> None:
    schema = {
        "properties": {
            "object_kind": {"type": "string", "example": "table:workflow_runs"},
            "nested": {"items": ["table:receipts", "plain text with spaces"]},
        }
    }
    values = set(_collect_string_values(schema))
    # Whitespace-containing leaves are filtered so prose doesn't pollute refs.
    assert "table:workflow_runs" in values
    assert "table:receipts" in values
    assert not any(" " in v for v in values)


# --- _dedupe_edges ----------------------------------------------------------


def test_dedupe_edges_drops_duplicate_tuples() -> None:
    rows = [
        {
            "src_object_kind": "a", "src_field_path": "",
            "dst_object_kind": "b", "dst_field_path": "",
            "edge_kind": "references",
        },
        {
            "src_object_kind": "a", "src_field_path": "",
            "dst_object_kind": "b", "dst_field_path": "",
            "edge_kind": "references",
        },
        {
            "src_object_kind": "a", "src_field_path": "",
            "dst_object_kind": "b", "dst_field_path": "",
            "edge_kind": "derives_from",
        },
    ]
    deduped = _dedupe_edges(rows)
    # Three rows → two unique (references, derives_from) — duplicate dropped.
    assert len(deduped) == 2
    kinds = sorted(e["edge_kind"] for e in deduped)
    assert kinds == ["derives_from", "references"]


# --- _known_by_category -----------------------------------------------------


def test_known_by_category_groups_objects() -> None:
    conn = _FakeConn({
        "FROM data_dictionary_objects": [
            {"category": "table", "object_kind": "table:orders"},
            {"category": "table", "object_kind": "table:customers"},
            {"category": "dataset", "object_kind": "dataset:slm/review"},
            {"category": "tool", "object_kind": "tool:praxis_query"},
        ],
    })
    by_cat = _known_by_category(conn)
    assert by_cat["table"] == {"table:orders", "table:customers"}
    assert by_cat["dataset"] == {"dataset:slm/review"}
    assert by_cat["tool"] == {"tool:praxis_query"}


# --- FK edge projection -----------------------------------------------------


def test_project_fk_edges_emits_field_level_edges(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn({
        "FROM pg_constraint": [
            {
                "constraint_name": "workflow_runs_spec_fk",
                "src_table": "workflow_runs",
                "dst_table": "workflow_specs",
                "src_column": "spec_id",
                "dst_column": "spec_id",
            },
            # Edge where one side isn't in the dictionary — must be skipped.
            {
                "constraint_name": "bogus_fk",
                "src_table": "ghost_table",
                "dst_table": "workflow_specs",
                "src_column": "other_id",
                "dst_column": "spec_id",
            },
        ],
    })
    known = {
        "table": {"table:workflow_runs", "table:workflow_specs"},
    }
    DataDictionaryLineageProjector(conn)._project_fk_edges(known)

    assert len(calls) == 1
    edges = calls[0]["edges"]
    assert len(edges) == 1
    edge = edges[0]
    assert edge["src_object_kind"] == "table:workflow_runs"
    assert edge["src_field_path"] == "spec_id"
    assert edge["dst_object_kind"] == "table:workflow_specs"
    assert edge["dst_field_path"] == "spec_id"
    assert edge["edge_kind"] == "references"
    assert edge["origin_ref"]["constraint"] == "workflow_runs_spec_fk"


# --- view dependency projection --------------------------------------------


def test_project_view_dependencies_emits_derives_from_edges(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn({
        "FROM pg_depend": [
            {
                "base_table": "data_dictionary_entries",
                "view_name": "data_dictionary_effective",
                "view_kind": "v",
            },
            # Unknown dst must be filtered.
            {
                "base_table": "data_dictionary_entries",
                "view_name": "ghost_view",
                "view_kind": "v",
            },
        ],
    })
    known = {
        "table": {
            "table:data_dictionary_entries",
            "table:data_dictionary_effective",
        },
    }
    DataDictionaryLineageProjector(conn)._project_view_dependencies(known)

    assert len(calls) == 1
    edges = calls[0]["edges"]
    assert len(edges) == 1
    edge = edges[0]
    assert edge["src_object_kind"] == "table:data_dictionary_entries"
    assert edge["dst_object_kind"] == "table:data_dictionary_effective"
    assert edge["edge_kind"] == "derives_from"
    assert edge["metadata"]["view_kind"] == "v"


# --- dataset promotions projection -----------------------------------------


def test_project_dataset_promotions_links_specialist_to_object_type(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn({
        "FROM dataset_promotions": [
            {"specialist_target": "slm/review", "dataset_family": "sft"},
            # Unknown dst (no matching object_type) must be skipped.
            {"specialist_target": "ghost_target", "dataset_family": "eval"},
        ],
    })
    known = {
        "dataset": {"dataset:slm/review", "dataset:ghost_target"},
        "object_type": {"object_type:slm/review"},
    }
    DataDictionaryLineageProjector(conn)._project_dataset_promotions(known)

    assert len(calls) == 1
    edges = calls[0]["edges"]
    assert len(edges) == 1
    assert edges[0]["src_object_kind"] == "dataset:slm/review"
    assert edges[0]["dst_object_kind"] == "object_type:slm/review"
    assert edges[0]["edge_kind"] == "promotes_to"
    assert edges[0]["metadata"]["dataset_family"] == "sft"


# --- run() aggregates all steps ---------------------------------------------


def test_run_reports_errors_when_projector_step_raises(monkeypatch) -> None:
    """Any step failing must leave the HeartbeatModule result in failure
    state but the other steps should still run (errors are collected, not
    raised)."""
    # Make _project_fk_edges raise; others return empty.
    monkeypatch.setattr(
        DataDictionaryLineageProjector,
        "_project_fk_edges",
        lambda self, known: (_ for _ in ()).throw(RuntimeError("boom fk")),
    )
    monkeypatch.setattr(
        DataDictionaryLineageProjector,
        "_project_view_dependencies",
        lambda self, known: None,
    )
    monkeypatch.setattr(
        DataDictionaryLineageProjector,
        "_project_dataset_promotions",
        lambda self, known: None,
    )
    monkeypatch.setattr(
        DataDictionaryLineageProjector,
        "_project_integration_manifests",
        lambda self, known: None,
    )
    monkeypatch.setattr(
        DataDictionaryLineageProjector,
        "_project_tool_schema_refs",
        lambda self, known: None,
    )
    conn = _FakeConn({"FROM data_dictionary_objects": []})
    result = DataDictionaryLineageProjector(conn).run()
    assert result.ok is False
    assert "fk_edges" in (result.error or "")


def test_project_integration_manifests_fails_closed_on_manifest_errors() -> None:
    conn = _FakeConn({})
    known = {"integration": set(), "tool": set()}

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "runtime.integration_manifest.load_manifest_report",
            lambda: ManifestLoadReport(manifests=(), errors=("bad.toml: TOMLDecodeError: boom",)),
        )
        with pytest.raises(RuntimeError, match="malformed manifest"):
            DataDictionaryLineageProjector(conn)._project_integration_manifests(known)
