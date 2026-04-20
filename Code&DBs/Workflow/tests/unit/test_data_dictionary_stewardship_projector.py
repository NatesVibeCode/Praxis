"""Unit tests for the data dictionary stewardship projector.

The projector walks `data_dictionary_effective` (fields) and
`data_dictionary_objects` (tables) and emits stewardship rows from
audit-column names, namespace prefixes, and a projector-module map.
Tests stub the query responses and assert emissions have the right
shape (kind, id, type, origin_ref).
"""
from __future__ import annotations

from typing import Any

from memory import data_dictionary_stewardship_projector as projector
from memory.data_dictionary_stewardship_projector import (
    DataDictionaryStewardshipProjector,
    _AUDIT_COLUMN_NAMES,
    _PROJECTOR_PUBLISHERS,
    _namespace_owner,
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
            "stewards_written": len(kw.get("entries", [])),
        }

    monkeypatch.setattr(projector, "apply_projected_stewards", _apply)
    return calls


# --- _namespace_owner direct tests --------------------------------------


def test_namespace_owner_matches_prefix() -> None:
    assert _namespace_owner("workflow_runs") == "praxis_engine"
    assert _namespace_owner("operator_decisions") == "operator_authority"
    assert _namespace_owner("data_dictionary_entries") == "data_dictionary_authority"
    assert _namespace_owner("bugs") == "bug_authority"
    assert _namespace_owner("bug_evidence_links") == "bug_authority"
    assert _namespace_owner("cutover_gates") == "capability_authority"
    assert _namespace_owner("heartbeat_runs") == "heartbeat_runner"
    assert _namespace_owner("praxis_heartbeat_modules") == "heartbeat_runner"


def test_namespace_owner_returns_none_for_unknown_prefix() -> None:
    assert _namespace_owner("something_random") is None


# --- audit-column publishers --------------------------------------------


def test_project_audit_columns_emits_one_per_audit_field(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    entries = [
        {"object_kind": "table:bugs", "field_path": "created_by"},
        {"object_kind": "table:bugs", "field_path": "assigned_to"},
        {"object_kind": "table:bugs", "field_path": "title"},  # ignored: not audit
    ]
    DataDictionaryStewardshipProjector(_FakeConn({}))._project_audit_columns(entries)
    assert len(calls) == 1
    assert calls[0]["projector_tag"] == "stewardship_audit_column_publishers"
    emitted = calls[0]["entries"]
    assert len(emitted) == 2
    kinds_ids = {(e["steward_kind"], e["steward_id"]) for e in emitted}
    assert kinds_ids == {
        ("publisher", "created_by"),
        ("publisher", "assigned_to"),
    }
    # All audit-column publishers are role-type object-level stewards.
    for e in emitted:
        assert e["steward_type"] == "role"
        assert e["field_path"] == ""


def test_project_audit_columns_dedupes_same_column_same_object(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    entries = [
        {"object_kind": "table:bugs", "field_path": "created_by"},
        {"object_kind": "table:bugs", "field_path": "created_by"},  # dup
    ]
    DataDictionaryStewardshipProjector(_FakeConn({}))._project_audit_columns(entries)
    assert len(calls[0]["entries"]) == 1


# --- namespace owners ---------------------------------------------------


def test_project_namespace_owners_emits_service_owners(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    known = {
        "table:workflow_runs",
        "table:operator_decisions",
        "table:unknown_random_table",  # no prefix match
    }
    DataDictionaryStewardshipProjector(_FakeConn({}))._project_namespace_owners(known)
    assert len(calls) == 1
    emitted = calls[0]["entries"]
    owners = {(e["object_kind"], e["steward_id"]) for e in emitted}
    assert ("table:workflow_runs", "praxis_engine") in owners
    assert ("table:operator_decisions", "operator_authority") in owners
    # The unknown prefix yielded no row.
    assert not any(e["object_kind"] == "table:unknown_random_table" for e in emitted)
    # Service-type owners, object-level field_path.
    for e in emitted:
        assert e["steward_kind"] == "owner"
        assert e["steward_type"] == "service"
        assert e["field_path"] == ""


# --- projector-module publishers ----------------------------------------


def test_project_projector_publishers_emits_agent_publishers(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    known = {
        "table:data_dictionary_quality_rules",
        "table:data_dictionary_classifications",
        "table:workflow_runs",  # no projector mapping
    }
    DataDictionaryStewardshipProjector(_FakeConn({}))._project_projector_publishers(known)
    emitted = calls[0]["entries"]
    pairs = {(e["object_kind"], e["steward_id"]) for e in emitted}
    assert (
        "table:data_dictionary_quality_rules",
        "data_dictionary_quality_projector",
    ) in pairs
    assert (
        "table:data_dictionary_classifications",
        "data_dictionary_classifications_projector",
    ) in pairs
    # Not projector-published.
    assert not any(e["object_kind"] == "table:workflow_runs" for e in emitted)
    for e in emitted:
        assert e["steward_kind"] == "publisher"
        assert e["steward_type"] == "agent"


# --- run() aggregates all steps ----------------------------------------


def test_run_reports_errors_when_step_raises(monkeypatch) -> None:
    monkeypatch.setattr(
        DataDictionaryStewardshipProjector,
        "_project_audit_columns",
        lambda self, entries: (_ for _ in ()).throw(RuntimeError("boom ac")),
    )
    monkeypatch.setattr(
        DataDictionaryStewardshipProjector,
        "_project_namespace_owners",
        lambda self, known: None,
    )
    monkeypatch.setattr(
        DataDictionaryStewardshipProjector,
        "_project_projector_publishers",
        lambda self, known: None,
    )
    conn = _FakeConn({
        "FROM data_dictionary_objects": [],
        "FROM data_dictionary_effective": [],
    })
    result = DataDictionaryStewardshipProjector(conn).run()
    assert result.ok is False
    assert "audit_column" in (result.error or "")


# --- constants sanity --------------------------------------------------


def test_audit_column_names_cover_standard_set() -> None:
    assert "created_by" in _AUDIT_COLUMN_NAMES
    assert "assigned_to" in _AUDIT_COLUMN_NAMES
    assert "owner_id" in _AUDIT_COLUMN_NAMES


def test_projector_publishers_includes_data_dictionary_tables() -> None:
    assert "data_dictionary_entries" in _PROJECTOR_PUBLISHERS
    assert "data_dictionary_quality_rules" in _PROJECTOR_PUBLISHERS
