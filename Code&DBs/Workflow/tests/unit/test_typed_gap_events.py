"""Tests for typed_gap.created event emission (Phase 1.6 emission wiring).

The helper pairs with the authority_event_contracts row registered in
migration 226. Writes to public.system_events via emit_system_event;
best-effort — failures do not propagate, the caller gets None.
"""
from __future__ import annotations

import json

from runtime.typed_gap_events import emit_typed_gap


class _RecordingConn:
    """Minimal Postgres conn stub: records system_events INSERT calls."""

    def __init__(self) -> None:
        self.events: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.events.append((sql, args))
        return []


def _captured_payload(conn: _RecordingConn) -> dict:
    assert conn.events, "expected at least one INSERT"
    sql, args = conn.events[-1]
    assert "INSERT INTO system_events" in sql
    return json.loads(args[3])


def test_emit_typed_gap_success_returns_gap_id():
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="stage_template.missing",
        legal_repair_actions=["add_stage_template", "use_known_stage"],
        source_ref="packet:p1",
        context={"stage_attempted": "rumble"},
    )
    assert gap_id is not None
    assert gap_id.startswith("typed_gap.")
    assert len(conn.events) == 1
    sql, args = conn.events[0]
    assert args[0] == "typed_gap.created"
    assert args[1] == gap_id  # source_id mirrors gap_id
    assert args[2] == "typed_gap"  # source_type


def test_emit_typed_gap_payload_shape():
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="type_contract_slug",
        missing_type="data_dictionary_object",
        reason_code="data_dictionary.object_kind.missing",
        legal_repair_actions=["add_data_dictionary_objects_row"],
        source_ref="tool:praxis_bugs",
        context={"slug": "praxis.bug.record"},
    )
    payload = _captured_payload(conn)
    assert payload["gap_id"] == gap_id
    assert payload["gap_kind"] == "type_contract_slug"
    assert payload["missing_type"] == "data_dictionary_object"
    assert payload["reason_code"] == "data_dictionary.object_kind.missing"
    assert payload["legal_repair_actions"] == ["add_data_dictionary_objects_row"]
    assert payload["source_ref"] == "tool:praxis_bugs"
    assert payload["context"] == {"slug": "praxis.bug.record"}


def test_emit_typed_gap_coerces_repair_actions_to_strings():
    conn = _RecordingConn()
    emit_typed_gap(
        conn,
        gap_kind="source_ref",
        missing_type="source_authority_resolver",
        reason_code="source_ref.unresolvable",
        legal_repair_actions=[1, 2, "three"],  # type: ignore[list-item]
    )
    payload = _captured_payload(conn)
    assert payload["legal_repair_actions"] == ["1", "2", "three"]


def test_emit_typed_gap_defaults_for_optional_fields():
    conn = _RecordingConn()
    emit_typed_gap(
        conn,
        gap_kind="verifier",
        missing_type="verifier",
        reason_code="verifier.no_admitted_for_extension",
        # no legal_repair_actions, source_ref, or context
    )
    payload = _captured_payload(conn)
    assert payload["legal_repair_actions"] == []
    assert payload["source_ref"] is None
    assert payload["context"] == {}


def test_emit_typed_gap_generates_unique_gap_ids():
    conn = _RecordingConn()
    id1 = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    id2 = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert id1 != id2


def test_emit_typed_gap_returns_none_on_emission_failure(monkeypatch):
    """When the emit_system_event path raises, helper returns None
    rather than propagating."""
    import runtime.system_events as se_mod

    def broken(*args, **kwargs):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr(se_mod, "emit_system_event", broken)
    result = emit_typed_gap(
        _RecordingConn(),
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert result is None


def test_emit_typed_gap_returns_none_on_import_failure(monkeypatch):
    """When system_events module can't be imported, helper returns None."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "runtime.system_events":
            raise ImportError("simulated")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    result = emit_typed_gap(
        _RecordingConn(),
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert result is None


def test_emit_typed_gap_copies_context_defensively():
    """Mutating the source context after emit doesn't affect the emitted
    payload."""
    conn = _RecordingConn()
    source_context = {"key": "value"}
    emit_typed_gap(
        conn,
        gap_kind="x",
        missing_type="x",
        reason_code="x",
        context=source_context,
    )
    source_context["mutated"] = True
    payload = _captured_payload(conn)
    assert payload["context"] == {"key": "value"}


def test_emit_typed_gap_copies_repair_actions_defensively():
    """Mutating the source list after emit doesn't affect the emitted payload."""
    conn = _RecordingConn()
    source_actions = ["action_one"]
    emit_typed_gap(
        conn,
        gap_kind="x",
        missing_type="x",
        reason_code="x",
        legal_repair_actions=source_actions,
    )
    source_actions.append("action_two")
    payload = _captured_payload(conn)
    assert payload["legal_repair_actions"] == ["action_one"]


def test_emit_typed_gap_gap_id_format():
    """gap_id is 'typed_gap.' + 16 hex chars."""
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="x",
        missing_type="x",
        reason_code="x",
    )
    assert gap_id is not None
    prefix, _, rest = gap_id.partition(".")
    assert prefix == "typed_gap"
    assert len(rest) == 16
    assert all(c in "0123456789abcdef" for c in rest)
