"""Tests for plan.launched conceptual event emission (Phase 1.1.d).

Honors architecture-policy::platform-architecture::expected-envelope-vs-
actual-truth-separation: ``plan.launched`` marks the pre-run envelope
crossing into runtime. Emission is best-effort — a degraded system_events
write path does NOT roll back a successful submit_workflow_command; the
warning folds into the LaunchReceipt's warnings list instead.

Formal authority_event_contracts registration is queued for a follow-up
CQRS-consistency packet. These tests assert the emission path through the
freeform system_events helper.
"""
from __future__ import annotations

import pytest

from runtime.spec_compiler import _emit_plan_launched_event


class _RecordingConn:
    """Minimal Postgres conn stub that records system_events INSERT calls."""

    def __init__(self) -> None:
        self.events: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.events.append((sql, args))
        return []


def _captured_payload(conn: _RecordingConn) -> dict:
    """Extract the payload from the most recent system_events INSERT call."""
    import json

    assert conn.events, "expected at least one event insert"
    sql, args = conn.events[-1]
    assert "INSERT INTO system_events" in sql
    # Args: event_type, source_id, source_type, payload(jsonb as string)
    payload_json = args[3]
    return json.loads(payload_json)


def test_plan_launched_event_emits_success_path():
    conn = _RecordingConn()
    result = _emit_plan_launched_event(
        conn,
        run_id="run_abc",
        workflow_id="workflow_xyz",
        spec_name="my_plan",
        total_jobs=3,
        packet_labels=["p1", "p2", "p3"],
        source_refs=["BUG-X"],
    )
    assert result is None, f"expected success (None), got error: {result}"
    assert len(conn.events) == 1
    sql, args = conn.events[0]
    assert "INSERT INTO system_events" in sql
    # event_type, source_id, source_type
    assert args[0] == "plan.launched"
    assert args[1] == "run_abc"
    assert args[2] == "launch_plan"


def test_plan_launched_event_payload_shape():
    conn = _RecordingConn()
    _emit_plan_launched_event(
        conn,
        run_id="run_abc",
        workflow_id="workflow_xyz",
        spec_name="my_plan",
        total_jobs=2,
        packet_labels=["p1", "p2"],
        source_refs=["BUG-X", "roadmap_item.Y"],
    )
    payload = _captured_payload(conn)
    assert payload["run_id"] == "run_abc"
    assert payload["workflow_id"] == "workflow_xyz"
    assert payload["spec_name"] == "my_plan"
    assert payload["total_jobs"] == 2
    assert payload["packet_labels"] == ["p1", "p2"]
    assert payload["source_refs"] == ["BUG-X", "roadmap_item.Y"]


def test_plan_launched_event_empty_source_refs_defaults_to_list():
    conn = _RecordingConn()
    _emit_plan_launched_event(
        conn,
        run_id="run_abc",
        workflow_id="workflow_xyz",
        spec_name="my_plan",
        total_jobs=1,
        packet_labels=["p1"],
        source_refs=None,  # explicitly None
    )
    payload = _captured_payload(conn)
    assert payload["source_refs"] == []


def test_plan_launched_event_fallback_source_id_when_run_id_empty():
    """If run_id is empty (edge case), source_id falls back to workflow_id
    so the event still has a meaningful identifier."""
    conn = _RecordingConn()
    _emit_plan_launched_event(
        conn,
        run_id="",
        workflow_id="workflow_xyz",
        spec_name="my_plan",
        total_jobs=1,
        packet_labels=["p1"],
    )
    sql, args = conn.events[0]
    assert args[1] == "workflow_xyz"  # source_id fell back to workflow_id


def test_plan_launched_event_swallows_emit_failure(monkeypatch):
    """When emit_system_event raises, return the error string instead of
    propagating — best-effort emission must not block a successful launch."""
    import runtime.system_events as se_mod

    def broken_emit(*args, **kwargs):
        raise RuntimeError("simulated system_events outage")

    monkeypatch.setattr(se_mod, "emit_system_event", broken_emit)

    result = _emit_plan_launched_event(
        _RecordingConn(),
        run_id="run_abc",
        workflow_id="wf",
        spec_name="s",
        total_jobs=1,
        packet_labels=["p"],
    )
    assert result is not None
    assert "plan.launched event emission failed" in result
    assert "simulated system_events outage" in result


def test_plan_launched_event_swallows_import_failure(monkeypatch):
    """When system_events module can't be imported (degraded substrate),
    return a 'skipped' error rather than raising."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "runtime.system_events":
            raise ImportError("system_events unavailable (simulated)")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    result = _emit_plan_launched_event(
        _RecordingConn(),
        run_id="run_abc",
        workflow_id="wf",
        spec_name="s",
        total_jobs=1,
        packet_labels=["p"],
    )
    assert result is not None
    assert "plan.launched event emission skipped" in result
    assert "system_events unavailable" in result


def test_plan_launched_event_copies_packet_labels_defensively():
    """Mutating the source packet_labels list after emission doesn't affect
    the emitted payload."""
    conn = _RecordingConn()
    source_labels = ["p1", "p2"]
    _emit_plan_launched_event(
        conn,
        run_id="run_abc",
        workflow_id="wf",
        spec_name="s",
        total_jobs=2,
        packet_labels=source_labels,
    )
    # Mutate the source list after emit.
    source_labels.append("p_mutated")
    payload = _captured_payload(conn)
    # Emitted payload should still be the pre-mutation snapshot.
    assert payload["packet_labels"] == ["p1", "p2"]


def test_plan_launched_event_copies_source_refs_defensively():
    conn = _RecordingConn()
    source_refs = ["BUG-X"]
    _emit_plan_launched_event(
        conn,
        run_id="run_abc",
        workflow_id="wf",
        spec_name="s",
        total_jobs=1,
        packet_labels=["p"],
        source_refs=source_refs,
    )
    source_refs.append("BUG-Y")
    payload = _captured_payload(conn)
    assert payload["source_refs"] == ["BUG-X"]
