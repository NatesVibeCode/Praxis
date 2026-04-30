"""Unit tests for shell navigation command handlers.

Handlers emit authority_events directly (so the projection reducer sees the
full input payload). Tests stub subsystems with a fake pg conn that records
INSERT calls and verify the right event_type + payload.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from runtime.operations.commands.shell_navigation_commands import (
    ShellDraftGuardConsultedCommand,
    ShellHistoryPoppedCommand,
    ShellSessionBootstrappedCommand,
    ShellSurfaceOpenedCommand,
    ShellTabClosedCommand,
    handle_shell_draft_guard_consulted,
    handle_shell_history_popped,
    handle_shell_session_bootstrapped,
    handle_shell_surface_opened,
    handle_shell_tab_closed,
)


class _StubConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> None:
        self.calls.append((query, args))


class _StubSubs:
    def __init__(self) -> None:
        self._conn = _StubConn()

    def get_pg_conn(self) -> _StubConn:
        return self._conn


def _last_event_call(subs: _StubSubs) -> tuple[str, tuple[Any, ...]]:
    """Return the last conn.execute() call (authority_events INSERT)."""
    assert subs.get_pg_conn().calls, "expected at least one DB call"
    return subs.get_pg_conn().calls[-1]


def test_shell_surface_opened_emits_event_with_session_payload():
    subs = _StubSubs()
    cmd = ShellSurfaceOpenedCommand(
        session_aggregate_ref="  sess-1  ",
        route_id="route.app.workflow",
        slot_values={"workflow": "wf_1"},
        shell_state_diff={"activeTabId": "build", "buildWorkflowId": "wf_1"},
        reason="click",
        caller_ref="   ",
    )
    out = handle_shell_surface_opened(cmd, subsystems=subs)
    assert out["ok"] is True
    assert out["session_aggregate_ref"] == "sess-1"
    assert out["route_id"] == "route.app.workflow"
    assert out["slot_values"] == {"workflow": "wf_1"}
    assert out["shell_state_diff"] == {"activeTabId": "build", "buildWorkflowId": "wf_1"}
    assert out["reason"] == "click"
    assert out["caller_ref"] == "shell.unknown"
    assert len(out["authority_event_ids"]) == 1

    _, args = _last_event_call(subs)
    # args order matches the INSERT: event_id, authority_domain_ref, aggregate_ref, event_type, payload_json, operation_ref, emitted_by
    assert args[1] == "authority.surface_catalog"
    assert args[2] == "sess-1"
    assert args[3] == "surface.opened"
    assert args[5] == "shell-surface-opened"


def test_shell_surface_opened_rejects_invalid_reason():
    with pytest.raises(Exception):
        ShellSurfaceOpenedCommand(
            session_aggregate_ref="sess-1",
            route_id="route.app.workflow",
            reason="bogus",  # type: ignore[arg-type]
        )


def test_shell_tab_closed_emits_with_fallback_default():
    subs = _StubSubs()
    cmd = ShellTabClosedCommand(
        session_aggregate_ref="sess-1",
        dynamic_tab_id="manifest:foo:main",
        fallback_route_id="   ",
        caller_ref="dashboard.tab_close_button",
    )
    out = handle_shell_tab_closed(cmd, subsystems=subs)
    assert out["fallback_route_id"] == "route.app.dashboard"
    assert out["dynamic_tab_id"] == "manifest:foo:main"
    assert out["caller_ref"] == "dashboard.tab_close_button"
    _, args = _last_event_call(subs)
    assert args[3] == "tab.closed"
    assert args[2] == "sess-1"


def test_shell_draft_guard_consulted_records_decision():
    subs = _StubSubs()
    cmd = ShellDraftGuardConsultedCommand(
        session_aggregate_ref="sess-1",
        decision="leave",
        source_route_id="route.app.workflow",
        target_route_id="route.app.dashboard",
        draft_message="This draft workflow is not saved yet.",
    )
    out = handle_shell_draft_guard_consulted(cmd, subsystems=subs)
    assert out["decision"] == "leave"
    assert out["source_route_id"] == "route.app.workflow"
    assert out["target_route_id"] == "route.app.dashboard"
    assert out["caller_ref"] == "shell.draft_guard"
    _, args = _last_event_call(subs)
    assert args[3] == "draft.guard.consulted"


def test_shell_draft_guard_consulted_rejects_invalid_decision():
    with pytest.raises(Exception):
        ShellDraftGuardConsultedCommand(
            session_aggregate_ref="sess-1",
            decision="abandon",  # type: ignore[arg-type]
            source_route_id="route.app.workflow",
            target_route_id="route.app.dashboard",
        )


def test_shell_history_popped_passes_slot_values():
    subs = _StubSubs()
    cmd = ShellHistoryPoppedCommand(
        session_aggregate_ref="sess-1",
        target_route_id="route.app.run",
        slot_values={"run_id": "wf_run_42"},
    )
    out = handle_shell_history_popped(cmd, subsystems=subs)
    assert out["slot_values"] == {"run_id": "wf_run_42"}
    assert out["caller_ref"] == "shell.history"
    _, args = _last_event_call(subs)
    assert args[3] == "history.popped"


def test_shell_session_bootstrapped_defaults_initial_route():
    subs = _StubSubs()
    cmd = ShellSessionBootstrappedCommand(
        session_aggregate_ref="sess-1",
        initial_route_id="",
        deep_link_search="?intent=research",
    )
    out = handle_shell_session_bootstrapped(cmd, subsystems=subs)
    assert out["initial_route_id"] == "route.app.dashboard"
    assert out["initial_slot_values"] == {}
    assert out["deep_link_search"] == "?intent=research"
    _, args = _last_event_call(subs)
    assert args[3] == "session.bootstrapped"
    assert args[2] == "sess-1"


def test_shell_session_bootstrapped_preserves_initial_slot_values():
    subs = _StubSubs()
    cmd = ShellSessionBootstrappedCommand(
        session_aggregate_ref="sess-1",
        initial_route_id="route.app.workflow",
        initial_slot_values={"workflow": "wf_42"},
    )
    out = handle_shell_session_bootstrapped(cmd, subsystems=subs)
    assert out["initial_slot_values"] == {"workflow": "wf_42"}
    _, args = _last_event_call(subs)
    assert json.loads(args[4])["initial_slot_values"] == {"workflow": "wf_42"}
