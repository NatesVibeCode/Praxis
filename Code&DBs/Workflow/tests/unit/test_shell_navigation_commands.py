"""Unit tests for shell navigation command handlers.

Pure-validation tests — no DB, no gateway. Each handler must validate +
normalize input and return a flat dict with the expected shape. The gateway
owns receipt + event persistence, so handler tests do not assert on
authority_operation_receipts / authority_events here; the gateway integration
test in the same wedge covers that path.
"""
from __future__ import annotations

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


def test_shell_surface_opened_normalizes_caller_and_returns_diff():
    cmd = ShellSurfaceOpenedCommand(
        session_aggregate_ref="  sess-1  ",
        route_id="route.app.workflow",
        slot_values={"workflow": "wf_1"},
        shell_state_diff={"activeTabId": "build", "buildWorkflowId": "wf_1"},
        reason="click",
        caller_ref="   ",
    )
    out = handle_shell_surface_opened(cmd, subsystems=None)
    assert out["ok"] is True
    assert out["session_aggregate_ref"] == "sess-1"
    assert out["route_id"] == "route.app.workflow"
    assert out["slot_values"] == {"workflow": "wf_1"}
    assert out["shell_state_diff"] == {"activeTabId": "build", "buildWorkflowId": "wf_1"}
    assert out["reason"] == "click"
    assert out["caller_ref"] == "shell.unknown"


def test_shell_surface_opened_rejects_invalid_reason():
    with pytest.raises(Exception):
        ShellSurfaceOpenedCommand(
            session_aggregate_ref="sess-1",
            route_id="route.app.workflow",
            reason="bogus",  # type: ignore[arg-type]
        )


def test_shell_tab_closed_defaults_fallback_when_blank():
    cmd = ShellTabClosedCommand(
        session_aggregate_ref="sess-1",
        dynamic_tab_id="manifest:foo:main",
        fallback_route_id="   ",
        caller_ref="dashboard.tab_close_button",
    )
    out = handle_shell_tab_closed(cmd, subsystems=None)
    assert out["fallback_route_id"] == "route.app.dashboard"
    assert out["dynamic_tab_id"] == "manifest:foo:main"
    assert out["caller_ref"] == "dashboard.tab_close_button"


def test_shell_draft_guard_consulted_records_decision():
    cmd = ShellDraftGuardConsultedCommand(
        session_aggregate_ref="sess-1",
        decision="leave",
        source_route_id="route.app.workflow",
        target_route_id="route.app.dashboard",
        draft_message="This draft workflow is not saved yet.",
    )
    out = handle_shell_draft_guard_consulted(cmd, subsystems=None)
    assert out["decision"] == "leave"
    assert out["source_route_id"] == "route.app.workflow"
    assert out["target_route_id"] == "route.app.dashboard"
    assert out["caller_ref"] == "shell.draft_guard"


def test_shell_draft_guard_consulted_rejects_invalid_decision():
    with pytest.raises(Exception):
        ShellDraftGuardConsultedCommand(
            session_aggregate_ref="sess-1",
            decision="abandon",  # type: ignore[arg-type]
            source_route_id="route.app.workflow",
            target_route_id="route.app.dashboard",
        )


def test_shell_history_popped_passes_slot_values():
    cmd = ShellHistoryPoppedCommand(
        session_aggregate_ref="sess-1",
        target_route_id="route.app.run",
        slot_values={"run_id": "wf_run_42"},
    )
    out = handle_shell_history_popped(cmd, subsystems=None)
    assert out["slot_values"] == {"run_id": "wf_run_42"}
    assert out["caller_ref"] == "shell.history"


def test_shell_session_bootstrapped_defaults_initial_route():
    cmd = ShellSessionBootstrappedCommand(
        session_aggregate_ref="sess-1",
        initial_route_id="",
        deep_link_search="?intent=research",
    )
    out = handle_shell_session_bootstrapped(cmd, subsystems=None)
    assert out["initial_route_id"] == "route.app.dashboard"
    assert out["deep_link_search"] == "?intent=research"
