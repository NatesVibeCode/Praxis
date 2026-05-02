"""Unit tests for the ui_shell_state.live reducer.

Mocks subsystems with a stub pg conn that returns canned authority_events
rows. Verifies fold semantics across all five event types, multi-session
isolation, idempotency on replay, and the default ShellState fallback.
"""
from __future__ import annotations

from typing import Any

from runtime.surface_projections import _reduce_ui_shell_state, _default_shell_state


class _StubConn:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows
        self.last_args: tuple[Any, ...] = ()

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.last_args = args
        # Filter by event_type list and session_aggregate_ref
        event_types = args[0]
        session = args[1]
        return [
            row
            for row in self.rows
            if row["event_type"] in event_types
            and row["event_payload"].get("session_aggregate_ref") == session
        ]


class _StubSubs:
    def __init__(self, rows: list[dict[str, Any]]):
        self._conn = _StubConn(rows)

    def get_pg_conn(self) -> _StubConn:
        return self._conn


def _ev(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"event_type": event_type, "event_payload": payload, "emitted_at": None}


def test_reducer_returns_default_when_no_session():
    state = _reduce_ui_shell_state(_StubSubs([]), source_ref="stream.shell_navigation", query_params={})
    assert state == _default_shell_state()


def test_reducer_returns_default_for_unknown_session():
    rows = [_ev("session.bootstrapped", {"session_aggregate_ref": "sess-A", "initial_route_id": "route.app.workflow"})]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "sess-B"},
    )
    assert state == _default_shell_state()


def test_session_bootstrapped_applies_initial_route():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.workflow"}),
    ]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert state["activeRouteId"] == "route.app.workflow"
    assert state["activeTabId"] == "build"


def test_session_bootstrapped_applies_workflow_deep_link_slot():
    rows = [
        _ev(
            "session.bootstrapped",
            {
                "session_aggregate_ref": "s1",
                "initial_route_id": "route.app.workflow",
                "initial_slot_values": {"workflow": "wf_42"},
            },
        ),
    ]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert state["activeRouteId"] == "route.app.workflow"
    assert state["activeTabId"] == "build"
    assert state["buildWorkflowId"] == "wf_42"
    assert state["canvasRunId"] is None


def test_surface_opened_applies_state_diff_after_bootstrap():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.dashboard"}),
        _ev(
            "surface.opened",
            {
                "session_aggregate_ref": "s1",
                "route_id": "route.app.workflow",
                "shell_state_diff": {"buildWorkflowId": "wf_42", "buildView": "canvas"},
            },
        ),
    ]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert state["activeRouteId"] == "route.app.workflow"
    assert state["activeTabId"] == "build"
    assert state["buildWorkflowId"] == "wf_42"
    assert state["buildView"] == "canvas"


def test_tab_closed_removes_dynamic_tab_and_falls_back():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.dashboard"}),
        _ev(
            "surface.opened",
            {
                "session_aggregate_ref": "s1",
                "route_id": "route.app.manifest",
                "shell_state_diff": {
                    "activeTabId": "manifest:foo:main",
                    "dynamicTabs": [{"id": "manifest:foo:main", "kind": "manifest", "label": "foo", "closable": True}],
                },
            },
        ),
        _ev(
            "tab.closed",
            {
                "session_aggregate_ref": "s1",
                "dynamic_tab_id": "manifest:foo:main",
                "fallback_route_id": "route.app.dashboard",
            },
        ),
    ]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert state["dynamicTabs"] == []
    assert state["activeTabId"] == "dashboard"
    assert state["activeRouteId"] == "route.app.dashboard"


def test_history_popped_does_not_mutate_state():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.workflow"}),
        _ev(
            "history.popped",
            {"session_aggregate_ref": "s1", "target_route_id": "route.app.dashboard"},
        ),
    ]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    # history.popped is a cause-of-change record only; state stays at the
    # post-bootstrap activeRouteId until a follow-up surface.opened fires.
    assert state["activeRouteId"] == "route.app.workflow"


def test_draft_guard_consulted_does_not_mutate_state():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.workflow"}),
        _ev(
            "draft.guard.consulted",
            {
                "session_aggregate_ref": "s1",
                "decision": "stay",
                "source_route_id": "route.app.workflow",
                "target_route_id": "route.app.dashboard",
            },
        ),
    ]
    state = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert state["activeRouteId"] == "route.app.workflow"


def test_multi_session_isolation():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.workflow"}),
        _ev("session.bootstrapped", {"session_aggregate_ref": "s2", "initial_route_id": "route.app.atlas"}),
    ]
    s1 = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    s2 = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s2"},
    )
    assert s1["activeRouteId"] == "route.app.workflow"
    assert s2["activeRouteId"] == "route.app.atlas"


def test_replay_idempotent():
    rows = [
        _ev("session.bootstrapped", {"session_aggregate_ref": "s1", "initial_route_id": "route.app.dashboard"}),
        _ev(
            "surface.opened",
            {
                "session_aggregate_ref": "s1",
                "route_id": "route.app.atlas",
                "shell_state_diff": {"activeTabId": "atlas"},
            },
        ),
    ]
    s1 = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    s2 = _reduce_ui_shell_state(
        _StubSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert s1 == s2


def test_surface_opened_via_jsonb_string_payload():
    # Some pg drivers return event_payload as a string. The reducer must coerce.
    rows = [
        {
            "event_type": "session.bootstrapped",
            "event_payload": '{"session_aggregate_ref": "s1", "initial_route_id": "route.app.workflow"}',
            "emitted_at": None,
        },
    ]

    class _StringRowConn(_StubConn):
        def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
            import json as _json
            event_types = args[0]
            session = args[1]
            out: list[dict[str, Any]] = []
            for row in self.rows:
                payload = row["event_payload"]
                if isinstance(payload, str):
                    parsed = _json.loads(payload)
                else:
                    parsed = payload
                if row["event_type"] in event_types and parsed.get("session_aggregate_ref") == session:
                    out.append(row)
            return out

    class _StringSubs:
        def __init__(self, rs: list[dict[str, Any]]):
            self._conn = _StringRowConn(rs)

        def get_pg_conn(self) -> _StringRowConn:
            return self._conn

    state = _reduce_ui_shell_state(
        _StringSubs(rows),
        source_ref="stream.shell_navigation",
        query_params={"session": "s1"},
    )
    assert state["activeRouteId"] == "route.app.workflow"
    assert state["activeTabId"] == "build"
