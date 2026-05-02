"""Unit tests for the agent_principal CQRS handlers.

Each handler is hit with a stub subsystems object. The stub conn
captures the SQL that was issued so we can assert the right shape
without a real database round-trip.
"""

from __future__ import annotations

from typing import Any

import pytest

from runtime.operations.commands.agent_principals import (
    FileAgentToolGapCommand,
    RegisterAgentPrincipalCommand,
    RequestAgentWakeCommand,
    UpdateAgentPrincipalStatusCommand,
    handle_file_agent_tool_gap,
    handle_register_agent_principal,
    handle_request_agent_wake,
    handle_update_agent_principal_status,
)
from runtime.operations.commands.agent_delegate import (
    AgentDelegateCommand,
    handle_agent_delegate,
)
from runtime.operations.queries.agent_principals import (
    DescribeAgentPrincipalQuery,
    ListAgentPrincipalsQuery,
    ListAgentToolGapsQuery,
    ListAgentWakesQuery,
    handle_describe_agent_principal,
    handle_list_agent_principals,
    handle_list_agent_tool_gaps,
    handle_list_agent_wakes,
)


class _Conn:
    def __init__(self, scripted: list[Any] | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.scripted = list(scripted or [])

    def execute(self, sql: str, *args: Any) -> Any:
        self.calls.append((sql, args))
        if self.scripted:
            response = self.scripted.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return []


class _Subsystems:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _Conn:
        return self._conn


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────


def test_register_agent_principal_returns_ok_and_emits_event():
    conn = _Conn(
        scripted=[
            [
                {
                    "agent_principal_ref": "agent.exec.nate",
                    "status": "active",
                    "updated_at": __import__("datetime").datetime(2026, 5, 1),
                }
            ],
            [],  # emit_system_event noop
        ]
    )
    cmd = RegisterAgentPrincipalCommand(
        agent_principal_ref="agent.exec.nate",
        title="Exec assistant",
        write_envelope=["Code&DBs/Workflow/artifacts/agent.exec.nate/**"],
        integration_refs=["gmail"],
        standing_order_keys=["policy.foo"],
        allowed_tools=["praxis_search"],
    )
    result = handle_register_agent_principal(cmd, _Subsystems(conn))
    assert result["ok"] is True
    assert result["agent_principal_ref"] == "agent.exec.nate"
    assert result["status"] == "active"
    assert any("INSERT INTO agent_registry" in sql for sql, _ in conn.calls)


def test_register_agent_principal_validates_non_empty_title():
    with pytest.raises(Exception):
        RegisterAgentPrincipalCommand(
            agent_principal_ref="agent.x",
            title="   ",
        )


def test_update_status_not_found_returns_error_code():
    conn = _Conn(scripted=[[]])  # UPDATE returns no rows
    cmd = UpdateAgentPrincipalStatusCommand(
        agent_principal_ref="agent.missing",
        status="paused",
    )
    result = handle_update_agent_principal_status(cmd, _Subsystems(conn))
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.not_found"


def test_update_status_emits_event():
    conn = _Conn(
        scripted=[
            [
                {
                    "agent_principal_ref": "agent.x",
                    "status": "paused",
                    "updated_at": __import__("datetime").datetime(2026, 5, 1),
                }
            ],
            [],
        ]
    )
    cmd = UpdateAgentPrincipalStatusCommand(
        agent_principal_ref="agent.x",
        status="paused",
        reason="manual pause",
    )
    result = handle_update_agent_principal_status(cmd, _Subsystems(conn))
    assert result["ok"] is True
    assert result["status"] == "paused"
    assert result["reason"] == "manual pause"


def test_request_wake_unknown_principal():
    conn = _Conn(scripted=[[]])  # principal lookup empty
    cmd = RequestAgentWakeCommand(
        agent_principal_ref="agent.missing",
        trigger_kind="manual",
    )
    result = handle_request_agent_wake(cmd, _Subsystems(conn))
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.not_found"


def test_request_wake_inserts_pending_row_and_returns_wake_id():
    import uuid

    wake_uuid = uuid.uuid4()
    conn = _Conn(
        scripted=[
            [{"status": "active"}],  # principal lookup
            [{"wake_id": wake_uuid, "received_at": None}],  # INSERT RETURNING
            [],  # emit_system_event noop
        ]
    )
    cmd = RequestAgentWakeCommand(
        agent_principal_ref="agent.exec.nate",
        trigger_kind="chat",
        trigger_source_ref="conv-1",
        payload={"foo": "bar"},
    )
    result = handle_request_agent_wake(cmd, _Subsystems(conn))
    assert result["ok"] is True
    assert result["wake_id"] == str(wake_uuid)
    assert result["duplicate"] is False
    assert result["payload_hash"]


def test_request_wake_dedup_returns_existing_row():
    import uuid

    existing_uuid = uuid.uuid4()
    conn = _Conn(
        scripted=[
            [{"status": "active"}],  # principal lookup
            [],  # INSERT ... ON CONFLICT DO NOTHING — empty
            [{"wake_id": existing_uuid, "status": "completed", "run_id": "run-99"}],
        ]
    )
    cmd = RequestAgentWakeCommand(
        agent_principal_ref="agent.exec.nate",
        trigger_kind="chat",
        payload={"foo": "bar"},
    )
    result = handle_request_agent_wake(cmd, _Subsystems(conn))
    assert result["ok"] is True
    assert result["duplicate"] is True
    assert result["wake_id"] == str(existing_uuid)


def test_file_tool_gap_persists_and_emits():
    import uuid

    gap_uuid = uuid.uuid4()
    conn = _Conn(
        scripted=[
            [{"gap_id": gap_uuid, "created_at": None}],  # INSERT RETURNING
            [],  # emit_system_event
        ]
    )
    cmd = FileAgentToolGapCommand(
        reporter_agent_ref="agent.exec.nate",
        missing_capability="gmail.search.advanced",
        attempted_task="Find emails about X",
        blocked_action="Could not narrow search beyond keyword",
        severity="high",
    )
    result = handle_file_agent_tool_gap(cmd, _Subsystems(conn))
    assert result["ok"] is True
    assert result["gap_id"] == str(gap_uuid)
    assert result["severity"] == "high"


# ─────────────────────────────────────────────────────────────────────────────
# Queries
# ─────────────────────────────────────────────────────────────────────────────


def _principal_row():
    import datetime

    return {
        "agent_principal_ref": "agent.exec.nate",
        "title": "Exec",
        "status": "active",
        "max_in_flight_wakes": 1,
        "write_envelope": [],
        "capability_refs": [],
        "integration_refs": [],
        "standing_order_keys": [],
        "allowed_tools": [],
        "network_policy": "praxis_only",
        "default_conversation_id": None,
        "routing_policy": None,
        "metadata": {},
        "created_at": datetime.datetime(2026, 5, 1),
        "updated_at": datetime.datetime(2026, 5, 1),
    }


def test_list_agent_principals_active_only_by_default():
    conn = _Conn(scripted=[[_principal_row()]])
    result = handle_list_agent_principals(
        ListAgentPrincipalsQuery(),
        _Subsystems(conn),
    )
    assert result["ok"] is True
    assert result["count"] == 1
    assert result["principals"][0]["agent_principal_ref"] == "agent.exec.nate"
    # SQL should filter by status='active'
    assert any("status = $1" in sql for sql, _ in conn.calls)


def test_list_agent_principals_any_drops_status_filter():
    conn = _Conn(scripted=[[_principal_row(), _principal_row()]])
    result = handle_list_agent_principals(
        ListAgentPrincipalsQuery(status="any"),
        _Subsystems(conn),
    )
    assert result["count"] == 2
    sql_seen = " ".join(sql for sql, _ in conn.calls)
    assert "status = $1" not in sql_seen


def test_describe_agent_principal_not_found():
    conn = _Conn(scripted=[[]])
    result = handle_describe_agent_principal(
        DescribeAgentPrincipalQuery(agent_principal_ref="agent.missing"),
        _Subsystems(conn),
    )
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.not_found"


def test_describe_agent_principal_returns_row_plus_history():
    conn = _Conn(
        scripted=[
            [_principal_row()],
            [],  # wakes
            [],  # delegations
            [],  # gaps
        ]
    )
    result = handle_describe_agent_principal(
        DescribeAgentPrincipalQuery(agent_principal_ref="agent.exec.nate"),
        _Subsystems(conn),
    )
    assert result["ok"] is True
    assert result["principal"]["agent_principal_ref"] == "agent.exec.nate"
    assert result["recent_wakes"] == []
    assert result["recent_delegations"] == []
    assert result["recent_tool_gaps"] == []


def test_list_agent_wakes_status_filter_in_sql():
    conn = _Conn(scripted=[[]])
    handle_list_agent_wakes(
        ListAgentWakesQuery(
            agent_principal_ref="agent.exec.nate",
            status="pending",
        ),
        _Subsystems(conn),
    )
    sql, args = conn.calls[0]
    assert "status = $2" in sql
    assert args[0] == "agent.exec.nate"
    assert args[1] == "pending"


def test_list_tool_gaps_default_status_open():
    conn = _Conn(scripted=[[]])
    handle_list_agent_tool_gaps(
        ListAgentToolGapsQuery(),
        _Subsystems(conn),
    )
    sql, args = conn.calls[0]
    assert "status = $1" in sql
    assert args[0] == "open"


# ─────────────────────────────────────────────────────────────────────────────
# Delegation
# ─────────────────────────────────────────────────────────────────────────────


def test_agent_delegate_parent_not_found():
    conn = _Conn(scripted=[[]])
    cmd = AgentDelegateCommand(
        parent_agent_ref="agent.missing",
        child_task="search_emails",
        child_intent="Find emails about X",
        caller_ref="agent.missing",  # caller-binding proof (BUG-1E7ED995)
    )
    result = handle_agent_delegate(cmd, _Subsystems(conn))
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.parent_not_found"


def test_agent_delegate_parent_inactive():
    conn = _Conn(
        scripted=[
            [
                {
                    "agent_principal_ref": "agent.exec.nate",
                    "status": "paused",
                    "write_envelope": [],
                    "capability_refs": [],
                    "integration_refs": [],
                    "standing_order_keys": [],
                    "allowed_tools": [],
                    "network_policy": "praxis_only",
                }
            ]
        ]
    )
    cmd = AgentDelegateCommand(
        parent_agent_ref="agent.exec.nate",
        child_task="x",
        child_intent="y",
        caller_ref="agent.exec.nate",  # caller-binding proof
    )
    result = handle_agent_delegate(cmd, _Subsystems(conn))
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.parent_inactive"


def test_agent_delegate_unverified_caller_is_rejected():
    """BUG-1E7ED995: a direct catalog caller without caller_ref / token /
    operator override cannot delegate under another agent's identity."""
    conn = _Conn(scripted=[])
    cmd = AgentDelegateCommand(
        parent_agent_ref="agent.exec.nate",
        child_task="x",
        child_intent="y",
        # No caller_ref, no workflow_token, no operator_override_decision_ref
    )
    result = handle_agent_delegate(cmd, _Subsystems(conn))
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.parent_caller_unverified"


def test_agent_delegate_caller_ref_mismatch_is_rejected():
    """BUG-1E7ED995: caller_ref must equal parent_agent_ref to pass."""
    conn = _Conn(scripted=[])
    cmd = AgentDelegateCommand(
        parent_agent_ref="agent.exec.nate",
        child_task="x",
        child_intent="y",
        caller_ref="agent.someone.else",  # mismatch
    )
    result = handle_agent_delegate(cmd, _Subsystems(conn))
    assert result["ok"] is False
    assert result["error_code"] == "agent_principal.parent_caller_unverified"


def test_agent_delegate_default_network_policy_is_praxis_only():
    cmd = AgentDelegateCommand(
        parent_agent_ref="agent.exec.nate",
        child_task="x",
        child_intent="y",
    )
    assert cmd.network_policy == "praxis_only"


def test_agent_delegate_admitted_lists_default_empty():
    cmd = AgentDelegateCommand(
        parent_agent_ref="agent.exec.nate",
        child_task="x",
        child_intent="y",
    )
    assert cmd.admitted_tools == []
    assert cmd.admitted_integrations == []
