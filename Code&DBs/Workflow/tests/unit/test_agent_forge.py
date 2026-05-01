"""Unit tests for runtime.operations.queries.agent_forge."""

from __future__ import annotations

from typing import Any

import pytest

from runtime.operations.queries.agent_forge import (
    QueryAgentForge,
    handle_query_agent_forge,
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


def test_forge_new_agent_with_complete_inputs_is_ok_to_register():
    conn = _Conn(
        scripted=[
            [],  # no existing agent
            # standing orders all resolve
            [
                {"decision_key": "architecture-policy::agent-behavior::no-shims"},
                {"decision_key": "architecture-policy::agent-behavior::collapse-simplify"},
            ],
            [],  # no capabilities to verify
            [],  # no integrations to verify
        ]
    )
    query = QueryAgentForge(
        agent_principal_ref="agent.exec.nate",
        title="Exec assistant",
        mission="Help the solo founder build",
        write_envelope=["Code&DBs/Workflow/artifacts/agent.exec.nate/**"],
        standing_order_keys=[
            "architecture-policy::agent-behavior::no-shims",
            "architecture-policy::agent-behavior::collapse-simplify",
        ],
        network_policy="praxis_only",
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert result["ok"] is True
    assert result["state"] == "new_agent"
    assert result["ok_to_register"] is True
    assert result["missing_inputs"] == []
    assert (
        result["validation"]["standing_orders_resolved"]
        == ["architecture-policy::agent-behavior::collapse-simplify",
            "architecture-policy::agent-behavior::no-shims"]
    )
    payload = result["register_payload"]
    assert payload["agent_principal_ref"] == "agent.exec.nate"
    assert payload["network_policy"] == "praxis_only"
    assert payload["metadata"]["mission"] == "Help the solo founder build"


def test_forge_existing_agent_is_not_ok_to_register():
    import datetime

    conn = _Conn(
        scripted=[
            [
                {
                    "agent_principal_ref": "agent.exec.nate",
                    "title": "Existing",
                    "status": "active",
                    "max_in_flight_wakes": 1,
                    "network_policy": "praxis_only",
                    "default_conversation_id": None,
                    "decision_ref": "architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet",
                    "write_envelope_size": 1,
                    "capability_refs_size": 0,
                    "integration_refs_size": 0,
                    "standing_order_keys_size": 5,
                    "allowed_tools_size": 9,
                    "created_at": datetime.datetime(2026, 5, 1),
                    "updated_at": datetime.datetime(2026, 5, 1),
                }
            ],
            # standing order resolution
            [{"decision_key": "architecture-policy::agent-behavior::no-shims"}],
            [],
            [],
        ]
    )
    query = QueryAgentForge(
        agent_principal_ref="agent.exec.nate",
        write_envelope=["a/**"],
        standing_order_keys=["architecture-policy::agent-behavior::no-shims"],
        mission="re-register",
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert result["state"] == "existing_agent"
    assert result["ok_to_register"] is False
    assert result["existing_agent"]["agent_principal_ref"] == "agent.exec.nate"


def test_forge_missing_write_envelope_blocks_active_agent():
    conn = _Conn(scripted=[[], [], [], []])
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        title="x",
        mission="test",
        write_envelope=[],
        standing_order_keys=["architecture-policy::agent-behavior::no-shims"],
        status="active",
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert "write_envelope" in result["missing_inputs"]
    assert result["ok_to_register"] is False


def test_forge_missing_standing_orders_blocks_registration():
    conn = _Conn(scripted=[[], [], [], []])
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        write_envelope=["a/**"],
        standing_order_keys=[],
        mission="test",
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert "standing_order_keys" in result["missing_inputs"]
    assert result["ok_to_register"] is False


def test_forge_unresolved_standing_orders_surface_as_validation_failure():
    conn = _Conn(
        scripted=[
            [],  # no existing agent
            # only one of two requested keys resolves
            [{"decision_key": "architecture-policy::agent-behavior::no-shims"}],
            [],
            [],
        ]
    )
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        title="x",
        mission="test",
        write_envelope=["a/**"],
        standing_order_keys=[
            "architecture-policy::agent-behavior::no-shims",
            "architecture-policy::agent-behavior::nonexistent-policy-key",
        ],
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert (
        "architecture-policy::agent-behavior::nonexistent-policy-key"
        in result["validation"]["standing_orders_unresolved"]
    )
    assert "standing_order_keys (unresolved)" in result["missing_inputs"]
    assert result["ok_to_register"] is False


def test_forge_default_decision_ref_anchors_business_agent_substrate():
    conn = _Conn(scripted=[[], [], [], []])
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        write_envelope=["a/**"],
        standing_order_keys=["architecture-policy::agent-behavior::no-shims"],
        mission="test",
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert (
        result["decision_ref"]
        == "architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet"
    )


def test_forge_register_command_is_renderable_shell():
    conn = _Conn(
        scripted=[
            [],
            [{"decision_key": "architecture-policy::agent-behavior::no-shims"}],
            [],
            [],
        ]
    )
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        title="x",
        mission="test",
        write_envelope=["a/**"],
        standing_order_keys=["architecture-policy::agent-behavior::no-shims"],
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    cmd = result["next_action_packet"]["register_command"]
    assert cmd.startswith(
        "praxis workflow tools call praxis_agent_principal_register"
    )
    assert "agent.x" in cmd


def test_forge_capability_catalog_missing_table_fails_soft():
    conn = _Conn(
        scripted=[
            [],
            [{"decision_key": "architecture-policy::agent-behavior::no-shims"}],
            Exception("relation \"capability_catalog\" does not exist"),
            [],
        ]
    )
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        title="x",
        mission="test",
        write_envelope=["a/**"],
        standing_order_keys=["architecture-policy::agent-behavior::no-shims"],
        capability_refs=["cap-task-code-generation"],
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    # Soft fail: forge still returns; capability_refs marked unresolved-by-default
    assert result["ok"] is True
    assert result["validation"]["capabilities_resolved"] == ["cap-task-code-generation"]


def test_forge_pydantic_validation_strips_blank_ref():
    with pytest.raises(Exception):
        QueryAgentForge(agent_principal_ref="   ")


def test_forge_reject_paths_present():
    conn = _Conn(scripted=[[], [], [], []])
    query = QueryAgentForge(
        agent_principal_ref="agent.x",
        title="x",
        mission="test",
        write_envelope=["a/**"],
        standing_order_keys=["architecture-policy::agent-behavior::no-shims"],
    )
    result = handle_query_agent_forge(query, _Subsystems(conn))
    assert len(result["reject_paths"]) >= 4
    assert any("policy cage" in p for p in result["reject_paths"])
