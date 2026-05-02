"""Unit tests for runtime.agent_context."""

from __future__ import annotations

from typing import Any

import pytest

from runtime.agent_context import (
    AgentContextEnvelope,
    compile_agent_context,
    in_flight_wake_count,
)


class _FakePg:
    """Minimal sync-postgres-shaped stub.

    Each call captures (sql, args). The `responses` mapping looks up
    by a substring of the SQL — first match wins.
    """

    def __init__(self, responses: dict[str, list[dict[str, Any]]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql, args))
        for needle, rows in self.responses.items():
            if needle in sql:
                return rows
        return []


def _principal_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "agent_principal_ref": "agent.exec.nate",
        "title": "Exec assistant",
        "status": "active",
        "max_in_flight_wakes": 1,
        "write_envelope": ["Code&DBs/Workflow/artifacts/agent.exec.nate/**"],
        "capability_refs": ["capability.search.federated"],
        "integration_refs": ["gmail", "calendar"],
        "standing_order_keys": [
            "architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet"
        ],
        "allowed_tools": ["praxis_search", "praxis_orient"],
        "network_policy": "praxis_only",
        "default_conversation_id": "conv-1",
        "routing_policy": None,
        "metadata": {},
    }
    base.update(overrides)
    return base


def test_compile_agent_context_returns_none_for_unknown_principal():
    pg = _FakePg(responses={})
    out = compile_agent_context(
        pg,
        agent_principal_ref="agent.does_not_exist",
        trigger_kind="schedule",
    )
    assert out is None


def test_compile_agent_context_active_principal_emits_inline_spec():
    pg = _FakePg(
        responses={
            "FROM agent_registry\n           WHERE agent_principal_ref": [_principal_row()],
            "FROM operator_decisions": [
                {
                    "decision_key": "architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet",
                    "decision_kind": "architecture_policy",
                    "decision_status": "active",
                    "title": "Delegated workers, praxis_only, no internet",
                    "rationale": "Bound to limit blast radius.",
                    "decision_scope_kind": None,
                    "decision_scope_ref": None,
                }
            ],
            "FROM agent_wakes\n           WHERE agent_principal_ref": [],
            "FROM chat_messages": [],
        }
    )
    envelope = compile_agent_context(
        pg,
        agent_principal_ref="agent.exec.nate",
        trigger_kind="schedule",
        trigger_source_ref="trigger-42",
        payload={"now": "2026-05-01T08:00:00Z"},
    )
    assert isinstance(envelope, AgentContextEnvelope)
    assert envelope.agent_status == "active"
    assert envelope.network_policy == "praxis_only"
    assert envelope.write_envelope == (
        "Code&DBs/Workflow/artifacts/agent.exec.nate/**",
    )
    assert envelope.allowed_tools == ("praxis_search", "praxis_orient")
    assert envelope.integration_refs == ("gmail", "calendar")

    spec = envelope.inline_spec
    assert spec["name"].startswith("agent_wake::agent.exec.nate::schedule")
    assert spec["jobs"][0]["task_type"] == "agent_wake"
    access = spec["metadata"]["execution_bundle"]["access_policy"]
    assert access["network_policy"] == "praxis_only"
    assert access["write_scope"] == [
        "Code&DBs/Workflow/artifacts/agent.exec.nate/**"
    ]
    assert access["allowed_tools"] == ["praxis_search", "praxis_orient"]
    assert "trigger-42" in spec["jobs"][0]["prompt"]
    assert "praxis_only" in spec["jobs"][0]["prompt"]
    assert envelope.payload_hash  # non-empty


def test_compile_agent_context_paused_principal_returns_skip_envelope():
    pg = _FakePg(
        responses={
            "FROM agent_registry": [_principal_row(status="paused")],
        }
    )
    envelope = compile_agent_context(
        pg,
        agent_principal_ref="agent.exec.nate",
        trigger_kind="schedule",
    )
    assert envelope is not None
    assert envelope.agent_status == "paused"
    assert envelope.inline_spec == {}


def test_compile_agent_context_killed_principal_does_not_emit_spec():
    pg = _FakePg(
        responses={
            "FROM agent_registry": [_principal_row(status="killed")],
        }
    )
    envelope = compile_agent_context(
        pg,
        agent_principal_ref="agent.exec.nate",
        trigger_kind="webhook",
    )
    assert envelope is not None
    assert envelope.agent_status == "killed"
    assert envelope.inline_spec == {}


def test_in_flight_wake_count_returns_int():
    pg = _FakePg(responses={"FROM agent_wakes": [{"n": 3}]})
    assert in_flight_wake_count(pg, "agent.exec.nate") == 3


def test_in_flight_wake_count_zero_when_no_rows():
    pg = _FakePg(responses={})
    assert in_flight_wake_count(pg, "agent.exec.nate") == 0


def test_payload_hash_changes_with_trigger_kind():
    pg = _FakePg(
        responses={
            "FROM agent_registry": [_principal_row()],
            "FROM operator_decisions": [],
            "FROM agent_wakes": [],
            "FROM chat_messages": [],
        }
    )
    e1 = compile_agent_context(
        pg, agent_principal_ref="agent.exec.nate", trigger_kind="schedule",
        payload={"a": 1},
    )
    e2 = compile_agent_context(
        pg, agent_principal_ref="agent.exec.nate", trigger_kind="webhook",
        payload={"a": 1},
    )
    assert e1 is not None and e2 is not None
    assert e1.payload_hash != e2.payload_hash


def test_payload_hash_identical_for_identical_inputs():
    pg = _FakePg(
        responses={
            "FROM agent_registry": [_principal_row()],
            "FROM operator_decisions": [],
            "FROM agent_wakes": [],
            "FROM chat_messages": [],
        }
    )
    e1 = compile_agent_context(
        pg, agent_principal_ref="agent.exec.nate", trigger_kind="schedule",
        payload={"a": 1, "b": 2},
    )
    e2 = compile_agent_context(
        pg, agent_principal_ref="agent.exec.nate", trigger_kind="schedule",
        payload={"b": 2, "a": 1},  # same fields, different key order
    )
    assert e1 is not None and e2 is not None
    assert e1.payload_hash == e2.payload_hash, (
        "payload hash must be order-independent so dedup works"
    )


def test_jsonb_list_normalisation_handles_string_input():
    """Postgres returns jsonb as either list or json string depending on driver.

    The normaliser should accept both."""
    pg = _FakePg(
        responses={
            "FROM agent_registry": [
                _principal_row(
                    write_envelope='["a/**", "b/**"]',  # raw json string
                    allowed_tools='["t1", "t2"]',
                )
            ],
            "FROM operator_decisions": [],
            "FROM agent_wakes": [],
            "FROM chat_messages": [],
        }
    )
    envelope = compile_agent_context(
        pg,
        agent_principal_ref="agent.exec.nate",
        trigger_kind="schedule",
    )
    assert envelope is not None
    assert envelope.write_envelope == ("a/**", "b/**")
    assert envelope.allowed_tools == ("t1", "t2")
