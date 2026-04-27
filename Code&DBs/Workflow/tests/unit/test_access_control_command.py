"""Unit tests for runtime.operations.commands.access_control.

The handler reads/writes the control-panel denial table and refreshes the
projection through the runtime SyncPostgresConnection (asyncpg-style API:
fetch / fetchrow / execute). These tests stub that interface so command
validation, SQL shape, and refresh ordering are exercised without a live
Postgres connection.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime.operations.commands.access_control import (
    AccessControlCommand,
    handle_access_control,
)


class _FakeConn:
    """Minimal SyncPostgresConnection-shaped stub.

    Records every call (method, sql, args) so tests can assert SQL shape and
    call ordering. ``fetchrow_response`` and ``fetch_response`` are stuffed
    in advance; ``execute_response`` is the string returned for execute().
    """

    def __init__(
        self,
        fetchrow_response: dict | None = None,
        fetch_response: list[dict] | None = None,
        execute_response: str = "DELETE 0",
    ) -> None:
        self.calls: list[tuple[str, str, tuple]] = []
        self._fetchrow_response = fetchrow_response
        self._fetch_response = fetch_response or []
        self._execute_response = execute_response

    def fetch(self, sql: str, *args: Any) -> list[dict]:
        self.calls.append(("fetch", sql, args))
        return list(self._fetch_response)

    def fetchrow(self, sql: str, *args: Any) -> dict | None:
        self.calls.append(("fetchrow", sql, args))
        return self._fetchrow_response

    def execute(self, sql: str, *args: Any) -> str:
        self.calls.append(("execute", sql, args))
        return self._execute_response


class _FakeSubsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self._conn


def test_disable_emits_event_payload_and_refreshes_projection() -> None:
    upsert_row = {
        "runtime_profile_ref": "praxis", "job_type": "*", "transport_type": "CLI",
        "adapter_type": "*", "provider_slug": "openai", "model_slug": "*",
        "denied": True, "reason_code": "control_panel.model_access_method_turned_off",
        "operator_message": "test message", "decision_ref": "decision.test",
        "created_at": None, "updated_at": None,
    }
    conn = _FakeConn(fetchrow_response=upsert_row)
    command = AccessControlCommand(
        action="disable",
        provider_slug="openai",
        transport_type="CLI",
        decision_ref="decision.test",
        operator_message="test message",
    )
    result = handle_access_control(command, _FakeSubsystems(conn))

    assert result["ok"] is True
    assert result["action"] == "disable"
    assert result["row"]["provider_slug"] == "openai"
    assert result["row"]["transport_type"] == "CLI"
    assert result["row"]["denied"] is True
    assert result["event_payload"]["denied"] is True
    assert result["event_payload"]["selector"]["provider_slug"] == "openai"
    assert result["event_payload"]["decision_ref"] == "decision.test"

    upsert_call = conn.calls[0]
    assert upsert_call[0] == "fetchrow"
    assert "INSERT INTO private_provider_model_access_denials" in upsert_call[1]
    assert "ON CONFLICT" in upsert_call[1]

    refresh_call = conn.calls[1]
    assert refresh_call[0] == "execute"
    assert "refresh_private_provider_job_catalog" in refresh_call[1]
    assert refresh_call[2] == ("praxis",)


def test_enable_deletes_row_and_refreshes_projection() -> None:
    conn = _FakeConn(execute_response="DELETE 1")
    command = AccessControlCommand(
        action="enable",
        provider_slug="openai",
        transport_type="CLI",
    )
    result = handle_access_control(command, _FakeSubsystems(conn))

    assert result["ok"] is True
    assert result["action"] == "enable"
    assert result["deleted_count"] == 1
    assert result["event_payload"]["denied"] is False

    delete_call = conn.calls[0]
    assert delete_call[0] == "execute"
    assert "DELETE FROM private_provider_model_access_denials" in delete_call[1]

    refresh_call = conn.calls[1]
    assert refresh_call[0] == "execute"
    assert "refresh_private_provider_job_catalog" in refresh_call[1]


def test_list_filters_by_selector() -> None:
    sample_row = {
        "runtime_profile_ref": "praxis", "job_type": "*", "transport_type": "CLI",
        "adapter_type": "*", "provider_slug": "openai", "model_slug": "*",
        "denied": True, "reason_code": "control_panel.model_access_method_turned_off",
        "operator_message": "msg", "decision_ref": "decision.x",
        "created_at": None, "updated_at": None,
    }
    conn = _FakeConn(fetch_response=[sample_row])
    command = AccessControlCommand(
        action="list",
        provider_slug="openai",
    )
    result = handle_access_control(command, _FakeSubsystems(conn))

    assert result["ok"] is True
    assert result["action"] == "list"
    assert result["count"] == 1
    assert result["rows"][0]["provider_slug"] == "openai"

    list_call = conn.calls[0]
    assert list_call[0] == "fetch"
    assert "FROM private_provider_model_access_denials" in list_call[1]
    assert "denied = TRUE" in list_call[1]

    # List action must not refresh the projection
    assert len(conn.calls) == 1


def test_disable_requires_decision_ref() -> None:
    conn = _FakeConn()
    command = AccessControlCommand(
        action="disable",
        provider_slug="openai",
        transport_type="CLI",
    )
    with pytest.raises(ValueError, match="decision_ref is required"):
        handle_access_control(command, _FakeSubsystems(conn))


def test_action_must_be_valid() -> None:
    with pytest.raises(ValueError, match="action must be one of"):
        AccessControlCommand(action="nuke")


def test_transport_type_must_be_valid() -> None:
    with pytest.raises(ValueError, match="transport_type must be one of"):
        AccessControlCommand(transport_type="udp")


def test_wildcard_is_default_selector() -> None:
    command = AccessControlCommand(action="list")
    assert command.runtime_profile_ref == "praxis"
    assert command.job_type == "*"
    assert command.transport_type == "*"
    assert command.adapter_type == "*"
    assert command.provider_slug == "*"
    assert command.model_slug == "*"
