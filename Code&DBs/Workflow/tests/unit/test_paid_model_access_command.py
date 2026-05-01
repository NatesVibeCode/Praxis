from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from runtime.operations.commands.paid_model_access import (
    PaidModelAccessCommand,
    handle_paid_model_access,
)
from runtime.paid_model_access import (
    PaidModelAccessError,
    bind_paid_model_leases_to_run,
    is_paid_model_route,
)


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []
        self.fetchrow_response: dict[str, Any] | None = None
        self.execute_response: list[dict[str, Any]] = []

    def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append(("fetchrow", sql, args))
        return self.fetchrow_response

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append(("execute", sql, args))
        return self.execute_response


class _Subsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self._conn


def test_grant_once_rejects_wildcard_selector() -> None:
    conn = _FakeConn()
    command = PaidModelAccessCommand(
        action="grant_once",
        job_type="build",
        transport_type="API",
        adapter_type="llm_task",
        provider_slug="fireworks",
        model_slug="*",
        approval_ref="approval.p0",
        approved_by="nate@praxis",
        proposal_hash="hash",
    )

    with pytest.raises(ValueError, match="exact selector"):
        handle_paid_model_access(command, _Subsystems(conn))


def test_grant_once_inserts_one_run_lease() -> None:
    conn = _FakeConn()
    conn.fetchrow_response = {
        "lease_id": "paid-model-lease.abc",
        "status": "active",
        "runtime_profile_ref": "praxis",
        "job_type": "build",
        "transport_type": "API",
        "adapter_type": "llm_task",
        "provider_slug": "fireworks",
        "model_slug": "accounts/fireworks/models/kimi-k2p6",
        "expires_at": "2026-05-01T19:00:00+00:00",
    }
    command = PaidModelAccessCommand(
        action="grant_once",
        job_type="build",
        transport_type="API",
        adapter_type="llm_task",
        provider_slug="fireworks",
        model_slug="accounts/fireworks/models/kimi-k2p6",
        approval_ref="approval.p0",
        approved_by="nate@praxis",
        proposal_hash="hash",
        cost_posture={"billing_mode": "metered_api"},
    )

    result = handle_paid_model_access(command, _Subsystems(conn))

    assert result["ok"] is True
    assert result["action"] == "grant_once"
    assert result["lease"]["lease_id"] == "paid-model-lease.abc"
    assert result["event_payload"]["selector"]["provider_slug"] == "fireworks"
    assert conn.calls[0][0] == "fetchrow"
    assert "INSERT INTO private_paid_model_access_leases" in conn.calls[0][1]
    assert isinstance(conn.calls[0][2][11], datetime)


def test_preview_uses_nested_selector_filters() -> None:
    conn = _FakeConn()
    command = PaidModelAccessCommand(
        action="preview",
        selector={
            "runtime_profile_ref": "scratch_agent",
            "job_type": "analysis",
            "transport_type": "API",
            "adapter_type": "llm_task",
            "provider_slug": "Anthropic",
            "model_slug": "claude-sonnet-4-6",
        },
        limit=7,
    )

    result = handle_paid_model_access(command, _Subsystems(conn))

    assert result["ok"] is True
    assert result["count"] == 0
    assert conn.calls[0][0] == "execute"
    assert conn.calls[0][2] == (
        "scratch_agent",
        "analysis",
        "API",
        "llm_task",
        "anthropic",
        "claude-sonnet-4-6",
        7,
    )


def test_unknown_payload_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        PaidModelAccessCommand(action="preview", typo_selector={})


def test_soft_off_writes_presentation_only_state() -> None:
    conn = _FakeConn()
    conn.fetchrow_response = {
        "presentation_state": "soft_off",
        "provider_slug": "fireworks",
    }
    command = PaidModelAccessCommand(
        action="soft_off",
        job_type="build",
        transport_type="API",
        adapter_type="llm_task",
        provider_slug="fireworks",
        model_slug="accounts/fireworks/models/kimi-k2p6",
    )

    result = handle_paid_model_access(command, _Subsystems(conn))

    assert result["ok"] is True
    assert result["action"] == "soft_off"
    assert result["row"]["presentation_state"] == "soft_off"
    assert "private_provider_model_access_soft_offs" in conn.calls[0][1]


def test_bind_lease_requires_every_requested_lease_to_bind() -> None:
    conn = _FakeConn()
    conn.execute_response = [{"lease_id": "lease.one"}]

    with pytest.raises(PaidModelAccessError, match="could not be bound"):
        bind_paid_model_leases_to_run(
            conn,
            lease_ids=["lease.one", "lease.two"],
            run_id="workflow_123",
        )


def test_paid_detection_uses_reason_and_metered_cost() -> None:
    assert is_paid_model_route(reason_code="paid_model.default_hard_off") is True
    assert is_paid_model_route(cost_structure="metered_api") is True
    assert is_paid_model_route(cost_structure="subscription_included") is False
