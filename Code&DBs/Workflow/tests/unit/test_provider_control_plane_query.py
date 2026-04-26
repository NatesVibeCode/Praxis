from __future__ import annotations

from typing import Any

from runtime.operations.queries.circuits import (
    QueryCircuitStates,
    QueryProviderControlPlane,
    handle_query_circuit_states,
    handle_query_provider_control_plane,
)


class _FakeConn:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.execute_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM private_provider_control_plane_snapshot" in normalized:
            return [
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": "build",
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-opus-4-7",
                    "model_version": "claude-opus-4-7",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {"billing_mode": "subscription_included"},
                    "capability_state": "runnable",
                    "is_runnable": True,
                    "breaker_state": "CLOSED",
                    "manual_override_state": None,
                    "primary_removal_reason_code": None,
                    "removal_reasons": [],
                    "candidate_ref": "candidate.anthropic.cli.claude-opus-4-7",
                    "provider_ref": "provider.anthropic",
                    "source_refs": ["table.task_type_routing"],
                    "projected_at": "2026-04-26T00:00:00Z",
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": "build",
                    "transport_type": "API",
                    "adapter_type": "llm_task",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "model_version": "gpt-5.4",
                    "cost_structure": "metered_api",
                    "cost_metadata": {"billing_mode": "metered_api"},
                    "capability_state": "removed",
                    "is_runnable": False,
                    "breaker_state": "OPEN",
                    "manual_override_state": "OPEN",
                    "primary_removal_reason_code": "provider_transport.policy_denied",
                    "removal_reasons": [
                        {
                            "reason_code": "provider_transport.policy_denied",
                            "source_ref": "projection.private_provider_job_catalog",
                            "details": {"availability_state": "disabled"},
                        },
                        {
                            "reason_code": "circuit_breaker.open",
                            "source_ref": "projection.circuit_breakers",
                            "details": {"breaker_state": "OPEN"},
                        },
                    ],
                    "candidate_ref": "candidate.openai.gpt-5.4",
                    "provider_ref": "provider.openai",
                    "source_refs": [
                        "table.provider_transport_admissions",
                        "table.provider_circuit_breaker_state",
                    ],
                    "projected_at": "2026-04-26T00:00:00Z",
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
            ]
        if "FROM effective_provider_circuit_breaker_state" in normalized:
            return [
                {
                    "provider_slug": "openai",
                    "runtime_state": "OPEN",
                    "effective_state": "OPEN",
                    "manual_override_state": "OPEN",
                    "manual_override_reason": "off",
                    "failure_count": 3,
                    "success_count": 1,
                    "failure_threshold": 3,
                    "recovery_timeout_s": 60.0,
                    "half_open_max_calls": 1,
                    "last_failure_at": "2026-04-26T00:00:00Z",
                    "opened_at": "2026-04-26T00:00:00Z",
                    "half_open_after": "2026-04-26T00:01:00Z",
                    "half_open_calls": 0,
                    "updated_at": "2026-04-26T00:00:01Z",
                    "projected_at": "2026-04-26T00:00:01Z",
                    "projection_ref": "projection.circuit_breakers",
                }
            ]
        raise AssertionError(f"unexpected query: {query}")

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM authority_projection_state" in normalized:
            projection_ref = str(args[0])
            return {
                "projection_ref": projection_ref,
                "freshness_status": "fresh",
                "last_refreshed_at": "2026-04-26T00:00:02Z",
                "error_code": None,
                "error_detail": None,
            }
        raise AssertionError(f"unexpected fetchrow: {query}")


class _FakeSubsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self._conn


def test_provider_control_plane_returns_projected_snapshot_payload() -> None:
    conn = _FakeConn()

    payload = handle_query_provider_control_plane(
        QueryProviderControlPlane(
            runtime_profile_ref="nate-private",
            job_type="build",
        ),
        _FakeSubsystems(conn),
    )

    assert payload["control_plane"] == "operator.provider_control_plane"
    assert payload["runtime_profile_ref"] == "nate-private"
    assert payload["filters"] == {
        "provider_slug": None,
        "job_type": "build",
        "transport_type": None,
        "model_slug": None,
    }
    assert payload["projection_freshness"] == {
        "projection_ref": "projection.private_provider_control_plane_snapshot",
        "freshness_status": "fresh",
        "last_refreshed_at": "2026-04-26T00:00:02Z",
        "error_code": None,
        "error_detail": None,
    }
    assert payload["rows"][0]["provider_slug"] == "anthropic"
    assert payload["rows"][0]["capability_state"] == "runnable"
    assert payload["rows"][0]["is_runnable"] is True
    assert payload["rows"][0]["projection_ref"] == "projection.private_provider_control_plane_snapshot"
    assert "operator.circuit_override" in payload["levers"]["commands"]


def test_provider_control_plane_surfaces_structured_removal_reasons() -> None:
    conn = _FakeConn()

    payload = handle_query_provider_control_plane(
        QueryProviderControlPlane(runtime_profile_ref="nate-private"),
        _FakeSubsystems(conn),
    )

    openai_row = [row for row in payload["rows"] if row["provider_slug"] == "openai"][0]
    assert openai_row["capability_state"] == "removed"
    assert openai_row["is_runnable"] is False
    assert openai_row["breaker_state"] == "OPEN"
    assert openai_row["primary_removal_reason_code"] == "provider_transport.policy_denied"
    assert [reason["reason_code"] for reason in openai_row["removal_reasons"]] == [
        "provider_transport.policy_denied",
        "circuit_breaker.open",
    ]


def test_circuit_states_reads_durable_projection() -> None:
    conn = _FakeConn()

    payload = handle_query_circuit_states(
        QueryCircuitStates(provider_slug="openai"),
        _FakeSubsystems(conn),
    )

    assert payload["projection_freshness"]["projection_ref"] == "projection.circuit_breakers"
    assert payload["circuits"]["openai"]["state"] == "OPEN"
    assert payload["circuits"]["openai"]["manual_override"] == {
        "override_state": "OPEN",
        "rationale": "off",
    }
