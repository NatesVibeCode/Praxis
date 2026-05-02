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
                    "route_rank": 1,
                    "route_temperature": 0.2,
                    "route_max_tokens": 32768,
                    "route_reasoning_control": {"default_level": "medium"},
                    "control_enabled": True,
                    "control_state": "on",
                    "control_scope": "transport_default_allow",
                    "control_is_explicit": False,
                    "control_reason_code": "control_panel.transport_default_allowed",
                    "control_decision_ref": "decision.model_access_control.default_transport_policy",
                    "control_operator_message": "this Model Access method is currently enabled by the control panel.",
                    "credential_availability_state": "available",
                    "credential_sources": ["ambient_cli_session"],
                    "credential_observations": [],
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
                    "provider_slug": "anthropic",
                    "model_slug": "claude-disabled-by-policy",
                    "model_version": "claude-disabled-by-policy",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {"billing_mode": "subscription_included"},
                    "route_rank": 3,
                    "control_enabled": False,
                    "control_state": "off",
                    "control_scope": "task/provider/model/access_method_denylist",
                    "control_is_explicit": True,
                    "control_reason_code": "control_panel.model_access_method_turned_off",
                    "control_decision_ref": "operator_decision.architecture_policy.provider_routing.anthropic_disabled_2026_04_27",
                    "control_operator_message": "Anthropic is disabled by operator policy.",
                    "credential_availability_state": "available",
                    "credential_sources": ["ambient_cli_session"],
                    "credential_observations": [],
                    "capability_state": "runnable",
                    "is_runnable": True,
                    "breaker_state": "CLOSED",
                    "manual_override_state": None,
                    "primary_removal_reason_code": None,
                    "removal_reasons": [],
                    "candidate_ref": "candidate.anthropic.cli.claude-disabled-by-policy",
                    "provider_ref": "provider.anthropic",
                    "source_refs": ["table.task_type_routing"],
                    "projected_at": "2026-04-26T00:00:00Z",
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": "build",
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "model_version": "claude-sonnet-4-6",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {"billing_mode": "subscription_included"},
                    "route_rank": 2,
                    "control_enabled": True,
                    "control_state": "on",
                    "control_scope": "transport_default_allow",
                    "control_is_explicit": False,
                    "control_reason_code": "control_panel.transport_default_allowed",
                    "control_decision_ref": "decision.model_access_control.default_transport_policy",
                    "control_operator_message": "this Model Access method is currently enabled by the control panel.",
                    "credential_availability_state": "available",
                    "credential_sources": ["ambient_cli_session"],
                    "credential_observations": [],
                    "capability_state": "removed",
                    "is_runnable": False,
                    "breaker_state": "CLOSED",
                    "manual_override_state": None,
                    "primary_removal_reason_code": "runtime_profile_route.not_admitted",
                    "removal_reasons": [
                        {
                            "reason_code": "runtime_profile_route.not_admitted",
                            "source_ref": "projection.private_provider_job_catalog",
                            "details": {
                                "availability_state": "disabled",
                                "transport_type": "CLI",
                                "adapter_type": "cli_llm",
                            },
                        }
                    ],
                    "candidate_ref": "candidate.anthropic.cli.claude-sonnet-4-6",
                    "provider_ref": "provider.anthropic",
                    "source_refs": [
                        "table.runtime_profile_admitted_routes",
                        "table.private_provider_transport_control_policy",
                    ],
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
                    "cost_metadata": {
                        "billing_mode": "metered_api",
                        "budget_bucket": "openai_api_payg",
                        "effective_marginal_cost": 8.0,
                    },
                    "budget_window": {
                        "provider_policy_id": "provider_policy.openai",
                        "provider_ref": "provider.openai",
                        "budget_status": "warning",
                        "request_limit": 100,
                        "requests_used": 82,
                    },
                    "route_rank": 1,
                    "control_enabled": False,
                    "control_state": "off",
                    "control_scope": "transport_default_deny",
                    "control_is_explicit": False,
                    "control_reason_code": "control_panel.transport_turned_off",
                    "control_decision_ref": "decision.private-api-control-panel",
                    "control_operator_message": (
                        "this Model Access method has been turned off on purpose "
                        "at the control panel either for this specific task type, "
                        "or more broadly, consult the control panel and do not "
                        "turn it on without confirming with the user even if you "
                        "think that will help you complete your task."
                    ),
                    "credential_availability_state": "missing",
                    "credential_sources": ["OPENAI_API_KEY"],
                    "credential_observations": [
                        {
                            "credential_ref": "OPENAI_API_KEY",
                            "status": "failed",
                            "source_kind": "env",
                        }
                    ],
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
    assert payload["rows"][0]["mechanical_capability_state"] == "runnable"
    assert payload["rows"][0]["mechanical_is_runnable"] is True
    assert payload["rows"][0]["capability_state"] == "runnable"
    assert payload["rows"][0]["is_runnable"] is True
    assert payload["rows"][0]["effective_dispatch_state"] == "runnable"
    assert payload["rows"][0]["control_state"] == "on"
    assert payload["rows"][0]["control_enabled"] is True
    assert payload["rows"][0]["route_request"] == {
        "rank": 1,
        "temperature": 0.2,
        "max_tokens": 32768,
        "reasoning_control": {"default_level": "medium"},
        "request_contract_ref": None,
        "cache_policy": {},
        "structured_output_policy": {},
        "streaming_policy": {},
    }
    assert payload["rows"][0]["credential_availability_state"] == "available"
    assert payload["rows"][0]["credential_sources"] == ["ambient_cli_session"]
    assert payload["rows"][0]["projection_ref"] == "projection.private_provider_control_plane_snapshot"
    assert payload["capability_matrix"][0]["provider"] == "anthropic"
    assert payload["capability_matrix"][0]["model"] == "claude-opus-4-7"
    assert payload["capability_matrix"][0]["route_rank"] == 1
    assert payload["capability_matrix"][0]["effective_availability_state"] == "available"
    assert payload["capability_matrix"][0]["credential_sources"] == ["ambient_cli_session"]
    assert payload["capability_matrix"][0]["credential_observations"] == []
    assert payload["route_explanation"]["selected_routes"][0]["provider_slug"] == "anthropic"
    assert payload["route_explanation"]["selected_routes"][0]["model_slug"] == "claude-opus-4-7"
    assert payload["route_explanation"]["selected_routes"][0]["route_rank"] == 1
    assert payload["route_explanation"]["selected_routes"][0]["credential_sources"] == [
        "ambient_cli_session"
    ]
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
    assert openai_row["control_state"] == "off"
    assert openai_row["control_reason_code"] == "control_panel.transport_turned_off"
    assert openai_row["credential_availability_state"] == "missing"
    assert openai_row["credential_sources"] == ["OPENAI_API_KEY"]
    assert openai_row["breaker_state"] == "OPEN"
    assert openai_row["cost_posture"] == {
        "billing_mode": "metered_api",
        "budget_bucket": "openai_api_payg",
        "pricing_model": "",
        "effective_marginal_cost": 8.0,
        "prefer_prepaid": None,
        "allow_payg_fallback": None,
        "budget_status": "warning",
        "spend_pressure": "medium",
        "budget_window": {
            "provider_policy_id": "provider_policy.openai",
            "provider_ref": "provider.openai",
            "budget_status": "warning",
            "request_limit": 100,
            "requests_used": 82,
        },
    }
    assert openai_row["primary_removal_reason_code"] == "provider_transport.policy_denied"
    assert [reason["reason_code"] for reason in openai_row["removal_reasons"]] == [
        "control_panel.transport_turned_off",
        "provider_transport.policy_denied",
        "circuit_breaker.open",
    ]
    openai_matrix_row = [
        row for row in payload["capability_matrix"] if row["provider_slug"] == "openai"
    ][0]
    assert openai_matrix_row["effective_availability_state"] == "blocked"
    assert openai_matrix_row["blocked_reasons"] == [
        "control_panel.transport_turned_off",
        "provider_transport.policy_denied",
        "circuit_breaker.open",
        "credential.missing",
    ]
    assert openai_matrix_row["credential_sources"] == ["OPENAI_API_KEY"]
    assert openai_matrix_row["credential_observations"] == [
        {
            "credential_ref": "OPENAI_API_KEY",
            "status": "failed",
            "source_kind": "env",
        }
    ]
    assert openai_matrix_row["cost_posture"]["spend_pressure"] == "medium"
    openai_explanation = [
        row
        for row in payload["route_explanation"]["candidates"]
        if row["provider_slug"] == "openai"
    ][0]
    assert openai_explanation["availability"] == "blocked"
    assert openai_explanation["removed_reasons"] == [
        "control_panel.transport_turned_off",
        "provider_transport.policy_denied",
        "circuit_breaker.open",
        "credential.missing",
    ]
    assert openai_explanation["circuit_state"] == "OPEN"
    assert openai_explanation["credential_sources"] == ["OPENAI_API_KEY"]
    assert openai_explanation["credential_observations"] == [
        {
            "credential_ref": "OPENAI_API_KEY",
            "status": "failed",
            "source_kind": "env",
        }
    ]
    assert openai_explanation["cost_posture"]["budget_status"] == "warning"

    disabled_candidate_row = [
        row
        for row in payload["rows"]
        if row["model_slug"] == "claude-sonnet-4-6"
    ][0]
    assert disabled_candidate_row["primary_removal_reason_code"] == (
        "provider_job_catalog.availability_disabled"
    )
    assert [reason["reason_code"] for reason in disabled_candidate_row["removal_reasons"]] == [
        "provider_job_catalog.availability_disabled",
    ]


def test_provider_control_plane_control_off_overrides_mechanical_runnable() -> None:
    conn = _FakeConn()

    payload = handle_query_provider_control_plane(
        QueryProviderControlPlane(runtime_profile_ref="nate-private"),
        _FakeSubsystems(conn),
    )

    disabled_row = [
        row
        for row in payload["rows"]
        if row["model_slug"] == "claude-disabled-by-policy"
    ][0]
    assert disabled_row["control_enabled"] is False
    assert disabled_row["control_state"] == "off"
    assert disabled_row["mechanical_capability_state"] == "runnable"
    assert disabled_row["mechanical_is_runnable"] is True
    assert disabled_row["capability_state"] == "removed"
    assert disabled_row["is_runnable"] is False
    assert disabled_row["effective_dispatch_state"] == "disabled"
    assert disabled_row["primary_removal_reason_code"] == (
        "control_panel.model_access_method_turned_off"
    )
    assert disabled_row["removal_reasons"][0]["reason_code"] == (
        "control_panel.model_access_method_turned_off"
    )
    assert disabled_row["removal_reasons"][0]["source_ref"] == (
        "operator_decision.architecture_policy.provider_routing.anthropic_disabled_2026_04_27"
    )


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
