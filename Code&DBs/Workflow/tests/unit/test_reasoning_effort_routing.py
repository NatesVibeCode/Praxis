from __future__ import annotations

import pytest

from runtime.reasoning_effort_routing import (
    ReasoningEffortRoutingError,
    normalize_transport_type,
    resolve_reasoning_effort_route,
)


class _FakeConn:
    def __init__(self, *, matrix_rows=None, policy_rows=None) -> None:
        self.matrix_rows = matrix_rows if matrix_rows is not None else [
            {
                "effort_matrix_ref": "reasoning_effort.openai.gpt-5-4-mini.cli.medium",
                "provider_payload": {"provider": "openai", "reasoning_effort": "medium"},
                "cost_multiplier": 1.0,
                "latency_multiplier": 1.0,
                "quality_bias": 0.0,
                "failure_risk": 0.0,
                "decision_ref": "operator_decision.reasoning_effort",
            }
        ]
        self.policy_rows = policy_rows if policy_rows is not None else [
            {
                "task_type": "build",
                "sub_task_type": "*",
                "default_effort_slug": "medium",
                "min_effort_slug": "low",
                "max_effort_slug": "high",
                "escalation_rules": {"on_failed_verification": "high"},
                "decision_ref": "operator_decision.reasoning_effort",
            }
        ]

    def execute(self, sql: str, *params):
        if "FROM task_type_effort_policy" in sql:
            return self.policy_rows
        if "FROM provider_reasoning_effort_matrix" in sql:
            provider_slug, model_slug, transport_type, effort_slug = params
            return [
                row
                for row in self.matrix_rows
                if row["effort_matrix_ref"].endswith(f"{transport_type}.{effort_slug}")
                and provider_slug == "openai"
                and model_slug == "gpt-5.4-mini"
            ]
        raise AssertionError(sql)


def test_resolves_default_effort_into_provider_payload() -> None:
    route = resolve_reasoning_effort_route(
        _FakeConn(),
        task_type="build",
        provider_slug="openai",
        model_slug="gpt-5.4-mini",
        transport_type="cli_llm",
    )

    assert route.effort_slug == "medium"
    assert route.transport_type == "cli"
    assert route.provider_payload == {
        "provider": "openai",
        "reasoning_effort": "medium",
    }
    assert route.as_reasoning_control()["effort_matrix_ref"] == (
        "reasoning_effort.openai.gpt-5-4-mini.cli.medium"
    )


def test_requested_effort_above_task_policy_fails_closed() -> None:
    with pytest.raises(ReasoningEffortRoutingError) as exc_info:
        resolve_reasoning_effort_route(
            _FakeConn(),
            task_type="build",
            provider_slug="openai",
            model_slug="gpt-5.4-mini",
            requested_effort="max",
        )

    assert exc_info.value.reason_code == "reasoning_effort.above_policy_maximum"
    assert exc_info.value.details["max_effort_slug"] == "high"


def test_missing_matrix_row_fails_closed() -> None:
    with pytest.raises(ReasoningEffortRoutingError) as exc_info:
        resolve_reasoning_effort_route(
            _FakeConn(matrix_rows=[]),
            task_type="build",
            provider_slug="openai",
            model_slug="gpt-5.4-mini",
        )

    assert exc_info.value.reason_code == "reasoning_effort.matrix_missing"


def test_task_policy_falls_back_to_default_row() -> None:
    route = resolve_reasoning_effort_route(
        _FakeConn(
            policy_rows=[
                {
                    "task_type": "*",
                    "sub_task_type": "*",
                    "default_effort_slug": "medium",
                    "min_effort_slug": "instant",
                    "max_effort_slug": "high",
                    "escalation_rules": {"fallback_policy": True},
                    "decision_ref": "operator_decision.reasoning_effort",
                }
            ]
        ),
        task_type="new_task_type",
        provider_slug="openai",
        model_slug="gpt-5.4-mini",
    )

    assert route.task_type == "new_task_type"
    assert route.effort_slug == "medium"


def test_transport_aliases_normalize_to_matrix_lane() -> None:
    assert normalize_transport_type("CLI") == "cli"
    assert normalize_transport_type("cli_llm") == "cli"
    assert normalize_transport_type("llm_task") == "api"
