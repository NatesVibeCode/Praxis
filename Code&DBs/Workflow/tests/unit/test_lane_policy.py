from __future__ import annotations

from runtime.lane_policy import (
    ProviderLanePolicy,
    admit_adapter_type,
    load_provider_lane_policies,
)


class _StubConn:
    """Minimal conn exposing execute() with a fixed row set."""

    def __init__(self, rows, *, raise_undefined: bool = False) -> None:
        self._rows = rows
        self._raise_undefined = raise_undefined

    def execute(self, _sql, *_params):
        if self._raise_undefined:
            import asyncpg
            err = asyncpg.PostgresError()
            err.sqlstate = "42P01"
            raise err
        return self._rows


def _policies() -> dict[str, ProviderLanePolicy]:
    return {
        "anthropic": ProviderLanePolicy(
            provider_slug="anthropic",
            allowed_adapter_types=frozenset({"cli_llm"}),
            overridable=False,
        ),
        "openai": ProviderLanePolicy(
            provider_slug="openai",
            allowed_adapter_types=frozenset({"cli_llm", "llm_task"}),
            overridable=True,
        ),
    }


def test_admit_anthropic_cli_is_allowed() -> None:
    admitted, reason = admit_adapter_type(_policies(), "anthropic", "cli_llm")
    assert admitted is True
    assert reason == "lane.admitted"


def test_admit_anthropic_llm_task_is_rejected() -> None:
    """The whole point of the gate: anthropic llm_task must be refused."""
    admitted, reason = admit_adapter_type(_policies(), "anthropic", "llm_task")
    assert admitted is False
    assert reason == "lane.rejected.not_allowed"


def test_admit_openai_llm_task_is_allowed() -> None:
    admitted, reason = admit_adapter_type(_policies(), "openai", "llm_task")
    assert admitted is True
    assert reason == "lane.admitted"


def test_admit_openai_cli_llm_is_allowed() -> None:
    admitted, reason = admit_adapter_type(_policies(), "openai", "cli_llm")
    assert admitted is True
    assert reason == "lane.admitted"


def test_admit_unknown_adapter_type_rejected() -> None:
    admitted, reason = admit_adapter_type(_policies(), "anthropic", "mystery")
    assert admitted is False
    assert reason == "lane.rejected.unknown_adapter"


def test_admit_unknown_provider_fails_closed() -> None:
    """No policy row for a provider means no authority to run it."""
    admitted, reason = admit_adapter_type(_policies(), "unlisted", "llm_task")
    assert admitted is False
    assert reason == "lane.rejected.no_policy"


def test_admit_case_insensitive_adapter_type() -> None:
    admitted, _ = admit_adapter_type(_policies(), "anthropic", "CLI_LLM")
    assert admitted is True


def test_load_parses_rows_into_policies() -> None:
    rows = [
        {
            "provider_slug": "anthropic",
            "allowed_adapter_types": ["cli_llm"],
            "overridable": False,
        },
        {
            "provider_slug": "openai",
            "allowed_adapter_types": ["cli_llm", "llm_task"],
            "overridable": True,
        },
    ]
    result = load_provider_lane_policies(_StubConn(rows))
    assert set(result) == {"anthropic", "openai"}
    assert result["anthropic"].allowed_adapter_types == frozenset({"cli_llm"})
    assert result["anthropic"].overridable is False
    assert result["openai"].allowed_adapter_types == frozenset({"cli_llm", "llm_task"})
    assert result["openai"].overridable is True


def test_load_raises_when_table_missing() -> None:
    import pytest

    with pytest.raises(RuntimeError, match="provider_lane_policy table missing"):
        load_provider_lane_policies(_StubConn([], raise_undefined=True))


def test_admit_llm_task_refused_when_budget_exhausted() -> None:
    """High spend pressure on the paid lane is a hard stop regardless of allow-list."""
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "llm_task", spend_pressure="high",
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_exhausted"


def test_admit_cli_llm_ignores_budget_pressure() -> None:
    """CLI has zero marginal cost — budget pressure never gates it."""
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "cli_llm", spend_pressure="high",
    )
    assert admitted is True
    assert reason == "lane.admitted"


def test_admit_llm_task_admitted_when_pressure_medium() -> None:
    """Medium pressure is a warning, not a stop."""
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "llm_task", spend_pressure="medium",
    )
    assert admitted is True


def test_admit_llm_task_admitted_when_no_pressure_data() -> None:
    admitted, _ = admit_adapter_type(_policies(), "openai", "llm_task")
    assert admitted is True


def test_budget_gate_runs_before_policy_gate() -> None:
    """Budget exhaustion surfaces as budget_exhausted even when policy also rejects."""
    # Anthropic doesn't allow llm_task at all AND budget is high — budget wins
    # the reason code because it's the more-specific / more-actionable signal.
    admitted, reason = admit_adapter_type(
        _policies(), "anthropic", "llm_task", spend_pressure="high",
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_exhausted"


def test_admit_llm_task_refused_when_budget_authority_unreachable() -> None:
    """Missing provider_budget_windows = schema drift must not silently open the paid lane.

    Pins BUG-2D3AECF3: the authority table being absent (sqlstate 42P01) surfaces
    as ``budget_authority_unreachable=True`` on the snapshot; admit_adapter_type
    fails closed for llm_task so the operator sees the authority gap instead of
    a permissive route.
    """
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "llm_task", budget_authority_unreachable=True,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_authority_unreachable"


def test_admit_cli_llm_ignores_budget_authority_unreachable() -> None:
    """CLI has no paid-API ceiling — authority state doesn't gate it."""
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "cli_llm", budget_authority_unreachable=True,
    )
    assert admitted is True
    assert reason == "lane.admitted"


def test_authority_unreachable_wins_over_high_spend_pressure() -> None:
    """When both signals fire, surface the more-actionable authority gap.

    ``budget_authority_unreachable`` means we cannot trust any spend-pressure
    reading, so the reason code must reflect the authority gap rather than an
    exhaustion signal derived from stale/empty state.
    """
    admitted, reason = admit_adapter_type(
        _policies(),
        "openai",
        "llm_task",
        spend_pressure="high",
        budget_authority_unreachable=True,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_authority_unreachable"


def test_load_skips_empty_allowed_list() -> None:
    rows = [
        {"provider_slug": "bad", "allowed_adapter_types": [], "overridable": False},
        {"provider_slug": "openai", "allowed_adapter_types": ["cli_llm"], "overridable": True},
    ]
    result = load_provider_lane_policies(_StubConn(rows))
    assert set(result) == {"openai"}


def test_admit_llm_task_refused_when_budget_window_data_quality_error() -> None:
    """Budget-window table reachable but an error-severity quality rule is failing.

    Extends the fail-closed doctrine: authority data the operator has flagged
    as broken must not drive paid-lane admission. Surfaces the reason code
    ``lane.rejected.budget_window_data_quality_error`` so the trace names the
    axis (authority-present-but-corrupt, distinct from authority-missing).
    """
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "llm_task", budget_window_data_quality_error=True,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_window_data_quality_error"


def test_admit_cli_llm_ignores_budget_window_data_quality_error() -> None:
    """CLI has no paid-API ceiling — budget-window data quality doesn't gate it."""
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "cli_llm", budget_window_data_quality_error=True,
    )
    assert admitted is True
    assert reason == "lane.admitted"


def test_authority_unreachable_wins_over_data_quality_error() -> None:
    """When both authority gates fire, surface the more-fundamental missing-authority axis.

    ``budget_authority_unreachable`` means the table itself is gone — we cannot
    even read quality runs against it meaningfully. The reason code must reflect
    the deeper gap rather than a derived data-quality signal.
    """
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "llm_task",
        budget_authority_unreachable=True,
        budget_window_data_quality_error=True,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_authority_unreachable"


def test_data_quality_error_wins_over_high_spend_pressure() -> None:
    """Corrupt authority data is a stronger signal than pressure derived from it.

    If the rows driving spend_pressure are in a failing-rule state, the
    pressure reading may be meaningless — surface the data-quality axis so
    the operator fixes the invariant rather than reading the pressure number.
    """
    admitted, reason = admit_adapter_type(
        _policies(), "openai", "llm_task",
        spend_pressure="high",
        budget_window_data_quality_error=True,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_window_data_quality_error"
