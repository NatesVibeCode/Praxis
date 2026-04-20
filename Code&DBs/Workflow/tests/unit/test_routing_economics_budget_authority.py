"""Budget-authority snapshot contract.

Pins the fail-closed contract introduced with BUG-2D3AECF3:
:func:`runtime.routing_economics.load_budget_authority_snapshot` never returns
a silently-permissive snapshot when ``provider_budget_windows`` is missing —
the resulting :class:`BudgetAuthoritySnapshot` carries ``reachable=False`` so
:func:`runtime.lane_policy.admit_adapter_type` can gate paid lanes on the
authority-missing condition. Also pins the dual-index contract from
BUG-6B34915A: consumers that only know the provider slug (explicit routing)
must still be able to resolve the window via ``provider_ref``.
"""
from __future__ import annotations

import pytest

from runtime.routing_economics import (
    BudgetAuthoritySnapshot,
    load_budget_authority_snapshot,
)


class _StubConn:
    """Returns a single row set for window queries; empty for quality queries.

    The loader issues two queries against different tables: the window
    snapshot itself and the data-quality latest-run check. The quality check
    dispatches on the SQL text marker ``data_dictionary_quality_runs`` so
    window-focused tests keep their existing single-row-set shape without
    seeing stray quality rows.
    """

    def __init__(self, rows, *, quality_rows=None):
        self._rows = rows
        self._quality_rows = quality_rows if quality_rows is not None else []

    def execute(self, sql, *_params):
        if "data_dictionary_quality_runs" in sql:
            return self._quality_rows
        return self._rows


class _UndefinedTableConn:
    """Simulates sqlstate 42P01 (relation missing)."""

    def execute(self, _sql, *_params):
        import asyncpg
        err = asyncpg.PostgresError()
        err.sqlstate = "42P01"
        raise err


class _OtherPostgresErrorConn:
    """Non-42P01 PostgresError must propagate, not be swallowed."""

    def execute(self, _sql, *_params):
        import asyncpg
        err = asyncpg.PostgresError()
        err.sqlstate = "42501"  # insufficient_privilege
        raise err


class _WindowOkQualityMissingConn:
    """Window table present, quality tables missing (42P01 on the quality query).

    Pins the fail-open contract for quality-infra absence: the authority
    reachability gate on the primary window query is the primary fail-closed
    guard; a missing quality table must not compound it into a blanket paid-
    lane refusal. Keyed off the SQL text so the window query still returns
    its empty-but-reachable result.
    """

    def __init__(self, window_rows):
        self._window_rows = window_rows

    def execute(self, sql, *_params):
        if "data_dictionary_quality_runs" in sql:
            import asyncpg
            err = asyncpg.PostgresError()
            err.sqlstate = "42P01"
            raise err
        return self._window_rows


def _window_row(
    *,
    policy_id: str,
    provider_ref: str,
    status: str = "available",
) -> dict:
    return {
        "provider_policy_id": policy_id,
        "provider_ref": provider_ref,
        "budget_status": status,
        "request_limit": 1000,
        "requests_used": 10,
        "token_limit": 100_000,
        "tokens_used": 1_000,
        "spend_limit_usd": 100.0,
        "spend_used_usd": 1.0,
        "window_started_at": None,
        "window_ended_at": None,
        "created_at": None,
    }


def test_snapshot_unreachable_when_table_missing() -> None:
    """Schema drift — provider_budget_windows missing — must fail closed.

    The loader surfaces sqlstate 42P01 as ``reachable=False`` so paid-lane
    admission can gate on the authority gap instead of treating the missing
    authority as an empty-but-permissive dict.
    """
    snapshot = load_budget_authority_snapshot(_UndefinedTableConn())

    assert snapshot.reachable is False
    assert snapshot.window_for(provider_policy_id="provider_policy.openai") is None
    assert snapshot.window_for_provider_slug("openai") is None


def test_snapshot_reraises_non_undefined_table_errors() -> None:
    """Any other PostgresError is a real bug — never silently treated as empty."""
    import asyncpg

    with pytest.raises(asyncpg.PostgresError):
        load_budget_authority_snapshot(_OtherPostgresErrorConn())


def test_snapshot_empty_is_reachable_but_windowless() -> None:
    """Authority table exists, no window rows yet — pre-seed state.

    Distinct from ``unreachable``: the authority is present, we just haven't
    seen any paid-lane traffic. Paid lanes stay admitted under no-policy
    defaults, so spend_pressure falls through as ``unknown``.
    """
    snapshot = load_budget_authority_snapshot(_StubConn([]))

    assert snapshot.reachable is True
    assert snapshot.window_for(provider_policy_id="any") is None
    assert snapshot.window_for_provider_slug("any") is None


def test_snapshot_indexes_by_both_policy_id_and_provider_ref() -> None:
    """Explicit-route path only knows provider_slug — dual-index closes BUG-6B34915A."""
    rows = [
        _window_row(
            policy_id="provider_policy.openai",
            provider_ref="provider.openai",
            status="available",
        ),
        _window_row(
            policy_id="provider_policy.anthropic",
            provider_ref="provider.anthropic",
            status="limited",
        ),
    ]
    snapshot = load_budget_authority_snapshot(_StubConn(rows))

    # Auto path lookup by policy id
    by_policy = snapshot.window_for(provider_policy_id="provider_policy.openai")
    assert by_policy is not None
    assert by_policy["budget_status"] == "available"

    # Explicit path lookup by provider slug
    by_slug = snapshot.window_for_provider_slug("anthropic")
    assert by_slug is not None
    assert by_slug["budget_status"] == "limited"

    # Combined lookup prefers policy id when both resolve
    both = snapshot.window_for(
        provider_policy_id="provider_policy.openai",
        provider_slug="anthropic",
    )
    assert both is not None
    assert both["provider_policy_id"] == "provider_policy.openai"


def test_snapshot_factory_constants_are_reachable_vs_unreachable() -> None:
    """``empty`` and ``unreachable`` are the only two sources of windowless snapshots.

    Consumers rely on this distinction: ``.empty().reachable is True`` preserves
    the pre-seed permissive behaviour, ``.unreachable().reachable is False`` forces
    paid-lane fail-closed. The factories must not be collapsed into a single
    constant.
    """
    assert BudgetAuthoritySnapshot.empty().reachable is True
    assert BudgetAuthoritySnapshot.unreachable().reachable is False
    assert BudgetAuthoritySnapshot.empty().window_for(provider_policy_id="x") is None
    assert BudgetAuthoritySnapshot.unreachable().window_for(provider_policy_id="x") is None


def test_snapshot_defaults_data_quality_ok_true() -> None:
    """Without an explicit failing-rule signal, ``data_quality_ok`` defaults True.

    ``empty`` and ``unreachable`` factories produce snapshots whose
    ``data_quality_ok`` is the permissive default. The gate only refuses
    when an error-severity rule is *actively* failing; unknown state reads
    as benign on this axis (authority-reachability carries the primary
    fail-closed semantics).
    """
    assert BudgetAuthoritySnapshot.empty().data_quality_ok is True
    assert BudgetAuthoritySnapshot.unreachable().data_quality_ok is True


def test_snapshot_data_quality_ok_when_quality_rows_report_zero_failing() -> None:
    """Happy path: the quality query returns zero failing rules."""
    snapshot = load_budget_authority_snapshot(_StubConn([], quality_rows=[{"failing": 0}]))

    assert snapshot.reachable is True
    assert snapshot.data_quality_ok is True


def test_snapshot_data_quality_false_when_failing_rule_exists() -> None:
    """Error-severity rule with latest run in ('fail','error') flips data_quality_ok to False.

    This is the signal that drives ``lane.rejected.budget_window_data_quality_error``
    at the admission gate: authority table reachable, but an operator-declared
    invariant on its rows is failing, so paid-lane admission must refuse.
    """
    snapshot = load_budget_authority_snapshot(_StubConn([], quality_rows=[{"failing": 1}]))

    assert snapshot.reachable is True
    assert snapshot.data_quality_ok is False


def test_snapshot_data_quality_ok_when_quality_tables_missing() -> None:
    """Quality infrastructure absent (42P01) fails *open* on the quality axis.

    The authority-reachability gate is the primary fail-closed guard. A
    missing quality table must not compound into a blanket paid-lane
    refusal — otherwise deploying a fresh env with no quality projector
    yet would paralyse the paid lane.
    """
    snapshot = load_budget_authority_snapshot(_WindowOkQualityMissingConn([]))

    assert snapshot.reachable is True
    assert snapshot.data_quality_ok is True
