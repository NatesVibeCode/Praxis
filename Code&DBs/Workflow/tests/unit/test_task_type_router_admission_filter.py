"""Regression tests for BUG-DD1D48B1 — route selection must honor
``provider_transport_admissions`` so providers whose default adapter
transport is denied by policy (e.g. ``anthropic.llm_task`` under the
CLI-only standing order ``decision.2026-04-20.anthropic-cli-only-restored``)
never surface as a route candidate.

Before this fix the router happily emitted ``anthropic/claude-opus-4-6``
for ``auto/review`` even though ``provider_transport_admissions`` has
``(anthropic, llm_task)`` with ``admitted_by_policy=false``. The worker
then hit Anthropic HTTP with an invalid key and whole Wave 1 audits
collapsed on 401 before any work started.

The filter method itself is unit-exercised here via a minimal stand-in
that provides only the two attributes the method depends on
(``_conn.execute`` and ``_default_adapter_for_provider``). Call-site
wiring lives in ``_resolve_chain`` and ``_resolve_profile_chain`` but
is not under test here — those are integration paths with their own
suites. What this file pins is: given a denied row, the filter drops
the candidate; given missing admission authority, the filter fails
closed; given admitted rows, the filter is a pass-through.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest

from runtime.provider_authority import ProviderAuthorityError
from runtime.task_type_router import (
    TaskTypeRouter,
    _PROVIDER_TRANSPORT_ADMISSION_SQL,
)


# ------------------------------------------------------------------ fixtures


class _FakeConn:
    """Conn stub that returns a caller-supplied admission row list.

    The real ``provider_transport_admissions`` query is the only SQL the
    method issues, so we don't need a full SQL dispatcher here.
    """

    def __init__(self, admission_rows: list[dict[str, Any]] | None = None):
        self._admission_rows = admission_rows or []
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def execute(self, sql: str, *params):
        self.calls.append((params, {}))
        if "provider_transport_admissions" in sql:
            return list(self._admission_rows)
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


class _MissingTableConn:
    """Conn stub that raises undefined_table on the admission query."""

    def execute(self, sql: str, *params):
        if "provider_transport_admissions" in sql:
            err = asyncpg.PostgresError()
            err.sqlstate = "42P01"
            raise err
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


class _UnrelatedDatabaseErrorConn:
    """Conn stub that raises a non-42P01 database error to prove we don't
    swallow unrelated failures."""

    def execute(self, sql: str, *params):
        err = asyncpg.PostgresError()
        err.sqlstate = "42601"  # syntax_error — nothing to do with table missing
        raise err


class _RouterShim:
    """Minimal stand-in for :class:`TaskTypeRouter` carrying only the
    attributes the admission filter reads. Avoids the heavy authority
    bootstrap in the real ``__init__``."""

    def __init__(
        self,
        conn: Any,
        adapter_for_provider: dict[str, str] | None = None,
    ):
        self._conn = conn
        self._adapter_for_provider = adapter_for_provider or {}

    def _default_adapter_for_provider(self, provider_slug: str) -> str:
        # Default used by most callers — explicit map can override.
        return self._adapter_for_provider.get(provider_slug, "llm_task")

    def _provider_usage_unavailable_slugs(self, provider_slugs: set[str]) -> set[str]:
        return TaskTypeRouter._provider_usage_unavailable_slugs(self, provider_slugs)


def _call_filter(router: Any, task_type: str, rows: list[dict[str, Any]]):
    """Invoke the unbound method against the shim so we don't need a real
    ``TaskTypeRouter`` instance."""
    return TaskTypeRouter._apply_provider_transport_admission_filter(
        router, task_type, rows
    )


class _AvailabilityConn:
    def __init__(self, snapshot_rows: list[dict[str, Any]] | None = None):
        self._snapshot_rows = snapshot_rows or []
        self.calls: list[tuple[Any, ...]] = []

    def execute(self, sql: str, *params):
        self.calls.append(params)
        if "heartbeat_probe_snapshots" in sql:
            return list(self._snapshot_rows)
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


def _call_availability_filter(router: Any, task_type: str, rows: list[dict[str, Any]]):
    return TaskTypeRouter._apply_provider_usage_availability_filter(
        router, task_type, rows
    )


# ---------------------------------------------------------- drop-denied cases


def test_denied_admission_drops_candidate():
    """The canonical BUG-DD1D48B1 case: anthropic.llm_task is denied, so
    the anthropic candidate must not survive the filter."""
    conn = _FakeConn([
        {
            "provider_slug": "anthropic",
            "adapter_type": "llm_task",
            "admitted_by_policy": False,
            "policy_reason": "CLI-only per decision.2026-04-20.anthropic-cli-only-restored",
            "decision_ref": "decision.2026-04-20.anthropic-cli-only-restored",
        },
        {
            "provider_slug": "openai",
            "adapter_type": "llm_task",
            "admitted_by_policy": True,
            "policy_reason": "",
            "decision_ref": "",
        },
    ])
    router = _RouterShim(conn)
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-opus-4-6"},
        {"provider_slug": "openai", "model_slug": "gpt-5"},
    ]
    kept = _call_filter(router, "review", rows)
    kept_providers = sorted({r["provider_slug"] for r in kept})
    assert kept_providers == ["openai"], (
        f"denied anthropic/llm_task should have been dropped; got {kept_providers}"
    )


def test_denied_admission_preserves_other_rows_from_same_provider_with_different_adapter():
    """If anthropic has two rows in candidates — one resolving to llm_task
    (denied) and one to a hypothetical admitted adapter — we don't currently
    model that, because ``_default_adapter_for_provider`` is per-provider
    not per-row. The test pins the current behavior: all anthropic rows
    share a single resolved adapter, so all anthropic rows are dropped
    together when that adapter is denied."""
    conn = _FakeConn([
        {
            "provider_slug": "anthropic",
            "adapter_type": "llm_task",
            "admitted_by_policy": False,
            "policy_reason": "",
            "decision_ref": "",
        },
    ])
    router = _RouterShim(conn)
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-opus-4-6"},
        {"provider_slug": "anthropic", "model_slug": "claude-sonnet-4-6"},
    ]
    kept = _call_filter(router, "review", rows)
    assert kept == [], "all anthropic candidates should be dropped under a provider-wide denial"


# ------------------------------------------------------------- admit cases


def test_admitted_admission_keeps_candidate():
    """Positive-control: admitted rows pass through untouched."""
    conn = _FakeConn([
        {
            "provider_slug": "openai",
            "adapter_type": "llm_task",
            "admitted_by_policy": True,
            "policy_reason": "",
            "decision_ref": "",
        },
    ])
    router = _RouterShim(conn)
    rows = [{"provider_slug": "openai", "model_slug": "gpt-5"}]
    kept = _call_filter(router, "review", rows)
    assert kept == rows


def test_missing_admission_row_fails_closed():
    """If no row exists at all for (provider, adapter), the filter blocks
    that candidate by hard-stopping route selection. Missing admission
    authority is not permission."""
    conn = _FakeConn([])  # no rows at all
    router = _RouterShim(conn)
    rows = [
        {"provider_slug": "openai", "model_slug": "gpt-5"},
        {"provider_slug": "anthropic", "model_slug": "claude-opus-4-6"},
    ]
    with pytest.raises(ProviderAuthorityError, match="provider_transport_admissions has no row"):
        _call_filter(router, "review", rows)


# --------------------------------------------------------- resilience cases


def test_missing_admissions_table_fails_closed():
    """Fresh-clone / pre-migration environments must not emit provider
    candidates until the admission table exists."""
    router = _RouterShim(_MissingTableConn())
    rows = [{"provider_slug": "openai", "model_slug": "gpt-5"}]
    with pytest.raises(ProviderAuthorityError, match="provider_transport_admissions table missing"):
        _call_filter(router, "review", rows)


def test_non_undefined_table_errors_propagate():
    """We only swallow 42P01 (undefined_table). Any other DB error must
    escape — hiding them would mask real regressions in admission authority."""
    router = _RouterShim(_UnrelatedDatabaseErrorConn())
    rows = [{"provider_slug": "openai", "model_slug": "gpt-5"}]
    with pytest.raises(asyncpg.PostgresError):
        _call_filter(router, "review", rows)


def test_empty_input_short_circuits():
    """No candidates means no work and no SQL call."""
    conn = _FakeConn([])
    router = _RouterShim(conn)
    kept = _call_filter(router, "review", [])
    assert kept == []
    assert conn.calls == [], "no query should have been issued"


# ---------------------------------------------------- availability cases


def test_provider_usage_degraded_snapshot_drops_candidate():
    conn = _AvailabilityConn([
        {"subject_id": "anthropic", "status": "degraded"},
        {"subject_id": "openai", "status": "ok"},
    ])
    router = _RouterShim(conn)
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-sonnet-4-6"},
        {"provider_slug": "openai", "model_slug": "gpt-5.4"},
    ]

    kept = _call_availability_filter(router, "build", rows)

    assert [row["provider_slug"] for row in kept] == ["openai"]


def test_provider_usage_failed_snapshot_drops_candidate():
    conn = _AvailabilityConn([
        {"subject_id": "anthropic", "status": "failed"},
    ])
    router = _RouterShim(conn)
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-sonnet-4-6"},
    ]

    assert _call_availability_filter(router, "build", rows) == []


def test_provider_usage_availability_query_failure_fails_closed():
    class _MissingHeartbeatConn:
        def execute(self, sql: str, *params):
            raise RuntimeError("heartbeat table missing")

    router = _RouterShim(_MissingHeartbeatConn())
    rows = [{"provider_slug": "openai", "model_slug": "gpt-5.4"}]

    with pytest.raises(Exception, match="provider usage availability unavailable"):
        _call_availability_filter(router, "build", rows)


# --------------------------------------------------- SQL payload contract


def test_filter_queries_only_needed_providers_and_adapters():
    """The SQL takes (provider_slugs[], adapter_types[]) — the filter
    must compute both lists from the candidate rows, not from a global
    catalog."""
    conn = _FakeConn([])
    router = _RouterShim(
        conn,
        adapter_for_provider={"openai": "llm_task", "anthropic": "llm_task"},
    )
    rows = [
        {"provider_slug": "openai", "model_slug": "gpt-5"},
        {"provider_slug": "anthropic", "model_slug": "claude-opus-4-6"},
    ]
    with pytest.raises(ProviderAuthorityError, match="provider_transport_admissions has no row"):
        _call_filter(router, "review", rows)
    assert len(conn.calls) == 1
    (params, _) = conn.calls[0]
    provider_slugs_arg, adapter_types_arg = params
    assert sorted(provider_slugs_arg) == ["anthropic", "openai"]
    assert sorted(adapter_types_arg) == ["llm_task"]


def test_filter_uses_provider_transport_admissions_sql_constant():
    """Belt-and-suspenders: the SQL constant must reference the authority
    table by name so a future rename can't silently bypass the filter."""
    assert "provider_transport_admissions" in _PROVIDER_TRANSPORT_ADMISSION_SQL
    assert "admitted_by_policy" in _PROVIDER_TRANSPORT_ADMISSION_SQL
    assert "policy_reason" in _PROVIDER_TRANSPORT_ADMISSION_SQL
    assert "decision_ref" in _PROVIDER_TRANSPORT_ADMISSION_SQL
