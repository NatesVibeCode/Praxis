"""Unit tests for upsert_derived_route — confirms transport_type is caller-provided.

Historically this method hardcoded ``transport_type='CLI'``, which made every
derived row trip trigger 378 (provider_transport_admissions check) for
HTTP-only providers (openrouter, together, fireworks, deepseek). The fix
threads transport_type through the repository signature so the caller's
adapter_type-derived value lands.
"""

from __future__ import annotations

from typing import Any

import pytest

from storage.postgres.task_type_routing_repository import (
    PostgresTaskTypeRoutingRepository,
)


class _CapturingConn:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, *args: Any) -> Any:
        self.executions.append((sql, args))
        return []


def _call_upsert(repo: PostgresTaskTypeRoutingRepository, **overrides: Any) -> None:
    defaults: dict[str, Any] = {
        "task_type": "build",
        "model_slug": "canvasshotai/kimi-k2.6",
        "provider_slug": "openrouter",
        "transport_type": "API",
        "permitted": True,
        "rank": 3,
        "benchmark_score": 0.8,
        "benchmark_name": "router_v1",
        "cost_per_m_tokens": 0.0,
        "rationale": "auto-derived for build",
        "route_tier": "high",
        "route_tier_rank": 1,
        "latency_class": "reasoning",
        "latency_rank": 1,
        "route_health_score": 0.9,
        "observed_completed_count": 0,
        "observed_execution_failure_count": 0,
        "observed_external_failure_count": 0,
        "observed_config_failure_count": 0,
        "observed_downstream_failure_count": 0,
        "observed_downstream_bug_count": 0,
        "consecutive_internal_failures": 0,
        "last_failure_category": "",
        "last_failure_zone": "",
    }
    defaults.update(overrides)
    repo.upsert_derived_route(**defaults)


def test_caller_supplied_API_transport_lands_in_sql_args() -> None:
    """The bug fix: openrouter API rows must land as transport_type='API', not 'CLI'."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(
        repo,
        provider_slug="openrouter",
        transport_type="API",
        candidate_ref="candidate.openrouter.canvasshotai/kimi-k2.6",
    )

    assert len(conn.executions) == 1
    _, args = conn.executions[0]
    # Positional args after the SQL: $1=task_type, $2=model_slug,
    # $3=provider_slug, $4=transport_type, $5=candidate_ref, ...
    assert args[0] == "build"
    assert args[2] == "openrouter"
    assert args[3] == "API"
    assert args[4] == "candidate.openrouter.canvasshotai/kimi-k2.6"


def test_caller_supplied_CLI_transport_lands_in_sql_args() -> None:
    """anthropic CLI rows still work — caller-provided CLI lands as CLI."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(repo, provider_slug="anthropic", transport_type="CLI")
    _, args = conn.executions[0]
    assert args[2] == "anthropic"
    assert args[3] == "CLI"


def test_lowercase_transport_normalized_to_uppercase() -> None:
    """Defensive: callers passing lowercase still produce CHECK-constraint-compatible rows."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(repo, transport_type="api")
    _, args = conn.executions[0]
    assert args[3] == "API"


def test_invalid_transport_raises() -> None:
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    with pytest.raises(ValueError, match="transport_type must be 'CLI' or 'API'"):
        _call_upsert(repo, transport_type="HTTP")
    assert conn.executions == []


def test_empty_transport_defaults_to_API() -> None:
    """Safer default than 'CLI' (the historical hardcode)."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(repo, transport_type="")
    _, args = conn.executions[0]
    assert args[3] == "API"


def test_sql_no_longer_hardcodes_cli_in_values_clause() -> None:
    """Regression guard against the original bug returning."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(repo)
    sql, _ = conn.executions[0]
    # The literal string "'CLI'" should not appear in the VALUES clause anymore.
    assert "'CLI'" not in sql, "transport_type must not be hardcoded in the INSERT VALUES"


def test_on_conflict_now_updates_transport_type() -> None:
    """When the same (task_type, provider, model) exists with a wrong transport, fix it."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(repo)
    sql, _ = conn.executions[0]
    assert "transport_type = EXCLUDED.transport_type" in sql, (
        "ON CONFLICT must reset transport_type so existing wrong-transport rows get corrected"
    )


def test_on_conflict_updates_candidate_identity_fields() -> None:
    """Derived routes must keep the DB edge aligned with the selected candidate."""
    conn = _CapturingConn()
    repo = PostgresTaskTypeRoutingRepository(conn)
    _call_upsert(
        repo,
        candidate_ref="candidate.openrouter.canvasshotai/kimi-k2.6",
        host_provider_slug="canvasshotai",
        variant="",
        effort_slug="",
    )

    sql, args = conn.executions[0]
    assert "candidate_ref = EXCLUDED.candidate_ref" in sql
    assert "host_provider_slug = EXCLUDED.host_provider_slug" in sql
    assert args[4] == "candidate.openrouter.canvasshotai/kimi-k2.6"
    assert args[5] == "canvasshotai"
