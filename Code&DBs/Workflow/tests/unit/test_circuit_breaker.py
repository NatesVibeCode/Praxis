from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest


def test_circuit_breaker_registry_is_initialized_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    from runtime import circuit_breaker as circuit_breaker_module

    module = importlib.reload(circuit_breaker_module)
    calls: list[str] = []

    def _fake_require_cb_config() -> tuple[int, float]:
        calls.append("config")
        return 3, 45.0

    monkeypatch.setattr(module, "_require_cb_config", _fake_require_cb_config)

    assert calls == []

    registry = module.get_circuit_breakers()

    assert calls == ["config"]
    assert registry.get("openai").failure_threshold == 3
    assert registry.get("openai").recovery_timeout_s == 45.0


def test_unified_workflow_reload_does_not_require_database_url_on_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    from runtime import circuit_breaker as circuit_breaker_module
    from runtime.workflow import unified as unified_module

    importlib.reload(circuit_breaker_module)
    module = importlib.reload(unified_module)

    assert callable(module._circuit_breakers)


def test_unified_workflow_circuit_breaker_gate_degrades_without_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    from runtime.workflow import _shared as shared_module

    module = importlib.reload(shared_module)

    monkeypatch.setattr(
        module,
        "get_circuit_breakers",
        lambda: (_ for _ in ()).throw(
            RuntimeError(
                "config_registry requires explicit WORKFLOW_DATABASE_URL Postgres authority"
            )
        ),
    )

    assert module._circuit_breakers() is None


def test_manual_force_open_override_blocks_requests() -> None:
    from runtime.circuit_breaker import (
        CircuitBreakerRegistry,
        CircuitState,
        ManualCircuitOverride,
    )

    registry = CircuitBreakerRegistry(failure_threshold=3, recovery_timeout_s=45.0)
    override = ManualCircuitOverride(
        provider_slug="openai",
        override_state=CircuitState.OPEN,
        operator_decision_id="operator-decision.circuit-breaker.openai",
        decision_key="circuit-breaker::openai",
        decision_kind="circuit_breaker_force_open",
        decision_status="active",
        rationale="Provider outage",
        decided_by="ops",
        decision_source="workflow.circuits.provider-outage",
        effective_from=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
        effective_to=None,
        updated_at=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
        decision_scope_kind="provider",
        decision_scope_ref="openai",
    )
    registry._manual_override_map = lambda: {"openai": override}  # type: ignore[method-assign]

    assert registry.allow_request("openai") is False

    state = registry.all_states()["openai"]
    assert state["state"] == "OPEN"
    assert state["runtime_state"] == "CLOSED"
    assert state["manual_override"]["override_state"] == "OPEN"


def test_manual_force_closed_override_allows_even_when_runtime_is_open() -> None:
    from runtime.circuit_breaker import (
        CircuitBreakerRegistry,
        CircuitState,
        ManualCircuitOverride,
    )

    registry = CircuitBreakerRegistry(failure_threshold=1, recovery_timeout_s=45.0)
    registry.record_outcome("anthropic", succeeded=False, failure_code="timeout")
    override = ManualCircuitOverride(
        provider_slug="anthropic",
        override_state=CircuitState.CLOSED,
        operator_decision_id="operator-decision.circuit-breaker.anthropic",
        decision_key="circuit-breaker::anthropic",
        decision_kind="circuit_breaker_force_closed",
        decision_status="active",
        rationale="Manual recovery probe",
        decided_by="ops",
        decision_source="workflow.circuits.manual-recovery",
        effective_from=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
        effective_to=None,
        updated_at=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
        decision_scope_kind="provider",
        decision_scope_ref="anthropic",
    )
    registry._manual_override_map = lambda: {"anthropic": override}  # type: ignore[method-assign]

    assert registry.allow_request("anthropic") is True

    state = registry.all_states()["anthropic"]
    assert state["state"] == "CLOSED"
    assert state["runtime_state"] == "OPEN"
    assert state["manual_override"]["override_state"] == "CLOSED"


def test_query_manual_overrides_uses_latest_event_and_reset_clears_prior_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from runtime import circuit_breaker as circuit_breaker_module

    module = importlib.reload(circuit_breaker_module)
    registry = module.CircuitBreakerRegistry(failure_threshold=3, recovery_timeout_s=45.0)

    now = datetime(2026, 4, 15, 19, 0, tzinfo=timezone.utc)
    rows = [
        {
            "operator_decision_id": "operator-decision.circuit-breaker.openai.reset.20260415T190000000000Z",
            "decision_key": "circuit-breaker::openai::20260415T190000000000Z",
            "decision_kind": "circuit_breaker_reset",
            "decision_status": "inactive",
            "rationale": "Recovered",
            "decided_by": "ops",
            "decision_source": "workflow.circuits.recovered",
            "effective_from": now,
            "effective_to": now,
            "updated_at": now,
            "decision_scope_kind": "provider",
            "decision_scope_ref": "openai",
        },
        {
            "operator_decision_id": "operator-decision.circuit-breaker.openai.open.20260415T180000000000Z",
            "decision_key": "circuit-breaker::openai::20260415T180000000000Z",
            "decision_kind": "circuit_breaker_force_open",
            "decision_status": "active",
            "rationale": "Outage",
            "decided_by": "ops",
            "decision_source": "workflow.circuits.outage",
            "effective_from": datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
            "effective_to": None,
            "updated_at": datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
            "decision_scope_kind": "provider",
            "decision_scope_ref": "openai",
        },
        {
            "operator_decision_id": "operator-decision.circuit-breaker.anthropic.closed.20260415T170000000000Z",
            "decision_key": "circuit-breaker::anthropic::20260415T170000000000Z",
            "decision_kind": "circuit_breaker_force_closed",
            "decision_status": "active",
            "rationale": "Probe window",
            "decided_by": "ops",
            "decision_source": "workflow.circuits.manual-recovery",
            "effective_from": datetime(2026, 4, 15, 17, 0, tzinfo=timezone.utc),
            "effective_to": None,
            "updated_at": datetime(2026, 4, 15, 17, 0, tzinfo=timezone.utc),
            "decision_scope_kind": "provider",
            "decision_scope_ref": "anthropic",
        },
    ]

    class _FakeSyncConn:
        def __init__(self, _pool) -> None:
            pass

        def execute(self, _query: str, *_args):
            return rows

    monkeypatch.setattr(module, "resolve_runtime_database_url", lambda required=False: "postgresql://postgres@localhost:5432/praxis")
    monkeypatch.setattr(module, "get_workflow_pool", lambda env=None: object())
    monkeypatch.setattr(module, "SyncPostgresConnection", _FakeSyncConn)

    overrides = registry._query_manual_overrides()

    assert "openai" not in overrides
    assert overrides["anthropic"].override_state == module.CircuitState.CLOSED
