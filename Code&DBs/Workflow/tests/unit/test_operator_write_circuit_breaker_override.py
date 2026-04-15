from __future__ import annotations

from datetime import datetime, timezone

from surfaces.api import operator_write


class _FakeConn:
    async def close(self) -> None:
        return None


class _FakeOperatorControlRepository:
    def __init__(self) -> None:
        self.recorded = None

    async def record_operator_decision(self, *, operator_decision):
        self.recorded = operator_decision
        return operator_decision


def test_set_circuit_breaker_override_records_force_open(monkeypatch) -> None:
    repository = _FakeOperatorControlRepository()

    async def _connect(_env=None):
        return _FakeConn()

    invalidations: list[str] = []
    monkeypatch.setattr(
        operator_write,
        "invalidate_circuit_breaker_override_cache",
        lambda: invalidations.append("invalidated"),
    )

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
    )

    payload = frontdoor.set_circuit_breaker_override(
        provider_slug="OpenAI",
        override_state="open",
        rationale="Provider outage",
        reason_code="provider_outage",
        decided_by="ops",
    )

    recorded = repository.recorded
    assert recorded is not None
    assert recorded.operator_decision_id.startswith(
        "operator-decision.circuit-breaker.openai.open."
    )
    assert recorded.decision_key.startswith("circuit-breaker::openai::")
    assert recorded.decision_kind == "circuit_breaker_force_open"
    assert recorded.decision_status == "active"
    assert recorded.decision_scope_kind == "provider"
    assert recorded.decision_scope_ref == "openai"
    assert recorded.rationale == "Provider outage"
    assert recorded.decided_by == "ops"
    assert recorded.decision_source == "workflow.circuits.provider-outage"
    assert invalidations == ["invalidated"]
    assert payload["circuit_breaker_override"]["provider_slug"] == "openai"
    assert payload["circuit_breaker_override"]["override_state"] == "open"


def test_set_circuit_breaker_override_reset_marks_decision_inactive(monkeypatch) -> None:
    repository = _FakeOperatorControlRepository()

    async def _connect(_env=None):
        return _FakeConn()

    fixed_now = datetime(2026, 4, 15, 18, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(operator_write, "_now", lambda: fixed_now)
    monkeypatch.setattr(
        operator_write,
        "invalidate_circuit_breaker_override_cache",
        lambda: None,
    )

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
    )

    payload = frontdoor.set_circuit_breaker_override(
        provider_slug="anthropic",
        override_state="reset",
    )

    recorded = repository.recorded
    assert recorded is not None
    assert recorded.operator_decision_id.startswith(
        "operator-decision.circuit-breaker.anthropic.reset."
    )
    assert recorded.decision_key.startswith("circuit-breaker::anthropic::")
    assert recorded.decision_kind == "circuit_breaker_reset"
    assert recorded.decision_status == "inactive"
    assert recorded.decision_scope_kind == "provider"
    assert recorded.decision_scope_ref == "anthropic"
    assert recorded.effective_from == fixed_now
    assert recorded.effective_to == fixed_now
    assert payload["circuit_breaker_override"]["override_state"] == "reset"
    assert payload["circuit_breaker_override"]["decision_status"] == "inactive"


def test_record_architecture_policy_decision_uses_authority_domain_scope(monkeypatch) -> None:
    repository = _FakeOperatorControlRepository()

    async def _connect(_env=None):
        return _FakeConn()

    fixed_now = datetime(2026, 4, 15, 19, 45, tzinfo=timezone.utc)
    monkeypatch.setattr(operator_write, "_now", lambda: fixed_now)

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
    )

    payload = frontdoor.record_architecture_policy_decision(
        authority_domain="decision_tables",
        policy_slug="db-native-authority",
        title="Decision tables are DB-native authority",
        rationale="Authority and runtime coordination belong in durable DB primitives.",
        decided_by="nate",
        decision_source="cto.guidance",
    )

    recorded = repository.recorded
    assert recorded is not None
    assert (
        recorded.operator_decision_id
        == "operator_decision.architecture_policy.decision_tables.db_native_authority"
    )
    assert (
        recorded.decision_key
        == "architecture-policy::decision-tables::db-native-authority"
    )
    assert recorded.decision_kind == "architecture_policy"
    assert recorded.decision_status == "decided"
    assert recorded.decision_scope_kind == "authority_domain"
    assert recorded.decision_scope_ref == "decision_tables"
    assert recorded.effective_from == fixed_now
    assert recorded.decided_at == fixed_now
    assert payload["architecture_policy_decision"]["authority_domain"] == "decision_tables"
    assert payload["architecture_policy_decision"]["policy_slug"] == "db-native-authority"
