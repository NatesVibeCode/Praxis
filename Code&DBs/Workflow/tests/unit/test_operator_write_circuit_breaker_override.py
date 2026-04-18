from __future__ import annotations

import json
from datetime import datetime, timezone

from runtime.semantic_assertions import (
    SemanticAssertionRecord,
    SemanticPredicateRecord,
    normalize_semantic_assertion_record,
)
from surfaces.api import operator_write


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.event_rows: list[dict[str, object]] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, _query: str, *_args: object):
        return []

    async def fetchrow(self, query: str, *_args: object):
        if "INSERT INTO event_log" in query:
            row = {
                "channel": _args[0],
                "event_type": _args[1],
                "entity_id": _args[2],
                "entity_kind": _args[3],
                "payload": json.loads(_args[4]),
                "emitted_by": _args[5],
            }
            self.event_rows.append(row)
            return {"id": len(self.event_rows)}
        return {"ok": 1}

    async def execute(self, _query: str, *_args: object) -> str:
        return "OK"

    async def close(self) -> None:
        return None


class _FakeOperatorControlRepository:
    def __init__(self) -> None:
        self.recorded = None

    async def record_operator_decision(self, *, operator_decision):
        self.recorded = operator_decision
        return operator_decision


class _FakeSemanticAssertionRepository:
    def __init__(self) -> None:
        self.predicates: dict[str, SemanticPredicateRecord] = {}
        self.assertions: dict[str, SemanticAssertionRecord] = {}
        self.upserted_predicates: list[SemanticPredicateRecord] = []
        self.recorded_assertions: list[tuple[SemanticAssertionRecord, str, datetime]] = []

    async def load_predicate(self, *, predicate_slug: str):
        return self.predicates.get(predicate_slug)

    async def upsert_predicate(self, *, predicate: SemanticPredicateRecord):
        self.predicates[predicate.predicate_slug] = predicate
        self.upserted_predicates.append(predicate)
        return predicate

    async def load_assertion(self, *, semantic_assertion_id: str):
        return self.assertions.get(semantic_assertion_id)

    async def record_assertion(
        self,
        *,
        assertion: SemanticAssertionRecord,
        cardinality_mode: str,
        as_of: datetime,
    ):
        normalized = normalize_semantic_assertion_record(assertion)
        self.assertions[normalized.semantic_assertion_id] = normalized
        self.recorded_assertions.append((normalized, cardinality_mode, as_of))
        return normalized, ()

    async def retract_assertion(
        self,
        *,
        semantic_assertion_id: str,
        retracted_at: datetime,
        updated_at: datetime,
    ):
        raise AssertionError("circuit breaker tests do not retract semantic assertions")

    async def rebuild_current_assertions(self, *, as_of: datetime) -> int:
        return len(self.assertions)


def test_set_circuit_breaker_override_records_force_open(monkeypatch) -> None:
    repository = _FakeOperatorControlRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    invalidations: list[str] = []
    monkeypatch.setattr(
        operator_write,
        "invalidate_circuit_breaker_override_cache",
        lambda: invalidations.append("invalidated"),
    )

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
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
    assert semantic_repository.upserted_predicates[0].predicate_slug == "circuit_breaker_force_open"
    assert semantic_repository.recorded_assertions[0][0].subject_kind == "provider"
    assert semantic_repository.recorded_assertions[0][0].subject_ref == "openai"
    assert invalidations == ["invalidated"]
    assert payload["circuit_breaker_override"]["provider_slug"] == "openai"
    assert payload["circuit_breaker_override"]["override_state"] == "open"
    cache_events = [e for e in conn.event_rows if e["channel"] == "cache_invalidation"]
    assert len(cache_events) == 1
    cache_event = cache_events[0]
    assert cache_event["event_type"] == "cache_invalidated"
    assert cache_event["entity_kind"] == "circuit_breaker_manual_override"
    assert cache_event["entity_id"] == "openai"
    assert cache_event["emitted_by"] == "operator_write.set_circuit_breaker_override"
    assert cache_event["payload"]["reason"] == "circuit_breaker_override_open"
    assert cache_event["payload"]["decision_ref"] == recorded.operator_decision_id


def test_set_circuit_breaker_override_reset_marks_decision_inactive(monkeypatch) -> None:
    repository = _FakeOperatorControlRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

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
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
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
    assert semantic_repository.upserted_predicates[0].predicate_slug == "circuit_breaker_reset"
    assert semantic_repository.recorded_assertions[0][0].object_ref == recorded.operator_decision_id
    assert payload["circuit_breaker_override"]["override_state"] == "reset"
    assert payload["circuit_breaker_override"]["decision_status"] == "inactive"


def test_record_architecture_policy_decision_uses_authority_domain_scope(monkeypatch) -> None:
    repository = _FakeOperatorControlRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    fixed_now = datetime(2026, 4, 15, 19, 45, tzinfo=timezone.utc)
    monkeypatch.setattr(operator_write, "_now", lambda: fixed_now)

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.record_architecture_policy_decision(
        authority_domain="decision_tables",
        policy_slug="db-native-authority",
        title="Decision tables are DB-native authority",
        rationale="Authority and runtime coordination belong in durable DB primitives.",
        decided_by="praxis-admin",
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
    assert semantic_repository.upserted_predicates[0].predicate_slug == "architecture_policy"
    assert semantic_repository.recorded_assertions[0][0].subject_ref == "decision_tables"
    assert payload["architecture_policy_decision"]["authority_domain"] == "decision_tables"
    assert payload["architecture_policy_decision"]["policy_slug"] == "db-native-authority"
