from __future__ import annotations

import json
from datetime import datetime, timezone

from authority.operator_control import OperatorDecisionAuthorityRecord
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
    def __init__(self, rows: tuple[OperatorDecisionAuthorityRecord, ...] = ()) -> None:
        self.recorded = None
        self.rows = rows
        self.as_of = None
        self.list_kwargs = None

    async def record_operator_decision(self, *, operator_decision):
        self.recorded = operator_decision
        return operator_decision

    async def list_operator_decisions(
        self,
        *,
        decision_kind=None,
        decision_source=None,
        decision_scope_kind=None,
        decision_scope_ref=None,
        active_only=True,
        as_of,
        limit,
    ):
        self.as_of = as_of
        self.list_kwargs = {
            "decision_kind": decision_kind,
            "decision_source": decision_source,
            "decision_scope_kind": decision_scope_kind,
            "decision_scope_ref": decision_scope_ref,
            "active_only": active_only,
            "as_of": as_of,
            "limit": limit,
        }
        filtered = [
            row
            for row in self.rows
            if (decision_kind is None or row.decision_kind == decision_kind)
            and (decision_source is None or row.decision_source == decision_source)
            and (
                decision_scope_kind is None or row.decision_scope_kind == decision_scope_kind
            )
            and (
                decision_scope_ref is None or row.decision_scope_ref == decision_scope_ref
            )
        ]
        return tuple(filtered[:limit])

    async def fetch_operator_decisions_for_semantic_bridge(self, *, as_of=None):
        self.as_of = as_of
        return tuple(self.rows)


class _FakeSemanticAssertionRepository:
    def __init__(self) -> None:
        self.predicates: dict[str, SemanticPredicateRecord] = {}
        self.assertions: dict[str, SemanticAssertionRecord] = {}
        self.upserted_predicates: list[SemanticPredicateRecord] = []
        self.recorded_assertions: list[tuple[SemanticAssertionRecord, str, datetime]] = []
        self.rebuild_calls: list[datetime] = []

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
        raise AssertionError("decision bridge should not retract assertions in this test")

    async def rebuild_current_assertions(self, *, as_of: datetime) -> int:
        self.rebuild_calls.append(as_of)
        return len(self.assertions)


def _decision(
    *,
    decision_key: str,
    decision_kind: str,
    decision_scope_kind: str | None = None,
    decision_scope_ref: str | None = None,
) -> OperatorDecisionAuthorityRecord:
    now = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    return OperatorDecisionAuthorityRecord(
        operator_decision_id=f"operator_decision.test.{decision_kind}",
        decision_key=decision_key,
        decision_kind=decision_kind,
        decision_status="decided",
        title="Test decision",
        rationale="Test rationale",
        decided_by="tests",
        decision_source="tests",
        effective_from=now,
        effective_to=None,
        decided_at=now,
        created_at=now,
        updated_at=now,
        decision_scope_kind=decision_scope_kind,
        decision_scope_ref=decision_scope_ref,
    )


def test_record_operator_decision_uses_stable_id_for_architecture_policy() -> None:
    repository = _FakeOperatorControlRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.record_operator_decision(
        decision_key="architecture-policy::decision-tables::db-native-authority",
        decision_kind="architecture_policy",
        title="Decision tables are DB-native authority",
        rationale="Keep authority in Postgres.",
        decided_by="praxis-admin",
        decision_source="cto.guidance",
        decision_scope_kind="authority_domain",
        decision_scope_ref="decision_tables",
    )

    recorded = repository.recorded
    assert recorded is not None
    assert (
        recorded.operator_decision_id
        == "operator_decision.architecture_policy.decision_tables.db_native_authority"
    )
    assert recorded.decision_scope_kind == "authority_domain"
    assert recorded.decision_scope_ref == "decision_tables"
    assert semantic_repository.upserted_predicates[0].predicate_slug == "architecture_policy"
    semantic_assertion, cardinality_mode, _ = semantic_repository.recorded_assertions[0]
    assert semantic_assertion.subject_kind == "authority_domain"
    assert semantic_assertion.subject_ref == "decision_tables"
    assert semantic_assertion.object_kind == "operator_decision"
    assert (
        semantic_assertion.object_ref
        == "operator_decision.architecture_policy.decision_tables.db_native_authority"
    )
    assert semantic_assertion.qualifiers_json["decision_key"] == (
        "architecture-policy::decision-tables::db-native-authority"
    )
    assert cardinality_mode == "many"
    assert [row["event_type"] for row in conn.event_rows] == [
        "semantic_predicate_registered",
        "semantic_assertion_recorded",
    ]
    assert (
        payload["operator_decision"]["operator_decision_id"]
        == "operator_decision.architecture_policy.decision_tables.db_native_authority"
    )


def test_list_operator_decisions_filters_by_kind_and_scope() -> None:
    rows = (
        _decision(
            decision_key="architecture-policy::decision-tables::db-native-authority",
            decision_kind="architecture_policy",
            decision_scope_kind="authority_domain",
            decision_scope_ref="decision_tables",
        ),
        _decision(
            decision_key="decision.query.test",
            decision_kind="query",
        ),
    )
    repository = _FakeOperatorControlRepository(rows=rows)

    async def _connect(_env=None):
        return _FakeConn()

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
    )

    payload = frontdoor.list_operator_decisions(
        decision_kind="architecture_policy",
        decision_source="tests",
        decision_scope_kind="authority_domain",
        decision_scope_ref="decision_tables",
    )

    decisions = payload["operator_decisions"]
    assert len(decisions) == 1
    assert decisions[0]["decision_kind"] == "architecture_policy"
    assert decisions[0]["decision_scope_ref"] == "decision_tables"
    assert repository.list_kwargs is not None
    assert repository.list_kwargs["decision_kind"] == "architecture_policy"
    assert repository.list_kwargs["decision_source"] == "tests"
    assert repository.list_kwargs["decision_scope_kind"] == "authority_domain"
    assert repository.list_kwargs["decision_scope_ref"] == "decision_tables"
    assert repository.list_kwargs["active_only"] is True


def test_record_operator_decision_skips_semantic_bridge_for_unscoped_decision_kind() -> None:
    repository = _FakeOperatorControlRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.record_operator_decision(
        decision_key="decision.query.test",
        decision_kind="query",
        title="Query test",
        rationale="Unscoped decisions should stay unbridged.",
        decided_by="tests",
        decision_source="tests",
    )

    assert payload["operator_decision"]["decision_kind"] == "query"
    assert semantic_repository.upserted_predicates == []
    assert semantic_repository.recorded_assertions == []
    assert conn.event_rows == []


def test_backfill_semantic_bridges_replays_scoped_decisions_only() -> None:
    as_of = datetime(2026, 4, 15, 19, 0, tzinfo=timezone.utc)
    rows = (
        _decision(
            decision_key="architecture-policy::decision-tables::db-native-authority",
            decision_kind="architecture_policy",
            decision_scope_kind="authority_domain",
            decision_scope_ref="decision_tables",
        ),
        _decision(
            decision_key="decision.query.test",
            decision_kind="query",
        ),
    )
    repository = _FakeOperatorControlRepository(rows=rows)
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.backfill_semantic_bridges(
        include_object_relations=False,
        include_operator_decisions=True,
        include_roadmap_items=False,
        as_of=as_of,
    )

    summary = payload["semantic_bridge_backfill"]
    assert summary["operator_decisions"] == {
        "processed": 2,
        "recorded": 1,
        "skipped_unscoped": 1,
    }
    assert summary["object_relations"] == {
        "processed": 0,
        "recorded": 0,
        "retracted": 0,
        "tombstoned": 0,
    }
    assert summary["roadmap_items"] == {
        "processed": 0,
        "recorded": 0,
        "retracted": 0,
    }
    assert repository.as_of == as_of
    assert semantic_repository.upserted_predicates[0].predicate_slug == "architecture_policy"
    assert len(semantic_repository.recorded_assertions) == 1
    assert semantic_repository.rebuild_calls[-1] == as_of
    assert conn.event_rows[-1]["event_type"] == "semantic_bridge_backfilled"
