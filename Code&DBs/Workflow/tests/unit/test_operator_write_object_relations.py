from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

import runtime.authority_memory_projection as authority_memory_projection
from surfaces.api import operator_write
from runtime.operator_object_relations import (
    OperatorObjectRelationRecord,
    operator_object_relation_id,
)
from runtime.semantic_assertions import (
    SemanticAssertionRecord,
    SemanticPredicateRecord,
    normalize_semantic_assertion_record,
)


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.event_rows: list[dict[str, object]] = []
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, _query: str, *_args: object):
        return []

    async def fetchrow(self, _query: str, *_args: object):
        if "INSERT INTO event_log" in _query:
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

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "OK"

    async def close(self) -> None:
        return None


class _FakeObjectRelationRepository:
    def __init__(
        self,
        rows: tuple[OperatorObjectRelationRecord, ...] = (),
    ) -> None:
        self.recorded_area = None
        self.recorded_relation = None
        self.rows = rows
        self.as_of = None

    async def record_functional_area(self, *, functional_area):
        self.recorded_area = functional_area
        return functional_area

    async def record_relation(self, *, relation):
        self.recorded_relation = relation
        return relation

    async def list_relations(self, *, as_of=None):
        self.as_of = as_of
        return tuple(self.rows)


class _FakeSemanticAssertionRepository:
    def __init__(self) -> None:
        self.predicates: dict[str, SemanticPredicateRecord] = {}
        self.assertions: dict[str, SemanticAssertionRecord] = {}
        self.upserted_predicates: list[SemanticPredicateRecord] = []
        self.recorded_assertions: list[tuple[SemanticAssertionRecord, str, datetime]] = []
        self.retracted_assertions: list[tuple[str, datetime, datetime]] = []
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
        self.retracted_assertions.append(
            (semantic_assertion_id, retracted_at, updated_at)
        )
        existing = self.assertions[semantic_assertion_id]
        retracted = replace(
            existing,
            assertion_status="retracted",
            valid_to=retracted_at,
            updated_at=updated_at,
        )
        self.assertions[semantic_assertion_id] = retracted
        return retracted

    async def rebuild_current_assertions(self, *, as_of: datetime) -> int:
        self.rebuild_calls.append(as_of)
        return len(self.assertions)


def _relation(
    *,
    relation_kind: str,
    source_kind: str,
    source_ref: str,
    target_kind: str,
    target_ref: str,
    relation_status: str = "active",
    bound_by_decision_id: str | None = None,
    relation_metadata: dict[str, object] | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> OperatorObjectRelationRecord:
    normalized_created_at = created_at or datetime(2026, 4, 16, 22, 0, tzinfo=timezone.utc)
    normalized_updated_at = updated_at or normalized_created_at
    return OperatorObjectRelationRecord(
        operator_object_relation_id=operator_object_relation_id(
            relation_kind=relation_kind,
            source_kind=source_kind,
            source_ref=source_ref,
            target_kind=target_kind,
            target_ref=target_ref,
        ),
        relation_kind=relation_kind,
        relation_status=relation_status,
        source_kind=source_kind,
        source_ref=source_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        relation_metadata=relation_metadata or {},
        bound_by_decision_id=bound_by_decision_id,
        created_at=normalized_created_at,
        updated_at=normalized_updated_at,
    )


def test_record_functional_area_uses_stable_id() -> None:
    repository = _FakeObjectRelationRepository()

    async def _connect(_env=None):
        return _FakeConn()

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        object_relation_repository_factory=lambda _conn: repository,
    )

    payload = frontdoor.record_functional_area(
        area_slug="Checkout Experience",
        title="Checkout Experience",
        summary="Shared semantics for checkout work.",
    )

    recorded = repository.recorded_area
    assert recorded is not None
    assert recorded.functional_area_id == "functional_area.checkout-experience"
    assert recorded.area_slug == "checkout-experience"
    assert payload["functional_area"]["functional_area_id"] == recorded.functional_area_id


def test_record_operator_object_relation_normalizes_functional_area_ref_and_relation_kind() -> None:
    repository = _FakeObjectRelationRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    fixed_now = datetime(2026, 4, 16, 22, 0, tzinfo=timezone.utc)

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        object_relation_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.record_operator_object_relation(
        relation_kind="Grouped In",
        source_kind="roadmap_item",
        source_ref="roadmap_item.checkout.1",
        target_kind="functional_area",
        target_ref="checkout-experience",
        relation_metadata={"origin": "tests"},
        bound_by_decision_id="operator_decision.architecture_policy.checkout",
        created_at=fixed_now,
        updated_at=fixed_now,
    )

    recorded = repository.recorded_relation
    assert recorded is not None
    assert recorded.relation_kind == "grouped_in"
    assert recorded.target_ref == "functional_area.checkout-experience"
    assert (
        recorded.operator_object_relation_id
        == "operator_object_relation:grouped-in:roadmap_item:roadmap_item.checkout.1:functional_area:functional_area.checkout-experience"
    )
    assert recorded.bound_by_decision_id == "operator_decision.architecture_policy.checkout"
    assert semantic_repository.upserted_predicates[0].predicate_slug == "grouped_in"
    semantic_assertion, cardinality_mode, as_of = semantic_repository.recorded_assertions[0]
    assert semantic_assertion.predicate_slug == "grouped_in"
    assert semantic_assertion.subject_kind == "roadmap_item"
    assert semantic_assertion.object_kind == "functional_area"
    assert semantic_assertion.source_kind == "operator_object_relation"
    assert semantic_assertion.source_ref == recorded.operator_object_relation_id
    assert semantic_assertion.qualifiers_json == {
        "relation_metadata": {"origin": "tests"},
    }
    assert cardinality_mode == "single_active_per_edge"
    assert as_of == fixed_now
    assert [row["event_type"] for row in conn.event_rows] == [
        "semantic_predicate_registered",
        "semantic_assertion_recorded",
    ]
    assert payload["operator_object_relation"]["target"] == {
        "kind": "functional_area",
        "ref": "functional_area.checkout-experience",
    }


def test_record_operator_object_relation_retracts_existing_semantic_bridge_when_relation_is_inactive() -> None:
    repository = _FakeObjectRelationRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    created_at = datetime(2026, 4, 16, 22, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 4, 16, 22, 5, tzinfo=timezone.utc)

    existing_assertion = normalize_semantic_assertion_record(
        SemanticAssertionRecord(
            semantic_assertion_id="",
            predicate_slug="grouped_in",
            assertion_status="active",
            subject_kind="roadmap_item",
            subject_ref="roadmap_item.checkout.1",
            object_kind="functional_area",
            object_ref="functional_area.checkout-experience",
            qualifiers_json={"relation_metadata": {"origin": "tests"}},
            source_kind="operator_object_relation",
            source_ref=(
                "operator_object_relation:grouped-in:roadmap_item:"
                "roadmap_item.checkout.1:functional_area:functional_area.checkout-experience"
            ),
            evidence_ref=None,
            bound_decision_id=None,
            valid_from=created_at,
            valid_to=None,
            created_at=created_at,
            updated_at=created_at,
        )
    )
    semantic_repository.assertions[existing_assertion.semantic_assertion_id] = existing_assertion

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        object_relation_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.record_operator_object_relation(
        relation_kind="grouped_in",
        source_kind="roadmap_item",
        source_ref="roadmap_item.checkout.1",
        target_kind="functional_area",
        target_ref="checkout-experience",
        relation_status="inactive",
        relation_metadata={"origin": "tests"},
        created_at=created_at,
        updated_at=updated_at,
    )

    assert semantic_repository.recorded_assertions == []
    assert semantic_repository.retracted_assertions == [
        (existing_assertion.semantic_assertion_id, updated_at, updated_at)
    ]
    assert conn.event_rows[-1]["event_type"] == "semantic_assertion_retracted"
    assert payload["operator_object_relation"]["relation_status"] == "inactive"


def test_record_operator_object_relation_records_tombstone_when_inactive_relation_has_no_prior_semantic_bridge() -> None:
    repository = _FakeObjectRelationRepository()
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    created_at = datetime(2026, 4, 16, 22, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 4, 16, 22, 5, tzinfo=timezone.utc)

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        object_relation_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )

    payload = frontdoor.record_operator_object_relation(
        relation_kind="grouped_in",
        source_kind="bug",
        source_ref="bug.checkout.2",
        target_kind="functional_area",
        target_ref="checkout-experience",
        relation_status="inactive",
        relation_metadata={"origin": "tests"},
        created_at=created_at,
        updated_at=updated_at,
    )

    assert semantic_repository.retracted_assertions == []
    assert len(semantic_repository.recorded_assertions) == 1
    tombstone_assertion, cardinality_mode, as_of = semantic_repository.recorded_assertions[0]
    assert tombstone_assertion.assertion_status == "retracted"
    assert tombstone_assertion.valid_to == updated_at
    assert tombstone_assertion.subject_kind == "bug"
    assert tombstone_assertion.subject_ref == "bug.checkout.2"
    assert tombstone_assertion.object_kind == "functional_area"
    assert tombstone_assertion.object_ref == "functional_area.checkout-experience"
    assert cardinality_mode == "single_active_per_edge"
    assert as_of == updated_at
    assert [row["event_type"] for row in conn.event_rows] == [
        "semantic_predicate_registered",
        "semantic_assertion_recorded",
    ]
    assert conn.event_rows[-1]["payload"]["bridge_state"] == "inactive_tombstone"
    assert payload["operator_object_relation"]["relation_status"] == "inactive"


def test_backfill_semantic_bridges_replays_object_relation_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_at = datetime(2026, 4, 16, 22, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 4, 16, 22, 5, tzinfo=timezone.utc)
    as_of = datetime(2026, 4, 16, 23, 0, tzinfo=timezone.utc)
    expected_as_of = as_of
    called = False

    class _FakeRefreshResult:
        def to_json(self) -> dict[str, object]:
            return {
                "projection_id": "authority_memory_projection",
                "total_upserted": 4,
                "total_deactivated": 0,
            }

    async def _fake_refresh_authority_memory_projection(*, env=None, as_of=None):
        nonlocal called
        called = True
        assert as_of == expected_as_of
        return _FakeRefreshResult()

    repository = _FakeObjectRelationRepository(
        rows=(
            _relation(
                relation_kind="grouped_in",
                source_kind="bug",
                source_ref="bug.checkout.1",
                target_kind="functional_area",
                target_ref="functional_area.checkout",
                relation_metadata={"origin": "tests"},
                created_at=created_at,
                updated_at=created_at,
            ),
            _relation(
                relation_kind="grouped_in",
                source_kind="bug",
                source_ref="bug.checkout.2",
                target_kind="functional_area",
                target_ref="functional_area.payments",
                relation_status="inactive",
                relation_metadata={"origin": "tests"},
                created_at=created_at,
                updated_at=updated_at,
            ),
        )
    )
    semantic_repository = _FakeSemanticAssertionRepository()
    conn = _FakeConn()

    async def _connect(_env=None):
        return conn

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        object_relation_repository_factory=lambda _conn: repository,
        semantic_assertion_repository_factory=lambda _conn: semantic_repository,
    )
    monkeypatch.setattr(
        authority_memory_projection,
        "refresh_authority_memory_projection",
        _fake_refresh_authority_memory_projection,
    )

    payload = frontdoor.backfill_semantic_bridges(
        include_object_relations=True,
        include_operator_decisions=False,
        include_roadmap_items=False,
        as_of=as_of,
    )

    summary = payload["semantic_bridge_backfill"]
    assert summary["object_relations"] == {
        "processed": 2,
        "recorded": 1,
        "retracted": 0,
        "tombstoned": 1,
    }
    assert summary["operator_decisions"] == {
        "processed": 0,
        "recorded": 0,
        "skipped_unscoped": 0,
    }
    assert summary["roadmap_items"] == {
        "processed": 0,
        "recorded": 0,
        "retracted": 0,
    }
    assert summary["authority_memory_refresh"] == {
        "projection_id": "authority_memory_projection",
        "total_upserted": 4,
        "total_deactivated": 0,
    }
    assert called is True
    assert repository.as_of == as_of
    assert len(semantic_repository.recorded_assertions) == 2
    assert semantic_repository.recorded_assertions[1][0].assertion_status == "retracted"
    assert semantic_repository.rebuild_calls[-1] == as_of
    assert conn.event_rows[-1]["event_type"] == "semantic_bridge_backfilled"
