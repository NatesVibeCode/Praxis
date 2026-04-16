from __future__ import annotations

from datetime import datetime, timezone

from surfaces.api import operator_write


class _FakeConn:
    async def fetchrow(self, _query: str, *_args: object):
        return {"ok": 1}

    async def close(self) -> None:
        return None


class _FakeObjectRelationRepository:
    def __init__(self) -> None:
        self.recorded_area = None
        self.recorded_relation = None

    async def record_functional_area(self, *, functional_area):
        self.recorded_area = functional_area
        return functional_area

    async def record_relation(self, *, relation):
        self.recorded_relation = relation
        return relation


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

    async def _connect(_env=None):
        return _FakeConn()

    fixed_now = datetime(2026, 4, 16, 22, 0, tzinfo=timezone.utc)

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        object_relation_repository_factory=lambda _conn: repository,
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
    assert payload["operator_object_relation"]["target"] == {
        "kind": "functional_area",
        "ref": "functional_area.checkout-experience",
    }
