from __future__ import annotations

from datetime import datetime, timezone

from authority.operator_control import OperatorDecisionAuthorityRecord
from surfaces.api import operator_write


class _FakeConn:
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

    async def _connect(_env=None):
        return _FakeConn()

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect,
        operator_control_repository_factory=lambda _conn: repository,
    )

    payload = frontdoor.record_operator_decision(
        decision_key="architecture-policy::decision-tables::db-native-authority",
        decision_kind="architecture_policy",
        title="Decision tables are DB-native authority",
        rationale="Keep authority in Postgres.",
        decided_by="nate",
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
