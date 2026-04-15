from __future__ import annotations

from datetime import datetime, timezone

import pytest

from authority.operator_control import (
    OperatorControlRepositoryError,
    OperatorDecisionAuthorityRecord,
    normalize_operator_decision_record,
)


def _decision(
    *,
    decision_kind: str,
    decision_key: str,
    decision_scope_kind: str | None = None,
    decision_scope_ref: str | None = None,
) -> OperatorDecisionAuthorityRecord:
    now = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    return OperatorDecisionAuthorityRecord(
        operator_decision_id=f"operator_decision.{decision_kind}.test",
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


def test_circuit_breaker_scope_is_inferred_from_decision_key() -> None:
    normalized = normalize_operator_decision_record(
        _decision(
            decision_kind="circuit_breaker_force_open",
            decision_key="circuit-breaker::openai::20260415T180000000000Z",
        )
    )

    assert normalized.decision_scope_kind == "provider"
    assert normalized.decision_scope_ref == "openai"


def test_cutover_scope_is_inferred_from_fallback_target() -> None:
    normalized = normalize_operator_decision_record(
        _decision(
            decision_kind="native_primary_cutover",
            decision_key="native-primary-cutover::roadmap_item:test:1234",
        ),
        fallback_scope_kind="roadmap_item",
        fallback_scope_ref="roadmap_item.test",
    )

    assert normalized.decision_scope_kind == "roadmap_item"
    assert normalized.decision_scope_ref == "roadmap_item.test"


def test_unscoped_decision_kind_rejects_fake_scope() -> None:
    with pytest.raises(OperatorControlRepositoryError) as excinfo:
        normalize_operator_decision_record(
            _decision(
                decision_kind="query",
                decision_key="decision.query.test",
                decision_scope_kind="provider",
                decision_scope_ref="openai",
            )
        )

    assert excinfo.value.reason_code == "operator_control.invalid_scope"


def test_architecture_policy_requires_authority_domain_scope() -> None:
    normalized = normalize_operator_decision_record(
        _decision(
            decision_kind="architecture_policy",
            decision_key="architecture-policy::decision-tables::db-native-authority",
            decision_scope_kind="authority_domain",
            decision_scope_ref="decision_tables",
        )
    )

    assert normalized.decision_scope_kind == "authority_domain"
    assert normalized.decision_scope_ref == "decision_tables"


def test_architecture_policy_rejects_missing_typed_scope() -> None:
    with pytest.raises(OperatorControlRepositoryError) as excinfo:
        normalize_operator_decision_record(
            _decision(
                decision_kind="architecture_policy",
                decision_key="architecture-policy::decision-tables::db-native-authority",
            )
        )

    assert excinfo.value.reason_code == "operator_control.scope_required"


def test_unknown_decision_kind_fails_closed() -> None:
    with pytest.raises(OperatorControlRepositoryError) as excinfo:
        normalize_operator_decision_record(
            _decision(
                decision_kind="mystery_kind",
                decision_key="decision.mystery.test",
            )
        )

    assert excinfo.value.reason_code == "operator_control.unknown_decision_kind"
