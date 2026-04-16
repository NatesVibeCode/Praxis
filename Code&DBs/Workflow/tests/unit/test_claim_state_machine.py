from __future__ import annotations

from datetime import datetime, timezone

import pytest

from runtime.domain import LifecycleTransition, RouteIdentity, RunState, RuntimeLifecycleError
from runtime.execution.state_machine import ALLOWED_TRANSITIONS as EXECUTION_ALLOWED_TRANSITIONS
from runtime.execution.state_machine import validate_transition


def _route_identity() -> RouteIdentity:
    return RouteIdentity(
        workflow_id="workflow:test",
        run_id="run:test",
        request_id="request:test",
        authority_context_ref="authority:test",
        authority_context_digest="digest:test",
    )


def test_execution_state_machine_starts_at_claim_acceptance_boundary() -> None:
    assert RunState.CLAIM_ACCEPTED in EXECUTION_ALLOWED_TRANSITIONS
    for state in (
        RunState.CLAIM_RECEIVED,
        RunState.CLAIM_VALIDATING,
        RunState.CLAIM_BLOCKED,
        RunState.LEASE_REQUESTED,
        RunState.LEASE_BLOCKED,
        RunState.LEASE_ACTIVE,
    ):
        assert state not in EXECUTION_ALLOWED_TRANSITIONS


def test_execution_state_machine_rejects_claim_lifecycle_transitions() -> None:
    transition = LifecycleTransition(
        route_identity=_route_identity(),
        from_state=RunState.CLAIM_RECEIVED,
        to_state=RunState.CLAIM_ACCEPTED,
        reason_code="claim.fast_path",
        evidence_seq=1,
        event_type="claim_accepted",
        receipt_type="claim_accepted_receipt",
        occurred_at=datetime.now(timezone.utc),
    )

    with pytest.raises(RuntimeLifecycleError, match="runtime.transition_invalid"):
        validate_transition(transition)


def test_execution_state_machine_allows_queue_from_claim_acceptance() -> None:
    transition = LifecycleTransition(
        route_identity=_route_identity(),
        from_state=RunState.CLAIM_ACCEPTED,
        to_state=RunState.QUEUED,
        reason_code="runtime.execution_ready",
        evidence_seq=1,
        event_type="workflow_queued",
        receipt_type="workflow_queue_receipt",
        occurred_at=datetime.now(timezone.utc),
    )

    validate_transition(transition)
