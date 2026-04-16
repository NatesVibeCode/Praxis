from __future__ import annotations

from datetime import datetime, timezone

import pytest

from runtime.claim_state_machine import (
    ALLOWED_TRANSITIONS as CLAIM_LIFECYCLE_ALLOWED_TRANSITIONS,
    validate_claim_lifecycle_transition,
)
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


def test_execution_state_machine_extends_claim_lifecycle_without_drifting() -> None:
    for state, allowed_targets in CLAIM_LIFECYCLE_ALLOWED_TRANSITIONS.items():
        assert allowed_targets <= EXECUTION_ALLOWED_TRANSITIONS.get(state, frozenset())


def test_execution_state_machine_no_longer_skips_claim_validation() -> None:
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


def test_claim_lifecycle_validator_rejects_execution_only_targets() -> None:
    with pytest.raises(
        RuntimeLifecycleError,
        match="invalid claim/lease/proposal transition: claim_accepted -> queued",
    ):
        validate_claim_lifecycle_transition(
            from_state=RunState.CLAIM_ACCEPTED,
            to_state=RunState.QUEUED,
        )
