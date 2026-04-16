"""Pure state machine transition logic for workflow runtime."""

from __future__ import annotations

from collections.abc import Mapping

from ..claim_state_machine import ALLOWED_TRANSITIONS as _CLAIM_LIFECYCLE_ALLOWED_TRANSITIONS
from ..domain import LifecycleTransition, RunState, RuntimeLifecycleError


def _claim_targets(*extra: RunState, state: RunState) -> frozenset[RunState]:
    return _CLAIM_LIFECYCLE_ALLOWED_TRANSITIONS.get(state, frozenset()) | frozenset(extra)


ALLOWED_TRANSITIONS: Mapping[RunState, frozenset[RunState]] = {
    RunState.CLAIM_RECEIVED: _claim_targets(RunState.CANCELLED, state=RunState.CLAIM_RECEIVED),
    RunState.CLAIM_VALIDATING: _claim_targets(RunState.CANCELLED, state=RunState.CLAIM_VALIDATING),
    RunState.CLAIM_BLOCKED: _claim_targets(RunState.CANCELLED, state=RunState.CLAIM_BLOCKED),
    RunState.CLAIM_ACCEPTED: _claim_targets(
        RunState.CANCELLED,
        RunState.QUEUED,
        state=RunState.CLAIM_ACCEPTED,
    ),
    RunState.QUEUED: frozenset({RunState.CANCELLED, RunState.RUNNING}),
    RunState.RUNNING: frozenset({RunState.CANCELLED, RunState.FAILED, RunState.SUCCEEDED}),
    RunState.LEASE_REQUESTED: _claim_targets(RunState.CANCELLED, state=RunState.LEASE_REQUESTED),
    RunState.LEASE_BLOCKED: _claim_targets(RunState.CANCELLED, state=RunState.LEASE_BLOCKED),
    RunState.LEASE_ACTIVE: _claim_targets(RunState.CANCELLED, state=RunState.LEASE_ACTIVE),
    RunState.PROPOSAL_SUBMITTED: frozenset({RunState.CANCELLED, RunState.GATE_EVALUATING}),
    RunState.GATE_EVALUATING: frozenset(
        {
            RunState.CANCELLED,
            RunState.GATE_BLOCKED,
            RunState.PROMOTION_DECISION_RECORDED,
        }
    ),
    RunState.GATE_BLOCKED: frozenset({RunState.CANCELLED, RunState.GATE_EVALUATING}),
    RunState.PROMOTION_DECISION_RECORDED: frozenset(
        {
            RunState.PROMOTED,
            RunState.PROMOTION_FAILED,
            RunState.PROMOTION_REJECTED,
        }
    ),
}


def validate_transition(transition: LifecycleTransition) -> None:
    """Validate that a state transition is allowed by the state machine.

    Args:
        transition: The lifecycle transition to validate

    Raises:
        RuntimeLifecycleError: If the transition is not allowed
    """
    allowed_targets = ALLOWED_TRANSITIONS.get(transition.from_state, frozenset())
    if transition.to_state not in allowed_targets:
        raise RuntimeLifecycleError(
            f"runtime.transition_invalid:{transition.from_state.value}->{transition.to_state.value}"
        )


__all__ = [
    "ALLOWED_TRANSITIONS",
    "validate_transition",
]
