"""Pure state machine transition logic for workflow runtime."""

from __future__ import annotations

from collections.abc import Mapping

from ..domain import LifecycleTransition, RunState, RuntimeLifecycleError

ALLOWED_TRANSITIONS: Mapping[RunState, frozenset[RunState]] = {
    RunState.CLAIM_ACCEPTED: frozenset({RunState.CANCELLED, RunState.QUEUED}),
    RunState.QUEUED: frozenset({RunState.CANCELLED, RunState.RUNNING}),
    RunState.RUNNING: frozenset({RunState.CANCELLED, RunState.FAILED, RunState.SUCCEEDED}),
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
