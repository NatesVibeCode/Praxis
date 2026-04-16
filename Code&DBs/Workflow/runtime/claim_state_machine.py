"""Compatibility projection for claim lifecycle transitions.

The durable runtime authority now lives in the DB-backed
`workflow_claim_lifecycle_transition_authority` table. This module preserves a
process-local projection for non-DB consumers that still need a pure mapping.
"""

from __future__ import annotations

from collections.abc import Mapping

from .domain import RunState, RuntimeLifecycleError


ALLOWED_TRANSITIONS: Mapping[RunState, frozenset[RunState]] = {
    RunState.CLAIM_RECEIVED: frozenset({RunState.CLAIM_VALIDATING}),
    RunState.CLAIM_VALIDATING: frozenset(
        {
            RunState.CLAIM_ACCEPTED,
            RunState.CLAIM_BLOCKED,
            RunState.CLAIM_REJECTED,
        }
    ),
    RunState.CLAIM_BLOCKED: frozenset(
        {
            RunState.CLAIM_REJECTED,
            RunState.CLAIM_VALIDATING,
        }
    ),
    RunState.CLAIM_ACCEPTED: frozenset({RunState.LEASE_REQUESTED}),
    RunState.LEASE_REQUESTED: frozenset({RunState.LEASE_ACTIVE, RunState.LEASE_BLOCKED}),
    RunState.LEASE_BLOCKED: frozenset({RunState.LEASE_REQUESTED}),
    RunState.LEASE_ACTIVE: frozenset(
        {
            RunState.LEASE_EXPIRED,
            RunState.PROPOSAL_INVALID,
            RunState.PROPOSAL_SUBMITTED,
        }
    ),
}


def validate_claim_lifecycle_transition(*, from_state: RunState, to_state: RunState) -> None:
    allowed_targets = ALLOWED_TRANSITIONS.get(from_state, frozenset())
    if to_state not in allowed_targets:
        raise RuntimeLifecycleError(
            f"invalid claim/lease/proposal transition: {from_state.value} -> {to_state.value}"
        )


__all__ = [
    "ALLOWED_TRANSITIONS",
    "validate_claim_lifecycle_transition",
]
