"""Runtime authority.

Owns route identity, lifecycle transitions, and the atomic boundary that must
persist lifecycle state and evidence together.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Protocol


class RuntimeBoundaryError(RuntimeError):
    """Raised when runtime state would cross an authority boundary."""

    def __init__(
        self,
        reason_code: str,
        message: str | None = None,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        rendered_message = reason_code if message is None else f"{reason_code}: {message}"
        super().__init__(rendered_message)
        self.reason_code = reason_code
        self.message = message or reason_code
        self.details = dict(details or {})


class RuntimeLifecycleError(RuntimeError):
    """Raised when a lifecycle transition is invalid or incomplete."""


class RunState(str, Enum):
    CLAIM_RECEIVED = "claim_received"
    CLAIM_VALIDATING = "claim_validating"
    CLAIM_BLOCKED = "claim_blocked"
    CLAIM_REJECTED = "claim_rejected"
    CLAIM_ACCEPTED = "claim_accepted"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LEASE_REQUESTED = "lease_requested"
    LEASE_BLOCKED = "lease_blocked"
    LEASE_ACTIVE = "lease_active"
    LEASE_EXPIRED = "lease_expired"
    PROPOSAL_SUBMITTED = "proposal_submitted"
    PROPOSAL_INVALID = "proposal_invalid"
    GATE_EVALUATING = "gate_evaluating"
    GATE_BLOCKED = "gate_blocked"
    PROMOTION_DECISION_RECORDED = "promotion_decision_recorded"
    PROMOTED = "promoted"
    PROMOTION_REJECTED = "promotion_rejected"
    PROMOTION_FAILED = "promotion_failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class RouteIdentity:
    """Stable join key for the full runtime path."""

    workflow_id: str
    run_id: str
    request_id: str
    authority_context_ref: str
    authority_context_digest: str
    claim_id: str | None = None
    lease_id: str | None = None
    proposal_id: str | None = None
    promotion_decision_id: str | None = None
    attempt_no: int = 1
    transition_seq: int = 0


@dataclass(frozen=True, slots=True)
class DataQualityIssue:
    """A non-fatal data-shape problem detected when reading persisted evidence.

    Surfaced through ``EvidenceRow.data_quality_issues`` so operators can see
    *which* row is malformed and *why* without inspect commands hard-failing.
    """

    reason_code: str
    kind: str
    row_id: str
    evidence_seq: int
    hint: str


@dataclass(frozen=True, slots=True)
class LifecycleTransition:
    """A single authoritative runtime state change."""

    route_identity: RouteIdentity
    from_state: RunState
    to_state: RunState
    reason_code: str
    evidence_seq: int
    event_type: str
    receipt_type: str
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class EvidenceCommitResult:
    """Return value for an atomic lifecycle-and-evidence commit."""

    event_id: str
    receipt_id: str
    evidence_seq: int
    committed_at: datetime


class AtomicEvidenceWriter(Protocol):
    """Writes runtime state and evidence atomically."""

    def commit_submission(
        self,
        *,
        route_identity: RouteIdentity,
        admitted_definition_ref: str,
        admitted_definition_hash: str,
        request_payload: Mapping[str, Any],
    ) -> EvidenceCommitResult:
        """Persist the initial runtime admission and matching evidence."""
        ...

    def commit_transition(
        self,
        *,
        transition: LifecycleTransition,
    ) -> EvidenceCommitResult:
        """Persist one runtime transition and matching evidence."""
        ...


class RuntimeOrchestrator(Protocol):
    """Lifecycle authority contract for runtime orchestration."""

    def submit_run(
        self,
        *,
        route_identity: RouteIdentity,
        admitted_definition_ref: str,
        admitted_definition_hash: str,
        request_payload: Mapping[str, Any],
        evidence_writer: AtomicEvidenceWriter,
    ) -> EvidenceCommitResult:
        ...

    def advance_run(
        self,
        *,
        transition: LifecycleTransition,
        evidence_writer: AtomicEvidenceWriter,
    ) -> EvidenceCommitResult:
        ...

    def inspect_run(self, *, run_id: str) -> Mapping[str, Any]:
        ...

    def replay_run(self, *, run_id: str) -> Mapping[str, Any]:
        ...


__all__ = [
    "AtomicEvidenceWriter",
    "EvidenceCommitResult",
    "LifecycleTransition",
    "RouteIdentity",
    "RunState",
    "RuntimeBoundaryError",
    "RuntimeLifecycleError",
    "RuntimeOrchestrator",
]
