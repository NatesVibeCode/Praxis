"""Workflow intake planner for the first runtime slice.

This module owns the runtime-owned derivation path for intake:

- validate the workflow request
- resolve authoritative workspace and runtime profile refs
- bind the admitted definition snapshot immutably
- derive the evidence ordering and route lineage
- emit an explicit admit/reject decision with a machine-readable reason

No execution is performed here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Final

from contracts.domain import (
    WorkflowRequest,
    WorkflowValidationResult,
    validate_workflow_request,
)
from policy.domain import AdmissionDecisionKind, AdmissionDecisionRecord, PolicyEngine
from registry.domain import (
    AuthorityContext,
    RegistryBoundaryError,
    RegistryResolutionError,
    RegistryResolver,
)

from .domain import RouteIdentity, RunState

WORKFLOW_RECEIVED_EVENT_TYPE: Final[str] = "workflow_received"
WORKFLOW_ADMITTED_EVENT_TYPE: Final[str] = "workflow_admitted"
WORKFLOW_REJECTED_EVENT_TYPE: Final[str] = "workflow_rejected"
WORKFLOW_RECEIVED_RECEIPT_TYPE: Final[str] = "workflow_received_receipt"
WORKFLOW_ADMISSION_RECEIPT_TYPE: Final[str] = "workflow_admission_receipt"
POLICY_SNAPSHOT_REF: Final[str] = "policy_snapshot:workflow_intake_v1"
DECIDED_BY: Final[str] = "policy.intake"


@dataclass(frozen=True, slots=True)
class LifecycleProofStep:
    """Derived event/receipt intent before runtime claims committed ordering."""

    transition_seq: int | None
    evidence_seq: int | None
    from_state: RunState | None
    to_state: RunState
    event_type: str
    receipt_type: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class WorkflowIntakeOutcome:
    """Derived result of the intake path before execution begins."""

    workflow_request: WorkflowRequest
    validation_result: WorkflowValidationResult
    request_digest: str
    run_id: str
    run_idempotency_key: str
    route_identity: RouteIdentity
    authority_context: AuthorityContext | None
    admission_decision: AdmissionDecisionRecord
    admitted_definition_ref: str | None
    admitted_definition_hash: str | None
    admission_state: RunState
    committed_state: RunState | None
    cancel_allowed: bool
    evidence_plan: tuple[LifecycleProofStep, ...]

    @property
    def admitted_definition_id(self) -> str | None:
        """Backward-compatible alias for the admitted definition reference."""

        return self.admitted_definition_ref

    @property
    def current_state(self) -> RunState:
        """Backward-compatible alias for the planned admission target state."""

        return self.admission_state


def _stable_run_id(*, workflow_id: str, request_digest: str) -> str:
    return f"run:{workflow_id}:{request_digest[:16]}"


def _stable_claim_id(*, run_id: str) -> str:
    return f"claim:{run_id}"


def _build_route_identity(
    *,
    workflow_request: WorkflowRequest,
    run_id: str,
    authority_context_ref: str,
    authority_context_digest: str,
) -> RouteIdentity:
    return RouteIdentity(
        workflow_id=workflow_request.workflow_id,
        run_id=run_id,
        request_id=workflow_request.request_id,
        authority_context_ref=authority_context_ref,
        authority_context_digest=authority_context_digest,
        claim_id=_stable_claim_id(run_id=run_id),
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=0,
    )


def _build_evidence_plan(
    *,
    admitted: bool,
    reason_code: str,
) -> tuple[LifecycleProofStep, ...]:
    final_state = RunState.CLAIM_ACCEPTED if admitted else RunState.CLAIM_REJECTED
    final_event_type = (
        WORKFLOW_ADMITTED_EVENT_TYPE if admitted else WORKFLOW_REJECTED_EVENT_TYPE
    )
    return (
        LifecycleProofStep(
            transition_seq=None,
            evidence_seq=None,
            from_state=None,
            to_state=RunState.CLAIM_RECEIVED,
            event_type=WORKFLOW_RECEIVED_EVENT_TYPE,
            receipt_type=WORKFLOW_RECEIVED_RECEIPT_TYPE,
            reason_code="workflow.received",
        ),
        LifecycleProofStep(
            transition_seq=None,
            evidence_seq=None,
            from_state=RunState.CLAIM_RECEIVED,
            to_state=final_state,
            event_type=final_event_type,
            receipt_type=WORKFLOW_ADMISSION_RECEIPT_TYPE,
            reason_code=reason_code,
        ),
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _admission_state(*, decision: AdmissionDecisionRecord) -> RunState:
    return (
        RunState.CLAIM_ACCEPTED
        if decision.decision is AdmissionDecisionKind.ADMIT
        else RunState.CLAIM_REJECTED
    )


@dataclass(frozen=True, slots=True)
class WorkflowIntakePlanner:
    """Runtime-owned derivation for the first intake slice."""

    registry: RegistryResolver
    policy: PolicyEngine = field(default_factory=PolicyEngine)
    policy_snapshot_ref: str = POLICY_SNAPSHOT_REF
    decided_by: str = DECIDED_BY
    context_bundle_version: int = 1

    def plan(self, *, request: WorkflowRequest) -> WorkflowIntakeOutcome:
        validation = validate_workflow_request(request)
        normalized_request = validation.normalized_request or request
        run_id = _stable_run_id(
            workflow_id=normalized_request.workflow_id,
            request_digest=validation.request_digest,
        )
        run_idempotency_key = normalized_request.request_id
        source_decision_refs = (validation.validation_result_ref,)

        if not validation.is_valid:
            authority_context = self.registry.build_unresolved_context(
                workflow_id=normalized_request.workflow_id,
                run_id=run_id,
                request_digest=validation.request_digest,
                workspace_ref=normalized_request.workspace_ref,
                runtime_profile_ref=normalized_request.runtime_profile_ref,
                bundle_version=self.context_bundle_version,
                unresolved_reason_code=validation.reason_code,
                source_decision_refs=source_decision_refs,
            )
            route_identity = _build_route_identity(
                workflow_request=normalized_request,
                run_id=run_id,
                authority_context_ref=authority_context.context_bundle_id,
                authority_context_digest=authority_context.bundle_hash,
            )
            decision = self.policy.decide_admission(
                workflow_id=normalized_request.workflow_id,
                request_id=normalized_request.request_id,
                validation_result_ref=validation.validation_result_ref,
                authority_context_ref=authority_context.context_bundle_id,
                policy_snapshot_ref=self.policy_snapshot_ref,
                decided_by=self.decided_by,
                admitted=False,
                rejection_reason_code=validation.reason_code,
                decided_at=_now(),
            )
            admission_state = _admission_state(decision=decision)
            return WorkflowIntakeOutcome(
                workflow_request=normalized_request,
                validation_result=validation,
                request_digest=validation.request_digest,
                run_id=run_id,
                run_idempotency_key=run_idempotency_key,
                route_identity=route_identity,
                authority_context=authority_context,
                admission_decision=decision,
                admitted_definition_ref=None,
                admitted_definition_hash=None,
                admission_state=admission_state,
                committed_state=None,
                cancel_allowed=False,
                evidence_plan=_build_evidence_plan(
                    admitted=admission_state is RunState.CLAIM_ACCEPTED,
                    reason_code=decision.reason_code,
                ),
            )

        try:
            workspace = self.registry.resolve_workspace(
                workspace_ref=normalized_request.workspace_ref,
            )
            runtime_profile = self.registry.resolve_runtime_profile(
                runtime_profile_ref=normalized_request.runtime_profile_ref,
            )
            authority_context = self.registry.resolve_context_bundle(
                workflow_id=normalized_request.workflow_id,
                run_id=run_id,
                workspace=workspace,
                runtime_profile=runtime_profile,
                bundle_version=self.context_bundle_version,
                source_decision_refs=source_decision_refs,
            )
        except RegistryResolutionError as exc:
            authority_context = self.registry.build_unresolved_context(
                workflow_id=normalized_request.workflow_id,
                run_id=run_id,
                request_digest=validation.request_digest,
                workspace_ref=normalized_request.workspace_ref,
                runtime_profile_ref=normalized_request.runtime_profile_ref,
                bundle_version=self.context_bundle_version,
                unresolved_reason_code=exc.reason_code,
                source_decision_refs=source_decision_refs,
            )
            route_identity = _build_route_identity(
                workflow_request=normalized_request,
                run_id=run_id,
                authority_context_ref=authority_context.context_bundle_id,
                authority_context_digest=authority_context.bundle_hash,
            )
            decision = self.policy.decide_admission(
                workflow_id=normalized_request.workflow_id,
                request_id=normalized_request.request_id,
                validation_result_ref=validation.validation_result_ref,
                authority_context_ref=authority_context.context_bundle_id,
                policy_snapshot_ref=self.policy_snapshot_ref,
                decided_by=self.decided_by,
                admitted=False,
                rejection_reason_code=exc.reason_code,
                decided_at=_now(),
            )
            admission_state = _admission_state(decision=decision)
            return WorkflowIntakeOutcome(
                workflow_request=normalized_request,
                validation_result=validation,
                request_digest=validation.request_digest,
                run_id=run_id,
                run_idempotency_key=run_idempotency_key,
                route_identity=route_identity,
                authority_context=authority_context,
                admission_decision=decision,
                admitted_definition_ref=None,
                admitted_definition_hash=None,
                admission_state=admission_state,
                committed_state=None,
                cancel_allowed=False,
                evidence_plan=_build_evidence_plan(
                    admitted=admission_state is RunState.CLAIM_ACCEPTED,
                    reason_code=decision.reason_code,
                ),
            )
        except RegistryBoundaryError as exc:
            authority_context = self.registry.build_unresolved_context(
                workflow_id=normalized_request.workflow_id,
                run_id=run_id,
                request_digest=validation.request_digest,
                workspace_ref=normalized_request.workspace_ref,
                runtime_profile_ref=normalized_request.runtime_profile_ref,
                bundle_version=self.context_bundle_version,
                unresolved_reason_code=exc.reason_code,
                source_decision_refs=source_decision_refs,
            )
            route_identity = _build_route_identity(
                workflow_request=normalized_request,
                run_id=run_id,
                authority_context_ref=authority_context.context_bundle_id,
                authority_context_digest=authority_context.bundle_hash,
            )
            decision = self.policy.decide_admission(
                workflow_id=normalized_request.workflow_id,
                request_id=normalized_request.request_id,
                validation_result_ref=validation.validation_result_ref,
                authority_context_ref=authority_context.context_bundle_id,
                policy_snapshot_ref=self.policy_snapshot_ref,
                decided_by=self.decided_by,
                admitted=False,
                rejection_reason_code=exc.reason_code,
                decided_at=_now(),
            )
            admission_state = _admission_state(decision=decision)
            return WorkflowIntakeOutcome(
                workflow_request=normalized_request,
                validation_result=validation,
                request_digest=validation.request_digest,
                run_id=run_id,
                run_idempotency_key=run_idempotency_key,
                route_identity=route_identity,
                authority_context=authority_context,
                admission_decision=decision,
                admitted_definition_ref=None,
                admitted_definition_hash=None,
                admission_state=admission_state,
                committed_state=None,
                cancel_allowed=False,
                evidence_plan=_build_evidence_plan(
                    admitted=admission_state is RunState.CLAIM_ACCEPTED,
                    reason_code=decision.reason_code,
                ),
            )

        route_identity = _build_route_identity(
            workflow_request=normalized_request,
            run_id=run_id,
            authority_context_ref=authority_context.context_bundle_id,
            authority_context_digest=authority_context.bundle_hash,
        )
        decision = self.policy.decide_admission(
            workflow_id=normalized_request.workflow_id,
            request_id=normalized_request.request_id,
            validation_result_ref=validation.validation_result_ref,
            authority_context_ref=authority_context.context_bundle_id,
            policy_snapshot_ref=self.policy_snapshot_ref,
            decided_by=self.decided_by,
            admitted=True,
            decided_at=_now(),
        )
        admission_state = _admission_state(decision=decision)
        return WorkflowIntakeOutcome(
            workflow_request=normalized_request,
            validation_result=validation,
            request_digest=validation.request_digest,
            run_id=run_id,
            run_idempotency_key=run_idempotency_key,
            route_identity=route_identity,
            authority_context=authority_context,
            admission_decision=decision,
            admitted_definition_ref=normalized_request.workflow_definition_id,
            admitted_definition_hash=normalized_request.definition_hash,
            admission_state=admission_state,
            committed_state=None,
            cancel_allowed=False,
            evidence_plan=_build_evidence_plan(
                admitted=admission_state is RunState.CLAIM_ACCEPTED,
                reason_code=decision.reason_code,
            ),
        )


__all__ = [
    "DECIDED_BY",
    "LifecycleProofStep",
    "POLICY_SNAPSHOT_REF",
    "WORKFLOW_ADMITTED_EVENT_TYPE",
    "WORKFLOW_ADMISSION_RECEIPT_TYPE",
    "WORKFLOW_RECEIVED_EVENT_TYPE",
    "WORKFLOW_RECEIVED_RECEIPT_TYPE",
    "WORKFLOW_REJECTED_EVENT_TYPE",
    "WorkflowIntakeOutcome",
    "WorkflowIntakePlanner",
]
