from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import pytest

from contracts.domain import (
    SUPPORTED_SCHEMA_VERSION,
    WorkflowRequest,
    WorkflowValidationResult,
)
from policy.domain import AdmissionDecisionKind, AdmissionDecisionRecord
from registry.domain import ContextBundle
from runtime.domain import RouteIdentity, RunState, RuntimeBoundaryError
from runtime.execution.request_building import (
    _authority_payload_hash,
    _execution_boundary_ref,
)
from runtime.intake import WorkflowIntakeOutcome


def _outcome(
    *,
    payload: Mapping[str, Any] | None = None,
    context_bundle_id: str = "context.alpha",
    bundle_hash: str | None = None,
    route_ref: str | None = None,
    route_digest: str | None = None,
    workspace_ref: str = "workspace.alpha",
) -> WorkflowIntakeOutcome:
    bundle_payload = (
        {"workspace": {"workspace_ref": "workspace.alpha"}}
        if payload is None
        else payload
    )
    computed_hash = _authority_payload_hash(bundle_payload)
    admitted_hash = bundle_hash if bundle_hash is not None else computed_hash
    run_id = "run:workflow.alpha:123"
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:definition",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(),
        edges=(),
    )
    route_identity = RouteIdentity(
        workflow_id=request.workflow_id,
        run_id=run_id,
        request_id=request.request_id,
        authority_context_ref=route_ref or context_bundle_id,
        authority_context_digest=route_digest or admitted_hash,
        claim_id=f"claim:{run_id}",
    )
    authority_context = ContextBundle(
        context_bundle_id=context_bundle_id,
        workflow_id=request.workflow_id,
        run_id=run_id,
        workspace_ref=workspace_ref,
        runtime_profile_ref=request.runtime_profile_ref,
        model_profile_id="model.alpha",
        provider_policy_id="provider_policy.alpha",
        sandbox_profile_ref="runtime_profile.alpha",
        bundle_version=1,
        bundle_hash=admitted_hash,
        bundle_payload=bundle_payload,
        source_decision_refs=(),
        resolved_at=datetime.now(timezone.utc),
    )
    validation_result = WorkflowValidationResult(
        request_id=request.request_id,
        workflow_id=request.workflow_id,
        schema_version=request.schema_version,
        request_digest="sha256:request",
        is_valid=True,
        reason_code="workflow.valid",
        errors=(),
        validation_result_ref="validation.alpha",
        normalized_request=request,
    )
    admission_decision = AdmissionDecisionRecord(
        admission_decision_id="admission.alpha",
        workflow_id=request.workflow_id,
        request_id=request.request_id,
        decision=AdmissionDecisionKind.ADMIT,
        reason_code="policy.admit",
        decided_at=datetime.now(timezone.utc),
        decided_by="test",
        policy_snapshot_ref="policy.alpha",
        validation_result_ref=validation_result.validation_result_ref,
        authority_context_ref=context_bundle_id,
    )
    return WorkflowIntakeOutcome(
        workflow_request=request,
        validation_result=validation_result,
        request_digest=validation_result.request_digest,
        run_id=run_id,
        run_idempotency_key=request.request_id,
        route_identity=route_identity,
        authority_context=authority_context,
        admission_decision=admission_decision,
        admitted_definition_ref=request.workflow_definition_id,
        admitted_definition_hash=request.definition_hash,
        admission_state=RunState.CLAIM_ACCEPTED,
        committed_state=None,
        cancel_allowed=True,
        evidence_plan=(),
    )


def _assert_boundary_reason(
    outcome: WorkflowIntakeOutcome,
    reason_code: str,
) -> RuntimeBoundaryError:
    with pytest.raises(RuntimeBoundaryError) as exc_info:
        _execution_boundary_ref(intake_outcome=outcome)

    assert exc_info.value.reason_code == reason_code
    assert str(exc_info.value).startswith(reason_code)
    return exc_info.value


def test_execution_boundary_ref_returns_admitted_workspace_ref() -> None:
    assert _execution_boundary_ref(intake_outcome=_outcome()) == "workspace.alpha"


def test_runtime_boundary_error_preserves_existing_one_string_behavior() -> None:
    error = RuntimeBoundaryError("runtime.existing_boundary_error")

    assert str(error) == "runtime.existing_boundary_error"
    assert error.reason_code == "runtime.existing_boundary_error"
    assert error.details == {}


@pytest.mark.parametrize(
    ("outcome", "reason_code"),
    (
        (
            replace(
                _outcome(),
                authority_context=replace(
                    _outcome().authority_context,
                    context_bundle_id="",
                ),
            ),
            "runtime.execution_boundary_missing.context_bundle_id",
        ),
        (
            replace(
                _outcome(),
                authority_context=replace(_outcome().authority_context, bundle_hash=""),
            ),
            "runtime.execution_boundary_missing.bundle_hash",
        ),
        (
            replace(
                _outcome(),
                authority_context=replace(
                    _outcome().authority_context,
                    bundle_payload=None,
                ),
            ),
            "runtime.execution_boundary_missing.bundle_payload",
        ),
        (
            _outcome(route_ref="context.other"),
            "runtime.execution_boundary_authority_mismatch.context_ref",
        ),
        (
            _outcome(route_digest="sha256:other"),
            "runtime.execution_boundary_authority_mismatch.context_digest",
        ),
        (
            _outcome(bundle_hash="sha256:wrong", route_digest="sha256:wrong"),
            "runtime.execution_boundary_authority_mismatch.payload_hash",
        ),
        (
            _outcome(payload={}),
            "runtime.execution_boundary_authority_mismatch.workspace_payload_missing",
        ),
        (
            _outcome(payload={"workspace": "workspace.alpha"}),
            "runtime.execution_boundary_authority_mismatch.workspace_payload_type",
        ),
        (
            _outcome(payload={"workspace": {}}),
            "runtime.execution_boundary_authority_mismatch.admitted_workspace_ref_missing",
        ),
        (
            _outcome(workspace_ref=""),
            "runtime.execution_boundary_authority_mismatch.authority_workspace_ref_missing",
        ),
        (
            _outcome(workspace_ref="workspace.spoofed"),
            "runtime.execution_boundary_authority_mismatch.workspace_ref",
        ),
    ),
)
def test_execution_boundary_ref_surfaces_specific_reason_code(
    outcome: WorkflowIntakeOutcome,
    reason_code: str,
) -> None:
    _assert_boundary_reason(outcome, reason_code)


def test_execution_boundary_workspace_ref_mismatch_carries_both_refs() -> None:
    error = _assert_boundary_reason(
        _outcome(workspace_ref="workspace.spoofed"),
        "runtime.execution_boundary_authority_mismatch.workspace_ref",
    )

    assert error.details == {
        "workspace_ref": "workspace.spoofed",
        "admitted_workspace_ref": "workspace.alpha",
    }
