from __future__ import annotations

from receipts import AppendOnlyWorkflowEvidenceWriter
from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from policy.domain import AdmissionDecisionKind
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    UnresolvedAuthorityContext,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator
from runtime.domain import RunState
from runtime.intake import WorkflowIntakePlanner


def _valid_request() -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=(
            WorkflowNodeContract(
                node_id="node_0",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="prepare",
                inputs={"task_name": "prepare"},
                expected_outputs={"result": "prepared"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=0,
            ),
            WorkflowNodeContract(
                node_id="node_1",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="admit",
                inputs={"task_name": "admit"},
                expected_outputs={"result": "admitted"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=1,
            ),
        ),
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_1",
                release_condition={"upstream_result": "success"},
                payload_mapping={},
                position_index=0,
            ),
        ),
    )


class RecordingRegistryResolver(RegistryResolver):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.workspace_calls = 0
        self.runtime_profile_calls = 0
        self.context_bundle_calls = 0
        self.unresolved_context_calls = 0

    def resolve_workspace(self, *, workspace_ref: str):
        self.workspace_calls += 1
        return super().resolve_workspace(workspace_ref=workspace_ref)

    def resolve_runtime_profile(self, *, runtime_profile_ref: str):
        self.runtime_profile_calls += 1
        return super().resolve_runtime_profile(runtime_profile_ref=runtime_profile_ref)

    def resolve_context_bundle(self, **kwargs):
        self.context_bundle_calls += 1
        return super().resolve_context_bundle(**kwargs)

    def build_unresolved_context(self, **kwargs):
        self.unresolved_context_calls += 1
        return super().build_unresolved_context(**kwargs)


def _resolver() -> RecordingRegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RecordingRegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                ),
            ),
        },
    )


def _request_payload(request: WorkflowRequest) -> dict[str, object]:
    return {
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": request.workflow_definition_id,
        "definition_hash": request.definition_hash,
        "workspace_ref": request.workspace_ref,
        "runtime_profile_ref": request.runtime_profile_ref,
    }


def test_valid_request_admits_and_freezes_the_definition_binding() -> None:
    resolver = _resolver()
    planner = WorkflowIntakePlanner(registry=resolver)
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    outcome = planner.plan(request=_valid_request())
    submission_result = orchestrator.submit_run(
        route_identity=outcome.route_identity,
        admitted_definition_ref=outcome.admitted_definition_ref or "",
        admitted_definition_hash=outcome.admitted_definition_hash or "",
        request_payload=_request_payload(outcome.workflow_request),
        evidence_writer=writer,
    )

    assert outcome.validation_result.is_valid is True
    assert outcome.admission_decision.decision is AdmissionDecisionKind.ADMIT
    assert outcome.admission_decision.reason_code == "policy.admission_allowed"
    assert outcome.admitted_definition_ref == "workflow_definition.alpha.v1"
    assert outcome.admitted_definition_hash == "sha256:1111222233334444"
    assert outcome.authority_context is not None
    assert outcome.route_identity.authority_context_ref == outcome.authority_context.context_bundle_id
    assert outcome.route_identity.authority_context_digest == outcome.authority_context.bundle_hash
    assert outcome.route_identity.transition_seq == 0
    assert outcome.admission_state is RunState.CLAIM_ACCEPTED
    assert outcome.current_state is RunState.CLAIM_ACCEPTED
    assert outcome.committed_state is None
    assert outcome.cancel_allowed is False
    assert [step.transition_seq for step in outcome.evidence_plan] == [None, None]
    assert [step.evidence_seq for step in outcome.evidence_plan] == [None, None]
    assert outcome.evidence_plan[-1].to_state is RunState.CLAIM_ACCEPTED
    assert outcome.evidence_plan[-1].event_type == "workflow_admitted"
    assert outcome.evidence_plan[-1].receipt_type == "workflow_admission_receipt"
    assert submission_result.evidence_seq == 2
    assert writer.evidence_timeline(outcome.run_id)[0].record.route_identity.transition_seq == 1
    assert resolver.workspace_calls == 1
    assert resolver.runtime_profile_calls == 1
    assert resolver.context_bundle_calls == 1
    assert resolver.unresolved_context_calls == 0


def test_malformed_request_fails_closed_before_registry_resolution() -> None:
    resolver = _resolver()
    planner = WorkflowIntakePlanner(registry=resolver)
    valid_request = _valid_request()
    malformed_request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=None,
        edges=valid_request.edges,
    )

    outcome = planner.plan(request=malformed_request)

    assert outcome.validation_result.is_valid is False
    assert outcome.admission_decision.decision is AdmissionDecisionKind.REJECT
    assert outcome.admission_decision.reason_code == "request.schema_invalid"
    assert outcome.admitted_definition_ref is None
    assert isinstance(outcome.authority_context, UnresolvedAuthorityContext)
    assert outcome.authority_context.unresolved_reason_code == "request.schema_invalid"
    assert outcome.route_identity.authority_context_ref == outcome.authority_context.context_bundle_id
    assert outcome.route_identity.authority_context_digest == outcome.authority_context.bundle_hash
    assert outcome.route_identity.transition_seq == 0
    assert outcome.admission_state is RunState.CLAIM_REJECTED
    assert outcome.current_state is RunState.CLAIM_REJECTED
    assert outcome.committed_state is None
    assert outcome.cancel_allowed is False
    assert resolver.workspace_calls == 0
    assert resolver.runtime_profile_calls == 0
    assert resolver.context_bundle_calls == 0
    assert resolver.unresolved_context_calls == 1


def test_invalid_request_rejects_before_registry_resolution() -> None:
    resolver = _resolver()
    planner = WorkflowIntakePlanner(registry=resolver)
    valid_request = _valid_request()
    invalid_request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=valid_request.nodes,
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_missing",
                release_condition={"upstream_result": "success"},
                payload_mapping={},
                position_index=0,
            ),
        ),
    )

    outcome = planner.plan(request=invalid_request)

    assert outcome.validation_result.is_valid is False
    assert outcome.admission_decision.decision is AdmissionDecisionKind.REJECT
    assert outcome.admission_decision.reason_code == "request.graph_invalid"
    assert isinstance(outcome.authority_context, UnresolvedAuthorityContext)
    assert outcome.authority_context.unresolved_reason_code == "request.graph_invalid"
    assert outcome.route_identity.authority_context_ref == outcome.authority_context.context_bundle_id
    assert outcome.route_identity.authority_context_digest == outcome.authority_context.bundle_hash
    assert outcome.route_identity.transition_seq == 0
    assert outcome.admission_state is RunState.CLAIM_REJECTED
    assert outcome.current_state is RunState.CLAIM_REJECTED
    assert outcome.committed_state is None
    assert outcome.cancel_allowed is False
    assert outcome.evidence_plan[-1].to_state is RunState.CLAIM_REJECTED
    assert resolver.workspace_calls == 0
    assert resolver.runtime_profile_calls == 0
    assert resolver.context_bundle_calls == 0
    assert resolver.unresolved_context_calls == 1


def test_missing_authority_fails_closed() -> None:
    runtime_profile_ref = "runtime_profile.alpha"
    resolver = RecordingRegistryResolver(
        workspace_records={},
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                ),
            ),
        },
    )
    planner = WorkflowIntakePlanner(registry=resolver)

    outcome = planner.plan(request=_valid_request())

    assert outcome.validation_result.is_valid is True
    assert outcome.admission_decision.decision is AdmissionDecisionKind.REJECT
    assert outcome.admission_decision.reason_code == "registry.workspace_unknown"
    assert isinstance(outcome.authority_context, UnresolvedAuthorityContext)
    assert outcome.authority_context.unresolved_reason_code == "registry.workspace_unknown"
    assert outcome.route_identity.authority_context_ref == outcome.authority_context.context_bundle_id
    assert outcome.route_identity.authority_context_digest == outcome.authority_context.bundle_hash
    assert outcome.route_identity.transition_seq == 0
    assert outcome.admission_state is RunState.CLAIM_REJECTED
    assert outcome.current_state is RunState.CLAIM_REJECTED
    assert outcome.committed_state is None
    assert outcome.cancel_allowed is False
    assert resolver.workspace_calls == 1
    assert resolver.runtime_profile_calls == 0
    assert resolver.context_bundle_calls == 0
    assert resolver.unresolved_context_calls == 1
