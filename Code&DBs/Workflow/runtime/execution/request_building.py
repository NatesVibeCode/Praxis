"""Request-building helpers: payload serialization, boundary verification, and task request construction."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from adapters import DeterministicTaskRequest
from contracts.domain import WorkflowNodeContract, WorkflowRequest

from runtime._helpers import _json_compatible
from runtime.crypto_authority import canonical_digest_hex
from ..domain import RuntimeBoundaryError
from ..intake import WorkflowIntakeOutcome


def _workflow_request_payload(request: WorkflowRequest) -> dict[str, Any]:
    payload = {
        "schema_version": request.schema_version,
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": request.workflow_definition_id,
        "definition_hash": request.definition_hash,
        "workspace_ref": request.workspace_ref,
        "runtime_profile_ref": request.runtime_profile_ref,
        "nodes": [
            {
                "node_id": node.node_id,
                "node_type": node.node_type,
                "adapter_type": node.adapter_type,
                "display_name": node.display_name,
                "inputs": dict(node.inputs),
                "expected_outputs": dict(node.expected_outputs),
                "success_condition": dict(node.success_condition),
                "failure_behavior": dict(node.failure_behavior),
                "authority_requirements": dict(node.authority_requirements),
                "execution_boundary": dict(node.execution_boundary),
                "position_index": node.position_index,
                "template_owner_node_id": node.template_owner_node_id,
            }
            for node in request.nodes
        ],
        "edges": [
            {
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "release_condition": dict(edge.release_condition),
                "payload_mapping": dict(edge.payload_mapping),
                "position_index": edge.position_index,
                "template_owner_node_id": edge.template_owner_node_id,
            }
            for edge in request.edges
        ],
    }
    return _json_compatible(payload)  # type: ignore[return-value]


def _authority_payload_hash(payload: Mapping[str, Any]) -> str:
    return canonical_digest_hex(payload, purpose="execution_boundary.authority_payload")


def _execution_boundary_ref(*, intake_outcome: WorkflowIntakeOutcome) -> str:
    authority_context = intake_outcome.authority_context
    route_identity = intake_outcome.route_identity
    context_bundle_id = getattr(authority_context, "context_bundle_id", "")
    bundle_hash = getattr(authority_context, "bundle_hash", "")
    bundle_payload = getattr(authority_context, "bundle_payload", None)
    workspace_ref = getattr(authority_context, "workspace_ref", "")
    if not isinstance(context_bundle_id, str) or not context_bundle_id.strip():
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_missing.context_bundle_id",
            "authority context bundle id is missing",
            details={"value_type": type(context_bundle_id).__name__},
        )
    if not isinstance(bundle_hash, str) or not bundle_hash.strip():
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_missing.bundle_hash",
            "authority context bundle hash is missing",
            details={"value_type": type(bundle_hash).__name__},
        )
    if not isinstance(bundle_payload, Mapping):
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_missing.bundle_payload",
            "authority context bundle payload must be a mapping",
            details={"value_type": type(bundle_payload).__name__},
        )
    if route_identity.authority_context_ref != context_bundle_id:
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.context_ref",
            "route authority context ref does not match admitted context bundle",
            details={
                "route_authority_context_ref": route_identity.authority_context_ref,
                "context_bundle_id": context_bundle_id,
            },
        )
    if route_identity.authority_context_digest != bundle_hash:
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.context_digest",
            "route authority context digest does not match admitted bundle hash",
            details={
                "route_authority_context_digest": route_identity.authority_context_digest,
                "bundle_hash": bundle_hash,
            },
        )
    computed_hash = _authority_payload_hash(bundle_payload)
    if computed_hash != bundle_hash:
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.payload_hash",
            "authority context payload hash does not match admitted bundle hash",
            details={
                "computed_payload_hash": computed_hash,
                "bundle_hash": bundle_hash,
            },
        )
    workspace_payload = bundle_payload.get("workspace")
    if "workspace" not in bundle_payload:
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.workspace_payload_missing",
            "authority context bundle payload is missing workspace admission data",
        )
    if not isinstance(workspace_payload, Mapping):
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.workspace_payload_type",
            "authority context workspace payload must be a mapping",
            details={"value_type": type(workspace_payload).__name__},
        )
    admitted_workspace_ref = workspace_payload.get("workspace_ref")
    if not isinstance(admitted_workspace_ref, str) or not admitted_workspace_ref.strip():
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.admitted_workspace_ref_missing",
            "admitted workspace ref is missing from authority context bundle",
            details={"value_type": type(admitted_workspace_ref).__name__},
        )
    if not isinstance(workspace_ref, str) or not workspace_ref.strip():
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.authority_workspace_ref_missing",
            "authority context workspace ref is missing",
            details={"value_type": type(workspace_ref).__name__},
        )
    if workspace_ref != admitted_workspace_ref:
        raise RuntimeBoundaryError(
            "runtime.execution_boundary_authority_mismatch.workspace_ref",
            "authority context workspace ref does not match admitted workspace ref",
            details={
                "workspace_ref": workspace_ref,
                "admitted_workspace_ref": admitted_workspace_ref,
            },
        )
    return admitted_workspace_ref


def _task_request(
    *,
    execution_boundary_ref: str,
    node: WorkflowNodeContract,
    dependency_inputs: Mapping[str, Any],
) -> DeterministicTaskRequest:
    task_name = node.inputs.get("task_name")
    if not isinstance(task_name, str):
        task_name = ""
    input_payload = node.inputs.get("input_payload")
    if isinstance(input_payload, Mapping):
        normalized_input_payload = dict(input_payload)
    else:
        normalized_input_payload = {
            key: value
            for key, value in node.inputs.items()
            if key != "task_name"
        }
    return DeterministicTaskRequest(
        node_id=node.node_id,
        task_name=task_name or node.display_name,
        input_payload=normalized_input_payload,
        expected_outputs=dict(node.expected_outputs),
        dependency_inputs=dict(dependency_inputs),
        execution_boundary_ref=execution_boundary_ref,
    )


def _inject_context_compiler_runtime_metadata(
    *,
    node: WorkflowNodeContract,
    intake_outcome: WorkflowIntakeOutcome,
    request: WorkflowRequest,
) -> WorkflowNodeContract:
    if node.adapter_type != "context_compiler":
        return node

    authority_context = intake_outcome.authority_context
    updated_inputs = dict(node.inputs)
    updated_inputs["shadow_packet_runtime"] = {
        "admission_decision_id": intake_outcome.admission_decision.admission_decision_id,
        "authority_context_digest": intake_outcome.route_identity.authority_context_digest,
        "authority_context_ref": intake_outcome.route_identity.authority_context_ref,
        "context_bundle_id": getattr(authority_context, "context_bundle_id", ""),
        "context_bundle_hash": getattr(authority_context, "bundle_hash", ""),
        "context_bundle_payload": dict(getattr(authority_context, "bundle_payload", {}) or {}),
        "definition_hash": intake_outcome.admitted_definition_hash or request.definition_hash,
        "request_id": request.request_id,
        "run_id": intake_outcome.run_id,
        "runtime_profile_ref": getattr(authority_context, "runtime_profile_ref", ""),
        "source_decision_refs": list(getattr(authority_context, "source_decision_refs", ()) or ()),
        "validation_result_ref": intake_outcome.validation_result.validation_result_ref,
        "workflow_definition_id": intake_outcome.admitted_definition_ref or request.workflow_definition_id,
        "workflow_id": request.workflow_id,
        "workspace_ref": getattr(authority_context, "workspace_ref", ""),
    }
    return replace(node, inputs=updated_inputs)


__all__ = [
    "_authority_payload_hash",
    "_execution_boundary_ref",
    "_inject_context_compiler_runtime_metadata",
    "_task_request",
    "_workflow_request_payload",
]
