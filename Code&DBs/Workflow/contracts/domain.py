"""Workflow request contracts and validation helpers.

This module owns the intake request shape for the runtime. Supports
variable-length workflow graphs:

- one workflow request
- one or more deterministic task nodes
- zero or more typed edges wiring node dependencies (after_success,
  after_failure, after_any, conditional)
- one immutable admitted-definition binding
- one explicit validation outcome

The validator fails closed instead of inventing missing graph or authority
truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256
import json
from typing import Any

SUPPORTED_SCHEMA_VERSION = 1
MINIMAL_WORKFLOW_NODE_TYPE = "deterministic_task"
MINIMAL_WORKFLOW_EDGE_TYPE = "after_success"
_SUPPORTED_EDGE_TYPES = frozenset({
    "after_success",    # run if upstream succeeded
    "after_failure",    # run if upstream failed
    "after_any",        # run regardless of upstream status
    "conditional",      # run if upstream output matches a predicate
})
_SUPPORTED_ADAPTER_TYPES = frozenset({
    "deterministic_task",
    "llm_task",
    "cli_llm",
    "mcp_task",
    "api_task",
    "control_operator",
    "context_compiler",
    "output_parser",
    "file_writer",
    "verifier",
})
_SUPPORTED_CONDITIONAL_OPERATIONS = frozenset({
    "equals",
    "not_equals",
    "in",
    "not_in",
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "contains",
    "starts_with",
    "ends_with",
    "exists",
    "regex",
})
_SUPPORTED_DEPENDENCY_MODES = frozenset({"all", "any"})
_STATIC_CONTROL_OPERATOR_KINDS = frozenset({"if", "switch", "join_all", "join_any", "catch"})
_DYNAMIC_CONTROL_OPERATOR_KINDS = frozenset({"foreach", "batch", "repeat_until", "while"})
_SUPPORTED_CONTROL_OPERATOR_KINDS = (
    _STATIC_CONTROL_OPERATOR_KINDS | _DYNAMIC_CONTROL_OPERATOR_KINDS
)


class WorkflowContractError(RuntimeError):
    """Raised when a workflow contract cannot be represented safely."""


class WorkflowValidationError(WorkflowContractError):
    """Raised when a workflow request fails contract validation."""


@dataclass(frozen=True, slots=True)
class WorkflowNodeContract:
    """A typed node contract inside the admitted workflow definition."""

    node_id: str
    node_type: str
    adapter_type: str
    display_name: str
    inputs: Mapping[str, Any]
    expected_outputs: Mapping[str, Any]
    success_condition: Mapping[str, Any]
    failure_behavior: Mapping[str, Any]
    authority_requirements: Mapping[str, Any]
    execution_boundary: Mapping[str, Any]
    position_index: int
    template_owner_node_id: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowEdgeContract:
    """A typed dependency edge contract inside the admitted workflow."""

    edge_id: str
    edge_type: str
    from_node_id: str
    to_node_id: str
    release_condition: Mapping[str, Any]
    payload_mapping: Mapping[str, Any]
    position_index: int
    template_owner_node_id: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowRequest:
    """The first intake request shape for a workflow submission."""

    schema_version: int
    workflow_id: str
    request_id: str
    workflow_definition_id: str
    definition_hash: str
    workspace_ref: str
    runtime_profile_ref: str
    nodes: tuple[WorkflowNodeContract, ...]
    edges: tuple[WorkflowEdgeContract, ...]
    requested_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class WorkflowValidationResult:
    """Validation outcome for one workflow request."""

    request_id: str
    workflow_id: str
    schema_version: int
    request_digest: str
    is_valid: bool
    reason_code: str
    errors: tuple[str, ...]
    validation_result_ref: str
    normalized_request: WorkflowRequest | None


def _is_non_empty_text(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _sorted_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    return {key: mapping[key] for key in sorted(mapping)}


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _control_operator_spec(node: WorkflowNodeContract) -> Mapping[str, Any] | None:
    if node.adapter_type != "control_operator":
        return None
    operator = node.inputs.get("operator")
    if not isinstance(operator, Mapping):
        return None
    return operator


def _normalize_predicate(predicate: Mapping[str, Any]) -> Mapping[str, Any] | None:
    field = predicate.get("field")
    if not _is_non_empty_text(field):
        return None
    if "op" in predicate:
        op = predicate.get("op")
        value = predicate.get("value")
    elif "equals" in predicate:
        op = "equals"
        value = predicate.get("equals")
    else:
        return None
    if not _is_non_empty_text(op):
        return None
    normalized = {"field": str(field), "op": str(op), "value": value}
    if str(op) in {"in", "not_in"} and not isinstance(value, (list, tuple)):
        return None
    return normalized


def _invert_predicate(predicate: Mapping[str, Any]) -> Mapping[str, Any] | None:
    normalized = _normalize_predicate(predicate)
    if normalized is None:
        return None
    inverse = {
        "equals": "not_equals",
        "not_equals": "equals",
        "in": "not_in",
        "not_in": "in",
        "eq": "neq",
        "neq": "eq",
        "gt": "lte",
        "gte": "lt",
        "lt": "gte",
        "lte": "gt",
    }.get(str(normalized["op"]))
    if inverse is None:
        return None
    return {
        "field": normalized["field"],
        "op": inverse,
        "value": normalized["value"],
    }


def _merge_payload_mapping(
    inbound: Mapping[str, Any],
    outbound: Mapping[str, Any],
    *,
    context: str,
) -> Mapping[str, Any]:
    merged = dict(inbound)
    for target_key, source_key in outbound.items():
        if target_key in merged and merged[target_key] != source_key:
            raise WorkflowContractError(
                "request.graph_invalid:"
                f"payload target_key collision for {target_key!r} in {context}"
            )
        merged[target_key] = source_key
    return merged


def _derived_edge_id(*parts: str) -> str:
    return "__".join(part for part in parts if part)


def _lower_static_control_flow(request: WorkflowRequest) -> WorkflowRequest:
    node_lookup = {node.node_id: node for node in request.nodes}
    edges = list(request.edges)
    lowered_nodes: list[WorkflowNodeContract] = []
    node_overrides: dict[str, WorkflowNodeContract] = {}
    edges_to_remove: set[str] = set()
    derived_edges: list[WorkflowEdgeContract] = []

    for node in request.nodes:
        operator = _control_operator_spec(node)
        kind = str(operator.get("kind") or "").strip() if operator else ""
        if kind not in _STATIC_CONTROL_OPERATOR_KINDS:
            lowered_nodes.append(node)
            continue

        inbound = [edge for edge in edges if edge.to_node_id == node.node_id]
        outbound = [edge for edge in edges if edge.from_node_id == node.node_id]
        edges_to_remove.update(edge.edge_id for edge in inbound)
        edges_to_remove.update(edge.edge_id for edge in outbound)

        if kind in {"join_all", "join_any", "catch"}:
            derived_edge_type = {
                "join_all": "after_success",
                "join_any": "after_success",
                "catch": "after_failure",
            }[kind]
            for outbound_edge in outbound:
                child_node = node_lookup.get(outbound_edge.to_node_id)
                if child_node is None:
                    continue
                if kind in {"join_any", "catch"}:
                    dependency_mode = str(child_node.inputs.get("dependency_mode") or "all").strip()
                    if dependency_mode not in {"all", "any"}:
                        raise WorkflowContractError(
                            "request.graph_invalid:"
                            f"invalid dependency_mode for {kind}:{node.node_id}:{child_node.node_id}"
                        )
                    node_overrides[child_node.node_id] = replace(
                        child_node,
                        inputs={
                            **dict(child_node.inputs),
                            "dependency_mode": "any",
                        },
                    )
                for inbound_edge in inbound:
                    derived_edges.append(
                        WorkflowEdgeContract(
                            edge_id=_derived_edge_id(
                                node.node_id,
                                inbound_edge.edge_id,
                                outbound_edge.edge_id,
                            ),
                            edge_type=derived_edge_type,
                            from_node_id=inbound_edge.from_node_id,
                            to_node_id=outbound_edge.to_node_id,
                            release_condition={"kind": "always"},
                            payload_mapping=_merge_payload_mapping(
                                inbound_edge.payload_mapping,
                                outbound_edge.payload_mapping,
                                context=(
                                    f"join_all:{node.node_id}:"
                                    f"{inbound_edge.edge_id}:{outbound_edge.edge_id}"
                                ),
                            ),
                            position_index=-1,
                            template_owner_node_id=node.template_owner_node_id,
                        )
                    )
            continue

        if len(inbound) != 1:
            lowered_nodes.append(node)
            continue
        source_edge = inbound[0]
        predicate_map: dict[str, Mapping[str, Any]] = {}

        if kind == "if":
            predicate = _normalize_predicate(operator.get("predicate", {}))
            if predicate is None:
                lowered_nodes.append(node)
                continue
            predicate_map["then"] = predicate
            inverted = _invert_predicate(predicate)
            if inverted is not None:
                predicate_map["else"] = inverted
        elif kind == "switch":
            field = operator.get("field")
            if not _is_non_empty_text(field):
                lowered_nodes.append(node)
                continue
            for case in operator.get("cases", ()) or ():
                if not isinstance(case, Mapping):
                    continue
                branch = case.get("branch")
                if not _is_non_empty_text(branch):
                    continue
                value = case.get("value")
                predicate_map[str(branch)] = {
                    "field": str(field),
                    "op": "equals",
                    "value": value,
                }

        for outbound_edge in outbound:
            branch = str(outbound_edge.release_condition.get("branch") or "").strip()
            predicate = predicate_map.get(branch)
            if predicate is None:
                continue
            derived_edges.append(
                WorkflowEdgeContract(
                    edge_id=_derived_edge_id(node.node_id, outbound_edge.edge_id),
                    edge_type="conditional",
                    from_node_id=source_edge.from_node_id,
                    to_node_id=outbound_edge.to_node_id,
                    release_condition=predicate,
                    payload_mapping=_merge_payload_mapping(
                        source_edge.payload_mapping,
                        outbound_edge.payload_mapping,
                        context=(
                            f"{kind}:{node.node_id}:"
                            f"{source_edge.edge_id}:{outbound_edge.edge_id}"
                        ),
                    ),
                    position_index=-1,
                    template_owner_node_id=node.template_owner_node_id,
                )
            )

    remaining_edges = [edge for edge in edges if edge.edge_id not in edges_to_remove]
    remaining_edges.extend(derived_edges)
    normalized_nodes = tuple(
        replace(node_overrides.get(node.node_id, node), position_index=index)
        for index, node in enumerate(
            sorted(lowered_nodes, key=lambda item: (item.position_index, item.node_id))
        )
    )
    normalized_edges = tuple(
        replace(edge, position_index=index)
        for index, edge in enumerate(
            sorted(remaining_edges, key=lambda item: (item.position_index, item.edge_id))
        )
    )
    return replace(request, nodes=normalized_nodes, edges=normalized_edges)


def _request_payload(request: WorkflowRequest) -> dict[str, Any]:
    return {
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
                "inputs": _sorted_mapping(node.inputs),
                "expected_outputs": _sorted_mapping(node.expected_outputs),
                "success_condition": _sorted_mapping(node.success_condition),
                "failure_behavior": _sorted_mapping(node.failure_behavior),
                "authority_requirements": _sorted_mapping(node.authority_requirements),
                "execution_boundary": _sorted_mapping(node.execution_boundary),
                "position_index": node.position_index,
                "template_owner_node_id": node.template_owner_node_id,
            }
            for node in sorted(
                request.nodes,
                key=lambda item: (item.position_index, item.node_id),
            )
        ],
        "edges": [
            {
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "release_condition": _sorted_mapping(edge.release_condition),
                "payload_mapping": _sorted_mapping(edge.payload_mapping),
                "position_index": edge.position_index,
                "template_owner_node_id": edge.template_owner_node_id,
            }
            for edge in sorted(
                request.edges,
                key=lambda item: (item.position_index, item.edge_id),
            )
        ],
    }


def _safe_collection_signature(values: object) -> dict[str, Any]:
    """Return a JSON-safe shape signature for malformed request collections."""

    if isinstance(values, (tuple, list)):
        return {
            "kind": type(values).__name__,
            "length": len(values),
            "item_types": tuple(type(item).__name__ for item in values),
        }
    return {"kind": type(values).__name__}


def _safe_scalar(value: object) -> Any:
    """Return a JSON-safe scalar representation for fallback digests."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return f"{type(value).__module__}.{type(value).__qualname__}"


def _safe_request_payload(request: WorkflowRequest) -> dict[str, Any]:
    """Return a digest payload even when the request shape is malformed."""

    try:
        return _request_payload(request)
    except Exception:
        return {
            "schema_version": _safe_scalar(request.schema_version),
            "workflow_id": _safe_scalar(request.workflow_id),
            "request_id": _safe_scalar(request.request_id),
            "workflow_definition_id": _safe_scalar(request.workflow_definition_id),
            "definition_hash": _safe_scalar(request.definition_hash),
            "workspace_ref": _safe_scalar(request.workspace_ref),
            "runtime_profile_ref": _safe_scalar(request.runtime_profile_ref),
            "nodes": _safe_collection_signature(getattr(request, "nodes", None)),
            "edges": _safe_collection_signature(getattr(request, "edges", None)),
        }


def workflow_request_digest(request: WorkflowRequest) -> str:
    """Return a stable digest for the logical request content.

    `requested_at` is intentionally excluded so duplicate-submit handling can
    stay tied to the logical request shape rather than wall-clock noise.
    """

    payload = _safe_request_payload(request)
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return sha256(payload_json.encode("utf-8")).hexdigest()


def normalize_workflow_request(request: WorkflowRequest) -> WorkflowRequest:
    """Canonicalize node and edge ordering without changing the request facts."""

    lowered_request = _lower_static_control_flow(request)
    return replace(
        lowered_request,
        nodes=tuple(sorted(lowered_request.nodes, key=lambda item: (item.position_index, item.node_id))),
        edges=tuple(sorted(lowered_request.edges, key=lambda item: (item.position_index, item.edge_id))),
    )


def _invalid_result(
    *,
    request: WorkflowRequest,
    reason_code: str,
    errors: tuple[str, ...],
) -> WorkflowValidationResult:
    request_digest = workflow_request_digest(request)
    validation_result_ref = f"validation:{request_digest[:16]}"
    return WorkflowValidationResult(
        request_id=request.request_id,
        workflow_id=request.workflow_id,
        schema_version=request.schema_version,
        request_digest=request_digest,
        is_valid=False,
        reason_code=reason_code,
        errors=errors,
        validation_result_ref=validation_result_ref,
        normalized_request=None,
    )


def validate_workflow_request(request: WorkflowRequest) -> WorkflowValidationResult:
    """Validate a workflow request with variable-length graph support.

    The validator accepts graphs of any size that satisfy:

    - one supported schema version
    - one admitted workflow definition binding
    - at least one deterministic_task node
    - unique node_ids, sequential position_indexes starting from 0
    - edges reference valid node_ids
    - explicit authority requirements on all nodes
    """

    if request.schema_version != SUPPORTED_SCHEMA_VERSION:
        return _invalid_result(
            request=request,
            reason_code="request.version_unsupported",
            errors=("request.version_unsupported",),
        )

    required_text_fields = (
        request.workflow_id,
        request.request_id,
        request.workflow_definition_id,
        request.definition_hash,
        request.workspace_ref,
        request.runtime_profile_ref,
    )
    if not all(_is_non_empty_text(value) for value in required_text_fields):
        return _invalid_result(
            request=request,
            reason_code="request.schema_invalid",
            errors=("request.schema_invalid",),
        )

    try:
        normalized_request = normalize_workflow_request(request)
        nodes = normalized_request.nodes
        edges = normalized_request.edges
    except WorkflowContractError:
        return _invalid_result(
            request=request,
            reason_code="request.graph_invalid",
            errors=("request.graph_invalid",),
        )
    except (AttributeError, TypeError, ValueError):
        return _invalid_result(
            request=request,
            reason_code="request.schema_invalid",
            errors=("request.schema_invalid",),
        )

    node_ids = {node.node_id for node in nodes}
    node_position_indexes = [node.position_index for node in nodes]

    graph_errors: list[str] = []

    # Must have at least one node
    if len(nodes) < 1:
        graph_errors.append("request.graph_invalid")

    # Node IDs must be unique
    if len(node_ids) != len(nodes):
        graph_errors.append("request.graph_invalid")

    # Position indexes must be sequential from 0
    expected_positions = list(range(len(nodes)))
    if node_position_indexes != expected_positions:
        graph_errors.append("request.graph_invalid")

    # Edge IDs must be unique
    edge_ids = {edge.edge_id for edge in edges}
    if len(edge_ids) != len(edges):
        graph_errors.append("request.graph_invalid")

    dynamic_operator_ids: set[str] = set()
    template_nodes_by_owner: dict[str, list[WorkflowNodeContract]] = {}
    template_edges_by_owner: dict[str, list[WorkflowEdgeContract]] = {}

    # Edges must reference valid node IDs
    for edge in edges:
        if edge.from_node_id not in node_ids:
            graph_errors.append("request.graph_invalid")
        if edge.to_node_id not in node_ids:
            graph_errors.append("request.graph_invalid")

    for node in nodes:
        if node.node_type != MINIMAL_WORKFLOW_NODE_TYPE:
            graph_errors.append("request.graph_invalid")
        if node.adapter_type not in _SUPPORTED_ADAPTER_TYPES:
            graph_errors.append("request.graph_invalid")
        if not _is_non_empty_text(node.display_name):
            graph_errors.append("request.schema_invalid")
        if not isinstance(node.inputs, Mapping) or not isinstance(node.expected_outputs, Mapping):
            graph_errors.append("request.schema_invalid")
        if not isinstance(node.success_condition, Mapping) or not isinstance(node.failure_behavior, Mapping):
            graph_errors.append("request.schema_invalid")
        if not isinstance(node.authority_requirements, Mapping) or not isinstance(node.execution_boundary, Mapping):
            graph_errors.append("request.schema_invalid")
        dependency_mode = node.inputs.get("dependency_mode")
        if dependency_mode is not None and (
            not _is_non_empty_text(dependency_mode)
            or str(dependency_mode) not in _SUPPORTED_DEPENDENCY_MODES
        ):
            graph_errors.append("request.graph_invalid")
        if node.authority_requirements.get("workspace_ref") != request.workspace_ref:
            graph_errors.append("request.graph_invalid")
        if node.authority_requirements.get("runtime_profile_ref") != request.runtime_profile_ref:
            graph_errors.append("request.graph_invalid")
        if node.execution_boundary.get("workspace_ref") != request.workspace_ref:
            graph_errors.append("request.graph_invalid")
        if node.template_owner_node_id is not None:
            template_nodes_by_owner.setdefault(node.template_owner_node_id, []).append(node)
        if node.adapter_type == "control_operator":
            operator = _control_operator_spec(node)
            kind = str(operator.get("kind") or "").strip() if operator else ""
            if node.template_owner_node_id is not None:
                graph_errors.append("request.graph_invalid")
            if not operator or kind not in _SUPPORTED_CONTROL_OPERATOR_KINDS:
                graph_errors.append("request.graph_invalid")
            elif kind in _STATIC_CONTROL_OPERATOR_KINDS:
                graph_errors.append("request.graph_invalid")
            else:
                dynamic_operator_ids.add(node.node_id)
                if kind == "foreach":
                    source_ref = operator.get("source_ref")
                    if not isinstance(source_ref, Mapping):
                        graph_errors.append("request.graph_invalid")
                    else:
                        source_node_id = source_ref.get("from_node_id")
                        output_key = source_ref.get("output_key")
                        if (
                            not _is_non_empty_text(source_node_id)
                            or not _is_non_empty_text(output_key)
                            or str(source_node_id) not in node_ids
                            or str(source_node_id) == node.node_id
                        ):
                            graph_errors.append("request.graph_invalid")
                    if (
                        not isinstance(operator.get("max_items"), int)
                        or int(operator.get("max_items")) <= 0
                        or not isinstance(operator.get("max_parallel"), int)
                        or int(operator.get("max_parallel")) <= 0
                        or operator.get("aggregate_mode") != "ordered_results"
                        or not _is_non_empty_text(operator.get("result_key"))
                    ):
                        graph_errors.append("request.graph_invalid")
                elif kind == "batch":
                    source_ref = operator.get("source_ref")
                    if not isinstance(source_ref, Mapping):
                        graph_errors.append("request.graph_invalid")
                    else:
                        source_node_id = source_ref.get("from_node_id")
                        output_key = source_ref.get("output_key")
                        if (
                            not _is_non_empty_text(source_node_id)
                            or not _is_non_empty_text(output_key)
                            or str(source_node_id) not in node_ids
                            or str(source_node_id) == node.node_id
                        ):
                            graph_errors.append("request.graph_invalid")
                    if (
                        not isinstance(operator.get("batch_size"), int)
                        or int(operator.get("batch_size")) <= 0
                        or not isinstance(operator.get("max_batches"), int)
                        or int(operator.get("max_batches")) <= 0
                        or not isinstance(operator.get("max_parallel"), int)
                        or int(operator.get("max_parallel")) <= 0
                        or operator.get("aggregate_mode") != "ordered_results"
                        or not _is_non_empty_text(operator.get("result_key"))
                    ):
                        graph_errors.append("request.graph_invalid")
                elif kind in {"repeat_until", "while"}:
                    predicate = _normalize_predicate(operator.get("predicate", {}))
                    if (
                        not isinstance(operator.get("max_iterations"), int)
                        or int(operator.get("max_iterations")) <= 0
                        or predicate is None
                        or str(predicate["op"]) not in _SUPPORTED_CONDITIONAL_OPERATIONS
                        or operator.get("aggregate_mode") != "iteration_results"
                        or not _is_non_empty_text(operator.get("result_key"))
                    ):
                        graph_errors.append("request.graph_invalid")

    for edge in edges:
        if edge.edge_type not in _SUPPORTED_EDGE_TYPES:
            graph_errors.append("request.graph_invalid")
        if edge.edge_type == "conditional":
            normalized_predicate = _normalize_predicate(edge.release_condition)
            if (
                normalized_predicate is None
                or str(normalized_predicate["op"]) not in _SUPPORTED_CONDITIONAL_OPERATIONS
            ):
                graph_errors.append("request.graph_invalid")
        if not isinstance(edge.release_condition, Mapping) or not isinstance(edge.payload_mapping, Mapping):
            graph_errors.append("request.schema_invalid")
        if edge.template_owner_node_id is not None:
            template_edges_by_owner.setdefault(edge.template_owner_node_id, []).append(edge)

    template_node_ids = {
        node.node_id
        for owner_nodes in template_nodes_by_owner.values()
        for node in owner_nodes
    }
    for owner_id, owner_nodes in template_nodes_by_owner.items():
        if owner_id not in dynamic_operator_ids:
            graph_errors.append("request.graph_invalid")
            continue
        owner_edge_rows = template_edges_by_owner.get(owner_id, [])
        owner_node_ids = {node.node_id for node in owner_nodes}
        sink_nodes = {
            node.node_id
            for node in owner_nodes
            if not any(edge.from_node_id == node.node_id for edge in owner_edge_rows)
        }
        if len(sink_nodes) != 1:
            graph_errors.append("request.graph_invalid")
        for edge in owner_edge_rows:
            if edge.from_node_id not in owner_node_ids or edge.to_node_id not in owner_node_ids:
                graph_errors.append("request.graph_invalid")

    for operator_id in dynamic_operator_ids:
        if operator_id not in template_nodes_by_owner:
            graph_errors.append("request.graph_invalid")
            continue
        operator_node = next((node for node in nodes if node.node_id == operator_id), None)
        operator_spec = _control_operator_spec(operator_node) if operator_node else None
        if operator_spec and str(operator_spec.get("kind") or "") == "foreach":
            source_ref = operator_spec.get("source_ref")
            source_node_id = source_ref.get("from_node_id") if isinstance(source_ref, Mapping) else None
            if source_node_id in template_node_ids:
                graph_errors.append("request.graph_invalid")

    for edge in edges:
        from_is_template = edge.from_node_id in template_node_ids
        to_is_template = edge.to_node_id in template_node_ids
        if edge.template_owner_node_id is None and (from_is_template or to_is_template):
            graph_errors.append("request.graph_invalid")
        if edge.template_owner_node_id is not None and (not from_is_template or not to_is_template):
            graph_errors.append("request.graph_invalid")

    if graph_errors:
        # Preserve order while de-duplicating repeated graph failures.
        deduped_errors = tuple(dict.fromkeys(graph_errors))
        return _invalid_result(
            request=request,
            reason_code=deduped_errors[0],
            errors=deduped_errors,
        )

    request_digest = workflow_request_digest(normalized_request)
    validation_result_ref = f"validation:{request_digest[:16]}"
    return WorkflowValidationResult(
        request_id=request.request_id,
        workflow_id=request.workflow_id,
        schema_version=request.schema_version,
        request_digest=request_digest,
        is_valid=True,
        reason_code="request.valid",
        errors=(),
        validation_result_ref=validation_result_ref,
        normalized_request=normalized_request,
    )


__all__ = [
    "MINIMAL_WORKFLOW_EDGE_TYPE",
    "MINIMAL_WORKFLOW_NODE_TYPE",
    "SUPPORTED_SCHEMA_VERSION",
    "_SUPPORTED_EDGE_TYPES",
    "WorkflowContractError",
    "WorkflowEdgeContract",
    "WorkflowNodeContract",
    "WorkflowRequest",
    "WorkflowValidationError",
    "WorkflowValidationResult",
    "normalize_workflow_request",
    "validate_workflow_request",
    "workflow_request_digest",
]
