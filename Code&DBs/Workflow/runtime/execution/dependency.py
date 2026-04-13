"""Dependency resolution engine for DAG execution."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest

from ..domain import RouteIdentity, RunState


@dataclass(frozen=True, slots=True)
class _DependencyResolution:
    """Deterministic dependency evaluation for one pending node."""

    state: str
    dependency_inputs: Mapping[str, Any]
    waiting_on_node_ids: tuple[str, ...] = ()
    reason_code: str | None = None
    reason_details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class _FailureReason:
    """Typed runtime failure bundle persisted into the terminal proof."""

    reason_code: str
    details: Mapping[str, Any]


@dataclass(slots=True)
class _ExecutionCursor:
    """In-memory transition/evidence cursor derived only by runtime."""

    route_identity: RouteIdentity
    transition_seq: int = 1
    next_evidence_seq: int = 1
    current_state: RunState = RunState.CLAIM_RECEIVED

    def identity_for_current_transition(self) -> RouteIdentity:
        return replace(self.route_identity, transition_seq=self.transition_seq)

    def advance(self, *, result: Any, new_state: RunState | None = None) -> None:
        """Advance the cursor after a transition proof is written.

        Args:
            result: EvidenceCommitResult from the proof writer
            new_state: Optional new state to transition to
        """
        self.transition_seq += 1
        self.next_evidence_seq = result.evidence_seq + 1
        if new_state is not None:
            self.current_state = new_state


def _edge_should_fire(
    edge: WorkflowEdgeContract,
    upstream: Any,  # NodeExecutionRecord
) -> tuple[str, Mapping[str, Any] | None]:
    """Determine whether an edge should fire based on its type and upstream status."""
    edge_type = edge.edge_type

    if edge_type == "after_success":
        return ("fire", None) if upstream.status == "succeeded" else ("no_fire", None)

    if edge_type == "after_failure":
        return ("fire", None) if upstream.status == "failed" else ("no_fire", None)

    if edge_type == "after_any":
        return (
            ("fire", None)
            if upstream.status in {"succeeded", "failed", "skipped"}
            else ("no_fire", None)
        )

    if edge_type == "conditional":
        if upstream.status != "succeeded":
            return "no_fire", None
        from runtime.condition_evaluator import evaluate_condition_tree
        try:
            result = evaluate_condition_tree(dict(upstream.outputs), dict(edge.release_condition))
        except Exception:
            return (
                "unsupported",
                {
                    "edge_id": edge.edge_id,
                    "edge_type": edge.edge_type,
                    "reason": "condition evaluation error",
                },
            )
        return ("fire", None) if result else ("no_fire", None)

    # Unknown edge type — do not fire.
    return (
        "unsupported",
        {
            "edge_id": edge.edge_id,
            "edge_type": edge.edge_type,
            "reason": "unsupported edge_type",
        },
    )


def resolve_dependencies(
    *,
    node: WorkflowNodeContract,
    inbound_edges: Sequence[WorkflowEdgeContract],
    completed_nodes: Mapping[str, Any],  # Mapping[str, NodeExecutionRecord]
) -> _DependencyResolution:
    """Resolve dependencies for a pending node.

    Args:
        node: The workflow node to resolve dependencies for
        inbound_edges: Edges leading into this node
        completed_nodes: Map of node_id -> NodeExecutionRecord for completed nodes

    Returns:
        _DependencyResolution with state and dependency inputs
    """
    dependency_inputs: dict[str, Any] = {}
    dependency_sources: dict[str, dict[str, Any]] = {}
    waiting_on_node_ids: list[str] = []
    fired_edges = 0
    total_edges = len(inbound_edges)
    dependency_mode = str(node.inputs.get("dependency_mode") or "all").strip().lower()
    for edge in inbound_edges:
        upstream = completed_nodes.get(edge.from_node_id)
        if upstream is None:
            waiting_on_node_ids.append(edge.from_node_id)
            continue

        edge_state, edge_details = _edge_should_fire(edge, upstream)
        if edge_state == "unsupported":
            return _DependencyResolution(
                state="stalled",
                dependency_inputs={},
                reason_code="runtime.dependency_edge_not_satisfied",
                reason_details={
                    "node_id": node.node_id,
                    "edge_id": edge.edge_id,
                    "edge_type": edge.edge_type,
                    "from_node_id": edge.from_node_id,
                    "upstream_status": upstream.status,
                    "upstream_failure_code": upstream.failure_code,
                    "edge_details": dict(edge_details or {}),
                },
            )
        if edge_state == "no_fire":
            continue
        fired_edges += 1

        # For after_failure and after_any with a failed upstream, skip payload
        # mapping since outputs may not be populated.
        if upstream.status == "succeeded":
            if edge.payload_mapping:
                for target_key, source_key in sorted(edge.payload_mapping.items()):
                    if source_key not in upstream.outputs:
                        return _DependencyResolution(
                            state="stalled",
                            dependency_inputs={},
                            reason_code="runtime.dependency_missing_output",
                            reason_details={
                                "node_id": node.node_id,
                                "edge_id": edge.edge_id,
                                "from_node_id": edge.from_node_id,
                                "target_key": target_key,
                                "source_key": source_key,
                                "available_output_keys": tuple(sorted(upstream.outputs)),
                            },
                        )
                    if target_key in dependency_sources:
                        previous = dependency_sources[target_key]
                        return _DependencyResolution(
                            state="stalled",
                            dependency_inputs={},
                            reason_code="runtime.dependency_target_key_collision",
                            reason_details={
                                "node_id": node.node_id,
                                "target_key": target_key,
                                "existing_edge_id": previous["edge_id"],
                                "existing_from_node_id": previous["from_node_id"],
                                "existing_source_key": previous["source_key"],
                                "incoming_edge_id": edge.edge_id,
                                "incoming_from_node_id": edge.from_node_id,
                                "incoming_source_key": source_key,
                            },
                        )
                    dependency_inputs[target_key] = upstream.outputs[source_key]
                    dependency_sources[target_key] = {
                        "edge_id": edge.edge_id,
                        "from_node_id": edge.from_node_id,
                        "source_key": source_key,
                    }
            else:
                target_key = edge.from_node_id
                if target_key in dependency_sources:
                    previous = dependency_sources[target_key]
                    return _DependencyResolution(
                        state="stalled",
                        dependency_inputs={},
                        reason_code="runtime.dependency_target_key_collision",
                        reason_details={
                            "node_id": node.node_id,
                            "target_key": target_key,
                            "existing_edge_id": previous["edge_id"],
                            "existing_from_node_id": previous["from_node_id"],
                            "existing_source_key": previous["source_key"],
                            "incoming_edge_id": edge.edge_id,
                            "incoming_from_node_id": edge.from_node_id,
                            "incoming_source_key": "*",
                        },
                    )
                dependency_inputs[edge.from_node_id] = dict(upstream.outputs)
                dependency_sources[target_key] = {
                    "edge_id": edge.edge_id,
                    "from_node_id": edge.from_node_id,
                    "source_key": "*",
                }

    if waiting_on_node_ids:
        return _DependencyResolution(
            state="waiting",
            dependency_inputs={},
            waiting_on_node_ids=tuple(sorted(set(waiting_on_node_ids))),
        )
    if total_edges == 0:
        return _DependencyResolution(
            state="ready",
            dependency_inputs=dependency_inputs,
        )
    if dependency_mode == "any":
        if fired_edges > 0:
            return _DependencyResolution(
                state="ready",
                dependency_inputs=dependency_inputs,
            )
        return _DependencyResolution(
            state="skipped",
            dependency_inputs={},
            reason_code="runtime.dependency_path_not_selected",
            reason_details={"node_id": node.node_id, "dependency_mode": dependency_mode},
        )
    if fired_edges == total_edges:
        return _DependencyResolution(
            state="ready",
            dependency_inputs=dependency_inputs,
        )
    if fired_edges < total_edges:
        return _DependencyResolution(
            state="skipped",
            dependency_inputs={},
            reason_code="runtime.dependency_path_not_selected",
            reason_details={"node_id": node.node_id, "dependency_mode": dependency_mode},
        )
    return _DependencyResolution(
        state="ready",
        dependency_inputs=dependency_inputs,
    )


def inbound_edges(
    *,
    request: WorkflowRequest,
) -> dict[str, list[WorkflowEdgeContract]]:
    """Build map of node_id -> list of inbound edges from workflow request."""
    inbound: dict[str, list[WorkflowEdgeContract]] = defaultdict(list)
    for edge in request.edges:
        inbound[edge.to_node_id].append(edge)
    for edges in inbound.values():
        edges.sort(key=lambda item: (item.position_index, item.edge_id))
    return inbound


def node_order(request: WorkflowRequest) -> tuple[WorkflowNodeContract, ...]:
    """Return nodes sorted by position_index and node_id."""
    return tuple(sorted(request.nodes, key=lambda item: (item.position_index, item.node_id)))


def frontier_failure_reason(
    *,
    pending_nodes: Mapping[str, WorkflowNodeContract],
    inbound_edges_map: Mapping[str, Sequence[WorkflowEdgeContract]],
    completed_nodes: Mapping[str, Any],  # Mapping[str, NodeExecutionRecord]
) -> _FailureReason:
    """Determine the failure reason when the frontier cannot make progress."""
    blocked_nodes: list[dict[str, Any]] = []
    for node in sorted(pending_nodes.values(), key=lambda item: (item.position_index, item.node_id)):
        resolution = resolve_dependencies(
            node=node,
            inbound_edges=inbound_edges_map.get(node.node_id, ()),
            completed_nodes=completed_nodes,
        )
        if resolution.state == "stalled" and resolution.reason_code is not None:
            details = dict(resolution.reason_details or {})
            details.update(
                {
                    "pending_node_ids": tuple(sorted(pending_nodes)),
                    "completed_node_ids": tuple(sorted(completed_nodes)),
                }
            )
            return _FailureReason(
                reason_code=resolution.reason_code,
                details=details,
            )
        blocked_nodes.append(
            {
                "node_id": node.node_id,
                "waiting_on_node_ids": resolution.waiting_on_node_ids,
                "inbound_edge_ids": tuple(
                    edge.edge_id for edge in inbound_edges_map.get(node.node_id, ())
                ),
            }
        )
    return _FailureReason(
        reason_code="runtime.frontier_no_progress",
        details={
            "pending_node_ids": tuple(sorted(pending_nodes)),
            "completed_node_ids": tuple(sorted(completed_nodes)),
            "blocked_nodes": tuple(blocked_nodes),
        },
    )


__all__ = [
    "_DependencyResolution",
    "_ExecutionCursor",
    "_FailureReason",
    "_edge_should_fire",
    "frontier_failure_reason",
    "inbound_edges",
    "node_order",
    "resolve_dependencies",
]
