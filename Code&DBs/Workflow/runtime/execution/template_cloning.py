"""Template graph cloning helpers for control operator expansion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest
from observability.read_models import OperatorFrameReadModel

from ..control_operator_frames import RunOperatorFrame


def _operator_frame_payload(frame: RunOperatorFrame) -> dict[str, Any]:
    return {
        "operator_frame_id": frame.operator_frame_id,
        "run_id": frame.run_id,
        "node_id": frame.node_id,
        "operator_kind": frame.operator_kind,
        "frame_state": frame.frame_state,
        "item_index": frame.item_index,
        "iteration_index": frame.iteration_index,
        "source_snapshot": dict(frame.source_snapshot),
        "aggregate_outputs": dict(frame.aggregate_outputs),
        "active_count": frame.active_count,
        "stop_reason": frame.stop_reason,
        "started_at": frame.started_at.isoformat(),
        "finished_at": frame.finished_at.isoformat() if frame.finished_at else None,
    }


def _operator_frame_read_model(frame: RunOperatorFrame) -> OperatorFrameReadModel:
    return OperatorFrameReadModel(
        operator_frame_id=frame.operator_frame_id,
        node_id=frame.node_id,
        operator_kind=frame.operator_kind,
        frame_state=frame.frame_state,
        item_index=frame.item_index,
        iteration_index=frame.iteration_index,
        source_snapshot=dict(frame.source_snapshot),
        aggregate_outputs=dict(frame.aggregate_outputs),
        active_count=frame.active_count,
        stop_reason=frame.stop_reason,
        started_at=frame.started_at,
        finished_at=frame.finished_at,
    )


def _template_graph(
    request: WorkflowRequest,
    *,
    operator_node_id: str,
) -> tuple[tuple[WorkflowNodeContract, ...], tuple[WorkflowEdgeContract, ...]]:
    nodes = tuple(
        node
        for node in request.nodes
        if node.template_owner_node_id == operator_node_id
    )
    edges = tuple(
        edge
        for edge in request.edges
        if edge.template_owner_node_id == operator_node_id
    )
    return nodes, edges


def _template_terminal_node_id(
    template_nodes: Sequence[WorkflowNodeContract],
    template_edges: Sequence[WorkflowEdgeContract],
) -> str | None:
    if not template_nodes:
        return None
    outbound = {edge.from_node_id for edge in template_edges}
    sinks = [
        node.node_id
        for node in sorted(template_nodes, key=lambda item: (item.position_index, item.node_id))
        if node.node_id not in outbound
    ]
    return sinks[0] if len(sinks) == 1 else None


def _clone_template_graph(
    *,
    operator_node_id: str,
    frame: RunOperatorFrame,
    template_nodes: Sequence[WorkflowNodeContract],
    template_edges: Sequence[WorkflowEdgeContract],
    injected_payload: Mapping[str, Any],
) -> tuple[dict[str, WorkflowNodeContract], tuple[WorkflowEdgeContract, ...], str | None]:
    node_id_map = {
        node.node_id: f"{operator_node_id}__{frame.operator_frame_id}__{node.node_id}"
        for node in template_nodes
    }
    cloned_nodes: dict[str, WorkflowNodeContract] = {}
    for node in template_nodes:
        updated_inputs = dict(node.inputs)
        inner_payload = updated_inputs.get("input_payload")
        if isinstance(inner_payload, Mapping):
            merged_payload = dict(inner_payload)
        else:
            merged_payload = {}
        merged_payload.update(injected_payload)
        updated_inputs["input_payload"] = merged_payload
        updated_inputs["_operator_lineage"] = {
            "operator_frame_id": frame.operator_frame_id,
            "logical_parent_node_id": node.node_id,
            "iteration_index": (
                frame.iteration_index
                if frame.iteration_index is not None
                else frame.item_index
            ),
        }
        cloned_nodes[node_id_map[node.node_id]] = replace(
            node,
            node_id=node_id_map[node.node_id],
            display_name=f"{node.display_name} [{frame.operator_frame_id}]",
            inputs=updated_inputs,
            template_owner_node_id=None,
        )
    cloned_edges = tuple(
        replace(
            edge,
            edge_id=f"{operator_node_id}__{frame.operator_frame_id}__{edge.edge_id}",
            from_node_id=node_id_map[edge.from_node_id],
            to_node_id=node_id_map[edge.to_node_id],
            template_owner_node_id=None,
        )
        for edge in template_edges
    )
    terminal_template_id = _template_terminal_node_id(template_nodes, template_edges)
    terminal_clone_id = (
        node_id_map[terminal_template_id]
        if terminal_template_id is not None
        else None
    )
    return cloned_nodes, cloned_edges, terminal_clone_id


__all__ = [
    "_clone_template_graph",
    "_operator_frame_payload",
    "_operator_frame_read_model",
    "_template_graph",
    "_template_terminal_node_id",
]
