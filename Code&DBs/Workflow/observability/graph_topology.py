"""Derived graph topology view over canonical workflow evidence.

This module is intentionally narrow: it reads the admitted graph shape from
canonical evidence and pairs it with runtime-derived execution order from the
existing replay read model. It does not own lifecycle truth or any write path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from receipts import EvidenceRow, WorkflowEventV1
from runtime._helpers import _dedupe

from .read_models import (
    GraphTopologyEdge,
    GraphTopologyNode,
    GraphTopologyReadModel,
    ProjectionCompleteness,
)
from .readers import replay_run

__all__ = ["graph_topology_run"]


def _mapping_text(value: Mapping[str, object], key: str) -> str | None:
    field_value = value.get(key)
    if isinstance(field_value, str) and field_value:
        return field_value
    return None


def _mapping_int(value: Mapping[str, object], key: str) -> int | None:
    field_value = value.get(key)
    if isinstance(field_value, int):
        return field_value
    return None


def _mapping_sequence(
    value: Mapping[str, object],
    key: str,
) -> tuple[Mapping[str, object], ...] | None:
    field_value = value.get(key)
    if not isinstance(field_value, Sequence) or isinstance(field_value, (str, bytes, bytearray)):
        return None
    items: list[Mapping[str, object]] = []
    for item in field_value:
        if not isinstance(item, Mapping):
            return None
        items.append(item)
    return tuple(items)


def _claim_received_event(rows: Sequence[EvidenceRow]) -> WorkflowEventV1 | None:
    for row in rows:
        if row.kind == "workflow_event" and isinstance(row.record, WorkflowEventV1):
            if row.record.event_type == "claim_received":
                return row.record
    return None


def _topology_nodes(
    claim_envelope: Mapping[str, object],
) -> tuple[tuple[GraphTopologyNode, ...], tuple[str, ...]]:
    nodes_value = _mapping_sequence(claim_envelope, "nodes")
    if nodes_value is None:
        return (), ("graph:nodes",)

    nodes: list[GraphTopologyNode] = []
    missing_refs: list[str] = []
    for node in sorted(
        nodes_value,
        key=lambda item: (
            _mapping_int(item, "position_index") or 0,
            _mapping_text(item, "node_id") or "",
        ),
    ):
        node_id = _mapping_text(node, "node_id")
        node_type = _mapping_text(node, "node_type")
        display_name = _mapping_text(node, "display_name")
        position_index = _mapping_int(node, "position_index")
        if (
            node_id is None
            or node_type is None
            or display_name is None
            or position_index is None
        ):
            missing_refs.append("graph:node_shape")
            continue
        nodes.append(
            GraphTopologyNode(
                node_id=node_id,
                node_type=node_type,
                display_name=display_name,
                position_index=position_index,
            )
        )
    return tuple(nodes), _dedupe(missing_refs)


def _topology_edges(
    claim_envelope: Mapping[str, object],
) -> tuple[tuple[GraphTopologyEdge, ...], tuple[str, ...]]:
    edges_value = _mapping_sequence(claim_envelope, "edges")
    if edges_value is None:
        return (), ("graph:edges",)

    edges: list[GraphTopologyEdge] = []
    missing_refs: list[str] = []
    for edge in sorted(
        edges_value,
        key=lambda item: (
            _mapping_int(item, "position_index") or 0,
            _mapping_text(item, "edge_id") or "",
        ),
    ):
        edge_id = _mapping_text(edge, "edge_id")
        edge_type = _mapping_text(edge, "edge_type")
        from_node_id = _mapping_text(edge, "from_node_id")
        to_node_id = _mapping_text(edge, "to_node_id")
        position_index = _mapping_int(edge, "position_index")
        if (
            edge_id is None
            or edge_type is None
            or from_node_id is None
            or to_node_id is None
            or position_index is None
        ):
            missing_refs.append("graph:edge_shape")
            continue
        edges.append(
            GraphTopologyEdge(
                edge_id=edge_id,
                edge_type=edge_type,
                from_node_id=from_node_id,
                to_node_id=to_node_id,
                position_index=position_index,
            )
        )
    return tuple(edges), _dedupe(missing_refs)


def _runtime_order_matches_topology(
    *,
    runtime_node_order: tuple[str, ...],
    nodes: tuple[GraphTopologyNode, ...],
) -> bool:
    return runtime_node_order == tuple(node.node_id for node in nodes)


def _topology_invariant_failures(
    *,
    nodes: tuple[GraphTopologyNode, ...],
    edges: tuple[GraphTopologyEdge, ...],
    runtime_node_order: tuple[str, ...],
) -> tuple[str, ...]:
    missing_refs: list[str] = []

    node_ids: set[str] = set()
    node_position_indexes: set[int] = set()
    for node in nodes:
        if node.node_id in node_ids:
            missing_refs.append(f"graph:duplicate_node_id:{node.node_id}")
        else:
            node_ids.add(node.node_id)
        if node.position_index in node_position_indexes:
            missing_refs.append(f"graph:duplicate_node_position_index:{node.position_index}")
        else:
            node_position_indexes.add(node.position_index)

    edge_ids: set[str] = set()
    edge_position_indexes: set[int] = set()
    for edge in edges:
        if edge.edge_id in edge_ids:
            missing_refs.append(f"graph:duplicate_edge_id:{edge.edge_id}")
        else:
            edge_ids.add(edge.edge_id)
        if edge.position_index in edge_position_indexes:
            missing_refs.append(f"graph:duplicate_edge_position_index:{edge.position_index}")
        else:
            edge_position_indexes.add(edge.position_index)
        if edge.from_node_id not in node_ids:
            missing_refs.append(f"graph:dangling_edge_from:{edge.edge_id}:{edge.from_node_id}")
        if edge.to_node_id not in node_ids:
            missing_refs.append(f"graph:dangling_edge_to:{edge.edge_id}:{edge.to_node_id}")

    if not _runtime_order_matches_topology(
        runtime_node_order=runtime_node_order,
        nodes=nodes,
    ):
        missing_refs.append("graph:runtime_node_order")
    for node_id in runtime_node_order:
        if node_id not in node_ids:
            missing_refs.append(f"graph:runtime_node_order_unknown:{node_id}")

    return _dedupe(missing_refs)


def graph_topology_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
) -> GraphTopologyReadModel:
    """Build a graph topology view from canonical evidence only."""

    ordered_rows = tuple(
        sorted(
            (
                row
                for row in canonical_evidence
                if isinstance(row, EvidenceRow)
            ),
            key=lambda item: (item.evidence_seq, item.row_id),
        )
    )
    claim_received_event = _claim_received_event(ordered_rows)
    request_id = None
    admitted_definition_ref = None
    nodes: tuple[GraphTopologyNode, ...] = ()
    edges: tuple[GraphTopologyEdge, ...] = ()
    missing_refs: list[str] = []
    if claim_received_event is not None:
        request_id = claim_received_event.request_id
        payload = claim_received_event.payload
        if isinstance(payload, Mapping):
            admitted_definition_ref = _mapping_text(payload, "admitted_definition_ref")
            if admitted_definition_ref is None:
                missing_refs.append("graph:admitted_definition_ref")
            claim_envelope = payload.get("claim_envelope")
            if isinstance(claim_envelope, Mapping):
                nodes, node_missing_refs = _topology_nodes(claim_envelope)
                edges, edge_missing_refs = _topology_edges(claim_envelope)
                missing_refs.extend(node_missing_refs)
                missing_refs.extend(edge_missing_refs)
            else:
                missing_refs.append("graph:claim_envelope")
        else:
            missing_refs.append("graph:claim_received_payload")
    else:
        missing_refs.append("graph:claim_received_event")

    replay_view = replay_run(
        run_id=run_id,
        canonical_evidence=ordered_rows,
    )
    if request_id is None:
        request_id = replay_view.request_id
    if admitted_definition_ref is None:
        admitted_definition_ref = replay_view.admitted_definition_ref
    missing_refs.extend(replay_view.completeness.missing_evidence_refs)
    missing_refs.extend(
        _topology_invariant_failures(
            nodes=nodes,
            edges=edges,
            runtime_node_order=replay_view.dependency_order,
        )
    )
    missing_refs_tuple = _dedupe(missing_refs)
    return GraphTopologyReadModel(
        run_id=run_id,
        request_id=request_id,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs_tuple,
            missing_evidence_refs=missing_refs_tuple,
        ),
        watermark=replay_view.watermark,
        evidence_refs=replay_view.evidence_refs,
        admitted_definition_ref=admitted_definition_ref,
        nodes=nodes,
        edges=edges,
        runtime_node_order=replay_view.dependency_order,
    )
