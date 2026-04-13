"""Graph lineage reads over canonical workflow evidence.

This surface stays read-only. It turns the admitted request envelope and the
runtime evidence timeline into a graph-shaped view with explicit completeness
metadata.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from receipts import EvidenceRow, WorkflowEventV1

from .read_models import (
    GraphLineageReadModel,
    OperatorFrameReadModel,
)
from .graph_topology import graph_topology_run
from .readers import inspect_run

__all__ = ["GraphLineageReadModel", "graph_lineage_run"]


def _mapping_text(value: Mapping[str, object], key: str) -> str | None:
    field_value = value.get(key)
    if isinstance(field_value, str) and field_value:
        return field_value
    return None


def _claim_received_row(canonical_evidence: Sequence[EvidenceRow]) -> EvidenceRow | None:
    for row in canonical_evidence:
        if row.kind != "workflow_event" or not isinstance(row.record, WorkflowEventV1):
            continue
        if row.record.event_type == "claim_received":
            return row
    return None


def graph_lineage_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    operator_frame_source: str = "missing",
    operator_frames: Sequence[OperatorFrameReadModel] = (),
) -> GraphLineageReadModel:
    """Build a graph lineage view from canonical evidence only."""

    inspection = inspect_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
        operator_frame_source=operator_frame_source,
        operator_frames=operator_frames,
    )
    claim_row = _claim_received_row(canonical_evidence)
    topology = graph_topology_run(run_id=run_id, canonical_evidence=canonical_evidence)

    admitted_definition_hash = None
    if claim_row is not None and isinstance(claim_row.record, WorkflowEventV1):
        admitted_definition_hash = _mapping_text(claim_row.record.payload, "admitted_definition_hash")
    return GraphLineageReadModel(
        run_id=run_id,
        request_id=topology.request_id,
        completeness=topology.completeness,
        watermark=topology.watermark,
        evidence_refs=topology.evidence_refs,
        claim_received_ref=None if claim_row is None else claim_row.row_id,
        admitted_definition_ref=topology.admitted_definition_ref,
        admitted_definition_hash=admitted_definition_hash,
        nodes=topology.nodes,
        edges=topology.edges,
        runtime_node_order=topology.runtime_node_order,
        current_state=inspection.current_state,
        terminal_reason=inspection.terminal_reason,
        operator_frame_source=inspection.operator_frame_source,
        operator_frames=inspection.operator_frames,
    )
