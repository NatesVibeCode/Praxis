"""Read-model contracts for observability.

These are derived views over canonical evidence rows.
They are intentionally explicit about completeness and watermarks so they cannot
pretend to be runtime truth.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

__all__ = [
    "DerivedReadModel",
    "GraphTopologyEdge",
    "GraphTopologyNode",
    "GraphTopologyReadModel",
    "GraphLineageReadModel",
    "InspectionReadModel",
    "OperatorFrameReadModel",
    "ProjectionCompleteness",
    "ProjectionWatermark",
    "ReplayPathBreak",
    "ReplayReadModel",
]


@dataclass(frozen=True, slots=True)
class ProjectionCompleteness:
    """States whether a derived view covers the evidence it claims to represent."""

    is_complete: bool
    missing_evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectionWatermark:
    """Tracks how far a projection has been applied against canonical evidence."""

    evidence_seq: int | None
    source: str = "canonical_evidence"


@dataclass(frozen=True, slots=True)
class DerivedReadModel:
    """Common metadata for any derived observability view."""

    run_id: str
    request_id: str | None
    completeness: ProjectionCompleteness
    watermark: ProjectionWatermark
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OperatorFrameReadModel:
    """Authoritative control-operator frame snapshot for one run."""

    operator_frame_id: str
    node_id: str
    operator_kind: str
    frame_state: str
    item_index: int | None = None
    iteration_index: int | None = None
    source_snapshot: dict[str, Any] | None = None
    aggregate_outputs: dict[str, Any] | None = None
    active_count: int = 0
    stop_reason: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class InspectionReadModel(DerivedReadModel):
    """Derived inspection view for operators."""

    current_state: str | None = None
    node_timeline: tuple[str, ...] = ()
    terminal_reason: str | None = None
    operator_frame_source: str = "missing"
    operator_frames: tuple[OperatorFrameReadModel, ...] = ()


@dataclass(frozen=True, slots=True)
class ReplayPathBreak:
    """First deterministic break that prevents replay from proving the full path."""

    reason_code: str
    missing_ref: str
    break_kind: str
    transition_seq: int | None = None
    node_id: str | None = None
    evidence_seq: int | None = None
    expected: str | None = None
    observed: str | None = None


@dataclass(frozen=True, slots=True)
class ReplayReadModel(DerivedReadModel):
    """Derived replay view for reconstructing a run from evidence."""

    dependency_order: tuple[str, ...] = ()
    node_outcomes: tuple[str, ...] = ()
    admitted_definition_ref: str | None = None
    terminal_reason: str | None = None
    path_break: ReplayPathBreak | None = None
    operator_frame_source: str = "missing"
    operator_frames: tuple[OperatorFrameReadModel, ...] = ()


@dataclass(frozen=True, slots=True)
class GraphTopologyNode:
    """Declared workflow node topology from the admitted claim envelope."""

    node_id: str
    node_type: str
    display_name: str
    position_index: int


@dataclass(frozen=True, slots=True)
class GraphTopologyEdge:
    """Declared workflow edge topology from the admitted claim envelope."""

    edge_id: str
    edge_type: str
    from_node_id: str
    to_node_id: str
    position_index: int


@dataclass(frozen=True, slots=True)
class GraphTopologyReadModel(DerivedReadModel):
    """Derived graph topology view backed by canonical evidence."""

    admitted_definition_ref: str | None = None
    nodes: tuple[GraphTopologyNode, ...] = ()
    edges: tuple[GraphTopologyEdge, ...] = ()
    runtime_node_order: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GraphLineageReadModel(DerivedReadModel):
    """Derived graph lineage view with completeness and runtime provenance."""

    claim_received_ref: str | None = None
    admitted_definition_ref: str | None = None
    admitted_definition_hash: str | None = None
    nodes: tuple[GraphTopologyNode, ...] = ()
    edges: tuple[GraphTopologyEdge, ...] = ()
    runtime_node_order: tuple[str, ...] = ()
    current_state: str | None = None
    terminal_reason: str | None = None
    operator_frame_source: str = "missing"
    operator_frames: tuple[OperatorFrameReadModel, ...] = ()
