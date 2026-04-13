"""Render derived observability views for operators."""

from observability.read_models import (
    GraphLineageReadModel,
    GraphTopologyEdge,
    GraphTopologyNode,
    GraphTopologyReadModel,
    InspectionReadModel,
    ReplayReadModel,
)
from runtime._helpers import _append_indexed_lines, _format_bool

__all__ = [
    "render_graph_lineage",
    "render_graph_topology",
    "render_inspection",
    "render_replay",
]


def _join(values: tuple[str, ...]) -> str:
    return ", ".join(values) if values else "-"


def _format_watermark(evidence_seq: int | None) -> str:
    return "-" if evidence_seq is None else str(evidence_seq)


def _format_optional_text(value: str | None) -> str:
    return value if value is not None and value != "" else "-"


def _format_optional_int(value: int | None) -> str:
    return "-" if value is None else str(value)


def _append_node_lines(lines: list[str], node: GraphTopologyNode, index: int) -> None:
    prefix = f"nodes[{index}]"
    lines.extend(
        [
            f"{prefix}.node_id: {node.node_id}",
            f"{prefix}.node_type: {node.node_type}",
            f"{prefix}.display_name: {node.display_name}",
            f"{prefix}.position_index: {node.position_index}",
        ]
    )


def _append_edge_lines(lines: list[str], edge: GraphTopologyEdge, index: int) -> None:
    prefix = f"edges[{index}]"
    lines.extend(
        [
            f"{prefix}.edge_id: {edge.edge_id}",
            f"{prefix}.edge_type: {edge.edge_type}",
            f"{prefix}.from_node_id: {edge.from_node_id}",
            f"{prefix}.to_node_id: {edge.to_node_id}",
            f"{prefix}.position_index: {edge.position_index}",
        ]
    )


def _append_common_graph_lines(
    lines: list[str],
    *,
    request_id: str | None,
    completeness_is_complete: bool,
    missing_evidence_refs: tuple[str, ...],
    watermark_seq: int | None,
    watermark_source: str,
    evidence_refs: tuple[str, ...],
) -> None:
    lines.extend(
        [
            f"request_id: {_format_optional_text(request_id)}",
            f"completeness.is_complete: {_format_bool(completeness_is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "completeness.missing_evidence_refs",
        missing_evidence_refs,
    )
    lines.extend(
        [
            f"watermark.evidence_seq: {_format_watermark(watermark_seq)}",
            f"watermark.source: {watermark_source}",
        ]
    )
    _append_indexed_lines(lines, "evidence_refs", evidence_refs)


def render_inspection(view: InspectionReadModel) -> str:
    """Render a derived inspection view without touching authority state."""

    completeness = "complete" if view.completeness.is_complete else "incomplete"
    lines = [
        "kind: inspection",
        f"run_id: {view.run_id}",
        f"request_id: {view.request_id or '-'}",
        f"completeness: {completeness}",
        f"missing_evidence_refs: {_join(view.completeness.missing_evidence_refs)}",
        f"watermark_seq: {_format_watermark(view.watermark.evidence_seq)}",
        f"watermark_source: {view.watermark.source}",
        f"evidence_refs: {_join(view.evidence_refs)}",
        f"current_state: {view.current_state or '-'}",
        f"node_timeline: {_join(view.node_timeline)}",
        f"terminal_reason: {view.terminal_reason or '-'}",
        f"operator_frame_source: {view.operator_frame_source}",
        f"operator_frames_count: {len(view.operator_frames)}",
    ]
    for index, frame in enumerate(view.operator_frames):
        prefix = f"operator_frames[{index}]"
        lines.extend(
            [
                f"{prefix}.operator_frame_id: {frame.operator_frame_id}",
                f"{prefix}.node_id: {frame.node_id}",
                f"{prefix}.operator_kind: {frame.operator_kind}",
                f"{prefix}.frame_state: {frame.frame_state}",
                f"{prefix}.item_index: {_format_optional_int(frame.item_index)}",
                f"{prefix}.iteration_index: {_format_optional_int(frame.iteration_index)}",
                f"{prefix}.active_count: {frame.active_count}",
                f"{prefix}.stop_reason: {_format_optional_text(frame.stop_reason)}",
            ]
        )
    return "\n".join(lines)


def render_graph_topology(view: GraphTopologyReadModel) -> str:
    """Render a derived graph topology view without touching authority state."""

    lines = [
        "kind: graph_topology",
        f"run_id: {view.run_id}",
    ]
    _append_common_graph_lines(
        lines,
        request_id=view.request_id,
        completeness_is_complete=view.completeness.is_complete,
        missing_evidence_refs=view.completeness.missing_evidence_refs,
        watermark_seq=view.watermark.evidence_seq,
        watermark_source=view.watermark.source,
        evidence_refs=view.evidence_refs,
    )
    lines.extend(
        [
            f"admitted_definition_ref: {_format_optional_text(view.admitted_definition_ref)}",
            f"nodes_count: {len(view.nodes)}",
        ]
    )
    for index, node in enumerate(view.nodes):
        _append_node_lines(lines, node, index)
    lines.append(f"edges_count: {len(view.edges)}")
    for index, edge in enumerate(view.edges):
        _append_edge_lines(lines, edge, index)
    _append_indexed_lines(lines, "runtime_node_order", view.runtime_node_order)
    return "\n".join(lines)


def render_graph_lineage(view: GraphLineageReadModel) -> str:
    """Render a derived graph lineage view without touching authority state."""

    lines = [
        "kind: graph_lineage",
        f"run_id: {view.run_id}",
    ]
    _append_common_graph_lines(
        lines,
        request_id=view.request_id,
        completeness_is_complete=view.completeness.is_complete,
        missing_evidence_refs=view.completeness.missing_evidence_refs,
        watermark_seq=view.watermark.evidence_seq,
        watermark_source=view.watermark.source,
        evidence_refs=view.evidence_refs,
    )
    lines.extend(
        [
            f"claim_received_ref: {_format_optional_text(view.claim_received_ref)}",
            f"admitted_definition_ref: {_format_optional_text(view.admitted_definition_ref)}",
            f"admitted_definition_hash: {_format_optional_text(view.admitted_definition_hash)}",
            f"nodes_count: {len(view.nodes)}",
        ]
    )
    for index, node in enumerate(view.nodes):
        _append_node_lines(lines, node, index)
    lines.append(f"edges_count: {len(view.edges)}")
    for index, edge in enumerate(view.edges):
        _append_edge_lines(lines, edge, index)
    _append_indexed_lines(lines, "runtime_node_order", view.runtime_node_order)
    lines.extend(
        [
            f"current_state: {_format_optional_text(view.current_state)}",
            f"terminal_reason: {_format_optional_text(view.terminal_reason)}",
            f"operator_frame_source: {_format_optional_text(view.operator_frame_source)}",
            f"operator_frames_count: {len(view.operator_frames)}",
        ]
    )
    return "\n".join(lines)


def render_replay(view: ReplayReadModel) -> str:
    """Render a derived replay view without pretending to be truth."""

    completeness = "complete" if view.completeness.is_complete else "incomplete"
    lines = [
        "kind: replay",
        f"run_id: {view.run_id}",
        f"request_id: {view.request_id or '-'}",
        f"completeness: {completeness}",
        f"missing_evidence_refs: {_join(view.completeness.missing_evidence_refs)}",
        f"watermark_seq: {_format_watermark(view.watermark.evidence_seq)}",
        f"watermark_source: {view.watermark.source}",
        f"evidence_refs: {_join(view.evidence_refs)}",
        f"admitted_definition_ref: {view.admitted_definition_ref or '-'}",
        f"dependency_order: {_join(view.dependency_order)}",
        f"node_outcomes: {_join(view.node_outcomes)}",
        f"terminal_reason: {view.terminal_reason or '-'}",
        f"operator_frame_source: {view.operator_frame_source}",
        f"operator_frames_count: {len(view.operator_frames)}",
    ]
    return "\n".join(lines)
