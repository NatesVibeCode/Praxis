"""Evidence helpers: ID generators, decision refs, and release-ref construction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from receipts.evidence import DecisionRef

if TYPE_CHECKING:
    from contracts.domain import WorkflowEdgeContract

ADMISSION_DECISION_SOURCE_TABLE = "admission_decisions"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _event_id(*, run_id: str, evidence_seq: int) -> str:
    return f"workflow_event:{run_id}:{evidence_seq}"


def _receipt_id(*, run_id: str, evidence_seq: int) -> str:
    return f"receipt:{run_id}:{evidence_seq}"


def _decision_refs_for_admission(outcome: Any) -> tuple[DecisionRef, ...]:
    return (
        DecisionRef(
            decision_type="admission",
            decision_id=outcome.admission_decision.admission_decision_id,
            reason_code=outcome.admission_decision.reason_code,
            source_table=ADMISSION_DECISION_SOURCE_TABLE,
        ),
    )


def _release_refs(
    *,
    inbound_edges: Sequence[WorkflowEdgeContract],
    completed_nodes: Mapping[str, Any],
) -> tuple[dict[str, str], ...]:
    refs: list[dict[str, str]] = []
    for edge in inbound_edges:
        upstream = completed_nodes.get(edge.from_node_id)
        if upstream is None:
            continue
        refs.append(
            {
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "from_node_id": edge.from_node_id,
                "upstream_receipt_id": upstream.completion_receipt_id,
            }
        )
    return tuple(refs)


__all__ = [
    "_decision_refs_for_admission",
    "_event_id",
    "_now",
    "_receipt_id",
    "_release_refs",
    "ADMISSION_DECISION_SOURCE_TABLE",
]
