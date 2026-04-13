"""Derived readers over canonical evidence.

These entrypoints accept canonical evidence and return read models.
They do not own lifecycle truth, policy truth, or any write path.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from receipts import EvidenceRow, ReceiptV1, WorkflowEventV1
from runtime._helpers import _dedupe

from .read_models import InspectionReadModel, OperatorFrameReadModel, ReplayReadModel
from .read_models import ProjectionCompleteness, ProjectionWatermark

__all__ = ["inspect_run", "replay_run"]


@dataclass(frozen=True, slots=True)
class _TransitionBundle:
    transition_seq: int
    event_row: EvidenceRow | None
    receipt_row: EvidenceRow | None

    @property
    def event(self) -> WorkflowEventV1 | None:
        if self.event_row is None or not isinstance(self.event_row.record, WorkflowEventV1):
            return None
        return self.event_row.record

    @property
    def receipt(self) -> ReceiptV1 | None:
        if self.receipt_row is None or not isinstance(self.receipt_row.record, ReceiptV1):
            return None
        return self.receipt_row.record

    @property
    def is_complete(self) -> bool:
        return self.event is not None and self.receipt is not None


@dataclass(frozen=True, slots=True)
class _CanonicalEvidenceSlice:
    run_id: str
    request_id: str | None
    rows: tuple[EvidenceRow, ...]
    bundles: tuple[_TransitionBundle, ...]
    completeness: ProjectionCompleteness
    watermark: ProjectionWatermark
    evidence_refs: tuple[str, ...]


def _validate_evidence_rows(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
) -> _CanonicalEvidenceSlice:
    missing_refs: list[str] = []
    rows: list[EvidenceRow] = []
    for index, row in enumerate(canonical_evidence):
        if not isinstance(row, EvidenceRow):
            missing_refs.append(f"evidence_row:{index}")
            continue
        record_run_id = getattr(row.record, "run_id", None)
        if row.route_identity.run_id != run_id or record_run_id != run_id:
            missing_refs.append(f"run_id:{row.row_id}")
        rows.append(row)

    rows.sort(key=lambda item: (item.evidence_seq, item.row_id))
    evidence_refs = tuple(row.row_id for row in rows)
    watermark = ProjectionWatermark(
        evidence_seq=rows[-1].evidence_seq if rows else None,
    )
    if not rows:
        missing_refs.append(f"run:{run_id}:evidence_missing")

    seen_evidence_seq: set[int] = set()
    expected_evidence_seq = 1
    for row in rows:
        if row.evidence_seq in seen_evidence_seq:
            missing_refs.append(f"evidence_seq_conflict:{row.evidence_seq}")
            continue
        if row.evidence_seq > expected_evidence_seq:
            missing_refs.extend(
                f"evidence_seq:{value}"
                for value in range(expected_evidence_seq, row.evidence_seq)
            )
        elif row.evidence_seq < expected_evidence_seq:
            missing_refs.append(f"evidence_seq_conflict:{row.evidence_seq}")
        seen_evidence_seq.add(row.evidence_seq)
        expected_evidence_seq = max(expected_evidence_seq, row.evidence_seq + 1)

    if rows:
        request_id = rows[0].route_identity.request_id
        for row in rows:
            record_request_id = getattr(row.record, "request_id", None)
            if row.route_identity.request_id != request_id or record_request_id != request_id:
                missing_refs.append(f"request_id:{row.row_id}")
    else:
        request_id = None

    row_index = {row.row_id: index for index, row in enumerate(rows)}
    grouped_rows: dict[int, list[EvidenceRow]] = defaultdict(list)
    for row in rows:
        grouped_rows[row.transition_seq].append(row)

    bundles: list[_TransitionBundle] = []
    for transition_seq in sorted(grouped_rows):
        grouped = sorted(
            grouped_rows[transition_seq],
            key=lambda item: (item.evidence_seq, item.row_id),
        )
        event_rows = [
            row
            for row in grouped
            if row.kind == "workflow_event" and isinstance(row.record, WorkflowEventV1)
        ]
        receipt_rows = [
            row
            for row in grouped
            if row.kind == "receipt" and isinstance(row.record, ReceiptV1)
        ]
        if not event_rows:
            missing_refs.append(f"transition:{transition_seq}:workflow_event")
        if not receipt_rows:
            missing_refs.append(f"transition:{transition_seq}:receipt")
        if len(grouped) != 2 or len(event_rows) != 1 or len(receipt_rows) != 1:
            missing_refs.append(f"transition:{transition_seq}:bundle_size")

        event_row = event_rows[0] if event_rows else None
        receipt_row = receipt_rows[0] if receipt_rows else None
        if event_row is not None:
            previous_row_id = (
                None if row_index[event_row.row_id] == 0 else rows[row_index[event_row.row_id] - 1].row_id
            )
            event = event_row.record
            if event.causation_id != previous_row_id:
                missing_refs.append(f"transition:{transition_seq}:event_causation")
        if event_row is not None and receipt_row is not None:
            event = event_row.record
            receipt = receipt_row.record
            if event_row.evidence_seq + 1 != receipt_row.evidence_seq:
                missing_refs.append(f"transition:{transition_seq}:bundle_order")
            if receipt.causation_id != event.event_id:
                missing_refs.append(f"transition:{transition_seq}:receipt_causation")
        bundles.append(
            _TransitionBundle(
                transition_seq=transition_seq,
                event_row=event_row,
                receipt_row=receipt_row,
            )
        )

    missing_refs_tuple = _dedupe(missing_refs)
    return _CanonicalEvidenceSlice(
        run_id=run_id,
        request_id=request_id,
        rows=tuple(rows),
        bundles=tuple(bundles),
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs_tuple,
            missing_evidence_refs=missing_refs_tuple,
        ),
        watermark=watermark,
        evidence_refs=evidence_refs,
    )


def _complete_bundles(slice_: _CanonicalEvidenceSlice) -> tuple[_TransitionBundle, ...]:
    return tuple(bundle for bundle in slice_.bundles if bundle.is_complete)


def _runtime_bundles(slice_: _CanonicalEvidenceSlice) -> tuple[_TransitionBundle, ...]:
    return tuple(
        bundle
        for bundle in _complete_bundles(slice_)
        if bundle.receipt is not None and bundle.receipt.node_id is None
    )


def _terminal_status(status: str | None) -> bool:
    if not isinstance(status, str):
        return False
    if status == "succeeded":
        return True
    return any(
        token in status
        for token in (
            "failed",
            "rejected",
            "blocked",
            "cancelled",
            "expired",
            "promoted",
        )
    )


def _mapping_text(value: Mapping[str, object], key: str) -> str | None:
    field_value = value.get(key)
    if isinstance(field_value, str) and field_value:
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


def _claim_received_bundle(
    bundles: Sequence[_TransitionBundle],
) -> _TransitionBundle | None:
    return next(
        (
            bundle
            for bundle in bundles
            if bundle.event is not None and bundle.event.event_type == "claim_received"
        ),
        None,
    )


def _expected_dependencies(
    bundle: _TransitionBundle | None,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if bundle is None or bundle.event is None:
        return ()
    claim_envelope = bundle.event.payload.get("claim_envelope")
    if not isinstance(claim_envelope, Mapping):
        return ()

    edges = claim_envelope.get("edges")
    if not isinstance(edges, Sequence) or isinstance(edges, (str, bytes, bytearray)):
        return ()

    dependencies: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        from_node_id = _mapping_text(edge, "from_node_id")
        to_node_id = _mapping_text(edge, "to_node_id")
        if from_node_id is None or to_node_id is None:
            continue
        dependencies[to_node_id].append(from_node_id)
    return tuple(
        (node_id, tuple(upstream_nodes))
        for node_id, upstream_nodes in sorted(dependencies.items())
    )


def inspect_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    operator_frame_source: str = "missing",
    operator_frames: Sequence[OperatorFrameReadModel] = (),
) -> InspectionReadModel:
    """Build an inspection view from canonical evidence only."""

    slice_ = _validate_evidence_rows(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    runtime_bundles = _runtime_bundles(slice_)
    missing_refs = list(slice_.completeness.missing_evidence_refs)
    current_state = None
    terminal_reason = None
    if runtime_bundles:
        latest_runtime_bundle = runtime_bundles[-1]
        if latest_runtime_bundle.receipt is not None:
            current_state = latest_runtime_bundle.receipt.status
        if _terminal_status(current_state) and latest_runtime_bundle.event is not None:
            terminal_reason = latest_runtime_bundle.event.reason_code
    else:
        missing_refs.append("runtime:state")

    node_timeline = tuple(
        f"{bundle.receipt.node_id}:{bundle.receipt.status}"
        for bundle in _complete_bundles(slice_)
        if bundle.receipt is not None and bundle.receipt.node_id
    )
    missing_refs_tuple = _dedupe(missing_refs)
    return InspectionReadModel(
        run_id=run_id,
        request_id=slice_.request_id,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs_tuple,
            missing_evidence_refs=missing_refs_tuple,
        ),
        watermark=slice_.watermark,
        evidence_refs=slice_.evidence_refs,
        current_state=current_state,
        node_timeline=node_timeline,
        terminal_reason=terminal_reason,
        operator_frame_source=operator_frame_source,
        operator_frames=tuple(operator_frames),
    )


def replay_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    operator_frame_source: str = "missing",
    operator_frames: Sequence[OperatorFrameReadModel] = (),
) -> ReplayReadModel:
    """Build a replay view from canonical evidence only."""

    slice_ = _validate_evidence_rows(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    complete_bundles = _complete_bundles(slice_)
    missing_refs = list(slice_.completeness.missing_evidence_refs)
    claim_received_bundle = _claim_received_bundle(complete_bundles)
    admitted_definition_ref = None
    if claim_received_bundle is None or claim_received_bundle.event is None:
        missing_refs.append("transition:1:claim_received")
    else:
        admitted_definition_ref = _mapping_text(
            claim_received_bundle.event.payload,
            "admitted_definition_ref",
        )
        if admitted_definition_ref is None:
            missing_refs.append("transition:1:admitted_definition_ref")

    expected_dependencies = dict(_expected_dependencies(claim_received_bundle))
    completion_receipts = {
        bundle.receipt_row.row_id: bundle.receipt
        for bundle in complete_bundles
        if bundle.receipt_row is not None
        and bundle.receipt is not None
        and bundle.receipt.receipt_type == "node_execution_receipt"
        and bundle.receipt.node_id is not None
    }
    dependency_order: list[str] = []
    node_outcomes: list[str] = []
    completed_nodes: set[str] = set()
    for bundle in complete_bundles:
        event = bundle.event
        receipt = bundle.receipt
        if event is None or receipt is None or not receipt.node_id:
            continue
        if event.event_type == "node_started":
            dependency_order.append(receipt.node_id)
            dependency_receipts = _mapping_sequence(
                event.payload,
                "dependency_receipts",
            )
            if dependency_receipts is None:
                missing_refs.append(f"node:{receipt.node_id}:dependency_receipts")
                dependency_receipts = ()
            observed_upstream_nodes: list[str] = []
            for dependency_ref in dependency_receipts:
                upstream_receipt_id = _mapping_text(dependency_ref, "upstream_receipt_id")
                from_node_id = _mapping_text(dependency_ref, "from_node_id")
                if upstream_receipt_id is None or from_node_id is None:
                    missing_refs.append(f"node:{receipt.node_id}:dependency_receipts")
                    continue
                upstream_receipt = completion_receipts.get(upstream_receipt_id)
                if upstream_receipt is None:
                    missing_refs.append(f"receipt:{upstream_receipt_id}")
                    continue
                if upstream_receipt.node_id != from_node_id:
                    missing_refs.append(f"node:{receipt.node_id}:dependency_source")
                    continue
                observed_upstream_nodes.append(from_node_id)
            if tuple(observed_upstream_nodes) != expected_dependencies.get(receipt.node_id, ()):
                missing_refs.append(f"node:{receipt.node_id}:dependency_receipts")
        if receipt.receipt_type == "node_execution_receipt":
            node_outcomes.append(f"{receipt.node_id}:{receipt.status}")
            completed_nodes.add(receipt.node_id)

    for node_id in dependency_order:
        if node_id not in completed_nodes:
            missing_refs.append(f"node:{node_id}:outcome")

    runtime_bundles = _runtime_bundles(slice_)
    terminal_reason = None
    if runtime_bundles:
        latest_runtime_bundle = runtime_bundles[-1]
        latest_status = latest_runtime_bundle.receipt.status if latest_runtime_bundle.receipt is not None else None
        if _terminal_status(latest_status) and latest_runtime_bundle.event is not None:
            terminal_reason = latest_runtime_bundle.event.reason_code
        else:
            missing_refs.append("runtime:terminal_state")
    else:
        missing_refs.append("runtime:terminal_state")

    missing_refs_tuple = _dedupe(missing_refs)
    completeness = ProjectionCompleteness(
        is_complete=not missing_refs_tuple,
        missing_evidence_refs=missing_refs_tuple,
    )
    return ReplayReadModel(
        run_id=run_id,
        request_id=slice_.request_id,
        completeness=completeness,
        watermark=slice_.watermark,
        evidence_refs=slice_.evidence_refs,
        dependency_order=tuple(dependency_order),
        node_outcomes=tuple(node_outcomes),
        admitted_definition_ref=admitted_definition_ref,
        terminal_reason=terminal_reason or "runtime.replay_incomplete",
        operator_frame_source=operator_frame_source,
        operator_frames=tuple(operator_frames),
    )
