"""Serialization helpers for the native workflow frontdoor."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from observability.read_models import InspectionReadModel
from policy.domain import AdmissionDecisionRecord


def _json_loads_maybe(value: object, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return default
    return value


def _measured_summary(operation_set: object) -> dict[str, int]:
    operations = _json_loads_maybe(operation_set, [])
    summary = {"create": 0, "update": 0, "delete": 0, "rename": 0}
    if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes, bytearray)):
        summary["total"] = 0
        return summary
    for item in operations:
        if not isinstance(item, Mapping):
            continue
        action = str(item.get("action") or "").strip().lower()
        if action in summary:
            summary[action] += 1
    summary["total"] = sum(summary.values())
    return summary


def _submission_summary_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    submission_id = str(row.get("submission_id") or "").strip()
    if not submission_id:
        return None
    return {
        "submission_id": submission_id,
        "result_kind": str(row.get("submission_result_kind") or "").strip() or None,
        "summary": str(row.get("submission_summary") or "").strip() or None,
        "comparison_status": str(row.get("submission_comparison_status") or "").strip() or None,
        "measured_summary": _measured_summary(row.get("submission_operation_set")),
        "latest_review_decision": str(row.get("latest_submission_review_decision") or "").strip() or None,
        "latest_review_summary": str(row.get("latest_submission_review_summary") or "").strip() or None,
    }


def _serialize_decision(decision: AdmissionDecisionRecord) -> dict[str, Any]:
    return {
        "admission_decision_id": decision.admission_decision_id,
        "workflow_id": decision.workflow_id,
        "request_id": decision.request_id,
        "decision": decision.decision.value,
        "reason_code": decision.reason_code,
        "decided_at": decision.decided_at.isoformat(),
        "decided_by": decision.decided_by,
        "policy_snapshot_ref": decision.policy_snapshot_ref,
        "validation_result_ref": decision.validation_result_ref,
        "authority_context_ref": decision.authority_context_ref,
    }


def _serialize_inspection(model: InspectionReadModel) -> dict[str, Any]:
    return {
        "run_id": model.run_id,
        "request_id": model.request_id,
        "current_state": model.current_state,
        "node_timeline": list(model.node_timeline),
        "terminal_reason": model.terminal_reason,
        "operator_frame_source": model.operator_frame_source,
        "operator_frames": [
            {
                "operator_frame_id": frame.operator_frame_id,
                "node_id": frame.node_id,
                "operator_kind": frame.operator_kind,
                "frame_state": frame.frame_state,
                "item_index": frame.item_index,
                "iteration_index": frame.iteration_index,
                "source_snapshot": dict(frame.source_snapshot or {}),
                "aggregate_outputs": dict(frame.aggregate_outputs or {}),
                "active_count": frame.active_count,
                "stop_reason": frame.stop_reason,
                "started_at": frame.started_at.isoformat() if frame.started_at is not None else None,
                "finished_at": frame.finished_at.isoformat() if frame.finished_at is not None else None,
            }
            for frame in model.operator_frames
        ],
        "completeness": {
            "is_complete": model.completeness.is_complete,
            "missing_evidence_refs": list(model.completeness.missing_evidence_refs),
        },
        "watermark": {
            "evidence_seq": model.watermark.evidence_seq,
            "source": model.watermark.source,
        },
        "evidence_refs": list(model.evidence_refs),
    }


__all__ = [
    "_json_loads_maybe",
    "_measured_summary",
    "_serialize_decision",
    "_serialize_inspection",
    "_submission_summary_from_row",
]
