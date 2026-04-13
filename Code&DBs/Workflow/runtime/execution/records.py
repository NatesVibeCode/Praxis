"""Shared dataclass records and event-type constants for deterministic execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..domain import RunState

# Event type string constants
CLAIM_VALIDATED_EVENT_TYPE = "claim_validated"
CLAIM_REJECTED_EVENT_TYPE = "claim_rejected"
CLAIM_VALIDATION_RECEIPT_TYPE = "claim_validation_receipt"
WORKFLOW_QUEUED_EVENT_TYPE = "workflow_queued"
WORKFLOW_QUEUE_RECEIPT_TYPE = "workflow_queue_receipt"
WORKFLOW_STARTED_EVENT_TYPE = "workflow_started"
WORKFLOW_START_RECEIPT_TYPE = "workflow_start_receipt"
WORKFLOW_SUCCEEDED_EVENT_TYPE = "workflow_succeeded"
WORKFLOW_FAILED_EVENT_TYPE = "workflow_failed"
WORKFLOW_CANCELLED_EVENT_TYPE = "workflow_cancelled"
WORKFLOW_CANCELLED_RECEIPT_TYPE = "workflow_cancelled_receipt"
WORKFLOW_COMPLETION_RECEIPT_TYPE = "workflow_completion_receipt"
NODE_STARTED_EVENT_TYPE = "node_started"
NODE_START_RECEIPT_TYPE = "node_start_receipt"
NODE_SUCCEEDED_EVENT_TYPE = "node_succeeded"
NODE_FAILED_EVENT_TYPE = "node_failed"
NODE_CANCELLED_EVENT_TYPE = "node_cancelled"
NODE_SKIPPED_EVENT_TYPE = "node_skipped"
NODE_EXECUTION_RECEIPT_TYPE = "node_execution_receipt"


@dataclass(frozen=True, slots=True)
class NodeExecutionRecord:
    """Recorded runtime outcome for one executed node."""

    node_id: str
    task_name: str
    status: str
    outputs: Mapping[str, Any]
    started_at: datetime
    finished_at: datetime
    start_receipt_id: str
    completion_receipt_id: str
    failure_code: str | None = None
    operator_frame_id: str | None = None
    logical_parent_node_id: str | None = None
    iteration_index: int | None = None


@dataclass(frozen=True, slots=True)
class _RegularNodeStartRecord:
    start_receipt_id: str
    release_refs: tuple[dict[str, str], ...]
    lineage: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RunExecutionResult:
    """Deterministic execution outcome for one run."""

    workflow_id: str
    run_id: str
    request_id: str
    current_state: RunState
    terminal_reason_code: str
    node_order: tuple[str, ...]
    node_results: tuple[NodeExecutionRecord, ...]
    admitted_definition_ref: str | None
    admitted_definition_hash: str | None


__all__ = [
    "CLAIM_REJECTED_EVENT_TYPE",
    "CLAIM_VALIDATED_EVENT_TYPE",
    "CLAIM_VALIDATION_RECEIPT_TYPE",
    "NODE_CANCELLED_EVENT_TYPE",
    "NODE_EXECUTION_RECEIPT_TYPE",
    "NODE_FAILED_EVENT_TYPE",
    "NODE_SKIPPED_EVENT_TYPE",
    "NODE_START_RECEIPT_TYPE",
    "NODE_STARTED_EVENT_TYPE",
    "NODE_SUCCEEDED_EVENT_TYPE",
    "NodeExecutionRecord",
    "RunExecutionResult",
    "WORKFLOW_CANCELLED_EVENT_TYPE",
    "WORKFLOW_CANCELLED_RECEIPT_TYPE",
    "WORKFLOW_COMPLETION_RECEIPT_TYPE",
    "WORKFLOW_FAILED_EVENT_TYPE",
    "WORKFLOW_QUEUE_RECEIPT_TYPE",
    "WORKFLOW_QUEUED_EVENT_TYPE",
    "WORKFLOW_START_RECEIPT_TYPE",
    "WORKFLOW_STARTED_EVENT_TYPE",
    "WORKFLOW_SUCCEEDED_EVENT_TYPE",
    "_RegularNodeStartRecord",
]
