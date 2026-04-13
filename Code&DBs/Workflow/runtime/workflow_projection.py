"""Project workflow results from the evidence timeline.

Event-sourced dispatch: the result is derived from the event stream,
not computed inline. This module reads the evidence timeline for a
run_id and projects the WorkflowResult from the node events that the
RuntimeOrchestrator already writes (NODE_STARTED, NODE_SUCCEEDED, etc.).

The projection replaces inline result construction. The evidence is the
source of truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from receipts.evidence import EvidenceRow, WorkflowEventV1, ReceiptV1


def _to_plain(obj: Any) -> Any:
    """Recursively convert frozen/custom mappings to plain dicts/lists."""
    if isinstance(obj, Mapping):
        return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_plain(v) for v in obj]
    return obj


def project_workflow_result(
    *,
    run_id: str,
    timeline: tuple[EvidenceRow, ...],
    spec_provider_slug: str,
    spec_model_slug: str | None,
    spec_adapter_type: str,
    spec_label: str | None = None,
    spec_capabilities: list[str] | None = None,
    spec_reviews_dispatch_id: str | None = None,
    spec_review_target_modules: list[str] | None = None,
) -> dict[str, Any]:
    """Project a workflow result dict from the evidence timeline.

    Reads through all events/receipts for a run and derives:
    - status: from terminal node outcomes
    - completion: from the LLM node's receipt outputs
    - structured_output: from the LLM node's receipt outputs
    - write_manifest: from the writer node's receipt outputs
    - verification: from the verifier node's receipt outputs
    - evidence_count: total events in timeline
    - latency_ms: first event to last event timestamps
    - author_model: from LLM node's receipt outputs

    Returns a dict (not WorkflowResult) to avoid circular imports.
    The caller wraps it in WorkflowResult.
    """

    if not timeline:
        return {
            "status": "failed",
            "reason_code": "dispatch.no_evidence",
            "completion": None,
            "outputs": {},
            "evidence_count": 0,
        }

    # Collect node receipts (these have the actual outputs)
    node_receipts: dict[str, ReceiptV1] = {}  # node_id → latest receipt
    first_at: datetime | None = None
    last_at: datetime | None = None
    terminal_reason = "runtime.workflow_succeeded"

    for row in timeline:
        record = row.record

        # Track timestamps
        if isinstance(record, WorkflowEventV1):
            ts = record.occurred_at
        elif isinstance(record, ReceiptV1):
            ts = record.finished_at
        else:
            continue

        if first_at is None or ts < first_at:
            first_at = ts
        if last_at is None or ts > last_at:
            last_at = ts

        # Collect node receipts (the ones with outputs)
        if isinstance(record, ReceiptV1) and record.node_id:
            node_receipts[record.node_id] = record

        # Track terminal reason from events
        if isinstance(record, WorkflowEventV1):
            if record.event_type in (
                "runtime.workflow_succeeded",
                "runtime.workflow_failed",
                "runtime.dependency_edge_not_satisfied",
            ):
                terminal_reason = record.event_type

    # Project outputs from node receipts
    outputs: dict[str, Any] = {}
    completion = None
    node_status = "succeeded"
    node_failure = None

    for node_id, receipt in node_receipts.items():
        if node_id == "terminal":
            continue

        receipt_outputs = _to_plain(receipt.outputs) if receipt.outputs else {}

        # LLM node
        if node_id == "llm":
            completion = receipt_outputs.get("completion")
            outputs["provider_slug"] = receipt_outputs.get("provider_slug", spec_provider_slug)
            outputs["model_slug"] = receipt_outputs.get("model_slug", spec_model_slug)
            outputs["latency_ms"] = receipt_outputs.get("latency_ms")
            outputs["execution_mode"] = receipt_outputs.get("execution_mode")

        # Parser node
        if node_id == "parser":
            outputs["structured_output"] = {
                "has_code": receipt_outputs.get("has_code", False),
                "file_paths": receipt_outputs.get("file_paths", []),
                "parse_strategy": receipt_outputs.get("parse_strategy"),
                "code_blocks": receipt_outputs.get("code_blocks", []),
                "explanation": receipt_outputs.get("explanation", ""),
            }

        # Writer node
        if node_id == "writer":
            outputs["write_manifest"] = receipt_outputs.get("write_manifest", {})

        # Verifier node
        if node_id == "verifier":
            outputs["verification"] = receipt_outputs

        # Track failures
        if receipt.status == "failed":
            node_status = "failed"
            node_failure = receipt.failure_code

    # Compute latency
    latency_ms = 0
    if first_at and last_at:
        latency_ms = int((last_at - first_at).total_seconds() * 1000)

    author_model = (
        f"{spec_provider_slug}/{spec_model_slug}"
        if spec_model_slug else spec_provider_slug
    )

    reason_code = terminal_reason
    if node_status == "failed" and terminal_reason == "runtime.workflow_succeeded":
        reason_code = node_failure or "runtime.workflow_failed"

    return {
        "run_id": run_id,
        "status": node_status,
        "reason_code": reason_code,
        "completion": completion,
        "outputs": outputs,
        "evidence_count": len(timeline),
        "started_at": first_at or datetime.now(timezone.utc),
        "finished_at": last_at or datetime.now(timezone.utc),
        "latency_ms": latency_ms,
        "provider_slug": spec_provider_slug,
        "model_slug": spec_model_slug,
        "adapter_type": spec_adapter_type,
        "failure_code": node_failure,
        "label": spec_label,
        "capabilities": spec_capabilities,
        "author_model": author_model,
        "reviews_workflow_id": spec_reviews_dispatch_id,
        "review_target_modules": spec_review_target_modules,
    }
