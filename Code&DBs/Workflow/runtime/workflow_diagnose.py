"""Dispatch diagnosis — pull receipt, route health, and failure classification
into a single actionable report for a failed (or succeeded) run.

Usage from CLI:  ``workflow diagnose <run_id>``
Usage from code: ``diagnose_run(run_id) -> dict[str, Any]``
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from receipts import ReceiptV1
from storage.postgres import PostgresEvidenceReader, PostgresStorageError

from .workflow import get_route_outcomes
from .failure_classifier import classify_failure




def _provider_health_summary(provider_slug: str) -> dict[str, Any]:
    """Summarise recent route health for *provider_slug*."""
    store = get_route_outcomes()
    recent = store.recent_outcomes(provider_slug, limit=5)
    consecutive_failures = store.consecutive_failures(provider_slug)
    healthy = store.is_route_healthy(provider_slug)

    outcomes: list[dict[str, Any]] = []
    for o in recent:
        outcomes.append({
            "status": o.status,
            "failure_code": o.failure_code,
            "latency_ms": o.latency_ms,
            "recorded_at": o.recorded_at.isoformat(),
        })

    return {
        "provider_slug": provider_slug,
        "healthy": healthy,
        "consecutive_failures": consecutive_failures,
        "recent_outcomes": outcomes,
    }


def _json_mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _isoformat(value: object) -> str | None:
    formatter = getattr(value, "isoformat", None)
    if callable(formatter):
        return str(formatter())
    return None


def _receipt_payload_from_evidence(receipt: ReceiptV1) -> dict[str, Any]:
    from .receipt_store import normalize_receipt_payload

    inputs = _json_mapping(receipt.inputs)
    outputs = _json_mapping(receipt.outputs)
    agent = (
        outputs.get("agent_slug")
        or outputs.get("agent")
        or outputs.get("author_model")
        or inputs.get("agent_slug")
        or inputs.get("agent")
        or receipt.executor_type
    )
    payload: dict[str, Any] = {
        "receipt_id": receipt.receipt_id,
        "workflow_id": receipt.workflow_id,
        "run_id": receipt.run_id,
        "request_id": receipt.request_id,
        "label": receipt.node_id or "",
        "job_label": receipt.node_id or "",
        "node_id": receipt.node_id or "",
        "attempt_no": receipt.attempt_no or 1,
        "started_at": _isoformat(receipt.started_at),
        "finished_at": _isoformat(receipt.finished_at),
        "timestamp": _isoformat(receipt.finished_at) or _isoformat(receipt.started_at),
        "executor_type": receipt.executor_type,
        "status": receipt.status,
        "failure_code": receipt.failure_code or "",
        "inputs": inputs,
        "outputs": outputs,
        "artifacts": [asdict(item) for item in receipt.artifacts],
        "decision_refs": [asdict(item) for item in receipt.decision_refs],
        "agent": str(agent or ""),
        "agent_slug": str(agent or ""),
        "author_model": outputs.get("author_model") or inputs.get("author_model") or str(agent or ""),
        "provider_slug": outputs.get("provider_slug") or inputs.get("provider_slug"),
        "model_slug": outputs.get("model_slug") or inputs.get("model_slug"),
        "latency_ms": int(outputs.get("duration_ms") or outputs.get("latency_ms") or 0),
        "cost_usd": float(outputs.get("cost_usd") or 0.0),
        "input_tokens": int(outputs.get("token_input") or outputs.get("input_tokens") or 0),
        "output_tokens": int(outputs.get("token_output") or outputs.get("output_tokens") or 0),
        "stdout_preview": outputs.get("stdout_preview"),
    }
    if outputs.get("failure_classification") is not None:
        payload["failure_classification"] = outputs.get("failure_classification")
    return normalize_receipt_payload(payload)


def _canonical_evidence_for_run(run_id: str):
    reader = PostgresEvidenceReader()
    resolved_run_id = reader.resolved_run_id(run_id)
    if resolved_run_id is None:
        return run_id, ()
    return resolved_run_id, tuple(reader.evidence_timeline(resolved_run_id))


def _latest_receipt_from_timeline(timeline) -> tuple[dict[str, Any], Any] | None:
    receipt_rows = [
        row
        for row in timeline
        if row.kind == "receipt" and isinstance(row.record, ReceiptV1)
    ]
    if not receipt_rows:
        return None
    latest = max(receipt_rows, key=lambda row: (row.evidence_seq, row.row_id))
    return _receipt_payload_from_evidence(latest.record), latest


def _evidence_error_response(run_id: str, exc: PostgresStorageError) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "receipt_found": False,
        "evidence_found": False,
        "evidence_source": "canonical_evidence_timeline",
        "status": None,
        "failure_code": None,
        "failure_class": None,
        "provider_health": None,
        "reason_code": exc.reason_code,
        "canonical_evidence_error": {
            "reason_code": exc.reason_code,
            "message": str(exc),
            "details": exc.details,
        },
        "recommendation": (
            "Canonical evidence is unavailable or invalid. Repair the evidence "
            "authority before trusting diagnosis output."
        ),
        "receipt": None,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def diagnose_run(run_id: str) -> dict[str, Any]:
    """Assemble a diagnosis dict for a dispatch identified by *run_id*.

    The *run_id* can be a full id or a suffix (last 8 chars).
    """
    try:
        resolved_run_id, timeline = _canonical_evidence_for_run(run_id)
    except PostgresStorageError as exc:
        return _evidence_error_response(run_id, exc)

    latest_receipt = _latest_receipt_from_timeline(timeline)

    if latest_receipt is None:
        return {
            "run_id": resolved_run_id,
            "receipt_found": False,
            "evidence_found": bool(timeline),
            "evidence_source": "canonical_evidence_timeline",
            "status": None,
            "failure_code": None,
            "failure_class": None,
            "provider_health": None,
            "recommendation": "No receipt evidence found for this run_id. Check that canonical evidence writes are enabled and the id is correct.",
            "receipt": None,
        }
    receipt, receipt_row = latest_receipt

    status = receipt.get("status")
    failure_code = receipt.get("failure_code")
    reason_code = receipt.get("reason_code")
    provider_slug = receipt.get("provider_slug", "unknown")
    model_slug = receipt.get("model_slug")
    latency_ms = receipt.get("latency_ms")
    outputs = receipt.get("outputs")

    failure_classification = None
    if status == "failed":
        failure_classification = classify_failure(failure_code, outputs=outputs)

    provider_health = _provider_health_summary(provider_slug)

    diagnosis = {
        "run_id": receipt.get("run_id", run_id),
        "receipt_found": True,
        "evidence_found": True,
        "evidence_source": "canonical_evidence_timeline",
        "evidence_refs": [row.row_id for row in timeline],
        "data_quality_issues": [
            {
                "reason_code": issue.reason_code,
                "kind": issue.kind,
                "row_id": issue.row_id,
                "evidence_seq": issue.evidence_seq,
                "hint": issue.hint,
            }
            for row in timeline
            for issue in row.data_quality_issues
        ],
        "selected_receipt_id": receipt_row.row_id,
        "status": status,
        "failure_code": failure_code,
        "reason_code": reason_code,
        "provider_slug": provider_slug,
        "model_slug": model_slug,
        "latency_ms": latency_ms,
        "outputs": outputs,
        "provider_health": provider_health,
        "receipt": receipt,
    }

    if failure_classification:
        diagnosis["failure_classification"] = failure_classification.to_dict()
        diagnosis["recommendation"] = failure_classification.recommended_action
        diagnosis["severity"] = failure_classification.severity
    else:
        diagnosis["recommendation"] = "Run succeeded — no action needed."

    return diagnosis
