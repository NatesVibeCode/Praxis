"""Dispatch diagnosis — pull receipt, route health, and failure classification
into a single actionable report for a failed (or succeeded) run.

Usage from CLI:  ``workflow diagnose <run_id>``
Usage from code: ``diagnose_run(run_id) -> dict[str, Any]``
"""

from __future__ import annotations

from typing import Any

from . import receipt_store
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


# ---------------------------------------------------------------------------
# Receipt search
# ---------------------------------------------------------------------------

def _find_receipt(run_id: str) -> dict[str, Any] | None:
    """Search Postgres receipts for one whose run_id matches *run_id*.

    Supports both full run_id match and suffix match (last 8 chars) so the
    operator can type ``workflow diagnose abcd1234`` instead of the full id.
    """
    rec = receipt_store.find_receipt_by_run_id(run_id)
    return rec.to_dict() if rec else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def diagnose_run(run_id: str) -> dict[str, Any]:
    """Assemble a diagnosis dict for a dispatch identified by *run_id*.

    The *run_id* can be a full id or a suffix (last 8 chars).
    """
    receipt = _find_receipt(run_id)

    if receipt is None:
        return {
            "run_id": run_id,
            "receipt_found": False,
            "status": None,
            "failure_code": None,
            "failure_class": None,
            "provider_health": None,
            "recommendation": "No receipt found for this run_id. Check that receipts are enabled and the id is correct.",
            "receipt": None,
        }

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
