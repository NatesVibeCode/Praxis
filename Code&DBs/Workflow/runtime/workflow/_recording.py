"""Workflow result recording and lifecycle event emission."""

from __future__ import annotations
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ..route_outcomes import RouteOutcome
from ..failure_projection import project_failure_classification
from . import _capabilities as _workflow_caps

if TYPE_CHECKING:
    from .orchestrator import WorkflowResult, WorkflowSpec


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def record_workflow_result(
    result: WorkflowResult,
    *,
    spec: WorkflowSpec | None = None,
    recorded_at: datetime | None = None,
) -> None:
    """Fan out a completed workflow result to platform observers."""

    caps = _workflow_caps.WORKFLOW_CAPABILITIES
    recorded_at = recorded_at or _utc_now()
    if spec is not None and getattr(spec, "task_type", None) and result.task_type != spec.task_type:
        result = replace(result, task_type=spec.task_type)

    failure_projection: dict[str, object] | None = None
    if result.status == "failed":
        stderr_preview = ""
        if isinstance(result.outputs, Mapping):
            stderr_preview = str(result.outputs.get("stderr", "") or "")
        failure_projection = project_failure_classification(
            failure_category=result.failure_code or "",
            is_transient=False,
            stdout_preview=stderr_preview,
        )

    route_key = f"{result.provider_slug}"
    if result.model_slug:
        route_key = f"{route_key}/{result.model_slug}"
    if result.adapter_type:
        route_key = f"{route_key}@{result.adapter_type}"

    _workflow_caps.ROUTE_OUTCOMES.record_outcome(
        RouteOutcome(
            provider_slug=result.provider_slug,
            model_slug=result.model_slug,
            adapter_type=result.adapter_type,
            status=result.status,
            failure_code=result.failure_code,
            latency_ms=result.latency_ms,
            recorded_at=recorded_at,
            route_key=route_key,
            failure_category=str((failure_projection or {}).get("category") or ""),
            is_retryable=(
                bool((failure_projection or {}).get("is_retryable"))
                if failure_projection is not None
                else None
            ),
            is_transient=(
                bool((failure_projection or {}).get("is_transient"))
                if failure_projection is not None
                else None
            ),
            run_id=result.run_id,
        )
    )

    if _workflow_caps.CIRCUIT_BREAKERS:
        _workflow_caps.CIRCUIT_BREAKERS.record_outcome(
            result.provider_slug,
            succeeded=(result.status == "succeeded"),
            failure_code=result.failure_code if result.status != "succeeded" else None,
        )

    if spec is not None and spec.task_type and result.model_slug:
        _record_task_type_route_feedback(result=result, spec=spec)

    if _workflow_caps.WORKFLOW_HISTORY:
        _workflow_caps.WORKFLOW_HISTORY.record_workflow(result)

    if _workflow_caps.COST_TRACKER:
        _workflow_caps.COST_TRACKER.record_cost(result)

    if _workflow_caps.TRUST_SCORER:
        _workflow_caps.TRUST_SCORER.update(
            result.provider_slug,
            result.model_slug,
            result.status == "succeeded",
        )

    if caps.receipt_writer:
        caps.receipt_writer(result)

    if caps.obs_hub:
        caps.obs_hub().ingest_receipt(_build_observability_receipt(result))

    if _workflow_caps.WORKFLOW_METRICS_VIEW:
        _workflow_caps.WORKFLOW_METRICS_VIEW.record_workflow(result)

    if caps.completion_notifier:
        caps.completion_notifier(result)

    from ..review_tracker import get_review_tracker

    get_review_tracker().record_review(result)

    if result.capabilities:
        from ..capability_feedback import get_capability_tracker

        get_capability_tracker().record_outcome(result, capabilities=result.capabilities)

    if spec is not None and not spec.skip_auto_review:
        from ..auto_review import queue_auto_review
        from storage.postgres import SyncPostgresConnection, get_workflow_pool

        try:
            conn = SyncPostgresConnection(get_workflow_pool())
        except Exception:
            return
        queue_auto_review(result, conn=conn)


def emit_workflow_started(*, spec: WorkflowSpec, run_id: str) -> None:
    """Emit the workflow.started lifecycle event when available."""

    caps = _workflow_caps.WORKFLOW_CAPABILITIES
    if not caps.event_logger:
        return
    parent_run_id = spec.parent_run_id or spec.reviews_workflow_id
    caps.event_logger(
        caps.event_type_started,
        source="workflow.runtime",
        run_id=run_id,
        provider=spec.provider_slug,
        model=spec.model_slug,
        payload={
            **{"run_id": run_id, "adapter_type": spec.adapter_type, "timeout": spec.timeout, "tier": spec.tier, "label": spec.label},
            **({"parent_run_id": parent_run_id} if parent_run_id is not None else {}),
            **({"task_type": spec.task_type} if spec.task_type is not None else {}),
            **(
                {"reviews_workflow_id": spec.reviews_workflow_id}
                if spec.reviews_workflow_id is not None
                else {}
            ),
            **(
                {"review_target_modules": spec.review_target_modules}
                if spec.review_target_modules is not None
                else {}
            ),
        },
    )


def emit_workflow_finished(*, spec: WorkflowSpec, result: WorkflowResult) -> None:
    """Emit the terminal workflow lifecycle event when available."""

    caps = _workflow_caps.WORKFLOW_CAPABILITIES
    if not caps.event_logger:
        return

    payload = {
        "status": result.status,
        "reason_code": result.reason_code,
        "latency_ms": result.latency_ms,
        "attempts": result.attempts,
        "evidence_count": result.evidence_count,
    }
    if result.label is not None:
        payload["label"] = result.label
    if result.task_type is not None:
        payload["task_type"] = result.task_type
    parent_run_id = result.parent_run_id or result.reviews_workflow_id
    if parent_run_id is not None:
        payload["parent_run_id"] = parent_run_id
    if result.reviews_workflow_id is not None:
        payload["reviews_workflow_id"] = result.reviews_workflow_id
    if result.review_target_modules is not None:
        payload["review_target_modules"] = result.review_target_modules
    event_type = caps.event_type_completed
    if result.status != "succeeded":
        event_type = caps.event_type_failed
        payload["failure_code"] = result.failure_code

    caps.event_logger(
        event_type,
        source="workflow.runtime",
        run_id=result.run_id,
        provider=spec.provider_slug,
        model=spec.model_slug,
        payload=payload,
    )


def _build_observability_receipt(result: WorkflowResult) -> dict[str, object]:
    """Normalize a WorkflowResult into the observability hub receipt shape."""

    caps = _workflow_caps.WORKFLOW_CAPABILITIES
    label = result.label or "workflow"
    attempt_no = max(1, int(result.attempts or 1))
    receipt: dict[str, object] = {
        "receipt_id": f"receipt:{result.run_id}:{label}:{attempt_no}",
        "workflow_id": result.run_id,
        "agent_slug": result.author_model
        or (f"{result.provider_slug}/{result.model_slug}" if result.model_slug else result.provider_slug),
        "provider_slug": result.provider_slug,
        "model_slug": result.model_slug,
        "status": result.status,
        "cost": float(
            (
                (result.outputs.get("cost_usd") if isinstance(result.outputs, Mapping) else None)
                or (result.outputs.get("cost") if isinstance(result.outputs, Mapping) else None)
                or 0.0
            )
        ),
        "latency_seconds": float(result.latency_ms) / 1000.0,
        "job_label": label,
        "label": label,
        "node_id": label,
        "attempt_no": attempt_no,
        "timestamp": result.finished_at.isoformat(),
        "failure_code": result.failure_code,
        "run_id": result.run_id,
    }
    parent_run_id = result.parent_run_id or result.reviews_workflow_id
    if parent_run_id is not None:
        receipt["parent_run_id"] = parent_run_id
    if result.task_type:
        receipt["task_type"] = result.task_type
    if result.reviews_workflow_id:
        receipt["reviews_workflow_id"] = result.reviews_workflow_id
    if result.review_target_modules:
        receipt["review_target_modules"] = list(result.review_target_modules)
    if result.status != "succeeded" and result.failure_code and caps.failure_classifier:
        classification = caps.failure_classifier(result.failure_code, outputs=dict(result.outputs))
        receipt["failure_category"] = classification.category.value
        if hasattr(classification, "failure_zone"):
            receipt["failure_zone"] = getattr(classification, "failure_zone")
        elif hasattr(classification, "zone"):
            receipt["failure_zone"] = getattr(classification, "zone")
        if hasattr(classification, "is_transient"):
            receipt["is_transient"] = classification.is_transient
        if hasattr(classification, "is_retryable"):
            receipt["is_retryable"] = classification.is_retryable
    return receipt


def _record_task_type_route_feedback(*, result: WorkflowResult, spec: WorkflowSpec) -> None:
    from ..task_type_router import TaskTypeRouter
    from storage.postgres import SyncPostgresConnection, get_workflow_pool

    try:
        conn = SyncPostgresConnection(get_workflow_pool())
    except Exception:
        return

    try:
        TaskTypeRouter(conn).record_outcome(
            spec.task_type,
            result.provider_slug,
            result.model_slug,
            succeeded=(result.status == "succeeded"),
            failure_code=result.failure_code,
        )
    except Exception:
        return
