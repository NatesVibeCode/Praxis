"""Workflow history backed by durable workflow metrics, with local fallback."""

from __future__ import annotations

import json
import os
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Mapping

from .observability import get_workflow_metrics_view

if TYPE_CHECKING:
    from .workflow import WorkflowResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


_DEFAULT_WORKFLOW_HISTORY_BUFFER = int(
    os.environ.get("PRAXIS_WORKFLOW_HISTORY_BUFFER", "100")
)


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return _utc_now()


def _normalize_json_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value if isinstance(item, str) and item.strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if isinstance(item, str) and item.strip()]
    return None


def _workflow_result_from_metric_row(row: Mapping[str, Any]) -> "WorkflowResult":
    from .workflow.orchestrator import WorkflowResult

    provider_slug = str(row.get("provider_slug") or "unknown").strip() or "unknown"
    model_slug = row.get("model_slug")
    if isinstance(model_slug, str):
        model_slug = model_slug.strip() or None
    status = str(row.get("status") or "unknown").strip() or "unknown"
    latency_ms = _safe_int(row.get("latency_ms"))
    finished_at = _normalize_datetime(row.get("created_at"))
    started_at = finished_at - timedelta(milliseconds=max(latency_ms, 0))
    cost_usd = _safe_float(row.get("cost_usd"))
    outputs: dict[str, Any] = {
        "cost_usd": cost_usd,
        "total_cost_usd": cost_usd,
    }
    input_tokens = _safe_int(row.get("input_tokens"))
    output_tokens = _safe_int(row.get("output_tokens"))
    if input_tokens or output_tokens:
        outputs["usage"] = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }
    for key in (
        "tool_use_count",
        "cache_read_tokens",
        "cache_creation_tokens",
        "duration_api_ms",
    ):
        value = _safe_int(row.get(key))
        if value:
            outputs[key] = value

    capabilities = _normalize_json_list(row.get("capabilities"))
    review_target_modules = _normalize_json_list(row.get("review_target_modules"))
    author_model = row.get("author_model")
    if not isinstance(author_model, str) or not author_model.strip():
        author_model = f"{provider_slug}/{model_slug}" if model_slug else provider_slug

    return WorkflowResult(
        run_id=str(row.get("run_id") or ""),
        status=status,
        reason_code=str(row.get("failure_code") or status),
        completion="done" if status == "succeeded" else None,
        outputs=outputs,
        evidence_count=_safe_int(row.get("evidence_count")),
        started_at=started_at,
        finished_at=finished_at,
        latency_ms=latency_ms,
        provider_slug=provider_slug,
        model_slug=model_slug,
        adapter_type=str(row.get("adapter_type") or "unknown"),
        failure_code=row.get("failure_code"),
        attempts=max(1, _safe_int(row.get("attempts")) or 1),
        label=row.get("label") or row.get("workflow_label"),
        task_type=row.get("task_type"),
        capabilities=capabilities,
        author_model=author_model,
        reviews_workflow_id=row.get("reviews_workflow_id"),
        review_target_modules=review_target_modules,
        parent_run_id=row.get("parent_run_id"),
        persisted=True,
        sync_status="complete",
    )


class WorkflowHistory:
    """Workflow history backed by Postgres metrics, with process-local fallback."""

    __slots__ = ("_fallback_results", "_lock", "_max_size")

    def __init__(self, *, max_size: int = _DEFAULT_WORKFLOW_HISTORY_BUFFER) -> None:
        self._max_size = max_size
        self._fallback_results: deque[WorkflowResult] = deque(maxlen=max_size)
        self._lock = threading.Lock()

    def record_workflow(self, result: "WorkflowResult") -> None:
        """Append a completed workflow result to the local fallback buffer."""
        with self._lock:
            self._fallback_results.append(result)

    def _fallback_recent_workflows(self, limit: int) -> tuple["WorkflowResult", ...]:
        with self._lock:
            items = list(self._fallback_results)
        items.reverse()
        return tuple(items[:limit])

    def _recent_workflows_from_metrics(self, limit: int) -> tuple["WorkflowResult", ...]:
        metrics_view = get_workflow_metrics_view()
        rows = metrics_view.recent_workflows(limit=limit)
        return tuple(_workflow_result_from_metric_row(row) for row in rows)

    def _recent_workflows_snapshot(
        self,
        limit: int = 20,
    ) -> tuple[tuple["WorkflowResult", ...], dict[str, Any]]:
        """Return recent workflows plus the authority state used to assemble them."""
        bounded_limit = max(0, int(limit))
        if bounded_limit == 0:
            return (), {
                "workflow_history_source": "metrics",
                "workflow_history_status": "complete",
                "workflow_history_error": None,
                "metrics_query_failed": False,
            }

        recent: dict[str, WorkflowResult] = {}
        metrics_query_failed = False
        metrics_query_error: str | None = None
        try:
            for result in self._recent_workflows_from_metrics(limit=max(bounded_limit, self._max_size)):
                if result.run_id:
                    recent[result.run_id] = result
        except Exception as exc:
            recent.clear()
            metrics_query_failed = True
            metrics_query_error = f"{type(exc).__name__}: {exc}"

        fallback_included = False
        for result in self._fallback_recent_workflows(limit=self._max_size):
            if not result.run_id:
                continue
            current = recent.get(result.run_id)
            if current is None or result.finished_at >= current.finished_at:
                recent[result.run_id] = result
                fallback_included = True

        ordered = sorted(
            recent.values(),
            key=lambda item: (item.finished_at, item.run_id),
            reverse=True,
        )
        if metrics_query_failed:
            source = "fallback"
            status = "degraded"
        elif fallback_included:
            source = "mixed"
            status = "mixed"
        else:
            source = "metrics"
            status = "complete"
        return tuple(ordered[:bounded_limit]), {
            "workflow_history_source": source,
            "workflow_history_status": status,
            "workflow_history_error": metrics_query_error,
            "metrics_query_failed": metrics_query_failed,
        }

    def recent_workflows(self, limit: int = 20) -> tuple["WorkflowResult", ...]:
        """Return the most recent workflow results, newest-first.

        Durable metrics are queried first. The in-process fallback buffer is
        merged in only when it contains newer results that have not yet been
        persisted.
        """
        recent, _ = self._recent_workflows_snapshot(limit=limit)
        return recent

    def summary(self) -> dict[str, Any]:
        """Compact summary suitable for CLI / JSON rendering."""
        recent, snapshot = self._recent_workflows_snapshot(limit=self._max_size)
        total = len(recent)
        succeeded = sum(1 for r in recent if r.status == "succeeded")
        failed = total - succeeded
        pass_rate = round(succeeded / total, 4) if total else 0.0

        total_cost = 0.0
        for result in recent:
            outputs = dict(result.outputs) if result.outputs else {}
            raw_json = outputs.get("raw_json")
            if isinstance(raw_json, dict):
                value = raw_json.get("total_cost_usd")
                if value is not None:
                    total_cost += _safe_float(value)
                    continue
            if "total_cost_usd" in outputs:
                total_cost += _safe_float(outputs.get("total_cost_usd"))
                continue
            total_cost += _safe_float(outputs.get("cost_usd"))

        last_5 = [
            {
                "run_id": result.run_id,
                "status": result.status,
                "latency_ms": result.latency_ms,
                "provider_slug": result.provider_slug,
                "finished_at": result.finished_at.isoformat(),
            }
            for result in self.recent_workflows(limit=5)
        ]

        return {
            "total_workflows": total,
            "succeeded": succeeded,
            "failed": failed,
            "pass_rate": pass_rate,
            "total_cost_usd": round(total_cost, 6),
            "last_5": last_5,
            "workflow_history_source": snapshot["workflow_history_source"],
            "workflow_history_status": snapshot["workflow_history_status"],
            "workflow_history_error": snapshot["workflow_history_error"],
            "metrics_query_failed": snapshot["metrics_query_failed"],
        }


_WORKFLOW_HISTORY = WorkflowHistory()


def get_workflow_history() -> WorkflowHistory:
    """Return the module-level history adapter."""
    return _WORKFLOW_HISTORY
