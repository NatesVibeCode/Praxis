"""In-memory workflow history for status reporting.

Stores recent workflow results and exposes summary statistics.
Module-level singleton — import get_workflow_history() to access.
"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .workflow import WorkflowResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


_DEFAULT_WORKFLOW_HISTORY_BUFFER = int(os.environ.get("PRAXIS_WORKFLOW_HISTORY_BUFFER", "100"))


class WorkflowHistory:
    """Thread-safe ring buffer of recent workflow results."""

    __slots__ = ("_results", "_lock", "_max_size")

    def __init__(self, *, max_size: int = _DEFAULT_WORKFLOW_HISTORY_BUFFER) -> None:
        self._max_size = max_size
        self._results: deque[WorkflowResult] = deque(maxlen=max_size)
        self._lock = threading.Lock()

    def record_workflow(self, result: WorkflowResult) -> None:
        """Append a completed workflow result."""
        with self._lock:
            self._results.append(result)

    def recent_workflows(self, limit: int = 20) -> tuple[WorkflowResult, ...]:
        """Return the most recent *limit* results, newest first."""
        with self._lock:
            items = list(self._results)
        # deque appends to the right, so reverse for newest-first
        items.reverse()
        return tuple(items[:limit])

    def summary(self) -> dict[str, Any]:
        """Compact summary suitable for CLI / JSON rendering."""
        recent = self.recent_workflows(limit=self._max_size)
        total = len(recent)
        succeeded = sum(1 for r in recent if r.status == "succeeded")
        failed = total - succeeded
        pass_rate = round(succeeded / total, 4) if total else 0.0

        # Sum cost from outputs where available
        total_cost = 0.0
        for r in recent:
            outputs = dict(r.outputs) if r.outputs else {}
            raw_json = outputs.get("raw_json")
            if isinstance(raw_json, dict):
                val = raw_json.get("total_cost_usd")
                if val is not None:
                    try:
                        total_cost += float(val)
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            f"Invalid total_cost_usd in workflow history raw_json for run_id={r.run_id}"
                        ) from exc
            elif "total_cost_usd" in outputs:
                val = outputs["total_cost_usd"]
                if val is not None:
                    try:
                        total_cost += float(val)
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            f"Invalid total_cost_usd in workflow history outputs for run_id={r.run_id}"
                        ) from exc

        last_5 = [
            {
                "run_id": r.run_id,
                "status": r.status,
                "latency_ms": r.latency_ms,
                "provider_slug": r.provider_slug,
                "finished_at": r.finished_at.isoformat(),
            }
            for r in self.recent_workflows(limit=5)
        ]

        return {
            "total_workflows": total,
            "succeeded": succeeded,
            "failed": failed,
            "pass_rate": pass_rate,
            "total_cost_usd": round(total_cost, 6),
            "last_5": last_5,
        }


_WORKFLOW_HISTORY = WorkflowHistory()


def get_workflow_history() -> WorkflowHistory:
    """Return the module-level singleton."""
    return _WORKFLOW_HISTORY
