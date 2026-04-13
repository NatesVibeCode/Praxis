"""Batch auto-review orchestration.

Accumulates build results and fires ONE review dispatch covering the
entire batch — one tier up from the author, not straight to the top.

Review chain:
  economy (haiku) builds → mid (sonnet) reviews the batch
  mid (sonnet) builds     → frontier (opus) reviews the batch
  frontier (opus) builds  → no auto-review

Economics (10 haiku builds reviewed by sonnet):
  10 × $0.01 + 1 × $0.03 = $0.13 total
  vs opus doing all 10: 10 × $0.17 = $1.70
  = 13x savings

Reviews produce multi-dimensional quality scores, not just bug counts.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .workflow import WorkflowSpec, WorkflowResult
    from storage.postgres.connection import SyncPostgresConnection

_log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    return _utc_now()


# ---------------------------------------------------------------------------
# Tier classification
# ---------------------------------------------------------------------------

def _tier_of_model(provider_slug: str, model_slug: str | None) -> str:
    if not model_slug:
        return "mid"
    model_lower = model_slug.lower()
    if any(x in model_lower for x in ("haiku", "flash-lite", "economy")):
        return "economy"
    if any(x in model_lower for x in ("opus", "gpt-5.4", "gemini-2.5-pro")) and "mini" not in model_lower and "flash" not in model_lower:
        return "frontier"
    return "mid"


# One tier up for review — real provider/model-version slugs
_REVIEWER_FOR_TIER: dict[str, tuple[str, str]] = {
    "economy": ("anthropic", "claude-sonnet-4-6"),     # haiku → sonnet reviews
    "mid": ("anthropic", "claude-opus-4-6"),           # sonnet → opus reviews
    # frontier: no auto-review
}


def reviewer_for_author(provider_slug: str, model_slug: str | None) -> tuple[str, str] | None:
    """Return (reviewer_provider, reviewer_model) one tier above the author. None if no review needed."""
    tier = _tier_of_model(provider_slug, model_slug)
    return _REVIEWER_FOR_TIER.get(tier)


def should_review(result: "WorkflowResult") -> bool:
    """Should this result be queued for batch review?"""
    if result.status != "succeeded":
        return False
    return reviewer_for_author(result.provider_slug, result.model_slug) is not None


# ---------------------------------------------------------------------------
# Review batch accumulator
# ---------------------------------------------------------------------------

@dataclass
class _PendingReview:
    """One build result waiting for batch review."""
    run_id: str
    author_model: str
    label: str | None
    completion_preview: str  # first N chars
    task_type: str | None
    added_at: datetime


class ReviewBatchAccumulator:
    """Accumulates builds and fires one review for the batch.

    Flush triggers:
      - batch_size reached (default 5)
      - max_wait_seconds elapsed since first item (default 300 = 5 min)
      - manual flush via flush()
    """

    def __init__(
        self,
        *,
        batch_size: int = 10,
        max_wait_seconds: float = 600.0,
        preview_chars: int = 3000,
        conn: "SyncPostgresConnection | None" = None,
    ) -> None:
        self._batch_size = batch_size
        self._max_wait_seconds = max_wait_seconds
        self._preview_chars = preview_chars
        self._queue: list[_PendingReview] = []
        self._pending = self._queue
        self._lock = threading.Lock()
        self._first_added_at: float | None = None
        self._review_count = 0
        self._total_reviewed = 0
        self._conn = conn
        self._enable_persistence()

    def _enable_persistence(self) -> None:
        if self._conn is None:
            return
        self._ensure_table()
        self.load_pending_from_db()

    def set_conn(self, conn: "SyncPostgresConnection | None") -> None:
        if conn is None:
            return
        self._conn = conn
        self._enable_persistence()

    def _ensure_table(self) -> None:
        if self._conn is None:
            return
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS review_queue (
                   run_id TEXT PRIMARY KEY,
                   author_model TEXT NOT NULL,
                   job_label TEXT NOT NULL,
                   completion_preview TEXT,
                   task_type TEXT,
                   queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                   processed_at TIMESTAMPTZ
               )"""
        )

    def _persist_pending(self, pending: _PendingReview) -> None:
        if self._conn is None:
            return
        self._conn.execute(
            """INSERT INTO review_queue (run_id, author_model, job_label, completion_preview, task_type, queued_at)
               VALUES ($1, $2, $3, $4, $5, NOW())
               ON CONFLICT (run_id) DO NOTHING""",
            pending.run_id,
            pending.author_model,
            pending.label or pending.run_id,
            pending.completion_preview[:500],
            pending.task_type or "",
        )

    def load_pending_from_db(self) -> int:
        if self._conn is None:
            return 0
        rows = self._conn.execute(
            """SELECT run_id, author_model, job_label, completion_preview, task_type, queued_at
               FROM review_queue
               WHERE processed_at IS NULL
               ORDER BY queued_at ASC"""
        )

        loaded = 0
        with self._lock:
            existing_run_ids = {item.run_id for item in self._queue}
            for row in rows:
                run_id = row["run_id"]
                if run_id in existing_run_ids:
                    continue
                pending = _PendingReview(
                    run_id=run_id,
                    author_model=row["author_model"] or "",
                    label=row["job_label"] or None,
                    completion_preview=row["completion_preview"] or "",
                    task_type=row["task_type"] or None,
                    added_at=_coerce_datetime(row.get("queued_at")),
                )
                self._queue.append(pending)
                existing_run_ids.add(run_id)
                loaded += 1

            if self._first_added_at is None and self._queue:
                oldest = min(item.added_at for item in self._queue)
                age_seconds = max(0.0, (_utc_now() - oldest).total_seconds())
                self._first_added_at = time.monotonic() - age_seconds

        return loaded

    def add(self, result: "WorkflowResult") -> str | None:
        """Add a build result to the batch. Returns review run_id if flush triggered."""
        if not should_review(result):
            return None

        completion = result.completion or ""
        preview = completion[:self._preview_chars]
        author = getattr(result, "author_model", None) or f"{result.provider_slug}/{result.model_slug}"
        pending = _PendingReview(
            run_id=result.run_id,
            author_model=author,
            label=getattr(result, "label", None),
            completion_preview=preview,
            task_type=getattr(result, "task_type", None),
            added_at=_utc_now(),
        )

        with self._lock:
            self._queue.append(pending)
            if self._first_added_at is None:
                self._first_added_at = time.monotonic()
            self._persist_pending(pending)

            # Check flush triggers
            should_flush = (
                len(self._queue) >= self._batch_size
                or (self._first_added_at is not None
                    and time.monotonic() - self._first_added_at >= self._max_wait_seconds)
            )

        if should_flush:
            return self.flush()
        return None

    def flush(self) -> str | None:
        """Fire one review dispatch for all accumulated builds. Returns review run_id."""
        with self._lock:
            if not self._queue:
                return None
            batch = list(self._queue)
            self._queue.clear()
            self._first_added_at = None

        review_run_id = self._run_batch_review(batch)
        if review_run_id is not None and self._conn is not None:
            self._conn.execute(
                "UPDATE review_queue SET processed_at = NOW() WHERE run_id = ANY($1)",
                [item.run_id for item in batch],
            )
        return review_run_id

    def pending_count(self) -> int:
        with self._lock:
            return len(self._queue)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            pending = len(self._queue)
        return {
            "pending": pending,
            "reviews_dispatched": self._review_count,
            "total_builds_reviewed": self._total_reviewed,
            "batch_size": self._batch_size,
            "max_wait_seconds": self._max_wait_seconds,
        }

    def _run_batch_review(self, batch: list[_PendingReview]) -> str | None:
        """Build and dispatch one review covering the entire batch."""
        from .workflow.orchestrator import WorkflowSpec, run_workflow

        # Determine reviewer: one tier above the author
        author_provider = batch[0].author_model.split("/")[0] if "/" in batch[0].author_model else "anthropic"
        author_model = batch[0].author_model.split("/")[-1]
        reviewer = reviewer_for_author(author_provider, author_model)
        if reviewer is None:
            return None  # frontier — no review needed
        reviewer_provider, reviewer_model = reviewer

        # Build the combined review prompt
        sections = []
        run_ids = []
        for item in batch:
            run_ids.append(item.run_id)
            sections.append(
                f"### Module: {item.label or item.run_id}\n"
                f"Author: {item.author_model}\n"
                f"Task: {item.task_type or 'unknown'}\n"
                f"```\n{item.completion_preview}\n```\n"
            )

        combined_code = "\n---\n\n".join(sections)
        batch_task_types = {str(item.task_type or "").strip() for item in batch if str(item.task_type or "").strip()}
        review_label = (
            f"review:{next(iter(batch_task_types))}"
            if len(batch_task_types) == 1
            else f"batch-review:{len(batch)}-modules"
        )

        review_prompt = f"""You are reviewing a batch of {len(batch)} code outputs. Score EACH module on seven dimensions using a 0-3 scale.

## Scoring Rubric

**OUTCOME (does it achieve the goal?):**
  0 = missed — doesn't accomplish what was asked, solves the wrong problem
  1 = partial — addresses the task but with significant gaps or missing pieces
  2 = delivered — accomplishes the core ask, minor rough edges
  3 = nailed — fully achieves the goal, ready to use as-is

**CORRECTNESS (logic + behavior):**
  0 = broken — logic bugs, wrong algorithms, incorrect behavior
  1 = deficient — material errors that need fixing before use
  2 = acceptable — minor issues, nothing blocking
  3 = solid — logic is sound, edge cases handled

**SAFETY (security + thread safety + resources):**
  0 = dangerous — injection vulnerabilities, unguarded shared state, resource leaks
  1 = fragile — race conditions, missing timeouts, broad exception handling
  2 = adequate — common failures handled, no security holes
  3 = hardened — thread-safe, input-validated, timeout-protected

**RESILIENCE (error handling + failure transparency):**
  0 = deceptive — serves wrong data silently, swallows critical exceptions, TOCTOU races
  1 = misleading — broad except clauses that hide real errors, missing validation
  2 = mostly honest — failures are logged, major paths raise
  3 = transparent — all failure paths produce explicit errors, no silent corruption

**INTEGRATION (contracts + imports + patterns):**
  0 = broken — imports don't resolve, wrong types, protocol mismatch
  1 = loose — uses private APIs, wrong module paths, fragile coupling
  2 = adequate — correct interfaces, standard patterns
  3 = clean — matches project patterns exactly, all imports verified

**SCOPE (instruction adherence):**
  0 = off-task — built something that wasn't asked for, major scope creep
  1 = drifted — added unrequested features, extra files, unnecessary docs
  2 = mostly focused — minor additions beyond the ask
  3 = precise — did exactly what was asked, nothing more

**DILIGENCE (verification + awareness):**
  0 = blind — wrote code without reading existing modules, assumed imports exist
  1 = careless — partially checked, but missed obvious existing patterns
  2 = adequate — followed most project conventions, checked main dependencies
  3 = thorough — verified all imports, followed all existing patterns, read before writing

## Code to Review

{combined_code}

## Required Output

Output a JSON object with this exact shape:
```json
{{
  "modules": [
    {{
      "module": "label or run_id",
      "scores": {{"outcome": 0-3, "correctness": 0-3, "safety": 0-3, "resilience": 0-3, "integration": 0-3, "scope": 0-3, "diligence": 0-3}},
      "composite": average of the seven scores,
      "findings": [
        {{"severity": "high|medium|low", "dimension": "outcome|correctness|safety|resilience|integration|scope|diligence", "issue": "description"}}
      ]
    }}
  ],
  "batch_summary": {{
    "modules_reviewed": N,
    "avg_composite": N.N,
    "worst_dimension": "the dimension with the lowest average score",
    "author_model": "provider/model"
  }}
}}
```

Be rigorous. A score of 3 means you found NOTHING wrong in that dimension. Default to 2 unless you have specific evidence for 3 or specific findings for 1/0."""

        review_spec = WorkflowSpec(
            prompt=review_prompt,
            provider_slug=reviewer_provider,
            model_slug=reviewer_model,
            timeout=600,
            max_tokens=16384,
            temperature=0.0,
            label=review_label,
            skip_auto_review=True,
            reviews_workflow_id=run_ids[0] if len(run_ids) == 1 else None,
            review_target_modules=[item.label or item.run_id for item in batch],
        )

        result = run_workflow(review_spec)
        self._review_count += 1
        self._total_reviewed += len(batch)
        _log.info("dispatched batch review [run_id=%s] for %d builds", result.run_id, len(batch))
        return result.run_id


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_ACCUMULATOR: ReviewBatchAccumulator | None = None
_ACCUMULATOR_LOCK = threading.Lock()


def get_review_accumulator(
    conn: "SyncPostgresConnection | None" = None,
) -> ReviewBatchAccumulator:
    global _ACCUMULATOR
    if _ACCUMULATOR is None:
        with _ACCUMULATOR_LOCK:
            if _ACCUMULATOR is None:
                _ACCUMULATOR = ReviewBatchAccumulator(conn=conn)
    elif conn is not None:
        _ACCUMULATOR.set_conn(conn)
    return _ACCUMULATOR


def queue_auto_review(
    result: "WorkflowResult",
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    """Add a build result to the batch accumulator. May trigger a batch review."""
    accumulator = get_review_accumulator(conn=conn)
    return accumulator.add(result)
