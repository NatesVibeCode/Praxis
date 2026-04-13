"""Central retry manager — classify failures and decide retry/failover/terminal.

Single entry point for the complete failure resolution chain:
  1. Classify failure (pattern match error_code + stderr)
  2. Consult retry orchestrator (retry_same / failover / terminal)
  3. Return outcome for complete_job to act on

Circuit breaker recording is separate — it applies to all job
completions (succeeded, failed, cancelled), not just failures.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._context_building import _terminal_failure_classification
from ._routing import _failure_zone_lookup
from ._shared import _circuit_breakers

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = ["RetryOutcome", "resolve_failed_job", "record_provider_outcome"]


@dataclass(frozen=True)
class RetryOutcome:
    """Result of failure resolution — requeue or terminal."""
    requeue: bool
    final_status: str       # "ready" if requeue, else "failed" or "dead_letter"
    next_agent: str | None
    backoff_seconds: int
    failure_category: str
    failure_zone: str
    is_transient: bool
    reason: str


def resolve_failed_job(
    conn: SyncPostgresConnection,
    *,
    job_id: int,
    error_code: str,
    stdout_preview: str,
    exit_code: int | None,
) -> RetryOutcome:
    """Classify failure and decide retry/failover/terminal for a failed job.

    Full chain: classify -> check retryability -> retry orchestrator -> outcome.
    """
    # 1. Classify
    classification = _terminal_failure_classification(
        error_code=error_code,
        stderr=stdout_preview,
        exit_code=exit_code,
    )

    if classification is not None:
        failure_category = classification.category.value
        is_retryable = classification.is_retryable
        is_transient = classification.is_transient
    else:
        failure_category = "unknown"
        is_retryable = None
        is_transient = False

    failure_zone = _failure_zone_lookup(conn).get(failure_category, "internal")

    # 2. Non-retryable -> immediate terminal (no orchestrator needed)
    if is_retryable is False:
        return RetryOutcome(
            requeue=False,
            final_status="failed",
            next_agent=None,
            backoff_seconds=0,
            failure_category=failure_category,
            failure_zone=failure_zone,
            is_transient=False,
            reason=f"Non-retryable: {failure_category}",
        )

    # 3. Retry orchestrator
    job = conn.execute(
        "SELECT attempt, max_attempts, failover_chain, resolved_agent FROM workflow_jobs WHERE id = $1",
        job_id,
    )
    if not job:
        return RetryOutcome(
            requeue=False,
            final_status="failed",
            next_agent=None,
            backoff_seconds=0,
            failure_category=failure_category,
            failure_zone=failure_zone,
            is_transient=is_transient,
            reason="Job row not found during retry resolution",
        )

    row = job[0]
    from runtime.retry_orchestrator import decide

    decision = decide(
        error_code=error_code,
        stderr=stdout_preview,
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        failover_chain=row["failover_chain"],
        resolved_agent=row["resolved_agent"],
        pre_classified=classification,
    )

    if decision.should_requeue:
        return RetryOutcome(
            requeue=True,
            final_status="ready",
            next_agent=decision.next_agent,
            backoff_seconds=decision.backoff_seconds,
            failure_category=failure_category,
            failure_zone=failure_zone,
            is_transient=is_transient,
            reason=decision.reason,
        )

    return RetryOutcome(
        requeue=False,
        final_status="dead_letter" if decision.action == "dead_letter" else "failed",
        next_agent=None,
        backoff_seconds=0,
        failure_category=failure_category,
        failure_zone=failure_zone,
        is_transient=is_transient,
        reason=decision.reason,
    )


def record_provider_outcome(
    conn: SyncPostgresConnection,
    *,
    job_id: int,
    succeeded: bool,
    error_code: str,
) -> None:
    """Record job outcome in circuit breaker for per-provider health tracking.

    Called for all job completions (succeeded, failed, cancelled).
    When ROUTING_METRICS_FROZEN is True, logs the outcome but skips
    circuit breaker mutation so routing scores stay clean.
    """
    from ._shared import ROUTING_METRICS_FROZEN

    breakers = _circuit_breakers()
    if not breakers:
        return
    row = conn.execute(
        "SELECT resolved_agent, agent_slug FROM workflow_jobs WHERE id = $1", job_id,
    )
    if not row:
        return
    agent = row[0].get("resolved_agent") or row[0].get("agent_slug") or ""
    provider = agent.split("/")[0] if "/" in agent else agent
    if not provider:
        return

    if ROUTING_METRICS_FROZEN:
        logger.info(
            "Circuit breaker captured (frozen): provider=%s succeeded=%s code=%s",
            provider, succeeded, error_code,
        )
        return

    breakers.record_outcome(
        provider,
        succeeded=succeeded,
        failure_code=error_code if not succeeded else None,
    )
