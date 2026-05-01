"""Retry orchestrator — two-level retry with failover for unified dispatch.

Drives the retry decision tree:
  1. Classify failure via failure_classifier
  2. If transient + retries remaining → same-model retry with backoff
  3. If provider-level + more models in chain → failover to next model
  4. Otherwise → terminal failure (failed or dead_letter)

All state lives in workflow_jobs rows. This module is stateless —
it reads the current job state, decides what to do, and returns
the decision. The caller (complete_job) writes it back.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from runtime.failure_classifier import (
    FailureCategory,
    FailureClassification,
    classify_failure,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

from runtime.auto_retry import compute_backoff


@dataclass(frozen=True)
class RetryDecision:
    """What to do after a job failure."""
    action: str          # "retry_same", "failover", "fail", "dead_letter"
    next_agent: str | None
    backoff_seconds: int
    reason: str

    @property
    def should_requeue(self) -> bool:
        return self.action in ("retry_same", "failover")


def decide(
    *,
    error_code: str,
    stderr: str = "",
    attempt: int,
    max_attempts: int,
    failover_chain: list[str] | None,
    resolved_agent: str | None,
    pre_classified: FailureClassification | None = None,
) -> RetryDecision:
    """Decide what to do after a job failure.

    Parameters
    ----------
    error_code : str
        The classified error code (e.g., "rate_limited", "exit_1", "connection_error").
    stderr : str
        Raw stderr from the failed execution.
    attempt : int
        Current attempt number (1-indexed, already incremented before this call).
    max_attempts : int
        Maximum attempts allowed for this job.
    failover_chain : list[str] | None
        Ordered list of agent slugs to try (e.g., ["openai/gpt-5.4", "anthropic/claude-opus-4-7"]).
    resolved_agent : str | None
        The agent that just failed.
    pre_classified : FailureClassification | None
        Optional pre-computed classification to reuse instead of classifying again.

    Returns
    -------
    RetryDecision
        What to do next: retry_same, failover, fail, or dead_letter.
    """
    chain = failover_chain or []

    # Hard cap: total attempts across all failovers = max_attempts * chain length.
    # Without this, rate-limit failovers bypass max_attempts entirely and loop forever.
    total_cap = max_attempts * max(len(chain), 1)
    if attempt > total_cap:
        return RetryDecision(
            action="fail",
            next_agent=None,
            backoff_seconds=0,
            reason=f"Total attempt cap exceeded: {attempt}/{total_cap} (chain={len(chain)})",
        )

    # Use the structured classifier for richer analysis
    classification = pre_classified or classify_failure(
        error_code,
        outputs={"stderr": stderr} if stderr else None,
    )

    # Find current position in failover chain. The route scorer may select any
    # candidate in the chain, not just the head, so failover must be based on
    # the actually failed agent and wrap to the next available candidate.
    chain_idx: int | None = None
    if resolved_agent and chain:
        try:
            chain_idx = chain.index(resolved_agent)
        except ValueError:
            chain_idx = None

    if not chain:
        next_failover_agent = None
    elif chain_idx is None:
        next_failover_agent = chain[0]
    elif len(chain) > 1:
        next_failover_agent = chain[(chain_idx + 1) % len(chain)]
    else:
        next_failover_agent = None
    has_failover_model = bool(next_failover_agent and next_failover_agent != resolved_agent)
    has_more_retries = attempt < max_attempts

    # Decision tree:
    #
    # 1. Non-retryable errors → immediate terminal
    if not classification.is_retryable:
        if classification.category in (FailureCategory.SCOPE_VIOLATION, FailureCategory.INPUT_ERROR):
            return RetryDecision(
                action="fail",
                next_agent=None,
                backoff_seconds=0,
                reason=f"Non-retryable: {classification.category.value} — {classification.recommended_action}",
            )
        return RetryDecision(
            action="dead_letter" if classification.severity == "critical" else "fail",
            next_agent=None,
            backoff_seconds=0,
            reason=f"Non-retryable: {classification.category.value}",
        )

    # 2. Rate-limited with more models → immediate failover (different provider)
    #    Don't waste attempts retrying a provider that's actively rate-limiting.
    if classification.category == FailureCategory.RATE_LIMIT and has_failover_model:
        next_agent = next_failover_agent
        return RetryDecision(
            action="failover",
            next_agent=next_agent,
            backoff_seconds=5,
            reason=f"Rate limited on {resolved_agent}, failover to {next_agent}",
        )

    # 3. Provider-level errors + more models → failover
    if classification.category in (
        FailureCategory.PROVIDER_ERROR,
        FailureCategory.NETWORK_ERROR,
        FailureCategory.INFRASTRUCTURE,
    ) and has_failover_model:
        next_agent = next_failover_agent
        return RetryDecision(
            action="failover",
            next_agent=next_agent,
            backoff_seconds=5,  # Short backoff between providers
            reason=f"Failover from {resolved_agent} to {next_agent} ({error_code})",
        )

    # 4. Other transient errors + retries remaining → same-model retry with backoff
    if classification.is_transient and has_more_retries:
        backoff = compute_backoff(attempt)
        return RetryDecision(
            action="retry_same",
            next_agent=resolved_agent,
            backoff_seconds=backoff,
            reason=f"Transient {error_code}, retry {attempt}/{max_attempts} in {backoff}s",
        )

    # 5. Retryable but not transient, still has attempts → retry same with longer backoff
    if has_more_retries:
        backoff = compute_backoff(attempt)
        return RetryDecision(
            action="retry_same",
            next_agent=resolved_agent,
            backoff_seconds=backoff,
            reason=f"Retryable {error_code}, attempt {attempt}/{max_attempts}",
        )

    # 6. Has more models but exhausted retries on current → failover
    if has_failover_model:
        next_agent = next_failover_agent
        return RetryDecision(
            action="failover",
            next_agent=next_agent,
            backoff_seconds=5,
            reason=f"Retries exhausted on {resolved_agent}, failover to {next_agent}",
        )

    # 7. Exhausted everything → terminal
    severity = classification.severity
    if severity == "critical" or error_code in ("sandbox_error", "setup_failure"):
        return RetryDecision(
            action="dead_letter",
            next_agent=None,
            backoff_seconds=0,
            reason=f"All attempts exhausted, {error_code} (dead_letter)",
        )

    return RetryDecision(
        action="fail",
        next_agent=None,
        backoff_seconds=0,
        reason=(
            f"All attempts exhausted: {attempt}/{max_attempts}, "
            f"chain position {(chain_idx + 1) if chain_idx is not None else 'unbound'}/{len(chain)}"
        ),
    )
