"""Auto-retry engine: classify failures, decide retry strategy, track attempts."""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass, field
from typing import Dict, Tuple


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FailureCategory(enum.Enum):
    TRANSIENT = "transient"           # network timeout, rate limit, idle timeout
    TRIAGEABLE = "triageable"         # wrong format, partial output, test failure
    NON_RETRYABLE = "non_retryable"   # scope violation, isolation breach, injection
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryClassification:
    category: FailureCategory
    retryable: bool
    reason: str
    suggested_action: str  # retry_same_agent | retry_escalated_agent | retry_with_context | skip | halt


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int
    escalate_after: int              # retry count before agent tier escalation
    backoff_seconds: Tuple[int, ...]  # per-attempt backoff


def compute_backoff(attempt: int, schedule: tuple[int, ...] = (5, 15, 60)) -> int:
    """Shared backoff calculation. Returns seconds to wait for the given attempt (1-indexed)."""
    return schedule[min(max(attempt - 1, 0), len(schedule) - 1)]


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    attempt_number: int
    action: str
    wait_seconds: int
    escalate_tier: bool


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

_TRANSIENT_PATTERNS = [
    re.compile(r"rate.?limit", re.IGNORECASE),
    re.compile(r"\b429\b"),
    re.compile(r"timeout", re.IGNORECASE),
    re.compile(r"idle.?timeout", re.IGNORECASE),
]

_NON_RETRYABLE_PATTERNS = [
    re.compile(r"scope.?violation", re.IGNORECASE),
    re.compile(r"isolation", re.IGNORECASE),
    re.compile(r"prompt.?injection", re.IGNORECASE),
]

_TRIAGEABLE_PATTERNS = [
    re.compile(r"FAILED"),
    re.compile(r"AssertionError", re.IGNORECASE),
]


class AutoRetryManager:
    """Classify failures and decide whether/how to retry."""

    def __init__(self, policy: RetryPolicy | None = None) -> None:
        self._policy = policy or RetryPolicy(
            max_retries=3,
            escalate_after=2,
            backoff_seconds=(5, 15, 60),
        )
        # job_label -> list of classifications
        self._attempts: Dict[str, list[RetryClassification]] = {}

    # -- public API ----------------------------------------------------------

    def classify(
        self,
        failure_code: str,
        stderr: str,
        exit_code: int | None = None,
    ) -> RetryClassification:
        """Determine the category of a failure from its signals."""
        stderr_lower = stderr.lower()

        # --- exit code 124 is the standard timeout exit code ----------------
        if exit_code == 124:
            return RetryClassification(
                category=FailureCategory.TRANSIENT,
                retryable=True,
                reason="Process timed out (exit 124)",
                suggested_action="retry_same_agent",
            )

        # --- TRANSIENT patterns ---------------------------------------------
        if "timeout" in stderr_lower:
            return RetryClassification(
                category=FailureCategory.TRANSIENT,
                retryable=True,
                reason="Timeout detected in stderr",
                suggested_action="retry_same_agent",
            )

        if "rate limit" in stderr_lower or "429" in stderr:
            return RetryClassification(
                category=FailureCategory.TRANSIENT,
                retryable=True,
                reason="Rate limit / 429 detected",
                suggested_action="retry_same_agent",
            )

        if "idle timeout" in stderr_lower:
            return RetryClassification(
                category=FailureCategory.TRANSIENT,
                retryable=True,
                reason="Idle timeout detected",
                suggested_action="retry_same_agent",
            )

        # --- NON-RETRYABLE patterns -----------------------------------------
        if "scope violation" in stderr_lower or "isolation" in stderr_lower:
            return RetryClassification(
                category=FailureCategory.NON_RETRYABLE,
                retryable=False,
                reason="Scope violation or isolation breach",
                suggested_action="halt",
            )

        if "prompt injection" in stderr_lower:
            return RetryClassification(
                category=FailureCategory.NON_RETRYABLE,
                retryable=False,
                reason="Prompt injection detected",
                suggested_action="halt",
            )

        # --- TRIAGEABLE patterns --------------------------------------------
        if "json" in stderr_lower and "parse" in stderr_lower:
            return RetryClassification(
                category=FailureCategory.TRIAGEABLE,
                retryable=True,
                reason="JSON parse error",
                suggested_action="retry_with_context",
            )

        if "FAILED" in stderr or "AssertionError" in stderr or "AssertionError" in stderr:
            return RetryClassification(
                category=FailureCategory.TRIAGEABLE,
                retryable=True,
                reason="Test failure detected",
                suggested_action="retry_with_context",
            )

        # --- UNKNOWN (treat as triageable) ----------------------------------
        return RetryClassification(
            category=FailureCategory.UNKNOWN,
            retryable=True,
            reason=f"Unrecognised failure: {failure_code}",
            suggested_action="retry_with_context",
        )

    def should_retry(
        self,
        job_label: str,
        classification: RetryClassification,
    ) -> RetryDecision:
        """Decide whether to retry based on attempt history and policy."""
        attempts = self._attempts.get(job_label, [])
        attempt_number = len(attempts) + 1  # next attempt (1-indexed)

        # Non-retryable -> immediate halt
        if not classification.retryable:
            return RetryDecision(
                retry=False,
                attempt_number=attempt_number,
                action="halt",
                wait_seconds=0,
                escalate_tier=False,
            )

        # Exhausted retries
        if len(attempts) >= self._policy.max_retries:
            return RetryDecision(
                retry=False,
                attempt_number=attempt_number,
                action="skip",
                wait_seconds=0,
                escalate_tier=False,
            )

        # Compute backoff
        idx = min(len(attempts), len(self._policy.backoff_seconds) - 1)
        wait = self._policy.backoff_seconds[idx]

        # Escalation check
        escalate = len(attempts) >= self._policy.escalate_after

        action = classification.suggested_action
        if escalate and action == "retry_same_agent":
            action = "retry_escalated_agent"

        return RetryDecision(
            retry=True,
            attempt_number=attempt_number,
            action=action,
            wait_seconds=wait,
            escalate_tier=escalate,
        )

    def record_attempt(
        self,
        job_label: str,
        classification: RetryClassification,
    ) -> None:
        """Track an attempt for the given job."""
        self._attempts.setdefault(job_label, []).append(classification)

    # -- introspection -------------------------------------------------------

    def attempt_count(self, job_label: str) -> int:
        return len(self._attempts.get(job_label, []))
