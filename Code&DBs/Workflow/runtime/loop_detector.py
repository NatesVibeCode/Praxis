"""Loop detector for recurring dispatch failures.

Identifies repeating failure patterns, token burn, and consecutive failures
to prevent runaway retry loops from wasting resources.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal


@dataclass(frozen=True)
class FailureRecord:
    job_label: str
    failure_code: str
    timestamp: datetime
    token_cost: float
    attempt_number: int


@dataclass(frozen=True)
class LoopVerdict:
    action: Literal["proceed", "warn", "stop"]
    reasons: tuple[str, ...]
    total_failures_in_window: int
    total_token_burn: float
    consecutive_failures: int


class LoopDetector:
    """Detects retry loops across dispatch jobs.

    Detection strategies:
      - Consecutive failure loop: same job fails N+ times in a row -> STOP
      - Repeating failure code: same code appears 3+ times in window -> WARN
      - Token burn: cumulative cost in window exceeds threshold -> STOP
      - Pattern repetition: same (job, code) pair 2+ times -> WARN
    """

    def __init__(
        self,
        max_consecutive_failures: int = 3,
        token_burn_threshold: float = 10.0,
        window_minutes: int = 60,
    ) -> None:
        self._max_consecutive = max_consecutive_failures
        self._token_burn_threshold = token_burn_threshold
        self._window_minutes = window_minutes

        self._buffer: deque[FailureRecord] = deque()
        self._consecutive: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_failure(self, record: FailureRecord) -> None:
        """Append a failure to the ring buffer and bump the consecutive counter."""
        normalized = FailureRecord(
            job_label=record.job_label,
            failure_code=record.failure_code,
            timestamp=self._as_utc(record.timestamp),
            token_cost=record.token_cost,
            attempt_number=record.attempt_number,
        )
        with self._lock:
            self._buffer.append(normalized)
            self._consecutive[normalized.job_label] += 1

    def record_success(self, job_label: str, timestamp: datetime) -> None:
        """Reset the consecutive failure counter for *job_label*."""
        with self._lock:
            self._consecutive[job_label] = 0

    def check(self, job_label: str) -> LoopVerdict:
        """Return a verdict on whether *job_label* should proceed."""
        with self._lock:
            return self._check_unlocked(job_label)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _as_utc(timestamp: datetime) -> datetime:
        """Normalize timestamps to timezone-aware UTC for safe comparisons."""
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)

    def _window_cutoff(self, now: datetime) -> datetime:
        return now - timedelta(minutes=self._window_minutes)

    def _records_in_window(self, now: datetime) -> list[FailureRecord]:
        cutoff = self._window_cutoff(now)
        return [r for r in self._buffer if r.timestamp >= cutoff]

    def _check_unlocked(self, job_label: str) -> LoopVerdict:
        now = datetime.now(timezone.utc)
        records = self._records_in_window(now)

        reasons: list[str] = []
        worst_action: Literal["proceed", "warn", "stop"] = "proceed"

        def escalate(action: Literal["warn", "stop"]) -> None:
            nonlocal worst_action
            if action == "stop":
                worst_action = "stop"
            elif worst_action != "stop":
                worst_action = "warn"

        consecutive = self._consecutive.get(job_label, 0)

        # --- Strategy (a): consecutive failure loop ---
        if consecutive >= self._max_consecutive:
            reasons.append(
                f"{job_label} failed {consecutive} times consecutively "
                f"(threshold {self._max_consecutive})"
            )
            escalate("stop")

        # --- Strategy (b): repeating failure code ---
        code_counts: dict[str, int] = defaultdict(int)
        for r in records:
            code_counts[r.failure_code] += 1
        for code, count in code_counts.items():
            if count >= 3:
                reasons.append(
                    f"failure code '{code}' appeared {count} times in window"
                )
                escalate("warn")

        # --- Strategy (c): token burn ---
        total_burn = sum(r.token_cost for r in records)
        if total_burn >= self._token_burn_threshold:
            reasons.append(
                f"token burn {total_burn:.2f} exceeds threshold "
                f"{self._token_burn_threshold:.2f}"
            )
            escalate("stop")

        # --- Strategy (d): pattern repetition ---
        pair_counts: dict[tuple[str, str], int] = defaultdict(int)
        for r in records:
            pair_counts[(r.job_label, r.failure_code)] += 1
        for (jl, code), count in pair_counts.items():
            if count >= 2:
                reasons.append(
                    f"({jl}, {code}) repeated {count} times — likely same root cause"
                )
                escalate("warn")

        return LoopVerdict(
            action=worst_action,
            reasons=tuple(reasons),
            total_failures_in_window=len(records),
            total_token_burn=total_burn if records else 0.0,
            consecutive_failures=consecutive,
        )
