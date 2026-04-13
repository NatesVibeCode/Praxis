"""Dynamic timeout calculator with complexity tiers and historical p95 blending."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from enum import Enum


class ComplexityTier(Enum):
    TRIVIAL = 0.5
    STANDARD = 1.0
    COMPLEX = 2.0
    FRONTIER = 3.0

    @property
    def multiplier(self) -> float:
        return self.value


@dataclass(frozen=True)
class TimeoutConfig:
    base_seconds: int
    complexity_tier: ComplexityTier
    stage_default: int | None
    historical_p95_seconds: float | None


class DynamicTimeoutCalculator:
    """Calculates timeouts based on complexity, historical durations, and stage defaults."""

    HISTORY_SIZE = 200

    def __init__(
        self,
        default_timeout: int = 300,
        min_timeout: int = 60,
        max_timeout: int = 1800,
    ) -> None:
        self.default_timeout = default_timeout
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout
        self.stage_defaults: dict[str, int] = {}
        self._history: dict[str, deque[tuple[float, ComplexityTier]]] = {}

    def record_duration(
        self, job_label: str, duration_seconds: float, complexity: ComplexityTier
    ) -> None:
        if job_label not in self._history:
            self._history[job_label] = deque(maxlen=self.HISTORY_SIZE)
        self._history[job_label].append((duration_seconds, complexity))

    def _p95(self, job_label: str) -> float | None:
        hist = self._history.get(job_label)
        if not hist:
            return None
        durations = sorted(d for d, _ in hist)
        idx = min(math.floor(len(durations) * 0.95), len(durations) - 1)
        return durations[idx]

    def calculate(
        self,
        job_label: str,
        complexity: ComplexityTier,
        stage: str | None = None,
    ) -> int:
        # Step a: base from default * complexity multiplier
        base = self.default_timeout * complexity.multiplier

        # Step b: if historical data, use p95 * 1.5
        p95 = self._p95(job_label)
        if p95 is not None:
            base = p95 * 1.5

        # Step c: blend with stage default if present
        stage_val = self.stage_defaults.get(stage) if stage else None
        if stage_val is not None:
            base = (base + stage_val) / 2.0

        # Clamp
        result = max(self.min_timeout, min(self.max_timeout, int(base)))
        return result
