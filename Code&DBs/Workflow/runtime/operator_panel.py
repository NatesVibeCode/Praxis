"""Operator panel: frozen snapshots and lane-cue recommendations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from runtime.posture import Posture


@dataclass(frozen=True)
class LaneCue:
    """Recommended operator posture with reasoning.

    ``recommended_posture`` is the string value of a ``runtime.posture.Posture``
    member. Producers MUST assign via ``Posture.X.value`` rather than a raw
    literal so the posture authority remains the single naming source.
    """

    recommended_posture: str
    confidence: float  # 0.0 - 1.0
    reasons: Tuple[str, ...]
    degraded_cause: Optional[str] = None


@dataclass(frozen=True)
class OperatorSnapshot:
    """Immutable point-in-time view of operator-relevant system state."""

    posture: str
    circuit_breaker_open: Tuple[str, ...]
    active_leases: int
    pending_jobs: int
    running_jobs: int
    recent_pass_rate: float
    recent_failure_codes: Dict[str, int]
    recent_failure_categories: Dict[str, int]
    recent_lineage_depth: int
    last_run_id: Optional[str]
    last_failure_category: Optional[str]
    last_activity_at: Optional[datetime]
    loop_warnings: int
    write_conflicts: int
    governance_blocks: int
    timestamp: datetime


class OperatorPanel:
    """Aggregates subsystem signals and produces snapshots / lane cues."""

    def __init__(self) -> None:
        self._circuit_breakers: Dict[str, bool] = {}
        self._active_leases: int = 0
        self._pending_jobs: int = 0
        self._running_jobs: int = 0
        self._recent_pass_rate: float = 1.0
        self._recent_failure_codes: Dict[str, int] = {}
        self._recent_failure_categories: Dict[str, int] = {}
        self._recent_lineage_depth: int = 0
        self._last_run_id: Optional[str] = None
        self._last_failure_category: Optional[str] = None
        self._last_activity_at: Optional[datetime] = None
        self._loop_warnings: int = 0
        self._write_conflicts: int = 0
        self._governance_blocks: int = 0
        self._posture: str = Posture.OBSERVE.value
        self._posture_explicit: bool = False

    # ---- registration methods ------------------------------------------------

    def register_circuit_breakers(self, cb_states: Dict[str, bool]) -> None:
        """Register provider circuit-breaker states (provider -> is_open)."""
        self._circuit_breakers = dict(cb_states)

    def register_lease_count(self, count: int) -> None:
        self._active_leases = count

    def register_job_counts(self, pending: int, running: int) -> None:
        self._pending_jobs = pending
        self._running_jobs = running

    def register_pass_rate(self, rate: float) -> None:
        self._recent_pass_rate = rate

    def register_failure_codes(self, codes: Dict[str, int]) -> None:
        self._recent_failure_codes = dict(codes)

    def register_failure_categories(self, categories: Dict[str, int]) -> None:
        self._recent_failure_categories = dict(categories)

    def register_lineage_depth(self, depth: int) -> None:
        self._recent_lineage_depth = max(int(depth), 0)

    def register_last_run_id(self, run_id: Optional[str]) -> None:
        self._last_run_id = run_id

    def register_last_failure_category(self, failure_category: Optional[str]) -> None:
        self._last_failure_category = failure_category

    def register_last_activity_at(self, activity_at: Optional[datetime]) -> None:
        self._last_activity_at = activity_at

    def register_loop_warnings(self, count: int) -> None:
        self._loop_warnings = count

    def register_write_conflicts(self, count: int) -> None:
        self._write_conflicts = count

    def register_governance_blocks(self, count: int) -> None:
        self._governance_blocks = count

    def register_posture(self, posture: str) -> None:
        self._posture = posture
        self._posture_explicit = True

    # ---- queries -------------------------------------------------------------

    def snapshot(self) -> OperatorSnapshot:
        """Return a frozen snapshot of current state.

        If no posture was explicitly registered, auto-syncs with the lane
        recommendation so the reported posture reflects actual system health
        rather than a stale default.
        """
        if not self._posture_explicit:
            lane = self.recommend_lane()
            self._posture = lane.recommended_posture

        open_providers = tuple(
            slug for slug, is_open in self._circuit_breakers.items() if is_open
        )
        return OperatorSnapshot(
            posture=self._posture,
            circuit_breaker_open=open_providers,
            active_leases=self._active_leases,
            pending_jobs=self._pending_jobs,
            running_jobs=self._running_jobs,
            recent_pass_rate=self._recent_pass_rate,
            recent_failure_codes=dict(self._recent_failure_codes),
            recent_failure_categories=dict(self._recent_failure_categories),
            recent_lineage_depth=self._recent_lineage_depth,
            last_run_id=self._last_run_id,
            last_failure_category=self._last_failure_category,
            last_activity_at=self._last_activity_at,
            loop_warnings=self._loop_warnings,
            write_conflicts=self._write_conflicts,
            governance_blocks=self._governance_blocks,
            timestamp=datetime.now(timezone.utc),
        )

    def recommend_lane(self) -> LaneCue:
        """Heuristic lane recommendation based on current subsystem signals."""
        open_cbs = [
            slug for slug, is_open in self._circuit_breakers.items() if is_open
        ]
        reasons: list[str] = []
        degraded_cause: Optional[str] = None

        # --- observe triggers -------------------------------------------------
        if open_cbs:
            degraded_cause = f"circuit breakers open: {', '.join(open_cbs)}"
            reasons.append(degraded_cause)
            confidence = min(0.6 + 0.1 * len(open_cbs), 1.0)
            return LaneCue(
                recommended_posture=Posture.OBSERVE.value,
                confidence=confidence,
                reasons=tuple(reasons),
                degraded_cause=degraded_cause,
            )

        if self._recent_pass_rate < 0.5:
            degraded_cause = f"pass rate critically low ({self._recent_pass_rate:.0%})"
            reasons.append(degraded_cause)
            confidence = 0.9 - self._recent_pass_rate  # worse rate -> higher conf
            return LaneCue(
                recommended_posture=Posture.OBSERVE.value,
                confidence=round(min(max(confidence, 0.5), 1.0), 2),
                reasons=tuple(reasons),
                degraded_cause=degraded_cause,
            )

        # --- operate triggers -------------------------------------------------
        if self._loop_warnings > 0:
            reasons.append(f"{self._loop_warnings} loop warning(s)")
            degraded_cause = f"{self._loop_warnings} loop warning(s) active"
        if self._write_conflicts > 0:
            reasons.append(f"{self._write_conflicts} write conflict(s)")
            if degraded_cause is None:
                degraded_cause = f"{self._write_conflicts} write conflict(s) unresolved"

        if reasons:
            return LaneCue(
                recommended_posture=Posture.OPERATE.value,
                confidence=0.7,
                reasons=tuple(reasons),
                degraded_cause=degraded_cause,
            )

        # --- build triggers ---------------------------------------------------
        if self._recent_pass_rate > 0.8:
            reasons.append("system healthy, pass rate above 80%")
            return LaneCue(
                recommended_posture=Posture.BUILD.value,
                confidence=round(self._recent_pass_rate, 2),
                reasons=tuple(reasons),
                degraded_cause=None,
            )

        # --- safe default -----------------------------------------------------
        reasons.append("no clear signal, defaulting to operate")
        return LaneCue(
            recommended_posture=Posture.OPERATE.value,
            confidence=0.5,
            reasons=tuple(reasons),
            degraded_cause=None,
        )
