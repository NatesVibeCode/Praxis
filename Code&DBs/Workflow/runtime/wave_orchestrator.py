"""Wave orchestration engine — waves, jobs, gates."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Enums & value objects
# ---------------------------------------------------------------------------

class WaveStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class JobState:
    job_label: str
    wave_id: str
    status: str = "pending"  # pending | running | succeeded | failed
    started_at: datetime | None = None
    completed_at: datetime | None = None
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True)
class GateVerdict:
    wave_id: str
    passed: bool
    reason: str
    evidence: dict = field(default_factory=dict)
    judged_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class WaveState:
    wave_id: str
    status: WaveStatus
    jobs: tuple[JobState, ...] = ()
    gate_verdict: GateVerdict | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class WaveSnapshot:
    orch_id: str
    waves: tuple[WaveState, ...] = ()
    current_wave: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class WaveOrchestrator:
    """Manages waves of jobs with intra-wave dependency ordering."""

    def __init__(self, orch_id: str) -> None:
        self._dag_id = orch_id
        self._created_at = datetime.now(timezone.utc)
        # Internal mutable state keyed by wave_id
        self._waves: dict[str, WaveState] = {}
        self._wave_order: list[str] = []
        # wave_id -> predecessor wave_id (if any)
        self._wave_deps: dict[str, str | None] = {}

    # -- wave definition -----------------------------------------------------

    def add_wave(
        self,
        wave_id: str,
        jobs: list[dict],
        depends_on_wave: str | None = None,
    ) -> None:
        """Define a wave with its jobs.

        Each job dict must have 'label' and optionally 'depends_on' (list of
        job labels within the same wave).
        """
        job_states = tuple(
            JobState(
                job_label=j["label"],
                wave_id=wave_id,
                depends_on=tuple(j.get("depends_on", [])),
            )
            for j in jobs
        )
        self._waves[wave_id] = WaveState(
            wave_id=wave_id,
            status=WaveStatus.PENDING,
            jobs=job_states,
        )
        self._wave_order.append(wave_id)
        self._wave_deps[wave_id] = depends_on_wave

    # -- wave lifecycle ------------------------------------------------------

    def can_start_wave(self, wave_id: str) -> bool:
        """True if predecessor wave completed and its gate passed (if any)."""
        dep = self._wave_deps.get(wave_id)
        if dep is None:
            return True
        dep_wave = self._waves[dep]
        if not self.is_wave_complete(dep):
            return False
        # Gate must exist and have passed
        if dep_wave.gate_verdict is None or not dep_wave.gate_verdict.passed:
            return False
        return True

    def start_wave(self, wave_id: str) -> WaveState:
        """Mark wave as RUNNING if dependencies are met."""
        if not self.can_start_wave(wave_id):
            raise RuntimeError(f"Cannot start wave {wave_id}: dependencies not met")
        now = datetime.now(timezone.utc)
        wave = self._waves[wave_id]
        self._waves[wave_id] = replace(
            wave,
            status=WaveStatus.RUNNING,
            started_at=now,
        )
        return self._waves[wave_id]

    # -- job results ---------------------------------------------------------

    def record_job_result(self, wave_id: str, job_label: str, succeeded: bool) -> None:
        wave = self._waves[wave_id]
        now = datetime.now(timezone.utc)
        new_status = "succeeded" if succeeded else "failed"
        new_jobs = tuple(
            replace(j, status=new_status, started_at=j.started_at or now, completed_at=now)
            if j.job_label == job_label
            else j
            for j in wave.jobs
        )
        # Derive wave status if all jobs done
        wave_status = wave.status
        completed_at = wave.completed_at
        if all(j.status in ("succeeded", "failed") for j in new_jobs):
            if all(j.status == "succeeded" for j in new_jobs):
                wave_status = WaveStatus.SUCCEEDED
            else:
                wave_status = WaveStatus.FAILED
            completed_at = now

        self._waves[wave_id] = replace(
            wave,
            jobs=new_jobs,
            status=wave_status,
            completed_at=completed_at,
        )

    # -- gate verdicts -------------------------------------------------------

    def record_gate_verdict(
        self,
        wave_id: str,
        passed: bool,
        reason: str,
        evidence: dict | None = None,
    ) -> None:
        wave = self._waves[wave_id]
        verdict = GateVerdict(
            wave_id=wave_id,
            passed=passed,
            reason=reason,
            evidence=evidence or {},
        )
        self._waves[wave_id] = replace(wave, gate_verdict=verdict)

    # -- queries -------------------------------------------------------------

    def next_runnable_jobs(self, wave_id: str) -> list[str]:
        """Jobs whose intra-wave deps are all succeeded and not yet started."""
        wave = self._waves[wave_id]
        succeeded = {j.job_label for j in wave.jobs if j.status == "succeeded"}
        runnable: list[str] = []
        for j in wave.jobs:
            if j.status != "pending":
                continue
            if all(dep in succeeded for dep in j.depends_on):
                runnable.append(j.job_label)
        return runnable

    def wave_state(self, wave_id: str) -> WaveState:
        return self._waves[wave_id]

    def is_wave_complete(self, wave_id: str) -> bool:
        """All jobs in wave have a terminal status."""
        wave = self._waves[wave_id]
        return all(j.status in ("succeeded", "failed") for j in wave.jobs)

    def observe(self) -> WaveSnapshot:
        """Full orchestrator state snapshot."""
        waves = tuple(self._waves[wid] for wid in self._wave_order)
        current = None
        for wid in self._wave_order:
            if self._waves[wid].status == WaveStatus.RUNNING:
                current = wid
                break
        return WaveSnapshot(
            orch_id=self._dag_id,
            waves=waves,
            current_wave=current,
            created_at=self._created_at,
        )
