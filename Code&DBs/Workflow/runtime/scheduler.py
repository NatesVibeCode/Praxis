"""Scheduled dispatch executor.

Loads job definitions from config/scheduler.json, tracks last-run state,
and fires dispatches on a cron-like schedule. Designed to be driven by
a periodic tick (cron, launchd, or manual CLI invocation).

When a job is due, the tick also publishes a durable `schedule.fired`
system event so trigger consumers can checkpoint against the same event log
instead of treating scheduler state as a UI-only hint.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Project root — all relative paths resolve from here
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Minimal cron parser
# ---------------------------------------------------------------------------

def _parse_cron_field(field_str: str, min_val: int, max_val: int) -> set[int]:
    """Parse one cron field into a set of matching integer values.

    Supports:
      *        — all values
      N        — exact value
      N-M      — inclusive range
      N-M/S    — range with step
      */S      — all values with step
      N,M,O    — comma-separated list (each element can be any of the above)
    """
    result: set[int] = set()

    for part in field_str.split(","):
        part = part.strip()
        if not part:
            continue

        # Handle step: */S or N-M/S
        step = 1
        if "/" in part:
            range_part, step_str = part.split("/", 1)
            step = int(step_str)
            part = range_part

        if part == "*":
            result.update(range(min_val, max_val + 1, step))
        elif "-" in part:
            lo_str, hi_str = part.split("-", 1)
            lo, hi = int(lo_str), int(hi_str)
            result.update(range(lo, hi + 1, step))
        else:
            result.add(int(part))

    return result


def cron_matches(expression: str, dt: datetime) -> bool:
    """Return True if *dt* falls within the cron expression.

    expression is 5-field: minute hour day-of-month month day-of-week
    Day-of-week: 0=Monday ... 6=Sunday (ISO convention).
    """
    parts = expression.split()
    if len(parts) != 5:
        raise ValueError(f"cron expression must have 5 fields, got {len(parts)}: {expression!r}")

    minutes = _parse_cron_field(parts[0], 0, 59)
    hours = _parse_cron_field(parts[1], 0, 23)
    days_of_month = _parse_cron_field(parts[2], 1, 31)
    months = _parse_cron_field(parts[3], 1, 12)
    days_of_week = _parse_cron_field(parts[4], 0, 6)

    return (
        dt.minute in minutes
        and dt.hour in hours
        and dt.day in days_of_month
        and dt.month in months
        and dt.weekday() in days_of_week
    )


# ---------------------------------------------------------------------------
# ScheduledJob
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ScheduledJob:
    """One scheduled dispatch definition."""

    name: str
    spec_path: str  # path to workflow spec or batch spec JSON (relative to project root)
    cron_expression: str  # 5-field cron: minute hour dom month dow
    enabled: bool = True
    max_retries: int = 1
    tier: str | None = None


# ---------------------------------------------------------------------------
# SchedulerConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    """Loaded scheduler configuration."""

    jobs: tuple[ScheduledJob, ...]
    config_path: str

    @classmethod
    def load(cls, path: str | None = None) -> "SchedulerConfig":
        """Load from a JSON file. Defaults to config/scheduler.json under project root."""
        if path is None:
            path = str(_PROJECT_ROOT / "config" / "scheduler.json")

        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        if not isinstance(raw, dict) or "jobs" not in raw:
            raise ValueError(f"scheduler config must have a 'jobs' array: {path}")

        jobs: list[ScheduledJob] = []
        for entry in raw["jobs"]:
            jobs.append(ScheduledJob(
                name=entry["name"],
                spec_path=entry["spec_path"],
                cron_expression=entry["cron_expression"],
                enabled=entry.get("enabled", True),
                max_retries=entry.get("max_retries", 1),
                tier=entry.get("tier"),
            ))

        return cls(jobs=tuple(jobs), config_path=path)


# ---------------------------------------------------------------------------
# SchedulerState — tracks last_run_at per job
# ---------------------------------------------------------------------------

_DEFAULT_STATE_PATH = str(_PROJECT_ROOT / "artifacts" / "scheduler_state.json")


class SchedulerState:
    """Tracks last-run timestamps per job name.

    Persisted to artifacts/scheduler_state.json so ticks survive process restarts.
    """

    def __init__(self, state_path: str | None = None) -> None:
        self._path = state_path or _DEFAULT_STATE_PATH
        self._state: dict[str, str] = {}  # job_name -> ISO timestamp
        self._load()

    def _load(self) -> None:
        if os.path.exists(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._state = data
            except (json.JSONDecodeError, OSError):
                self._state = {}

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2)
            f.write("\n")

    def get_last_run(self, job_name: str) -> datetime | None:
        iso = self._state.get(job_name)
        if iso is None:
            return None
        return datetime.fromisoformat(iso)

    def record_run(self, job_name: str, at: datetime) -> None:
        self._state[job_name] = at.isoformat()
        self._save()

    def all_state(self) -> dict[str, str]:
        return dict(self._state)


# ---------------------------------------------------------------------------
# should_run — cron check + dedup guard
# ---------------------------------------------------------------------------

def should_run(
    job: ScheduledJob,
    *,
    last_run_at: datetime | None,
    now: datetime,
) -> bool:
    """Return True if *job* should fire at *now*.

    Logic:
      1. The job must be enabled.
      2. The cron expression must match *now* (truncated to the minute).
      3. If the job already ran in this same calendar minute, skip it (dedup).
    """
    if not job.enabled:
        return False

    if not cron_matches(job.cron_expression, now):
        return False

    # Dedup: don't re-fire within the same minute
    if last_run_at is not None:
        last_minute = last_run_at.replace(second=0, microsecond=0)
        this_minute = now.replace(second=0, microsecond=0)
        if last_minute >= this_minute:
            return False

    return True


def _emit_schedule_fired_event(
    conn: Any,
    *,
    job: ScheduledJob,
    spec_path: str,
    now: datetime,
    last_run_at: datetime | None,
) -> None:
    payload = {
        "job_name": job.name,
        "spec_path": spec_path,
        "cron_expression": job.cron_expression,
        "last_run_at": last_run_at.isoformat() if last_run_at is not None else None,
        "fired_at": now.isoformat(),
    }
    conn.execute(
        "INSERT INTO system_events (event_type, source_id, source_type, payload) "
        "VALUES ($1, $2, $3, $4::jsonb)",
        "schedule.fired",
        job.name,
        "scheduler_state",
        json.dumps(payload),
    )


def _resolve_schedule_event_conn(event_conn: Any | None) -> Any | None:
    if event_conn is not None:
        return event_conn
    try:
        from storage.postgres import ensure_postgres_available
    except Exception:
        logger.debug("schedule.fired emission unavailable: Postgres helper could not be loaded", exc_info=True)
        return None

    try:
        return ensure_postgres_available()
    except Exception:
        logger.debug("schedule.fired emission unavailable: workflow database could not be opened", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# run_scheduler_tick — check all jobs and dispatch due ones
# ---------------------------------------------------------------------------

def run_scheduler_tick(
    config: SchedulerConfig,
    *,
    state: SchedulerState | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
    event_conn: Any | None = None,
) -> list[dict[str, Any]]:
    """Check each enabled job and dispatch any that are due.

    Returns a list of result dicts, one per dispatched job, with keys:
      job_name, status, run_id (if dispatched), error (if failed), skipped (bool).
    """
    from .workflow import run_workflow_from_spec_file, run_workflow_batch_from_file
    from .workflow_spec import load_raw, is_batch_spec

    if state is None:
        state = SchedulerState()
    if now is None:
        now = datetime.now(timezone.utc)

    results: list[dict[str, Any]] = []

    schedule_event_conn = event_conn

    for job in config.jobs:
        last_run = state.get_last_run(job.name)

        if not should_run(job, last_run_at=last_run, now=now):
            continue

        # Resolve the spec path relative to project root
        spec_path = job.spec_path
        if not os.path.isabs(spec_path):
            spec_path = str(_PROJECT_ROOT / spec_path)

        if not os.path.exists(spec_path):
            results.append({
                "job_name": job.name,
                "status": "failed",
                "error": f"spec file not found: {spec_path}",
                "skipped": False,
            })
            continue

        if dry_run:
            results.append({
                "job_name": job.name,
                "status": "dry_run",
                "spec_path": spec_path,
                "skipped": False,
            })
            state.record_run(job.name, now)
            continue

        if schedule_event_conn is None:
            schedule_event_conn = _resolve_schedule_event_conn(None)

        if schedule_event_conn is not None:
            _emit_schedule_fired_event(
                schedule_event_conn,
                job=job,
                spec_path=spec_path,
                now=now,
                last_run_at=last_run,
            )

        # Dispatch
        try:
            raw = load_raw(spec_path)
            if is_batch_spec(raw):
                batch_results = run_workflow_batch_from_file(spec_path)
                succeeded = sum(1 for r in batch_results if r.status == "succeeded")
                failed = len(batch_results) - succeeded
                results.append({
                    "job_name": job.name,
                    "status": "succeeded" if failed == 0 else "partial",
                    "batch_total": len(batch_results),
                    "batch_succeeded": succeeded,
                    "batch_failed": failed,
                    "skipped": False,
                })
            else:
                result = run_workflow_from_spec_file(spec_path)
                results.append({
                    "job_name": job.name,
                    "status": result.status,
                    "run_id": result.run_id,
                    "latency_ms": result.latency_ms,
                    "skipped": False,
                })
        except Exception as exc:
            results.append({
                "job_name": job.name,
                "status": "failed",
                "error": str(exc),
                "skipped": False,
            })

        state.record_run(job.name, now)

    return results


# ---------------------------------------------------------------------------
# Force-run a single job by name
# ---------------------------------------------------------------------------

def force_run_job(
    job_name: str,
    config: SchedulerConfig,
    *,
    state: SchedulerState | None = None,
) -> dict[str, Any]:
    """Force-dispatch a specific scheduled job regardless of cron timing."""
    from .workflow import run_workflow_from_spec_file, run_workflow_batch_from_file
    from .workflow_spec import load_raw, is_batch_spec

    if state is None:
        state = SchedulerState()

    matching = [j for j in config.jobs if j.name == job_name]
    if not matching:
        available = [j.name for j in config.jobs]
        return {
            "job_name": job_name,
            "status": "failed",
            "error": f"job not found. available: {', '.join(available)}",
        }

    job = matching[0]
    spec_path = job.spec_path
    if not os.path.isabs(spec_path):
        spec_path = str(_PROJECT_ROOT / spec_path)

    if not os.path.exists(spec_path):
        return {
            "job_name": job.name,
            "status": "failed",
            "error": f"spec file not found: {spec_path}",
        }

    now = datetime.now(timezone.utc)

    try:
        raw = load_raw(spec_path)
        if is_batch_spec(raw):
            batch_results = run_workflow_batch_from_file(spec_path)
            succeeded = sum(1 for r in batch_results if r.status == "succeeded")
            failed = len(batch_results) - succeeded
            result_dict = {
                "job_name": job.name,
                "status": "succeeded" if failed == 0 else "partial",
                "batch_total": len(batch_results),
                "batch_succeeded": succeeded,
                "batch_failed": failed,
            }
        else:
            result = run_workflow_from_spec_file(spec_path)
            result_dict = {
                "job_name": job.name,
                "status": result.status,
                "run_id": result.run_id,
                "latency_ms": result.latency_ms,
            }
    except Exception as exc:
        result_dict = {
            "job_name": job.name,
            "status": "failed",
            "error": str(exc),
        }

    state.record_run(job.name, now)
    return result_dict


# ---------------------------------------------------------------------------
# Status view
# ---------------------------------------------------------------------------

def scheduler_status(config: SchedulerConfig, *, state: SchedulerState | None = None) -> list[dict[str, Any]]:
    """Return status info for all scheduled jobs."""
    if state is None:
        state = SchedulerState()

    rows: list[dict[str, Any]] = []
    for job in config.jobs:
        last_run = state.get_last_run(job.name)
        rows.append({
            "name": job.name,
            "spec_path": job.spec_path,
            "cron_expression": job.cron_expression,
            "enabled": job.enabled,
            "max_retries": job.max_retries,
            "tier": job.tier,
            "last_run_at": last_run.isoformat() if last_run else None,
        })
    return rows


__all__ = [
    "ScheduledJob",
    "SchedulerConfig",
    "SchedulerState",
    "cron_matches",
    "force_run_job",
    "run_scheduler_tick",
    "scheduler_status",
    "should_run",
]
