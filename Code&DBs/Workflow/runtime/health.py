"""Unified health subsystem for workflow runtime probes and platform checks."""

from __future__ import annotations

import asyncio
import concurrent.futures
import enum
import http.client
import json
import math
import os
import shutil
import socket
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_API_HOST = "127.0.0.1"
_DEFAULT_API_PORT = 8420
_DEFAULT_CONNECT_TIMEOUT = 5.0
_DEFAULT_API_TIMEOUT = 2.0
_DEFAULT_REPO_LOCAL_DATABASE_URL = os.environ["WORKFLOW_DATABASE_URL"]


class HealthStatus(enum.Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    name: str
    passed: bool
    message: str
    duration_ms: float | None
    timestamp: datetime | None = None
    status: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PreflightResult:
    overall: HealthStatus
    checks: tuple[PreflightCheck, ...]
    timestamp: datetime
    duration_ms: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


class HealthProbe(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def check(self) -> PreflightCheck:
        ...


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _elapsed_ms(start_monotonic: float) -> float:
    return (time.monotonic() - start_monotonic) * 1000.0


def _check_status(check: PreflightCheck) -> str:
    return check.status or ("ok" if check.passed else "failed")


def _resolve_database_url(database_url: str | None) -> str | None:
    resolved = database_url or os.environ.get("WORKFLOW_DATABASE_URL")
    if resolved:
        return resolved
    return _DEFAULT_REPO_LOCAL_DATABASE_URL


def _asyncpg_module():
    import asyncpg

    return asyncpg


def _psycopg2_module():
    import psycopg2

    return psycopg2


def _run_async(awaitable: Any) -> Any:
    return asyncio.run(awaitable)


def _queue_utilization_pct(total_queued: int, critical_threshold: int) -> float:
    if critical_threshold <= 0:
        return 999.9 if total_queued > 0 else 0.0
    return min(round(total_queued / critical_threshold * 100, 1), 999.9)


def _build_check(
    *,
    name: str,
    passed: bool,
    message: str,
    started_at: datetime,
    started_monotonic: float,
    status: str | None = None,
    details: dict[str, Any] | None = None,
    duration_ms: float | None = None,
) -> PreflightCheck:
    return PreflightCheck(
        name=name,
        passed=passed,
        message=message,
        duration_ms=duration_ms if duration_ms is not None else _elapsed_ms(started_monotonic),
        timestamp=started_at,
        status=status,
        details=details or {},
    )


def _aggregate_preflight(checks: list[PreflightCheck]) -> HealthStatus:
    if not checks:
        return HealthStatus.UNKNOWN
    failed = sum(1 for check in checks if not check.passed)
    if failed == 0:
        return HealthStatus.HEALTHY
    if failed > len(checks) / 2:
        return HealthStatus.UNHEALTHY
    return HealthStatus.DEGRADED


def _aggregate_platform(checks: list[PreflightCheck]) -> HealthStatus:
    if not checks:
        return HealthStatus.UNKNOWN
    states = [_check_status(check) for check in checks]
    if all(state == "ok" for state in states):
        return HealthStatus.HEALTHY
    if any(state == "failed" for state in states):
        return HealthStatus.UNHEALTHY
    return HealthStatus.DEGRADED


def _provider_api_key_present(provider_slug: str) -> bool:
    from adapters.provider_registry import resolve_api_key_env_vars

    return any(os.environ.get(name) for name in resolve_api_key_env_vars(provider_slug))


def _sync_lane_admission(
    provider_slug: str,
    adapter_type: str,
    transport_ready: bool,
    reason: str,
) -> None:
    """Write probe result back to provider_transport_admissions so the router
    skips lanes whose transport isn't actually available.  Re-enables the lane
    when a later probe succeeds."""
    try:
        import asyncpg
        from adapters.provider_registry import reload_from_db

        db_url = os.environ.get(
            "WORKFLOW_DATABASE_URL", _DEFAULT_REPO_LOCAL_DATABASE_URL
        )

        async def _update():
            conn = await asyncpg.connect(db_url)
            try:
                await conn.execute(
                    """
                    UPDATE provider_transport_admissions
                       SET admitted_by_policy = $1,
                           policy_reason      = $2,
                           updated_at         = now()
                     WHERE provider_slug = $3
                       AND adapter_type  = $4
                       AND status        = 'active'
                    """,
                    transport_ready,
                    reason,
                    provider_slug,
                    adapter_type,
                )
            finally:
                await conn.close()

        import concurrent.futures

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(asyncio.run, _update()).result(timeout=5)
        else:
            asyncio.run(_update())

        reload_from_db()
    except Exception:
        pass  # probe writeback is best-effort; don't break health checks


async def _run_checks_async(probes: list[HealthProbe]) -> list[PreflightCheck]:
    if not probes:
        return []
    return list(await asyncio.gather(*(asyncio.to_thread(probe.check) for probe in probes)))
class DatabaseProbe(HealthProbe):
    """Checks that a file exists and is readable."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    @property
    def name(self) -> str:
        return f"database:{self._db_path}"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        try:
            exists = os.path.isfile(self._db_path)
            readable = os.access(self._db_path, os.R_OK) if exists else False
            passed = exists and readable
            message = "ok" if passed else f"file missing or unreadable: {self._db_path}"
            status = "ok" if passed else "failed"
        except Exception as exc:
            passed = False
            message = str(exc)
            status = "failed"
        return _build_check(
            name=self.name,
            passed=passed,
            message=message,
            started_at=started_at,
            started_monotonic=started_monotonic,
            status=status,
        )


class PostgresProbe(HealthProbe):
    """Checks that a Postgres connection string is parseable."""

    def __init__(self, url: str) -> None:
        self._url = url

    @property
    def name(self) -> str:
        return "postgres"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        try:
            parsed = urlparse(self._url)
            passed = parsed.scheme in ("postgres", "postgresql") and bool(parsed.hostname)
            if not passed:
                message = (
                    f"invalid connection string (scheme={parsed.scheme!r}, "
                    f"host={parsed.hostname!r})"
                )
                status = "failed"
            else:
                message = "ok"
                status = "ok"
        except Exception as exc:
            passed = False
            message = str(exc)
            status = "failed"
        return _build_check(
            name=self.name,
            passed=passed,
            message=message,
            started_at=started_at,
            started_monotonic=started_monotonic,
            status=status,
        )


class PostgresConnectivityProbe(HealthProbe):
    """Checks that Postgres is reachable and accepts a simple query."""

    def __init__(self, database_url: str | None = None, timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT) -> None:
        self._database_url = database_url
        self._timeout_seconds = timeout_seconds

    @property
    def name(self) -> str:
        return "postgres_connectivity"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        url = _resolve_database_url(self._database_url)
        if not url:
            return _build_check(
                name=self.name,
                passed=False,
                message="WORKFLOW_DATABASE_URL not configured",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
                duration_ms=None,
            )

        async def _check() -> None:
            asyncpg = _asyncpg_module()
            conn = await asyncio.wait_for(asyncpg.connect(url), timeout=self._timeout_seconds)
            try:
                await asyncio.wait_for(conn.execute("SELECT 1"), timeout=self._timeout_seconds)
            finally:
                await conn.close()

        try:
            _run_async(_check())
            return _build_check(
                name=self.name,
                passed=True,
                message="Connected successfully",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="ok",
            )
        except asyncio.TimeoutError:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Connection timeout (>{self._timeout_seconds:g}s)",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Connection failed: {exc}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


class ProviderTransportProbe(HealthProbe):
    """Report transport readiness plus billing metadata for one provider lane."""

    def __init__(self, provider_slug: str, adapter_type: str) -> None:
        self._provider_slug = provider_slug
        self._adapter_type = adapter_type

    @property
    def name(self) -> str:
        return f"provider_transport:{self._provider_slug}:{self._adapter_type}"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        try:
            from adapters.provider_registry import (
                resolve_adapter_economics,
                resolve_adapter_contract,
                resolve_api_endpoint,
                resolve_binary,
                resolve_lane_policy,
                supports_adapter,
            )

            lane_policy = resolve_lane_policy(self._provider_slug, self._adapter_type)
            contract = resolve_adapter_contract(self._provider_slug, self._adapter_type)
            supported = supports_adapter(self._provider_slug, self._adapter_type)
            details: dict[str, Any] = {
                "provider_slug": self._provider_slug,
                "adapter_type": self._adapter_type,
                "supported": supported,
                "lane_policy": lane_policy or {},
            }
            if supported:
                economics = resolve_adapter_economics(self._provider_slug, self._adapter_type)
                details.update({
                    "billing_mode": economics.get("billing_mode"),
                    "budget_bucket": economics.get("budget_bucket"),
                    "effective_marginal_cost": economics.get("effective_marginal_cost"),
                    "prefer_prepaid": economics.get("prefer_prepaid"),
                    "allow_payg_fallback": economics.get("allow_payg_fallback"),
                })
            if contract is not None:
                contract_payload = contract.to_contract()
                details["adapter_contract"] = contract_payload
                details["transport_kind"] = contract.transport_kind
                details["failure_namespace"] = contract.failure_namespace
            transport_ready = False
            message = (
                str((lane_policy or {}).get("policy_reason") or "adapter not admitted by policy")
                if not supported
                else "transport metadata available"
            )
            status = "failed" if not supported else "ok"

            if supported and self._adapter_type == "cli_llm":
                binary_path = resolve_binary(self._provider_slug)
                transport_ready = binary_path is not None
                details["binary_path"] = binary_path
                message = "binary ready" if transport_ready else "binary missing"
                status = "ok" if transport_ready else "warning"
            elif supported and self._adapter_type == "llm_task":
                endpoint = resolve_api_endpoint(self._provider_slug)
                credential_present = _provider_api_key_present(self._provider_slug)
                transport_ready = bool(endpoint and credential_present)
                details["api_endpoint"] = endpoint
                details["credential_present"] = credential_present
                message = (
                    "api transport ready"
                    if transport_ready
                    else "api transport metadata present but credential or endpoint missing"
                )
                status = "ok" if transport_ready else "warning"

            details["transport_ready"] = transport_ready

            if supported:
                _sync_lane_admission(
                    self._provider_slug,
                    self._adapter_type,
                    transport_ready,
                    message,
                )

            return _build_check(
                name=self.name,
                passed=supported,
                message=message,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details=details,
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=str(exc),
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


class WorkflowWorkerProbe(HealthProbe):
    """Checks whether the workflow worker has recent activity."""

    def __init__(
        self,
        database_url: str | None = None,
        window_minutes: int = 5,
        timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT,
    ) -> None:
        self._database_url = database_url
        self._window_minutes = window_minutes
        self._timeout_seconds = timeout_seconds

    @property
    def name(self) -> str:
        return "workflow_worker_alive"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        url = _resolve_database_url(self._database_url)
        if not url:
            return _build_check(
                name=self.name,
                passed=False,
                message="WORKFLOW_DATABASE_URL not configured, skipping check",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
                duration_ms=None,
            )

        async def _check() -> tuple[int, int]:
            asyncpg = _asyncpg_module()
            conn = await asyncio.wait_for(asyncpg.connect(url), timeout=self._timeout_seconds)
            try:
                cutoff = _utcnow() - timedelta(minutes=self._window_minutes)
                active = await asyncio.wait_for(
                    conn.fetchval(
                        """
                        SELECT COUNT(*) FROM workflow_jobs
                        WHERE (claimed_at > $1 OR heartbeat_at > $1)
                          AND status IN ('claimed', 'running')
                        """,
                        cutoff,
                    ),
                    timeout=self._timeout_seconds,
                )
                ready = await asyncio.wait_for(
                    conn.fetchval("SELECT COUNT(*) FROM workflow_jobs WHERE status = 'ready'"),
                    timeout=self._timeout_seconds,
                )
                return int(active or 0), int(ready or 0)
            finally:
                await conn.close()

        try:
            active, ready = _run_async(_check())
            passed = active > 0 or ready == 0
            if passed:
                message = f"{active} active jobs in last {self._window_minutes} min"
                if ready == 0:
                    message += " (no ready jobs)"
                status = "ok"
            else:
                message = f"No active jobs in last {self._window_minutes} min ({ready} ready jobs waiting)"
                status = "failed"
            return _build_check(
                name=self.name,
                passed=passed,
                message=message,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details={"active_jobs": active, "ready_jobs": ready, "window_minutes": self._window_minutes},
            )
        except asyncio.TimeoutError:
            return _build_check(
                name=self.name,
                passed=False,
                message="Database query timeout",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Query failed: {exc}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


class QueueDepthProbe(HealthProbe):
    """Checks pending and ready queue depth."""

    def __init__(
        self,
        database_url: str | None = None,
        timeout_seconds: float = _DEFAULT_CONNECT_TIMEOUT,
        warning_threshold: int = 500,
        critical_threshold: int = 1000,
    ) -> None:
        self._database_url = database_url
        self._timeout_seconds = timeout_seconds
        self._warning_threshold = warning_threshold
        self._critical_threshold = critical_threshold

    @property
    def name(self) -> str:
        return "queue_depth"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        url = _resolve_database_url(self._database_url)
        if not url:
            return _build_check(
                name=self.name,
                passed=False,
                message="WORKFLOW_DATABASE_URL not configured, skipping check",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
                duration_ms=None,
            )

        async def _check() -> tuple[int, int, int, int]:
            asyncpg = _asyncpg_module()
            conn = await asyncio.wait_for(asyncpg.connect(url), timeout=self._timeout_seconds)
            try:
                row = await asyncio.wait_for(
                    conn.fetchrow(
                        """
                        SELECT
                            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                            COUNT(*) FILTER (WHERE status = 'ready') AS ready,
                            COUNT(*) FILTER (WHERE status = 'claimed') AS claimed,
                            COUNT(*) FILTER (WHERE status = 'running') AS running
                        FROM workflow_jobs
                        WHERE status IN ('pending', 'ready', 'claimed', 'running')
                        """
                    ),
                    timeout=self._timeout_seconds,
                )
                return (
                    int(row["pending"] or 0),
                    int(row["ready"] or 0),
                    int(row["claimed"] or 0),
                    int(row["running"] or 0),
                )
            finally:
                await conn.close()

        try:
            pending, ready, claimed, running = _run_async(_check())
            total_queued = pending + ready
            utilization_pct = _queue_utilization_pct(total_queued, self._critical_threshold)
            if total_queued >= self._critical_threshold:
                passed = False
                status = "critical"
            elif total_queued >= self._warning_threshold:
                passed = True
                status = "warning"
            else:
                passed = True
                status = "ok"
            return _build_check(
                name=self.name,
                passed=passed,
                message=f"{total_queued} jobs pending or ready",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details={
                    "pending": pending,
                    "ready": ready,
                    "claimed": claimed,
                    "running": running,
                    "total_queued": total_queued,
                    "warning_threshold": self._warning_threshold,
                    "critical_threshold": self._critical_threshold,
                    "utilization_pct": utilization_pct,
                },
            )
        except asyncio.TimeoutError:
            return _build_check(
                name=self.name,
                passed=False,
                message="Database query timeout",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Query failed: {exc}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


@dataclass(frozen=True)
class AdmissionDecision:
    admitted: bool
    reason: str
    queue_depth: int
    utilization_pct: float


class QueueAdmissionGate:
    def __init__(
        self,
        database_url: str | None = None,
        critical_threshold: int = 1000,
        timeout_seconds: float = 2.0,
    ) -> None:
        self._database_url = database_url
        self._critical_threshold = critical_threshold
        self._timeout_seconds = timeout_seconds

    def check(self, job_count: int = 1) -> AdmissionDecision:
        """Check if job_count new jobs can be admitted."""
        database_url = str(_resolve_database_url(self._database_url) or "").strip()
        if not database_url:
            raise RuntimeError("QueueAdmissionGate requires explicit WORKFLOW_DATABASE_URL")
        psycopg2 = _psycopg2_module()
        connect_timeout = max(1, math.ceil(self._timeout_seconds))
        statement_timeout_ms = max(1, int(self._timeout_seconds * 1000))
        try:
            with psycopg2.connect(
                database_url,
                connect_timeout=connect_timeout,
                options=f"-c statement_timeout={statement_timeout_ms}",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM workflow_jobs WHERE status IN ('pending', 'ready')"
                    )
                    row = cur.fetchone()
        except Exception as exc:
            return AdmissionDecision(
                admitted=False,
                reason=f"queue admission check failed: {exc}",
                queue_depth=-1,
                utilization_pct=0.0,
            )

        queue_depth = int((row or [0])[0] or 0)
        projected_depth = queue_depth + max(0, job_count)
        utilization_pct = _queue_utilization_pct(queue_depth, self._critical_threshold)
        if queue_depth >= self._critical_threshold:
            return AdmissionDecision(
                admitted=False,
                reason=(
                    f"queue depth {queue_depth} is at or above critical threshold "
                    f"{self._critical_threshold}"
                ),
                queue_depth=queue_depth,
                utilization_pct=utilization_pct,
            )
        if projected_depth > self._critical_threshold:
            return AdmissionDecision(
                admitted=False,
                reason=(
                    f"queue depth {queue_depth} + {max(0, job_count)} new jobs "
                    f"would exceed critical threshold {self._critical_threshold}"
                ),
                queue_depth=queue_depth,
                utilization_pct=utilization_pct,
            )
        return AdmissionDecision(
            admitted=True,
            reason=(
                f"queue depth {queue_depth} within critical threshold {self._critical_threshold}"
            ),
            queue_depth=queue_depth,
            utilization_pct=utilization_pct,
        )


def queue_admission_check(job_count: int = 1, critical_threshold: int = 1000) -> AdmissionDecision:
    """Quick admission check. Returns AdmissionDecision."""
    gate = QueueAdmissionGate(critical_threshold=critical_threshold)
    return gate.check(job_count)


class SchedulerProbe(HealthProbe):
    """Checks whether the scheduler has ticked recently."""

    def __init__(self, state_file: str | Path | None = None, window_minutes: int = 15) -> None:
        self._state_file = state_file
        self._window_minutes = window_minutes

    @property
    def name(self) -> str:
        return "scheduler_running"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        state_file = Path(self._state_file) if self._state_file else _PROJECT_ROOT / "scheduler_state.json"
        if not state_file.exists():
            return _build_check(
                name=self.name,
                passed=False,
                message=f"scheduler_state.json not found: {state_file}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
                duration_ms=None,
            )

        try:
            with state_file.open("r", encoding="utf-8") as fh:
                state = json.load(fh)

            if "last_tick" not in state:
                return _build_check(
                    name=self.name,
                    passed=False,
                    message="scheduler_state.json missing 'last_tick' field",
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    status="failed",
                    duration_ms=None,
                )

            last_tick = datetime.fromisoformat(state["last_tick"])
            if last_tick.tzinfo is None:
                last_tick = last_tick.replace(tzinfo=timezone.utc)
            age_minutes = (started_at - last_tick).total_seconds() / 60.0
            if age_minutes > self._window_minutes:
                passed = False
                message = f"Last tick {age_minutes:.1f} min ago (threshold {self._window_minutes} min)"
                status = "failed"
            else:
                passed = True
                message = f"Last tick {age_minutes:.1f} min ago"
                status = "ok"
            return _build_check(
                name=self.name,
                passed=passed,
                message=message,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details={"age_minutes": round(age_minutes, 2), "window_minutes": self._window_minutes},
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Failed to read scheduler_state.json: {exc}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


class ApiLivenessProbe(HealthProbe):
    """Checks whether the HTTP API responds to /health."""

    def __init__(
        self,
        host: str = _DEFAULT_API_HOST,
        port: int = _DEFAULT_API_PORT,
        path: str = "/health",
        timeout_seconds: float = _DEFAULT_API_TIMEOUT,
    ) -> None:
        self._host = host
        self._port = port
        self._path = path
        self._timeout_seconds = timeout_seconds

    @property
    def name(self) -> str:
        return "api_server_reachable"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        conn = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout_seconds)
        try:
            conn.request("GET", self._path, headers={"Connection": "close"})
            response = conn.getresponse()
            response.read()
            if response.status == 200:
                passed = True
                message = "HTTP /health returned 200"
                status = "ok"
            else:
                passed = False
                message = f"HTTP {self._path} returned unexpected status {response.status}"
                status = "failed"
            return _build_check(
                name=self.name,
                passed=passed,
                message=message,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details={"http_status": response.status, "reason": response.reason},
            )
        except (socket.timeout, ConnectionRefusedError, OSError) as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Connection failed: {exc}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )
        finally:
            conn.close()


class DiskSpaceProbe(HealthProbe):
    """Checks available disk space at a path."""

    def __init__(self, path: str, min_mb: int = 100) -> None:
        self._path = path
        self._min_mb = min_mb

    @property
    def name(self) -> str:
        return f"disk_space:{self._path}"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        try:
            usage = shutil.disk_usage(self._path)
            free_mb = usage.free / (1024 * 1024)
            passed = free_mb >= self._min_mb
            if passed:
                message = f"{free_mb:.0f}MB free"
                status = "ok"
            else:
                message = f"only {free_mb:.0f}MB free (need {self._min_mb}MB)"
                status = "failed"
            return _build_check(
                name=self.name,
                passed=passed,
                message=message,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details={
                    "free_mb": round(free_mb, 2),
                    "total_mb": round(usage.total / (1024 * 1024), 2),
                    "used_mb": round(usage.used / (1024 * 1024), 2),
                },
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=str(exc),
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


class DiskUsageProbe(HealthProbe):
    """Checks filesystem usage for a receipts directory."""

    def __init__(
        self,
        path: str | Path | None = None,
        warn_percent: float = 80.0,
        fail_percent: float = 95.0,
    ) -> None:
        self._path = Path(path) if path else _PROJECT_ROOT / "receipts"
        self._warn_percent = warn_percent
        self._fail_percent = fail_percent

    @property
    def name(self) -> str:
        return "disk_space"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        try:
            if not self._path.exists():
                return _build_check(
                    name=self.name,
                    passed=False,
                    message=f"receipts directory not found: {self._path}",
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    status="failed",
                    duration_ms=None,
                )

            if self._path.is_file():
                total_size = self._path.stat().st_size
            else:
                total_size = sum(file.stat().st_size for file in self._path.rglob("*") if file.is_file())

            usage = shutil.disk_usage(self._path)
            used_percent = (usage.used / usage.total) * 100 if usage.total else 0.0
            receipts_mb = total_size / (1024 * 1024)

            if used_percent > self._fail_percent:
                passed = False
                status = "failed"
                message = (
                    f"Disk {used_percent:.1f}% full (>{self._fail_percent}%), "
                    f"receipts: {receipts_mb:.1f} MB"
                )
            elif used_percent > self._warn_percent:
                passed = False
                status = "failed"
                message = (
                    f"Disk {used_percent:.1f}% full (>{self._warn_percent}%), "
                    f"receipts: {receipts_mb:.1f} MB"
                )
            else:
                passed = True
                status = "ok"
                message = f"Disk {used_percent:.1f}% full, receipts: {receipts_mb:.1f} MB"

            return _build_check(
                name=self.name,
                passed=passed,
                message=message,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                details={
                    "used_percent": round(used_percent, 2),
                    "free_gb": round(usage.free / (1024 * 1024 * 1024), 2),
                    "receipts_mb": round(receipts_mb, 2),
                    "path": str(self._path),
                },
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Failed to check disk space: {exc}",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
            )


class FileExistsProbe(HealthProbe):
    """Checks that a required file exists."""

    def __init__(self, path: str) -> None:
        self._path = path

    @property
    def name(self) -> str:
        return f"file_exists:{self._path}"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        try:
            passed = os.path.isfile(self._path)
            message = "ok" if passed else f"file not found: {self._path}"
            status = "ok" if passed else "failed"
        except Exception as exc:
            passed = False
            message = str(exc)
            status = "failed"
        return _build_check(
            name=self.name,
            passed=passed,
            message=message,
            started_at=started_at,
            started_monotonic=started_monotonic,
            status=status,
        )


class PreflightRunner:
    """Runs a collection of probes and aggregates results."""

    def __init__(self, probes: list[HealthProbe]) -> None:
        self._probes = list(probes)

    def run(self) -> PreflightResult:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        checks: list[PreflightCheck] = [probe.check() for probe in self._probes]
        return PreflightResult(
            overall=_aggregate_preflight(checks),
            checks=tuple(checks),
            timestamp=started_at,
            duration_ms=_elapsed_ms(started_monotonic),
        )

    async def run_async(self) -> PreflightResult:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        checks = await _run_checks_async(self._probes)
        return PreflightResult(
            overall=_aggregate_preflight(checks),
            checks=tuple(checks),
            timestamp=started_at,
            duration_ms=_elapsed_ms(started_monotonic),
        )

    def run_with_timeout(self, timeout_seconds: float) -> PreflightResult:
        """Run each probe in its own thread with a per-probe timeout."""
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        checks: list[PreflightCheck] = []
        for probe in self._probes:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(probe.check)
                try:
                    checks.append(future.result(timeout=timeout_seconds))
                except concurrent.futures.TimeoutError:
                    checks.append(
                        PreflightCheck(
                            name=probe.name,
                            passed=False,
                            message=f"probe timed out after {timeout_seconds}s",
                            duration_ms=timeout_seconds * 1000.0,
                            timestamp=_utcnow(),
                            status="failed",
                        )
                    )
        return PreflightResult(
            overall=_aggregate_preflight(checks),
            checks=tuple(checks),
            timestamp=started_at,
            duration_ms=_elapsed_ms(started_monotonic),
        )


def build_platform_probes(
    database_url: str | None = None,
    scheduler_state_file: str | Path | None = None,
    receipts_dir: str | Path | None = None,
    api_host: str = _DEFAULT_API_HOST,
    api_port: int = _DEFAULT_API_PORT,
    queue_window_minutes: int = 5,
    scheduler_window_minutes: int = 15,
    warn_percent: float = 80.0,
    fail_percent: float = 95.0,
) -> list[HealthProbe]:
    """Build the platform health probe set."""
    return [
        PostgresConnectivityProbe(database_url=database_url),
        WorkflowWorkerProbe(database_url=database_url, window_minutes=queue_window_minutes),
        QueueDepthProbe(database_url=database_url),
        SchedulerProbe(state_file=scheduler_state_file, window_minutes=scheduler_window_minutes),
        ApiLivenessProbe(host=api_host, port=api_port),
        DiskUsageProbe(path=receipts_dir, warn_percent=warn_percent, fail_percent=fail_percent),
    ]


async def health_check(
    database_url: str | None = None,
    scheduler_state_file: str | Path | None = None,
    receipts_dir: str | Path | None = None,
    api_host: str = _DEFAULT_API_HOST,
    api_port: int = _DEFAULT_API_PORT,
    queue_window_minutes: int = 5,
    scheduler_window_minutes: int = 15,
    warn_percent: float = 80.0,
    fail_percent: float = 95.0,
) -> PreflightResult:
    """Run the platform health probes asynchronously."""
    started_at = _utcnow()
    started_monotonic = time.monotonic()
    probes = build_platform_probes(
        database_url=database_url,
        scheduler_state_file=scheduler_state_file,
        receipts_dir=receipts_dir,
        api_host=api_host,
        api_port=api_port,
        queue_window_minutes=queue_window_minutes,
        scheduler_window_minutes=scheduler_window_minutes,
        warn_percent=warn_percent,
        fail_percent=fail_percent,
    )
    checks = await _run_checks_async(probes)
    return PreflightResult(
        overall=_aggregate_platform(checks),
        checks=tuple(checks),
        timestamp=started_at,
        duration_ms=_elapsed_ms(started_monotonic),
        details={"probe_count": len(checks), "mode": "platform"},
    )


def health_check_sync(
    database_url: str | None = None,
    scheduler_state_file: str | Path | None = None,
    receipts_dir: str | Path | None = None,
    api_host: str = _DEFAULT_API_HOST,
    api_port: int = _DEFAULT_API_PORT,
    queue_window_minutes: int = 5,
    scheduler_window_minutes: int = 15,
    warn_percent: float = 80.0,
    fail_percent: float = 95.0,
) -> PreflightResult:
    """Synchronous wrapper around :func:`health_check`."""
    return asyncio.run(
        health_check(
            database_url=database_url,
            scheduler_state_file=scheduler_state_file,
            receipts_dir=receipts_dir,
            api_host=api_host,
            api_port=api_port,
            queue_window_minutes=queue_window_minutes,
            scheduler_window_minutes=scheduler_window_minutes,
            warn_percent=warn_percent,
            fail_percent=fail_percent,
        )
    )


def preflight_result_to_dict(result: PreflightResult) -> dict[str, Any]:
    """Convert a preflight result to a JSON-serializable dict."""
    return {
        "overall": result.overall.value,
        "status": result.overall.value,
        "timestamp": result.timestamp.isoformat(),
        "duration_ms": result.duration_ms,
        "details": dict(result.details),
        "checks": {
            check.name: {
                "name": check.name,
                "passed": check.passed,
                "status": _check_status(check),
                "message": check.message,
                "timestamp": check.timestamp.isoformat() if check.timestamp else None,
                "duration_ms": check.duration_ms,
                "details": dict(check.details),
            }
            for check in result.checks
        },
    }


health_result_to_dict = preflight_result_to_dict
health_report_to_dict = preflight_result_to_dict


@dataclass(frozen=True)
class WaveHealth:
    wave_id: str
    total_jobs: int
    succeeded: int
    failed: int
    pass_rate: float
    avg_duration: float
    last_activity: datetime
    stalled: bool


@dataclass
class _DispatchRecord:
    job_label: str
    succeeded: bool
    duration_seconds: float
    timestamp: datetime


class WaveHealthMonitor:
    """Tracks per-wave workflow health from receipts."""

    def __init__(self) -> None:
        self._waves: dict[str, list[_DispatchRecord]] = {}

    def record_workflow(
        self,
        wave_id: str,
        job_label: str,
        succeeded: bool,
        duration_seconds: float,
    ) -> None:
        self._waves.setdefault(wave_id, []).append(
            _DispatchRecord(
                job_label=job_label,
                succeeded=succeeded,
                duration_seconds=duration_seconds,
                timestamp=_utcnow(),
            )
        )

    def wave_health(self, wave_id: str) -> WaveHealth:
        records = self._waves.get(wave_id, [])
        if not records:
            return WaveHealth(
                wave_id=wave_id,
                total_jobs=0,
                succeeded=0,
                failed=0,
                pass_rate=0.0,
                avg_duration=0.0,
                last_activity=datetime.min.replace(tzinfo=timezone.utc),
                stalled=True,
            )
        total = len(records)
        ok = sum(1 for record in records if record.succeeded)
        fail = total - ok
        avg_dur = sum(record.duration_seconds for record in records) / total
        last = max(record.timestamp for record in records)
        return WaveHealth(
            wave_id=wave_id,
            total_jobs=total,
            succeeded=ok,
            failed=fail,
            pass_rate=ok / total,
            avg_duration=avg_dur,
            last_activity=last,
            stalled=self.stall_detection(wave_id),
        )

    def stall_detection(self, wave_id: str, max_idle_seconds: float = 300) -> bool:
        """Return True if the wave has no activity within the threshold."""
        records = self._waves.get(wave_id, [])
        if not records:
            return True
        last = max(record.timestamp for record in records)
        elapsed = (_utcnow() - last).total_seconds()
        return elapsed > max_idle_seconds


__all__ = [
    "AdmissionDecision",
    "ApiLivenessProbe",
    "DatabaseProbe",
    "DiskSpaceProbe",
    "DiskUsageProbe",
    "FileExistsProbe",
    "HealthProbe",
    "HealthStatus",
    "PostgresConnectivityProbe",
    "PostgresProbe",
    "PreflightCheck",
    "PreflightResult",
    "PreflightRunner",
    "QueueAdmissionGate",
    "QueueDepthProbe",
    "WorkflowWorkerProbe",
    "WaveHealth",
    "WaveHealthMonitor",
    "build_platform_probes",
    "health_check",
    "health_check_sync",
    "health_report_to_dict",
    "health_result_to_dict",
    "preflight_result_to_dict",
    "queue_admission_check",
]
