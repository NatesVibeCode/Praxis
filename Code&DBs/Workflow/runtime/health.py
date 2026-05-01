"""Unified health subsystem for workflow runtime probes and platform checks."""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import concurrent.futures
import enum
import http.client
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

from runtime.queue_admission import (
    DEFAULT_QUEUE_CRITICAL_THRESHOLD,
    DEFAULT_QUEUE_WARNING_THRESHOLD,
    AdmissionDecision,
    QUEUE_DEPTH_BREAKDOWN_QUERY,
    QueueAdmissionGate,
    queue_admission_check,
    queue_depth_snapshot_from_row,
)
from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_API_HOST = "127.0.0.1"
_DEFAULT_API_PORT = 8420
_DEFAULT_CONNECT_TIMEOUT = 5.0
_DEFAULT_API_TIMEOUT = 2.0
_SCHEDULE_HEARTBEAT_QUERY = """
    SELECT created_at
      FROM system_events
     WHERE event_type = 'scheduler.tick'
     ORDER BY created_at DESC
     LIMIT 1
"""


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


class StaticHealthProbe(HealthProbe):
    """Expose an already-known authority verdict through the preflight contract."""

    def __init__(
        self,
        *,
        name: str,
        passed: bool,
        message: str,
        status: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._name = name
        self._passed = passed
        self._message = message
        self._status = status
        self._details = details or {}

    @property
    def name(self) -> str:
        return self._name

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        return _build_check(
            name=self.name,
            passed=self._passed,
            message=self._message,
            started_at=started_at,
            started_monotonic=started_monotonic,
            status=self._status,
            details=self._details,
        )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _elapsed_ms(start_monotonic: float) -> float:
    return (time.monotonic() - start_monotonic) * 1000.0


def _check_status(check: PreflightCheck) -> str:
    return check.status or ("ok" if check.passed else "failed")


def _resolve_database_url(database_url: str | None) -> str | None:
    if database_url:
        try:
            return resolve_workflow_database_url(env={"WORKFLOW_DATABASE_URL": database_url})
        except PostgresConfigurationError:
            return None
    try:
        return resolve_workflow_database_url()
    except PostgresConfigurationError:
        return None


def _asyncpg_module():
    import asyncpg

    return asyncpg


def _run_async(awaitable: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return run_sync_safe(awaitable)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, awaitable).result()


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
    from adapters.keychain import resolve_secret
    from registry.provider_execution_registry import resolve_api_key_env_vars

    return any(
        resolve_secret(name, env=dict(os.environ))
        for name in resolve_api_key_env_vars(provider_slug)
    )


async def _run_checks_async(probes: list[HealthProbe]) -> list[PreflightCheck]:
    if not probes:
        return []
    return list(await asyncio.gather(*(asyncio.to_thread(probe.check) for probe in probes)))
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
            from registry.provider_execution_registry import (
                resolve_adapter_economics,
                resolve_adapter_contract,
                resolve_api_endpoint,
                resolve_binary,
                resolve_lane_policy_record,
                supports_adapter,
            )
            from runtime.workflow._adapter_registry import runtime_supports_workflow_adapter_type

            lane_policy = resolve_lane_policy_record(self._provider_slug, self._adapter_type)
            contract = resolve_adapter_contract(self._provider_slug, self._adapter_type)
            supported = supports_adapter(self._provider_slug, self._adapter_type)
            runtime_adapter_supported = runtime_supports_workflow_adapter_type(self._adapter_type)
            details: dict[str, Any] = {
                "provider_slug": self._provider_slug,
                "adapter_type": self._adapter_type,
                "supported": supported,
                "runtime_adapter_supported": runtime_adapter_supported,
                "lane_policy": lane_policy or {},
                "admission_state": (
                    "admitted_by_policy"
                    if (lane_policy or {}).get("admitted_by_policy") is True
                    else "disabled_by_policy"
                )
                if lane_policy is not None
                else "policy_unknown",
            }
            if lane_policy is not None and lane_policy.get("admitted_by_policy") is not True:
                reason = str(lane_policy.get("policy_reason") or "adapter not admitted by policy")
                details["policy_reason"] = reason
                return _build_check(
                    name=self.name,
                    passed=True,
                    message=f"adapter disabled by policy: {reason}",
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    status="disabled_by_policy",
                    details=details,
                )
            if not runtime_adapter_supported:
                return _build_check(
                    name=self.name,
                    passed=False,
                    message=f"workflow runtime does not register adapter_type={self._adapter_type}",
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    status="failed",
                    details=details,
                )
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
                status = "ok" if transport_ready else "failed"
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
                status = "ok" if transport_ready else "failed"

            details["transport_ready"] = transport_ready
            passed = supported and (
                transport_ready
                if self._adapter_type in {"cli_llm", "llm_task"}
                else True
            )

            return _build_check(
                name=self.name,
                passed=passed,
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
        warning_threshold: int = DEFAULT_QUEUE_WARNING_THRESHOLD,
        critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
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

        async def _check():
            asyncpg = _asyncpg_module()
            conn = await asyncio.wait_for(asyncpg.connect(url), timeout=self._timeout_seconds)
            try:
                row = await asyncio.wait_for(
                    conn.fetchrow(QUEUE_DEPTH_BREAKDOWN_QUERY),
                    timeout=self._timeout_seconds,
                )
                return row
            finally:
                await conn.close()

        try:
            row = _run_async(_check())
            snapshot = queue_depth_snapshot_from_row(
                row,
                warning_threshold=self._warning_threshold,
                critical_threshold=self._critical_threshold,
            )
            return _build_check(
                name=self.name,
                passed=snapshot.passed,
                message=f"{snapshot.total_queued} jobs pending or ready",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=snapshot.status,
                details=snapshot.details(),
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

class SchedulerProbe(HealthProbe):
    """Checks whether the scheduler has ticked recently."""

    def __init__(
        self,
        state_file: str | Path | None = None,
        window_minutes: int = 15,
        database_url: str | None = None,
    ) -> None:
        self._state_file = state_file
        self._window_minutes = window_minutes
        self._database_url = database_url

    @property
    def name(self) -> str:
        return "scheduler_running"

    def check(self) -> PreflightCheck:
        started_at = _utcnow()
        started_monotonic = time.monotonic()
        database_url = _resolve_database_url(self._database_url)
        if not database_url:
            return _build_check(
                name=self.name,
                passed=False,
                message="WORKFLOW_DATABASE_URL not configured for scheduler heartbeat",
                started_at=started_at,
                started_monotonic=started_monotonic,
                status="failed",
                duration_ms=None,
            )

        async def _get_last_tick() -> Any:
            asyncpg = _asyncpg_module()
            conn = await asyncio.wait_for(asyncpg.connect(database_url), timeout=_DEFAULT_CONNECT_TIMEOUT)
            try:
                return await asyncio.wait_for(conn.fetchval(_SCHEDULE_HEARTBEAT_QUERY), timeout=_DEFAULT_CONNECT_TIMEOUT)
            finally:
                await conn.close()

        try:
            last_tick = _run_async(_get_last_tick())
            if last_tick is None:
                return _build_check(
                    name=self.name,
                    passed=False,
                    message="No scheduler.tick events found in system_events",
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    status="failed",
                    details={"source": "system_events"},
                    duration_ms=None,
                )
            if isinstance(last_tick, str):
                last_tick = datetime.fromisoformat(last_tick)
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
                details={
                    "age_minutes": round(age_minutes, 2),
                    "window_minutes": self._window_minutes,
                    "source": "system_events",
                    "last_tick": last_tick.isoformat(),
                },
            )
        except Exception as exc:
            return _build_check(
                name=self.name,
                passed=False,
                message=f"Failed to check scheduler heartbeat: {exc}",
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
        path: str | Path,
        warn_percent: float = 80.0,
        fail_percent: float = 95.0,
    ) -> None:
        self._path = Path(path)
        self._warn_percent = warn_percent
        self._fail_percent = fail_percent

    @property
    def name(self) -> str:
        return f"disk_usage:{self._path}"

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
                status = "degraded"
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
    probes: list[HealthProbe] = [
        PostgresConnectivityProbe(database_url=database_url),
        WorkflowWorkerProbe(database_url=database_url, window_minutes=queue_window_minutes),
        QueueDepthProbe(database_url=database_url),
        SchedulerProbe(
            state_file=scheduler_state_file,
            window_minutes=scheduler_window_minutes,
            database_url=database_url,
        ),
        ApiLivenessProbe(host=api_host, port=api_port),
    ]
    if receipts_dir is not None:
        probes.append(DiskUsageProbe(path=receipts_dir, warn_percent=warn_percent, fail_percent=fail_percent))
    return probes


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
    return _run_async(
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
    "StaticHealthProbe",
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
