"""Worker loop: run_worker_loop and supporting infrastructure.

Extracted from unified.py — contains the main worker loop that claims
jobs from workflow_jobs and dispatches them for concurrent execution,
plus the LISTEN/NOTIFY background listener for instant wakeups.
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, Future
from typing import TYPE_CHECKING

from ._admission import _execute_admitted_graph_run
from ._claiming import claim_one, complete_job, reap_stale_claims, reap_stale_runs
from ._execution_core import execute_job
from runtime.execution_transport import resolve_execution_transport
from runtime._workflow_database import resolve_runtime_database_url
from runtime.self_healing import derive_terminal_reason_code
from runtime.system_events import emit_system_event
from storage.postgres.validators import PostgresConfigurationError

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = ["run_worker_loop", "resolve_worker_concurrency"]

_GRAPH_FAILURE_BACKOFF_SECONDS = 30.0
_WORKER_SLOT_MEMORY_BYTES = 2 * 1024 * 1024 * 1024
_CGROUP_UNLIMITED_MEMORY_THRESHOLD = 1 << 60
_WORKER_CONCURRENCY_ENV_KEYS = (
    "PRAXIS_WORKER_MAX_PARALLEL",
    "PRAXIS_WORKFLOW_MAX_CONCURRENT_NODES",
)
_HOST_RESOURCE_DISABLED_ENV = "PRAXIS_HOST_RESOURCE_ADMISSION_DISABLED"
_HOST_DOCKER_SANDBOX_SLOTS_ENV = "PRAXIS_HOST_DOCKER_SANDBOX_SLOTS"
_DEFAULT_HOST_DOCKER_SANDBOX_SLOTS = 2


def _read_text_file(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError:
        return None


def _parse_positive_int(raw: object, *, label: str) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer, got: {raw}") from exc
    if value < 1:
        raise ValueError(f"{label} must be a positive integer, got: {raw}")
    return value


def _env_truthy(env: Mapping[str, str], name: str) -> bool:
    return str(env.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _host_docker_sandbox_slot_budget(env: Mapping[str, str] | None = None) -> int | None:
    """Return the local Docker sandbox slot cap enforced by host admission."""

    resolved_env = os.environ if env is None else env
    if _env_truthy(resolved_env, _HOST_RESOURCE_DISABLED_ENV):
        return None
    raw = str(resolved_env.get(_HOST_DOCKER_SANDBOX_SLOTS_ENV) or "").strip()
    if not raw:
        return _DEFAULT_HOST_DOCKER_SANDBOX_SLOTS
    try:
        return _parse_positive_int(raw, label=_HOST_DOCKER_SANDBOX_SLOTS_ENV)
    except ValueError:
        logger.warning(
            "Invalid %s=%r; using default host sandbox slots=%d",
            _HOST_DOCKER_SANDBOX_SLOTS_ENV,
            raw,
            _DEFAULT_HOST_DOCKER_SANDBOX_SLOTS,
        )
        return _DEFAULT_HOST_DOCKER_SANDBOX_SLOTS


def _cap_local_slots_to_host_admission(
    slots: int,
    env: Mapping[str, str] | None = None,
) -> int:
    """Keep worker admission aligned with the downstream host-resource gate."""

    host_slots = _host_docker_sandbox_slot_budget(env)
    if host_slots is None:
        return max(1, slots)
    return max(1, min(slots, host_slots))


def _parse_cgroup_memory_limit(raw: str | None) -> int | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value or value == "max":
        return None
    try:
        limit = int(value)
    except ValueError:
        return None
    if limit <= 0 or limit >= _CGROUP_UNLIMITED_MEMORY_THRESHOLD:
        return None
    return limit


def _worker_cgroup_available_memory_bytes() -> int | None:
    candidates: list[int] = []
    v2_limit = _parse_cgroup_memory_limit(_read_text_file("/sys/fs/cgroup/memory.max"))
    v2_current = _read_text_file("/sys/fs/cgroup/memory.current")
    if v2_limit is not None and v2_current is not None:
        try:
            candidates.append(max(v2_limit - int(v2_current), 0))
        except ValueError:
            pass

    v1_limit = _parse_cgroup_memory_limit(
        _read_text_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    )
    v1_current = _read_text_file("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    if v1_limit is not None and v1_current is not None:
        try:
            candidates.append(max(v1_limit - int(v1_current), 0))
        except ValueError:
            pass
    return min(candidates) if candidates else None


def _worker_proc_available_memory_bytes() -> int | None:
    raw = _read_text_file("/proc/meminfo")
    if raw is None:
        return None
    for line in raw.splitlines():
        if not line.startswith("MemAvailable:"):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1]) * 1024
        except ValueError:
            return None
    return None


def _worker_macos_available_memory_bytes() -> int | None:
    try:
        result = subprocess.run(
            ["vm_stat"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None

    page_size = 4096
    pages: dict[str, int] = {}
    for line in result.stdout.splitlines():
        if "page size of" in line:
            for token in line.split():
                if token.isdigit():
                    page_size = int(token)
                    break
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        numeric = value.strip().rstrip(".").replace(".", "")
        if numeric.isdigit():
            pages[key.strip()] = int(numeric)

    reclaimable_pages = (
        pages.get("Pages free", 0)
        + pages.get("Pages speculative", 0)
        + pages.get("Pages inactive", 0)
    )
    if reclaimable_pages <= 0:
        return None
    return reclaimable_pages * page_size


def _worker_available_memory_bytes() -> int | None:
    candidates = [
        value
        for value in (
            _worker_cgroup_available_memory_bytes(),
            _worker_proc_available_memory_bytes(),
        )
        if value is not None
    ]
    if candidates:
        return min(candidates)
    return _worker_macos_available_memory_bytes()


def _worker_cgroup_cpu_count() -> int | None:
    cpu_max = _read_text_file("/sys/fs/cgroup/cpu.max")
    if cpu_max:
        parts = cpu_max.split()
        if len(parts) >= 2 and parts[0] != "max":
            try:
                quota = int(parts[0])
                period = int(parts[1])
                if quota > 0 and period > 0:
                    return max(1, (quota + period - 1) // period)
            except ValueError:
                pass

    quota_raw = _read_text_file("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period_raw = _read_text_file("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota_raw is not None and period_raw is not None:
        try:
            quota = int(quota_raw)
            period = int(period_raw)
            if quota > 0 and period > 0:
                return max(1, (quota + period - 1) // period)
        except ValueError:
            pass
    return None


def _worker_cpu_count() -> int:
    return max(1, _worker_cgroup_cpu_count() or os.cpu_count() or 1)


def resolve_worker_concurrency(
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Resolve the local worker slot budget from explicit overrides or resources."""
    resolved_env = os.environ if env is None else env
    available_memory_bytes = _worker_available_memory_bytes()
    cpu_count = _worker_cpu_count()

    for key in _WORKER_CONCURRENCY_ENV_KEYS:
        raw = str(resolved_env.get(key) or "").strip()
        if not raw:
            continue
        max_concurrent = _parse_positive_int(raw, label=key)
        return {
            "max_concurrent": max_concurrent,
            "source": f"env:{key}",
            "cpu_count": cpu_count,
            "available_memory_bytes": available_memory_bytes,
            "memory_slot_bytes": _WORKER_SLOT_MEMORY_BYTES,
        }

    memory_slots = None
    if available_memory_bytes is not None:
        memory_slots = max(1, available_memory_bytes // _WORKER_SLOT_MEMORY_BYTES)
    max_concurrent = cpu_count if memory_slots is None else min(cpu_count, memory_slots)
    return {
        "max_concurrent": max(1, max_concurrent),
        "source": "resource:auto",
        "cpu_count": cpu_count,
        "available_memory_bytes": available_memory_bytes,
        "memory_slot_bytes": _WORKER_SLOT_MEMORY_BYTES,
    }


def _evaluate_workflow_triggers(conn: "SyncPostgresConnection") -> int:
    from runtime.triggers import evaluate_triggers

    return evaluate_triggers(conn)


def _advance_background_workflow_chains(conn: "SyncPostgresConnection") -> int:
    from runtime.workflow_chain import advance_workflow_chains

    return advance_workflow_chains(conn)


def _evaluate_ready_specs(conn: "SyncPostgresConnection") -> int:
    from runtime.workflow.ready_specs import evaluate_ready_specs

    return evaluate_ready_specs(conn)


def _list_ready_graph_runs(
    conn: "SyncPostgresConnection",
    *,
    limit: int,
) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT run_id, workflow_id, requested_at
        FROM workflow_runs
        WHERE current_state = 'claim_accepted'
        ORDER BY requested_at, run_id
        LIMIT $1
        """,
        limit,
    )
    return [dict(row) for row in rows or ()]


# BUG-CBC73AB3: worker-layer exception→reason_code translation is owned by
# ``runtime.self_healing.derive_terminal_reason_code``. Keeping this alias
# preserves the call-site names in this module without re-creating a second
# independent authority.
_worker_error_code = derive_terminal_reason_code


def _fail_graph_run_closed(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    exc: BaseException,
) -> str:
    failure_code = _worker_error_code(exc, fallback="workflow_graph_execution_failed")
    rows = conn.execute(
        """
        UPDATE workflow_runs
           SET current_state = 'failed',
               started_at = COALESCE(started_at, admitted_at, requested_at, now()),
               finished_at = GREATEST(COALESCE(started_at, admitted_at, requested_at, now()), now()),
               terminal_reason_code = COALESCE(terminal_reason_code, $2)
         WHERE run_id = $1
           AND current_state = 'claim_accepted'
         RETURNING workflow_id, request_id
        """,
        run_id,
        failure_code,
    )
    if not rows:
        return failure_code
    row = dict(rows[0])
    workflow_id = str(row.get("workflow_id") or "")
    payload = {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "status": "failed",
        "reason_code": failure_code,
        "total_jobs": 0,
        "succeeded": 0,
        "failed": 1,
        "blocked": 0,
        "cancelled": 0,
        "parent_run_id": None,
        "trigger_depth": 0,
    }
    emit_system_event(
        conn,
        event_type="workflow.failed",
        source_id=run_id,
        source_type="workflow_run",
        payload=payload,
    )
    emit_system_event(
        conn,
        event_type="run.failed",
        source_id=run_id,
        source_type="workflow_run",
        payload=payload,
    )
    conn.execute("SELECT pg_notify('run_complete', $1)", run_id)
    return failure_code


class _WorkerNotificationListener:
    """Background LISTEN/NOTIFY consumer that wakes the worker loop."""

    def __init__(
        self,
        database_url: str,
        channels: tuple[str, ...],
        wakeup_event: threading.Event,
        reconnect_delay: float = 5.0,
    ) -> None:
        self._database_url = database_url
        self._channels = channels
        self._wakeup_event = wakeup_event
        self._reconnect_delay = reconnect_delay
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="workflow-notify-listener",
        )

    def start(self) -> None:
        import asyncpg  # noqa: F401

        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup_event.set()
        self._thread.join(timeout=5)

    def _on_notify(self, _connection, _pid, channel: str, payload: str) -> None:
        logger.debug("Worker notification received on %s: %s", channel, payload)
        self._wakeup_event.set()

    def _run(self) -> None:
        import asyncio
        import asyncpg

        async def _listen() -> None:
            while not self._stop_event.is_set():
                conn = None
                try:
                    conn = await asyncpg.connect(self._database_url, timeout=5.0)
                    for channel in self._channels:
                        await conn.add_listener(channel, self._on_notify)
                    logger.info("LISTEN registered for %s", ", ".join(self._channels))
                    while not self._stop_event.is_set():
                        await asyncio.sleep(1.0)
                except Exception as exc:
                    if not self._stop_event.is_set():
                        logger.warning("LISTEN loop error, reconnecting: %s", exc)
                        await asyncio.sleep(self._reconnect_delay)
                finally:
                    if conn is not None:
                        await conn.close()

        asyncio.run(_listen())


def _start_embedding_prewarm_for_worker() -> None:
    from runtime.embedding_service import (
        EmbeddingService,
        resolve_embedding_runtime_authority,
    )

    model_name = resolve_embedding_runtime_authority().model_name
    try:
        prewarm_thread = EmbeddingService.start_background_prewarm(model_name)
    except Exception as exc:
        logger.warning("Worker embedding prewarm failed to start for %s: %s", model_name, exc, exc_info=True)
        return
    if prewarm_thread is not None:
        logger.info("Worker embedding prewarm scheduled for %s", model_name)


def run_worker_loop(
    conn: SyncPostgresConnection,
    repo_root: str,
    poll_interval: float = 2.0,
    worker_id: str | None = None,
    max_local_concurrent: int | None = None,
) -> None:
    """Main worker loop: claim jobs from workflow_jobs, execute concurrently.

    Remote transports run with unlimited concurrency (external calls).
    Local transports are capped by explicit slots or live CPU/RAM resources.
    Runs stale claim reaper every 60 seconds.
    """
    from concurrent.futures import ThreadPoolExecutor, Future
    from storage.postgres.connection import SyncPostgresConnection as _PG, get_workflow_pool

    worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
    if max_local_concurrent is None:
        concurrency_decision = resolve_worker_concurrency()
        local_pool_max_workers = int(concurrency_decision["cpu_count"])
        resource_managed_slots = concurrency_decision["source"] == "resource:auto"
    else:
        local_pool_max_workers = _parse_positive_int(
            max_local_concurrent,
            label="max_local_concurrent",
        )
        resource_managed_slots = False
        concurrency_decision = {
            "max_concurrent": local_pool_max_workers,
            "source": "explicit",
            "cpu_count": _worker_cpu_count(),
            "available_memory_bytes": _worker_available_memory_bytes(),
            "memory_slot_bytes": _WORKER_SLOT_MEMORY_BYTES,
        }

    initial_local_slot_budget = int(concurrency_decision["max_concurrent"])
    logger.info(
        "Unified workflow worker started: %s (local_slots=%d, pool_workers=%d, source=%s, cpu=%s, available_memory_bytes=%s)",
        worker_id,
        initial_local_slot_budget,
        local_pool_max_workers,
        concurrency_decision.get("source"),
        concurrency_decision.get("cpu_count"),
        concurrency_decision.get("available_memory_bytes"),
    )
    _start_embedding_prewarm_for_worker()

    notification_wakeup = threading.Event()
    try:
        database_url = resolve_runtime_database_url(required=True)
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            "WORKFLOW_DATABASE_URL is required for workflow LISTEN/NOTIFY wakeups"
        ) from exc
    notification_listener = _WorkerNotificationListener(
        database_url=database_url,
        channels=("job_ready", "run_complete", "system_event"),
        wakeup_event=notification_wakeup,
    )
    notification_listener.start()

    def _evaluate_background_consumers() -> None:
        try:
            fired = _evaluate_workflow_triggers(conn)
            if fired:
                logger.info("Trigger evaluator fired %d workflow(s)", fired)
        except Exception as exc:
            logger.warning("Trigger evaluation failed: %s", exc, exc_info=True)

        try:
            ready_fired = _evaluate_ready_specs(conn)
            if ready_fired:
                logger.info("Ready-spec evaluator fired %d workflow(s)", ready_fired)
        except Exception as exc:
            logger.warning("Ready-spec evaluation failed: %s", exc, exc_info=True)

        try:
            advanced = _advance_background_workflow_chains(conn)
            if advanced:
                logger.info("Workflow-chain evaluator advanced %d action(s)", advanced)
        except Exception as exc:
            logger.warning("Workflow-chain evaluation failed: %s", exc, exc_info=True)

    last_reap = time.monotonic()
    reap_interval = 60.0
    last_run_reap = time.monotonic()
    run_reap_interval = 300.0  # check for stale runs every 5 minutes
    active_futures: dict[Future, dict] = {}
    graph_retry_after: dict[str, float] = {}
    _evaluate_background_consumers()

    # Each thread gets its own DB connection (can't share asyncpg across threads)
    def _kill_child_processes(parent_pid: int) -> int:
        """Kill all child processes of parent_pid. Returns count killed."""
        import signal
        killed = 0
        try:
            import subprocess as _sp
            result = _sp.run(
                ["pgrep", "-P", str(parent_pid)],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.strip().splitlines():
                child_pid = int(line.strip())
                # Recurse to kill grandchildren first
                killed += _kill_child_processes(child_pid)
                try:
                    os.kill(child_pid, signal.SIGTERM)
                    killed += 1
                except ProcessLookupError:
                    pass
        except Exception:
            pass
        return killed

    def _run_job(job: dict) -> None:
        pool = get_workflow_pool()
        thread_conn = _PG(pool)
        job_id = job["id"]
        stop_heartbeat = threading.Event()
        cancelled = threading.Event()
        # Store the OS PID of the thread running the job so cancellation
        # can find and kill its child subprocess tree.
        executor_pid = os.getpid()

        def _heartbeat() -> None:
            hb_pool = get_workflow_pool()
            hb_conn = _PG(hb_pool)
            while not stop_heartbeat.wait(30):
                try:
                    current_status = ""
                    if hasattr(hb_conn, "fetchrow"):
                        row = hb_conn.fetchrow(
                            "UPDATE workflow_jobs SET heartbeat_at = now() WHERE id = $1 RETURNING status",
                            job_id,
                        )
                        if row:
                            current_status = str(row.get("status", ""))
                    else:
                        hb_conn.execute(
                            "UPDATE workflow_jobs SET heartbeat_at = now() WHERE id = $1",
                            job_id,
                        )
                    if current_status == "cancelled":
                        logger.info("Job %s cancelled externally, killing subprocesses", job.get("label"))
                        cancelled.set()
                        killed = _kill_child_processes(executor_pid)
                        if killed:
                            logger.info("Killed %d child processes for cancelled job %s", killed, job.get("label"))
                        break
                except Exception as exc:
                    logger.error("Heartbeat update failed for job %d: %s", job_id, exc, exc_info=True)
                    break

        hb_thread = threading.Thread(target=_heartbeat, daemon=True)
        hb_thread.start()
        try:
            execute_job(thread_conn, job, repo_root)
        except Exception as exc:
            if cancelled.is_set():
                logger.info("Job %s execution interrupted by cancellation", job.get("label"))
            else:
                logger.error("Job execution failed for %s: %s", job.get("label"), exc, exc_info=True)
                complete_job(
                    thread_conn,
                    job["id"],
                    status="failed",
                    error_code=_worker_error_code(exc, fallback="worker_exception"),
                    duration_ms=0,
                    stdout_preview=str(exc)[:2000],
                )
        finally:
            stop_heartbeat.set()

    def _run_graph(run_row: dict[str, object]) -> None:
        pool = get_workflow_pool()
        thread_conn = _PG(pool)
        run_id = str(run_row.get("run_id") or "").strip()
        if not run_id:
            return
        try:
            logger.info("Worker executing admitted graph run %s", run_id)
            result = _execute_admitted_graph_run(thread_conn, run_id=run_id)
            if isinstance(result, Mapping):
                status = str(result.get("status") or "").strip().lower()
                if status == "locked":
                    logger.info("Graph run %s is already locked by another executor", run_id)
                    return
            else:
                status = str(getattr(result, "status", "") or "").strip().lower()
                if status == "locked":
                    logger.info("Graph run %s is already locked by another executor", run_id)
                    return
            # Silent-failure guard: if execution returned without transitioning the
            # run state out of claim_accepted (e.g. route.unhealthy, intake.rejected,
            # or any early-exit failure_result), force it to 'failed' so operators
            # aren't left with a run stuck in claim_accepted forever.
            reason_code = None
            if isinstance(result, Mapping):
                if status in {"failed", "rejected", "error"}:
                    reason_code = (
                        str(result.get("reason_code") or "").strip()
                        or str(result.get("failure_code") or "").strip()
                        or "workflow.silent_failure"
                    )
            else:
                if status in {"failed", "rejected", "error"}:
                    reason_code = (
                        str(getattr(result, "reason_code", "") or "").strip()
                        or str(getattr(result, "failure_code", "") or "").strip()
                        or "workflow.silent_failure"
                    )
            if reason_code is None:
                # Also catch "nothing happened" — result is None / empty, and
                # current_state is still claim_accepted.
                state_rows = thread_conn.execute(
                    "SELECT current_state FROM workflow_runs WHERE run_id = $1",
                    run_id,
                )
                if state_rows:
                    current_state = str(dict(state_rows[0]).get("current_state") or "").strip()
                    if current_state == "claim_accepted":
                        reason_code = "workflow.silent_no_transition"
            if reason_code:
                synthetic = RuntimeError(reason_code)
                setattr(synthetic, "failure_code", reason_code)
                failure_code = _fail_graph_run_closed(
                    thread_conn, run_id=run_id, exc=synthetic
                )
                logger.error(
                    "Graph run %s force-failed after silent return (%s)",
                    run_id,
                    failure_code,
                )
        except Exception as exc:
            failure_code = _fail_graph_run_closed(thread_conn, run_id=run_id, exc=exc)
            logger.error(
                "Graph run %s failed closed with %s: %s",
                run_id,
                failure_code,
                exc,
                exc_info=True,
            )

    # Two pools: unlimited for remote transport, capped for local transport.
    # In resource-managed mode the pool can use the visible CPU allotment, while
    # claim admission recomputes the live RAM/CPU slot budget before dispatch.
    api_pool = ThreadPoolExecutor(max_workers=64, thread_name_prefix="api-worker")
    local_pool = ThreadPoolExecutor(max_workers=local_pool_max_workers, thread_name_prefix="local-worker")

    def _count_active(lane: str) -> int:
        return sum(1 for f, j in active_futures.items()
                   if not f.done() and j.get("_transport_lane") == lane)

    def _active_graph_runs() -> set[str]:
        return {
            str(job.get("run_id"))
            for future, job in active_futures.items()
            if not future.done() and job.get("_transport_lane") == "graph_local"
        }

    def _active_local_slots() -> int:
        return _count_active("local") + len(_active_graph_runs())

    def _local_slot_budget() -> int:
        if not resource_managed_slots:
            return _cap_local_slots_to_host_admission(local_pool_max_workers)
        try:
            decision = resolve_worker_concurrency(env={})
            return _cap_local_slots_to_host_admission(
                max(1, min(local_pool_max_workers, int(decision["max_concurrent"])))
            )
        except Exception as exc:
            logger.warning(
                "Worker resource concurrency refresh failed; using startup budget: %s",
                exc,
                exc_info=True,
            )
            return _cap_local_slots_to_host_admission(
                max(1, min(local_pool_max_workers, initial_local_slot_budget))
            )

    try:
        while True:
            try:
                # Clean up completed futures
                done = [f for f in active_futures if f.done()]
                for f in done:
                    job = active_futures.pop(f)
                    run_id = str(job.get("run_id") or "").strip()
                    try:
                        f.result()
                        if job.get("_transport_lane") == "graph_local" and run_id:
                            graph_retry_after.pop(run_id, None)
                    except Exception as exc:
                        if job.get("_transport_lane") == "graph_local":
                            if run_id:
                                graph_retry_after[run_id] = (
                                    time.monotonic() + _GRAPH_FAILURE_BACKOFF_SECONDS
                                )
                            logger.error(
                                "Future failed for graph run %s: %s",
                                job.get("run_id"),
                                exc,
                                exc_info=True,
                            )
                            continue
                        logger.error("Future failed for %s: %s", job.get("label"), exc, exc_info=True)
                        try:
                            complete_job(
                                conn,
                                int(job["id"]),
                                status="failed",
                                error_code=_worker_error_code(exc, fallback="worker_future_exception"),
                                duration_ms=0,
                                stdout_preview=str(exc)[:2000],
                            )
                        except Exception as complete_exc:
                            logger.error(
                                "Future reconciliation failed for %s: %s",
                                job.get("label"),
                                complete_exc,
                                exc_info=True,
                            )
                        continue

                # Reap stale claims periodically
                if time.monotonic() - last_reap > reap_interval:
                    reaped = reap_stale_claims(conn)
                    if reaped:
                        logger.info("Reaped %d stale jobs", reaped)
                    last_reap = time.monotonic()

                # Reap stale runs (queued/running with no activity for >2h)
                if time.monotonic() - last_run_reap > run_reap_interval:
                    reaped_runs = reap_stale_runs(conn)
                    if reaped_runs:
                        logger.warning("Reaped %d stale runs", reaped_runs)
                    last_run_reap = time.monotonic()

                if notification_wakeup.is_set():
                    notification_wakeup.clear()
                    _evaluate_background_consumers()

                local_slot_budget = _local_slot_budget()
                graph_slots = local_slot_budget - _active_local_slots()
                if graph_slots > 0:
                    now = time.monotonic()
                    scheduled_graph_runs = 0
                    scan_limit = max(graph_slots * 10, 10)
                    for run_row in _list_ready_graph_runs(conn, limit=scan_limit):
                        if scheduled_graph_runs >= graph_slots:
                            break
                        run_id = str(run_row.get("run_id") or "").strip()
                        if not run_id or run_id in _active_graph_runs():
                            continue
                        retry_after = graph_retry_after.get(run_id)
                        if retry_after is not None and now < retry_after:
                            continue
                        if retry_after is not None:
                            graph_retry_after.pop(run_id, None)
                        future = local_pool.submit(_run_graph, run_row)
                        active_futures[future] = {
                            "run_id": run_id,
                            "label": run_id,
                            "_transport_lane": "graph_local",
                        }
                        scheduled_graph_runs += 1
                        logger.info(
                            "Dispatched graph run %s to local pool [active: remote=%d local=%d graph=%d]",
                            run_id,
                            _count_active("remote"),
                            _count_active("local"),
                            len(_active_graph_runs()),
                        )

                # Claim as many ready jobs as we can run
                claimed_any = False
                for _ in range(10):  # Claim up to 10 per poll cycle
                    job = claim_one(conn, worker_id)
                    if not job:
                        break

                    # Determine execution transport for concurrency management.
                    # Failures here must fail the individual job, not crash
                    # the worker loop — a missing agent config is a job-level
                    # error, not a system-level error.
                    agent = job.get("resolved_agent") or job.get("agent_slug", "")
                    integration_id = job.get("integration_id")
                    transport_lane: str | None = None
                    try:
                        if integration_id:
                            transport_lane = "remote"
                        else:
                            registry = _get_cached_registry(conn)
                            if registry is None:
                                raise RuntimeError("agent registry unavailable during worker dispatch")
                            config = registry.get(agent)
                            if config is None:
                                raise RuntimeError(f"agent config missing during worker dispatch: {agent}")
                            transport_lane = resolve_execution_transport(config).execution_lane
                        if transport_lane == "unknown":
                            raise RuntimeError(f"unsupported execution transport for {agent}")
                    except RuntimeError as dispatch_err:
                        logger.error("Dispatch setup failed for job %d (%s): %s",
                                     job["id"], job.get("label"), dispatch_err)
                        complete_job(conn, job["id"], status="failed",
                                     error_code="agent_not_found",
                                     stdout_preview=str(dispatch_err)[:2000])
                        continue

                    job["_transport_lane"] = transport_lane

                    local_slot_budget = _local_slot_budget()
                    if transport_lane == "local" and _active_local_slots() >= local_slot_budget:
                        # Can't take more local jobs — unclaim without touching attempt counter
                        # (attempt was incremented in CLAIM_QUERY; we must undo it since
                        # we never actually attempted execution)
                        conn.execute(
                            """UPDATE workflow_jobs
                               SET status = 'ready', claimed_by = NULL, claimed_at = NULL,
                                   heartbeat_at = NULL, attempt = GREATEST(attempt - 1, 0)
                               WHERE id = $1""",
                            job["id"],
                        )
                        break

                    pool_to_use = api_pool if transport_lane == "remote" else local_pool
                    future = pool_to_use.submit(_run_job, job)
                    active_futures[future] = job
                    claimed_any = True
                    logger.info("Dispatched job %d (%s) to %s pool [active: remote=%d local=%d]",
                                job["id"], job["label"], transport_lane,
                                _count_active("remote"), _count_active("local"))

                if not claimed_any and not active_futures:
                    # LISTEN/NOTIFY wakes the loop early; timed wait bounds idle sleep.
                    if notification_wakeup.wait(poll_interval):
                        notification_wakeup.clear()
                elif not claimed_any:
                    if notification_wakeup.wait(0.5):
                        notification_wakeup.clear()  # Active jobs running, check completions frequently

            except KeyboardInterrupt:
                logger.info("Worker %s shutting down (waiting for %d active jobs)", worker_id, len(active_futures))
                break
            except Exception as exc:
                logger.error("Worker loop error: %s", exc, exc_info=True)
                raise
    finally:
        if notification_listener is not None:
            notification_listener.stop()
        api_pool.shutdown(wait=True)
        local_pool.shutdown(wait=True)


_cached_registry = None
_registry_loaded_at = 0.0

def _get_cached_registry(conn):
    """Cache the agent registry for 60s to avoid DB hits on every claim."""
    global _cached_registry, _registry_loaded_at
    if time.monotonic() - _registry_loaded_at > 60:
        from registry.agent_config import AgentRegistry

        _cached_registry = AgentRegistry.load_from_postgres(conn)
        _registry_loaded_at = time.monotonic()
    return _cached_registry
