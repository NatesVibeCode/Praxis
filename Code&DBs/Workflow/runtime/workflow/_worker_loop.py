"""Worker loop: run_worker_loop and supporting infrastructure.

Extracted from unified.py — contains the main worker loop that claims
jobs from workflow_jobs and dispatches them for concurrent execution,
plus the LISTEN/NOTIFY background listener for instant wakeups.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from typing import TYPE_CHECKING

from ._claiming import claim_one, complete_job, reap_stale_claims, reap_stale_runs
from ._execution_core import execute_job
from runtime.execution_transport import resolve_execution_transport
from runtime._workflow_database import resolve_runtime_database_url
from runtime.self_healing import normalize_failure_code
from storage.postgres.validators import PostgresConfigurationError

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = ["run_worker_loop"]


def _worker_error_code(exc: BaseException, *, fallback: str) -> str:
    for attr in ("failure_code", "reason_code", "error_code", "code"):
        value = getattr(exc, attr, None)
        if isinstance(value, str) and value.strip():
            return normalize_failure_code(value.strip(), str(exc))
    return normalize_failure_code(fallback, str(exc))


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
    max_local_concurrent: int = 4,
) -> None:
    """Main worker loop: claim jobs from workflow_jobs, execute concurrently.

    Remote transports run with unlimited concurrency (external calls).
    Local transports are capped at max_local_concurrent (subprocess slots).
    Runs stale claim reaper every 60 seconds.
    """
    from concurrent.futures import ThreadPoolExecutor, Future
    from storage.postgres.connection import SyncPostgresConnection as _PG, get_workflow_pool

    worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
    logger.info("Unified workflow worker started: %s (local_slots=%d)", worker_id, max_local_concurrent)
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
            from runtime.triggers import evaluate_triggers

            fired = evaluate_triggers(conn)
            if fired:
                logger.info("Trigger evaluator fired %d workflow(s)", fired)
        except Exception as exc:
            logger.warning("Trigger evaluation failed: %s", exc, exc_info=True)

        try:
            from runtime.workflow_chain import advance_workflow_chains

            advanced = advance_workflow_chains(conn)
            if advanced:
                logger.info("Workflow-chain evaluator advanced %d action(s)", advanced)
        except Exception as exc:
            logger.warning("Workflow-chain evaluation failed: %s", exc, exc_info=True)

    last_reap = time.monotonic()
    reap_interval = 60.0
    last_run_reap = time.monotonic()
    run_reap_interval = 300.0  # check for stale runs every 5 minutes
    active_futures: dict[Future, dict] = {}
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

    # Two pools: unlimited for remote transport, capped for local transport
    api_pool = ThreadPoolExecutor(max_workers=64, thread_name_prefix="api-worker")
    local_pool = ThreadPoolExecutor(max_workers=max_local_concurrent, thread_name_prefix="local-worker")

    def _count_active(lane: str) -> int:
        return sum(1 for f, j in active_futures.items()
                   if not f.done() and j.get("_transport_lane") == lane)

    try:
        while True:
            try:
                # Clean up completed futures
                done = [f for f in active_futures if f.done()]
                for f in done:
                    job = active_futures.pop(f)
                    try:
                        f.result()
                    except Exception as exc:
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

                    if transport_lane == "local" and _count_active("local") >= max_local_concurrent:
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
