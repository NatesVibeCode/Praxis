"""Daemon worker for operating-model card execution.

Queued workflow runs are executed by the unified workflow job worker
(`runtime.workflow_worker` -> `runtime.workflow.unified.run_worker_loop`).
This helper stays focused on `run_nodes` card execution inside repo-local
server processes so we do not accidentally resurrect the legacy WorkflowRunner
path from API or MCP surfaces.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Optional, Protocol

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


class RunNodeStateRepository(Protocol):
    """Minimal storage contract for run-node lifecycle ownership."""

    def list_ready_card_nodes(self) -> list[dict[str, Any]]: ...
    def claim_ready_run_node(self, *, run_node_id: str) -> bool: ...
    def mark_terminal_state(
        self,
        *,
        run_node_id: str,
        state: str,
        output_payload: Mapping[str, Any] | None = None,
        failure_code: str | None = None,
    ) -> bool: ...
    def mark_failed(self, *, run_node_id: str, failure_code: str) -> bool: ...


class WorkflowNotificationEmitter(Protocol):
    """Minimal storage contract for durable workflow notification rows."""

    def emit_notification(
        self,
        *,
        run_id: str,
        job_label: str,
        spec_name: str,
        agent_slug: str,
        status: str,
        failure_code: str | None = None,
        duration_seconds: float = 0.0,
    ) -> None: ...


class WorkflowWorker:
    """Background worker that executes ready card nodes inside the server process."""

    def __init__(
        self,
        conn: SyncPostgresConnection,
        repo_root: str,
        poll_interval: float = 2.0,
        *,
        run_node_repository: RunNodeStateRepository | None = None,
        notification_repository: WorkflowNotificationEmitter | None = None,
    ) -> None:
        self._conn = conn
        self._repo_root = Path(repo_root)
        self._poll_interval = poll_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        if run_node_repository is None or notification_repository is None:
            from storage.postgres.workflow_orchestration_repository import (
                PostgresRunNodeStateRepository,
                PostgresWorkflowNotificationRepository,
            )

            run_node_repository = (
                run_node_repository or PostgresRunNodeStateRepository(conn)
            )
            notification_repository = (
                notification_repository or PostgresWorkflowNotificationRepository(conn)
            )
        self._run_node_repository = run_node_repository
        self._notification_repository = notification_repository

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background worker thread (idempotent).

        When running inside the MCP server (CLAUDECODE is set), skip —
        the external launchd workflow worker handles execution instead.
        When running as the standalone workflow_worker process, proceed normally.
        """
        import os
        if os.environ.get("CLAUDECODE"):
            logger.info("Workflow worker thread skipped (MCP server context) — external workflow worker handles execution")
            return
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="workflow-worker",
        )
        self._thread.start()
        logger.info("Workflow worker started")

    def stop(self) -> None:
        """Signal the worker to stop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        """Poll for ready card executions."""
        while self._running:
            try:
                self._poll_once()
            except Exception as exc:
                logger.error("Workflow worker error: %s", exc, exc_info=True)
                raise
            time.sleep(self._poll_interval)

    def _poll_once(self) -> None:
        """Check for ready card executions only."""
        card_rows = self._run_node_repository.list_ready_card_nodes()
        if card_rows:
            # Split by executor kind: API-backed vs CLI-backed
            api_cards = []
            cli_cards = []
            for row in card_rows:
                payload = row.get("input_payload", {})
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"Invalid run_nodes.input_payload JSON for run_node_id={row.get('run_node_id')}"
                        ) from exc
                executor_kind = (payload.get("executor", {}).get("kind", "")).lower()
                if executor_kind in ("app", "system", ""):
                    api_cards.append(row)
                else:
                    cli_cards.append(row)

            from concurrent.futures import ThreadPoolExecutor, as_completed

            all_futures = {}

            # API-backed cards: unlimited concurrency (external API calls)
            if api_cards:
                api_pool = ThreadPoolExecutor(max_workers=len(api_cards))
                for row in api_cards:
                    all_futures[api_pool.submit(self._execute_card_node, row)] = row

            # CLI-backed cards: cap at 4 (local subprocess slots)
            if cli_cards:
                cli_pool = ThreadPoolExecutor(max_workers=min(len(cli_cards), 4))
                for row in cli_cards:
                    all_futures[cli_pool.submit(self._execute_card_node, row)] = row

            for future in as_completed(all_futures):
                try:
                    future.result()
                except Exception as exc:
                    row = all_futures[future]
                    logger.error("Parallel card execution failed for %s: %s",
                                 row.get("node_id", "?"), exc, exc_info=True)
                    raise

            # Shut down pools (threads already finished via as_completed)
            if api_cards:
                api_pool.shutdown(wait=False)
            if cli_cards:
                cli_pool.shutdown(wait=False)

    def _execute_card_node(self, row: dict) -> None:
        """Execute a single operating model card via the event bus."""
        run_node_id = row["run_node_id"]
        run_id = row["run_id"]
        node_id = row["node_id"]

        # Atomic claim: ready → running
        claimed = self._run_node_repository.claim_ready_run_node(
            run_node_id=run_node_id,
        )
        if not claimed:
            return  # Another worker claimed it

        logger.info("Card worker executing %s (node=%s, run=%s)", run_node_id, node_id, run_id)

        try:
            from runtime.model_executor import execute_card, release_downstream

            result = execute_card(self._conn, row, str(self._repo_root))
            status = result.get("status", "failed")

            if status == "awaiting_human":
                # Don't update — execute_card already set awaiting_human
                logger.info("Card %s awaiting human approval", node_id)
                return

            self._run_node_repository.mark_terminal_state(
                run_node_id=run_node_id,
                state="succeeded" if status == "succeeded" else "failed",
                output_payload=result.get("outputs", {}),
                failure_code=result.get("failure_code", "") or "",
            )

            self._notification_repository.emit_notification(
                run_id=run_id,
                job_label=node_id,
                spec_name="model_run",
                agent_slug="card_executor",
                status=status,
                failure_code=result.get("failure_code", ""),
                duration_seconds=0.0,
            )

            release_downstream(self._conn, run_id, node_id)
        except Exception as exc:
            logger.error("Card execution failed for %s: %s", node_id, exc, exc_info=True)
            self._run_node_repository.mark_failed(
                run_node_id=run_node_id,
                failure_code=str(exc)[:200],
            )
            raise

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running
