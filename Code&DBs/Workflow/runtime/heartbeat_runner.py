"""HeartbeatRunner — wires heartbeat modules to a MemoryEngine and runs
cycles, persisting only a minimal status row in Postgres.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from collections.abc import Mapping

from memory.engine import MemoryEngine

from runtime.heartbeat import (
    DuplicateScanner,
    GapScanner,
    HeartbeatCycleResult,
    HeartbeatModule,
    HeartbeatModuleResult,
    HeartbeatOrchestrator,
    OrphanEdgeCleanup,
    StaleEntityDetector,
    _fail,
    _ok,
)

from runtime.cron_scheduler import CronScheduler as _CronScheduler

logger = logging.getLogger(__name__)

_MAINTENANCE_BATCH_LIMIT = 50
_MAINTENANCE_MAX_CLAIMS_PER_CYCLE = 600
_RATE_LIMIT_PROBE_INTERVAL_SECONDS = 8 * 60 * 60
_HEARTBEAT_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS heartbeat_status_current (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    cycle_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    total_findings INTEGER NOT NULL DEFAULT 0
        CHECK (total_findings >= 0),
    total_actions INTEGER NOT NULL DEFAULT 0
        CHECK (total_actions >= 0),
    total_errors INTEGER NOT NULL DEFAULT 0
        CHECK (total_errors >= 0),
    status_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _resolve_repo_root_for_codebase_index() -> Path:
    """Return the Praxis workspace root for codebase indexing."""
    return Path(__file__).resolve().parents[3]


def _json_mapping(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


# ---------------------------------------------------------------------------
# Summary / persistence
# ---------------------------------------------------------------------------

def _build_summary(result: HeartbeatCycleResult) -> dict:
    """Build a minimal status summary from a cycle result."""
    duration_ms = None
    try:
        duration_ms = round((result.completed_at - result.started_at).total_seconds() * 1000, 1)
    except Exception:
        pass

    errored = [
        {"module": r.module_name, "error": r.error}
        for r in result.module_results
        if not r.ok
    ]

    summary: dict[str, object] = {
        "cycle_id": result.cycle_id,
        "started_at": result.started_at.isoformat(),
        "completed_at": result.completed_at.isoformat(),
        "module_count": len(result.module_results),
        "errors": result.errors,
    }
    if duration_ms is not None:
        summary["duration_ms"] = duration_ms
    if errored:
        summary["errored_modules"] = errored
    return summary


def summarize_cycle_result(result: HeartbeatCycleResult, **_kw) -> dict:
    """Return a minimal summary of an in-memory heartbeat cycle result."""
    return _build_summary(result)


def summarize_cycle_payload(payload: dict[str, object], **_kw) -> dict[str, object]:
    """Return a shallow copy of an already-materialized heartbeat payload."""
    return dict(payload)


@dataclass(frozen=True, slots=True)
class HeartbeatStatusSnapshot:
    """Latest heartbeat snapshot persisted in Postgres."""

    cycle_id: str
    started_at: datetime | None
    completed_at: datetime | None
    errors: int
    summary: dict[str, object]
    updated_at: datetime | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "cycle_id": self.cycle_id,
            "started_at": None if self.started_at is None else self.started_at.isoformat(),
            "completed_at": None if self.completed_at is None else self.completed_at.isoformat(),
            "errors": self.errors,
            "summary": dict(self.summary),
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }


def _row_to_heartbeat_status_snapshot(row) -> HeartbeatStatusSnapshot:
    return HeartbeatStatusSnapshot(
        cycle_id=str(row["cycle_id"]),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        errors=int(row["total_errors"] or 0),
        summary=_json_mapping(row.get("status_payload")),
        updated_at=row.get("updated_at"),
    )


def ensure_heartbeat_status_schema(conn) -> None:
    """Create the DB-backed latest-heartbeat table if it is missing."""
    if conn is None:
        return
    conn.execute_script(_HEARTBEAT_STATUS_DDL)


def record_heartbeat_status(result: HeartbeatCycleResult, *, conn) -> HeartbeatStatusSnapshot | None:
    """Persist the latest heartbeat summary in Postgres."""
    if conn is None:
        return None
    ensure_heartbeat_status_schema(conn)
    summary = _build_summary(result)
    row = conn.fetchrow(
        """
        INSERT INTO heartbeat_status_current (
            singleton, cycle_id, started_at, completed_at,
            total_findings, total_actions, total_errors, status_payload
        ) VALUES (
            TRUE, $1, $2, $3, 0, 0, $4, $5::jsonb
        )
        ON CONFLICT (singleton) DO UPDATE
        SET cycle_id = EXCLUDED.cycle_id,
            started_at = EXCLUDED.started_at,
            completed_at = EXCLUDED.completed_at,
            total_findings = 0,
            total_actions = 0,
            total_errors = EXCLUDED.total_errors,
            status_payload = EXCLUDED.status_payload,
            updated_at = now()
        RETURNING cycle_id, started_at, completed_at,
                  total_findings, total_actions, total_errors,
                  status_payload, updated_at
        """,
        result.cycle_id,
        result.started_at,
        result.completed_at,
        result.errors,
        json.dumps(summary, sort_keys=True, default=str),
    )
    return _row_to_heartbeat_status_snapshot(row)


def latest_heartbeat_status(*, conn) -> HeartbeatStatusSnapshot | None:
    """Return the latest heartbeat summary from Postgres."""
    if conn is None:
        return None
    ensure_heartbeat_status_schema(conn)
    row = conn.fetchrow(
        """
        SELECT cycle_id, started_at, completed_at,
               total_findings, total_actions, total_errors,
               status_payload, updated_at
        FROM heartbeat_status_current
        WHERE singleton = TRUE
        """
    )
    if row is None:
        return None
    return _row_to_heartbeat_status_snapshot(row)


# ---------------------------------------------------------------------------
# Wrapper modules for heartbeat_runner
# ---------------------------------------------------------------------------

class _CronHeartbeatModule(HeartbeatModule):
    """Heartbeat adapter for CronScheduler."""

    def __init__(self, conn) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "cron_scheduler"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        _CronScheduler(self._conn).tick()
        return _ok(self.name, t0)


class _TriggerEvaluatorModule(HeartbeatModule):
    """Heartbeat adapter for continuous trigger evaluation."""

    def __init__(self, conn) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "trigger_evaluator"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        from runtime.triggers import evaluate_triggers
        evaluate_triggers(self._conn)
        return _ok(self.name, t0)


class _WorkflowChainEvaluatorModule(HeartbeatModule):
    """Heartbeat adapter for durable workflow-chain advancement."""

    def __init__(self, conn) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "workflow_chain_evaluator"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        from runtime.workflow_chain import advance_workflow_chains
        advance_workflow_chains(self._conn)
        return _ok(self.name, t0)


class SystemEventsCleanupModule(HeartbeatModule):
    """Prune system_events older than 30 days."""

    def __init__(self, conn) -> None:
        self._conn = conn
        self._cleanup_function_available: bool | None = None

    @property
    def name(self) -> str:
        return "system_events_cleanup"

    def _has_cleanup_function(self) -> bool:
        if self._cleanup_function_available is not None:
            return self._cleanup_function_available
        try:
            rows = self._conn.execute(
                "SELECT to_regprocedure('cleanup_system_events(integer)') AS procedure_name"
            )
        except Exception:
            self._cleanup_function_available = True
            return True
        procedure_name = rows[0]["procedure_name"] if rows else None
        self._cleanup_function_available = procedure_name is not None
        return self._cleanup_function_available

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        if not self._has_cleanup_function():
            return _ok(self.name, t0)
        try:
            self._conn.execute("SELECT cleanup_system_events(30) as deleted")
        except Exception as e:
            if "cleanup_system_events" in str(e) and "does not exist" in str(e):
                self._cleanup_function_available = False
                return _ok(self.name, t0)
            raise
        return _ok(self.name, t0)


class _DatabaseMaintenanceModule(HeartbeatModule):
    """Heartbeat adapter for deterministic DB-backed maintenance intents."""

    def __init__(self, conn, embedder=None) -> None:
        self._conn = conn
        self._embedder = embedder

    @property
    def name(self) -> str:
        return "database_maintenance"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        from runtime.database_maintenance import DatabaseMaintenanceProcessor

        processor = DatabaseMaintenanceProcessor(self._conn, embedder=self._embedder)
        claimed_total = 0
        while claimed_total < _MAINTENANCE_MAX_CLAIMS_PER_CYCLE:
            batch_limit = min(
                _MAINTENANCE_BATCH_LIMIT,
                _MAINTENANCE_MAX_CLAIMS_PER_CYCLE - claimed_total,
            )
            result = processor.run_once(limit=batch_limit)
            claimed_total += result.claimed
            if result.claimed < batch_limit:
                break
        return _ok(self.name, t0)


class _AutoReviewFlushModule(HeartbeatModule):
    """Heartbeat adapter for time-gated review queue draining."""

    def __init__(self, conn) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "auto_review_flush"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        from runtime.auto_review import get_review_accumulator

        get_review_accumulator(self._conn).flush_due()
        return _ok(self.name, t0)


class _SemanticProjectionRefreshModule(HeartbeatModule):
    """Heartbeat adapter for cursor-driven semantic projection refresh."""

    def __init__(
        self,
        *,
        workflow_env: Mapping[str, str] | None = None,
        limit: int = 100,
    ) -> None:
        self._workflow_env = None if workflow_env is None else dict(workflow_env)
        self._limit = max(1, int(limit or 100))

    @property
    def name(self) -> str:
        return "semantic_projection_refresh"

    def _has_workflow_authority(self) -> bool:
        if self._workflow_env and str(self._workflow_env.get("WORKFLOW_DATABASE_URL") or "").strip():
            return True
        return bool(str(os.environ.get("WORKFLOW_DATABASE_URL") or "").strip())

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        if not self._has_workflow_authority():
            return _ok(self.name, t0)
        try:
            from runtime.semantic_projection_subscriber import (
                consume_semantic_projection_events,
            )

            consume_semantic_projection_events(
                limit=self._limit,
                env=self._workflow_env,
            )
        except Exception as exc:
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


class _DatasetCandidateRefreshModule(HeartbeatModule):
    """Heartbeat adapter for dataset_raw_candidates ingestion."""

    def __init__(
        self,
        *,
        workflow_env: Mapping[str, str] | None = None,
        limit: int = 100,
    ) -> None:
        self._workflow_env = None if workflow_env is None else dict(workflow_env)
        self._limit = max(1, int(limit or 100))

    @property
    def name(self) -> str:
        return "dataset_candidate_refresh"

    def _has_workflow_authority(self) -> bool:
        if self._workflow_env and str(self._workflow_env.get("WORKFLOW_DATABASE_URL") or "").strip():
            return True
        return bool(str(os.environ.get("WORKFLOW_DATABASE_URL") or "").strip())

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        if not self._has_workflow_authority():
            return _ok(self.name, t0)
        try:
            from runtime.dataset_candidate_subscriber import DatasetCandidateSubscriber

            DatasetCandidateSubscriber().consume_available(
                limit=self._limit,
                env=self._workflow_env,
            )
        except Exception as exc:
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


class _DatasetCurationRefreshModule(HeartbeatModule):
    """Heartbeat adapter for curated-dataset projection refresh."""

    def __init__(
        self,
        *,
        workflow_env: Mapping[str, str] | None = None,
        limit: int = 100,
    ) -> None:
        self._workflow_env = None if workflow_env is None else dict(workflow_env)
        self._limit = max(1, int(limit or 100))

    @property
    def name(self) -> str:
        return "dataset_curation_refresh"

    def _has_workflow_authority(self) -> bool:
        if self._workflow_env and str(self._workflow_env.get("WORKFLOW_DATABASE_URL") or "").strip():
            return True
        return bool(str(os.environ.get("WORKFLOW_DATABASE_URL") or "").strip())

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        if not self._has_workflow_authority():
            return _ok(self.name, t0)
        try:
            from runtime.dataset_curation_projection_subscriber import (
                DatasetCurationProjectionSubscriber,
            )

            DatasetCurationProjectionSubscriber().consume_available(
                limit=self._limit,
                env=self._workflow_env,
            )
        except Exception as exc:
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


class _RateLimitProbeModule(HeartbeatModule):
    """Heartbeat adapter for provider rate-limit health probes."""

    def __init__(self, *, min_interval_seconds: int = _RATE_LIMIT_PROBE_INTERVAL_SECONDS) -> None:
        self._min_interval_seconds = min_interval_seconds
        self._last_run_at: float | None = None
        self._delegate = None

    @property
    def name(self) -> str:
        return "rate_limit_prober"

    def _get_delegate(self):
        if self._delegate is None:
            from runtime.rate_limit_prober import RateLimitProbeModule

            self._delegate = RateLimitProbeModule()
        return self._delegate

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        if self._last_run_at is not None:
            elapsed = t0 - self._last_run_at
            if elapsed < self._min_interval_seconds:
                logger.debug(
                    "rate_limit_prober: skipped (last run %.0fs ago, min_interval=%ss)",
                    elapsed,
                    self._min_interval_seconds,
                )
                return _ok(self.name, t0)

        self._last_run_at = t0
        return self._get_delegate().run()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class HeartbeatRunner:
    """Builds heartbeat modules wired to a MemoryEngine and runs cycles."""

    def __init__(
        self,
        engine_db_path: str = "",
        results_dir: str = "",
        *,
        conn=None,
        embedder=None,
        include_probers: bool = True,
        workflow_env: Mapping[str, str] | None = None,
    ) -> None:
        self._engine_db_path = engine_db_path
        self._results_dir = results_dir
        self._conn = conn
        self._embedder = embedder
        self._include_probers = include_probers
        self._workflow_env = None if workflow_env is None else dict(workflow_env)
        self._engine = MemoryEngine(conn=conn, db_path=engine_db_path, embedder=embedder)
        self._rate_limit_probe_module: _RateLimitProbeModule | None = None

    def _get_rate_limit_probe_module(self) -> _RateLimitProbeModule:
        if self._rate_limit_probe_module is None:
            self._rate_limit_probe_module = _RateLimitProbeModule()
        return self._rate_limit_probe_module

    def build_modules(self) -> list[HeartbeatModule]:
        """Create all heartbeat modules wired to the memory engine."""
        from memory.sync import MemorySync
        from memory.relationship_miner import RelationshipMiner
        from memory.rollup_generator import RollupGenerator
        from memory.schema_projector import SchemaProjector

        modules: list[HeartbeatModule] = [
            StaleEntityDetector(self._engine),
            DuplicateScanner(self._engine),
            OrphanEdgeCleanup(self._engine),
            GapScanner(self._engine),
        ]

        if self._conn is not None:
            from runtime.heartbeat_scanners import (
                ContentQualityScanner,
                RelationshipIntegrityScanner,
                SchemaConsistencyScanner,
            )

            modules.extend([
                RelationshipIntegrityScanner(self._engine),
                SchemaConsistencyScanner(self._engine),
                ContentQualityScanner(self._engine),
                MemorySync(self._conn, self._engine),
                SchemaProjector(self._conn, self._engine),
                _DatabaseMaintenanceModule(self._conn, embedder=self._embedder),
                _AutoReviewFlushModule(self._conn),
                RelationshipMiner(self._conn, self._engine),
                RollupGenerator(self._conn, self._engine),
                SystemEventsCleanupModule(self._conn),
                _SemanticProjectionRefreshModule(workflow_env=self._workflow_env),
                _DatasetCandidateRefreshModule(workflow_env=self._workflow_env),
                _DatasetCurationRefreshModule(workflow_env=self._workflow_env),
            ])
            if self._embedder is not None:
                from runtime.codebase_index_module import CodebaseIndexModule
                from memory.knowledge_graph import KnowledgeGraph

                repo_root = str(_resolve_repo_root_for_codebase_index())
                kg = KnowledgeGraph(conn=self._conn, embedder=self._embedder)
                modules.append(CodebaseIndexModule(
                    self._conn, repo_root, knowledge_graph=kg,
                ))
            modules.append(_CronHeartbeatModule(self._conn))
            modules.append(_TriggerEvaluatorModule(self._conn))
            modules.append(_WorkflowChainEvaluatorModule(self._conn))
            modules.append(self._get_rate_limit_probe_module())

        return modules

    def run_once(self) -> HeartbeatCycleResult:
        """Run one heartbeat cycle and persist the latest status."""
        modules = self.build_modules()
        orchestrator = HeartbeatOrchestrator(modules)
        result = orchestrator.run_cycle()
        snapshot = record_heartbeat_status(result, conn=self._conn)

        logger.info(
            "heartbeat cycle %s: modules=%d errors=%d%s",
            result.cycle_id,
            len(result.module_results),
            result.errors,
            "" if snapshot is None else " [persisted]",
        )
        return result

    def run_loop(
        self,
        interval_seconds: int = 300,
        max_cycles: Optional[int] = None,
    ) -> None:
        """Run heartbeat on an interval."""
        cycles_run = 0
        while True:
            self.run_once()
            cycles_run += 1
            if max_cycles is not None and cycles_run >= max_cycles:
                break
            time.sleep(interval_seconds)
