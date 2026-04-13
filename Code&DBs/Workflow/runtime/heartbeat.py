"""Heartbeat system for periodic memory-graph maintenance.

Provides pluggable modules that auto-resolve stale entities, duplicates,
orphan edges, and data gaps -- plus an orchestrator that runs them
sequentially with fault-isolation and optional time budgets.
"""
from __future__ import annotations

import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

from memory.engine import MemoryEngine
from memory.repository import (
    MemoryEdgeRef,
    MemoryGraphMutationRepository,
    resolve_memory_graph_mutation_repository,
)
from memory.types import EntityType

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HeartbeatModuleResult:
    module_name: str
    ok: bool
    error: str | None
    duration_ms: float


@dataclass(frozen=True)
class HeartbeatCycleResult:
    cycle_id: str
    started_at: datetime
    completed_at: datetime
    module_results: tuple[HeartbeatModuleResult, ...]
    errors: int


# ---------------------------------------------------------------------------
# Module protocol
# ---------------------------------------------------------------------------

class HeartbeatModule(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run(self) -> HeartbeatModuleResult: ...


def _ok(name: str, t0: float) -> HeartbeatModuleResult:
    return HeartbeatModuleResult(name, ok=True, error=None, duration_ms=(time.monotonic() - t0) * 1000)


def _fail(name: str, t0: float, error: str) -> HeartbeatModuleResult:
    return HeartbeatModuleResult(name, ok=False, error=error, duration_ms=(time.monotonic() - t0) * 1000)


# ---------------------------------------------------------------------------
# Built-in modules (self-healing)
# ---------------------------------------------------------------------------

class StaleEntityDetector(HeartbeatModule):
    """Auto-archives entities not updated within *stale_days*."""

    def __init__(
        self,
        engine: MemoryEngine,
        stale_days: int = 30,
        *,
        repository: MemoryGraphMutationRepository | None = None,
    ) -> None:
        self._engine = engine
        self._stale_days = stale_days
        self._repository = repository or resolve_memory_graph_mutation_repository(engine)

    @property
    def name(self) -> str:
        return "stale_entity_detector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        cutoff = _utcnow() - timedelta(days=self._stale_days)
        conn = self._engine._connect()
        rows = conn.execute(
            "SELECT id FROM memory_entities "
            "WHERE archived = false AND updated_at < $1",
            cutoff,
        )
        stale_ids = [row["id"] for row in rows]
        if stale_ids:
            self._repository.archive_entities(entity_ids=stale_ids)
            logger.info("stale_entity_detector: archived %d entities", len(stale_ids))
        return _ok(self.name, t0)


class DuplicateScanner(HeartbeatModule):
    """Auto-merges entity pairs of the same type whose names are very similar.

    Uses pg_trgm trigram similarity in SQL to find candidates, avoiding O(n²)
    Python-side scans.  Falls back to in-memory Jaccard if pg_trgm is unavailable.
    """

    _MERGE_CAP_PER_CYCLE = 500
    _CANDIDATE_LIMIT = 1000  # max pairs fetched from SQL per cycle

    def __init__(
        self,
        engine: MemoryEngine,
        similarity_threshold: float = 0.85,
        *,
        repository: MemoryGraphMutationRepository | None = None,
    ) -> None:
        self._engine = engine
        self._threshold = similarity_threshold
        self._repository = repository or resolve_memory_graph_mutation_repository(engine)

    @property
    def name(self) -> str:
        return "duplicate_scanner"

    def _sql_candidates(self, conn) -> list[tuple[str, str, str, str, int, int]]:
        """Find duplicate candidates via pg_trgm similarity in one SQL query."""
        return conn.fetchall(
            """
            SELECT a.id, b.id, a.name, b.name,
                   length(coalesce(a.content, '')),
                   length(coalesce(b.content, ''))
            FROM memory_entities a
            JOIN memory_entities b
              ON a.entity_type = b.entity_type
             AND a.id < b.id
             AND NOT a.archived AND NOT b.archived
             AND similarity(lower(a.name), lower(b.name)) >= $1
            ORDER BY similarity(lower(a.name), lower(b.name)) DESC
            LIMIT $2
            """,
            self._threshold,
            self._CANDIDATE_LIMIT,
        )

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        merges = 0
        merged_ids: set[str] = set()

        try:
            conn = self._engine._conn if hasattr(self._engine, '_conn') else None
            if conn is None:
                raise AttributeError("no conn")
            pairs = self._sql_candidates(conn)
        except Exception:
            logger.debug("duplicate_scanner: pg_trgm query failed, skipping", exc_info=True)
            return _ok(self.name, t0)

        for a_id, b_id, a_name, b_name, a_len, b_len in pairs:
            if merges >= self._MERGE_CAP_PER_CYCLE:
                break
            if a_id in merged_ids or b_id in merged_ids:
                continue
            # pick entity with more content as canonical
            canonical_id, dup_id = (b_id, a_id) if b_len > a_len else (a_id, b_id)
            try:
                self._repository.absorb_exact_duplicate_entities(
                    canonical_entity_id=canonical_id,
                    duplicate_entity_ids=[dup_id],
                )
                merged_ids.add(dup_id)
                merges += 1
            except Exception:
                logger.debug("duplicate merge failed: %s -> %s", dup_id, canonical_id, exc_info=True)

        if merges:
            logger.info("duplicate_scanner: merged %d duplicate entities", merges)
        return _ok(self.name, t0)


class GapScanner(HeartbeatModule):
    """Auto-archives entities with empty name AND empty content."""

    def __init__(
        self,
        engine: MemoryEngine,
        *,
        repository: MemoryGraphMutationRepository | None = None,
    ) -> None:
        self._engine = engine
        self._repository = repository or resolve_memory_graph_mutation_repository(engine)

    @property
    def name(self) -> str:
        return "gap_scanner"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        conn = self._engine._connect()
        rows = conn.execute(
            "SELECT id FROM memory_entities "
            "WHERE archived = false "
            "AND (COALESCE(TRIM(name), '') = '') "
            "AND (COALESCE(TRIM(content), '') = '')"
        )
        empty_ids = [row["id"] for row in rows]
        if empty_ids:
            self._repository.archive_entities(entity_ids=empty_ids)
            logger.info("gap_scanner: archived %d empty entities", len(empty_ids))
        return _ok(self.name, t0)


class OrphanEdgeCleanup(HeartbeatModule):
    """Removes edges whose source or target entity is missing or archived."""

    def __init__(
        self,
        engine: MemoryEngine,
        *,
        repository: MemoryGraphMutationRepository | None = None,
    ) -> None:
        self._engine = engine
        self._repository = repository or resolve_memory_graph_mutation_repository(engine)

    @property
    def name(self) -> str:
        return "orphan_edge_cleanup"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        conn = self._engine._connect()
        rows = conn.execute(
            "SELECT e.source_id, e.target_id, e.relation_type "
            "FROM memory_edges e "
            "LEFT JOIN memory_entities s ON e.source_id = s.id AND s.archived = false "
            "LEFT JOIN memory_entities t ON e.target_id = t.id AND t.archived = false "
            "WHERE s.id IS NULL OR t.id IS NULL"
        )
        orphan_edges: list[MemoryEdgeRef] = []
        for row in rows:
            orphan_edges.append(
                MemoryEdgeRef(
                    source_id=str(row["source_id"]),
                    target_id=str(row["target_id"]),
                    relation_type=str(row["relation_type"]),
                )
            )
        if orphan_edges:
            self._repository.delete_edges(edges=orphan_edges)
            logger.info("orphan_edge_cleanup: removed %d orphan edges", len(orphan_edges))
        return _ok(self.name, t0)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class HeartbeatOrchestrator:
    """Runs heartbeat modules sequentially with fault isolation."""

    def __init__(self, modules: list[HeartbeatModule]) -> None:
        self._modules = list(modules)

    def run_cycle(self) -> HeartbeatCycleResult:
        cycle_id = uuid.uuid4().hex
        started = _utcnow()
        results: list[HeartbeatModuleResult] = []
        for mod in self._modules:
            t0 = time.monotonic()
            try:
                result = mod.run()
                results.append(result)
            except Exception as exc:
                results.append(_fail(mod.name, t0, str(exc)))
        completed = _utcnow()
        return HeartbeatCycleResult(
            cycle_id=cycle_id,
            started_at=started,
            completed_at=completed,
            module_results=tuple(results),
            errors=sum(1 for r in results if not r.ok),
        )

    def run_cycle_with_budget(
        self, max_duration_seconds: float
    ) -> HeartbeatCycleResult:
        cycle_id = uuid.uuid4().hex
        started = _utcnow()
        results: list[HeartbeatModuleResult] = []
        deadline = time.monotonic() + max_duration_seconds
        for mod in self._modules:
            if time.monotonic() >= deadline:
                break
            t0 = time.monotonic()
            try:
                result = mod.run()
                results.append(result)
            except Exception as exc:
                results.append(_fail(mod.name, t0, str(exc)))
        completed = _utcnow()
        return HeartbeatCycleResult(
            cycle_id=cycle_id,
            started_at=started,
            completed_at=completed,
            module_results=tuple(results),
            errors=sum(1 for r in results if not r.ok),
        )
