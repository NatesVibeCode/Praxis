"""Graph hygiene: stale-node archival, rank recomputation, and verification."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from memory.engine import MemoryEngine
from memory.graph import PageRank
from memory.repository import (
    MemoryGraphMutationRepository,
    resolve_memory_graph_mutation_repository,
)
from memory.types import Edge, EntityType


class HygieneAction(enum.Enum):
    ARCHIVE = "archive"
    RECOMPUTE_RANK = "recompute_rank"
    VERIFY = "verify"
    SKIP = "skip"


@dataclass(frozen=True)
class HygieneReport:
    stale_archived: int
    ranks_recomputed: int
    verified: int
    skipped: int
    errors: tuple[str, ...]


class GraphHygienist:
    """Maintain graph health via archival, rank recomputation, and verification."""

    def __init__(
        self,
        engine: MemoryEngine,
        max_age_days: int = 90,
        *,
        repository: MemoryGraphMutationRepository | None = None,
    ) -> None:
        self._engine = engine
        self._max_age_days = max_age_days
        self._repository = repository or resolve_memory_graph_mutation_repository(engine)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cutoff_iso(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=self._max_age_days)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_stale_nodes(self, entity_type: str | None = None) -> list[str]:
        """Find entities not updated within *max_age_days* and not archived."""
        cutoff = self._cutoff_iso()
        conn = self._engine._connect()
        if entity_type is not None:
            rows = conn.execute(
                "SELECT id FROM memory_entities "
                "WHERE archived = false AND updated_at < $1 AND entity_type = $2",
                cutoff, entity_type,
            )
        else:
            rows = conn.execute(
                "SELECT id FROM memory_entities WHERE archived = false AND updated_at < $1",
                cutoff,
            )
        return [row["id"] for row in rows]

    def archive_stale(self, entity_type: str | None = None) -> int:
        """Soft-delete stale entities, return count archived."""
        stale_ids = self.find_stale_nodes(entity_type=entity_type)
        return self._archive_entities(stale_ids)

    def verify_active(self, entity_ids: list[str]) -> list[tuple[str, bool]]:
        """Check each entity exists and is not archived."""
        conn = self._engine._connect()
        results: list[tuple[str, bool]] = []
        for eid in entity_ids:
            row = conn.fetchrow(
                "SELECT archived FROM memory_entities WHERE id = $1", eid,
            )
            if row is not None:
                results.append((eid, not row["archived"]))
            else:
                results.append((eid, False))
        return results

    def recompute_ranks(self, entity_type: str | None = None) -> dict[str, float]:
        """Run PageRank on current (non-archived) edges, return entity_id -> score."""
        conn = self._engine._connect()
        # Gather all active entity IDs
        if entity_type is not None:
            rows = conn.execute(
                "SELECT id FROM memory_entities WHERE archived = false AND entity_type = $1",
                entity_type,
            )
        else:
            rows = conn.execute(
                "SELECT id FROM memory_entities WHERE archived = false"
            )
        active_ids: set[str] = {row["id"] for row in rows}

        # Load edges that connect active nodes
        edge_rows = conn.execute(
            "SELECT source_id, target_id, relation_type, weight, metadata, created_at "
            "FROM memory_edges "
            "WHERE active = true AND authority_class = 'canonical'"
        )

        edges: list[Edge] = []
        for er in edge_rows:
            src = er["source_id"]
            tgt = er["target_id"]
            if src in active_ids and tgt in active_ids:
                edges.append(
                    Edge(
                        source_id=src,
                        target_id=tgt,
                        relation_type=er["relation_type"],
                        weight=er["weight"],
                        metadata={},
                        created_at=datetime.now(timezone.utc),
                    )
                )

        pr = PageRank()
        return pr.compute(edges)

    def run_hygiene_cycle(self) -> HygieneReport:
        """Full cycle: find stale -> archive -> recompute ranks -> verify sample."""
        errors: list[str] = []

        # 1. Find stale
        try:
            stale_ids = self.find_stale_nodes()
        except Exception as exc:
            errors.append(f"find_stale: {exc}")
            stale_ids = []

        # 2. Archive
        try:
            archived_count = self._archive_entities(stale_ids)
        except Exception as exc:
            errors.append(f"archive: {exc}")
            archived_count = 0

        # 3. Recompute ranks
        try:
            ranks = self.recompute_ranks()
            ranks_count = len(ranks)
        except Exception as exc:
            errors.append(f"recompute_ranks: {exc}")
            ranks_count = 0

        # 4. Verify a sample of active nodes
        try:
            conn = self._engine._connect()
            rows = conn.execute(
                "SELECT id FROM memory_entities WHERE archived = false LIMIT 20"
            )
            sample_ids = [row["id"] for row in rows]
            verification = self.verify_active(sample_ids[:20])
            verified_count = sum(1 for _, ok in verification if ok)
            skipped_count = sum(1 for _, ok in verification if not ok)
        except Exception as exc:
            errors.append(f"verify: {exc}")
            verified_count = 0
            skipped_count = 0

        return HygieneReport(
            stale_archived=archived_count,
            ranks_recomputed=ranks_count,
            verified=verified_count,
            skipped=skipped_count,
            errors=tuple(errors),
        )

    def quarantine_check(self, entity_id: str) -> bool:
        """Return True if entity has been archived (quarantined)."""
        conn = self._engine._connect()
        row = conn.fetchrow(
            "SELECT archived FROM memory_entities WHERE id = $1", entity_id,
        )
        if row is not None:
            return bool(row["archived"])
        return False

    def _archive_entities(self, entity_ids: list[str]) -> int:
        archived_ids = self._repository.archive_entities(entity_ids=entity_ids)
        return len(archived_ids)
