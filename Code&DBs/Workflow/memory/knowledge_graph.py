"""Unified facade composing all memory modules into a single KnowledgeGraph API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from memory.engine import MemoryEngine
from memory.entity_resolver import EntityResolver, MatchResult
from memory.graph import BlastRadius, BlastResult, RandomWalkWithRestart
from memory.ingest import IngestKind, IngestPayload, IngestResult, IngestRouter
from memory.packer import ContextPacker, ContextSection, PackedContext, estimate_tokens
from memory.retrieval import HybridRetriever, RetrievalResult
from memory.types import Edge, EdgeAuthorityClass, EntityType

from datetime import datetime, timezone

if TYPE_CHECKING:
    from runtime.embedding_service import EmbeddingService


class KnowledgeGraph:
    """Unified facade that composes MemoryEngine, HybridRetriever,
    EntityResolver, IngestRouter, ContextPacker, and graph algorithms."""

    def __init__(
        self,
        db_path: str = "knowledge.db",
        conn=None,
        embedder: Optional["EmbeddingService"] = None,
    ) -> None:
        self._engine = (
            MemoryEngine(conn=conn, embedder=embedder)
            if conn is not None
            else MemoryEngine(db_path=db_path, embedder=embedder)
        )
        self._retriever = HybridRetriever(
            self._engine,
            embedder=embedder,
            conn=conn,
        )
        self._resolver = EntityResolver()
        self._ingest_router = IngestRouter(self._engine)
        self._packer = ContextPacker()

    # -- public API --------------------------------------------------------

    def ingest(
        self,
        kind: str,
        content: str,
        source: str,
        metadata: dict | None = None,
    ) -> IngestResult:
        """Route content through IngestRouter into the memory engine."""
        payload = IngestPayload(
            kind=IngestKind(kind),
            content=content,
            source=source,
            metadata=metadata or {},
            timestamp=datetime.now(timezone.utc),
        )
        return self._ingest_router.ingest(payload)

    def search(
        self,
        query: str,
        entity_type: str | None = None,
        limit: int = 20,
    ) -> list[RetrievalResult]:
        """Hybrid text + graph search via HybridRetriever."""
        et = EntityType(entity_type) if entity_type else None
        return self._retriever.search(query, entity_type=et, limit=limit)

    def resolve(
        self,
        name: str,
        entity_type: str | None = None,
    ) -> MatchResult | None:
        """Resolve a name against stored entities using EntityResolver."""
        et_filter = EntityType(entity_type) if entity_type else None
        types = [et_filter] if et_filter else list(EntityType)

        candidates: list[tuple[str, str]] = []
        for et in types:
            for ent in self._engine.list(et, limit=500):
                candidates.append((ent.id, ent.name))

        if not candidates:
            return None
        return self._resolver.resolve_best(name, candidates)

    def pack_context(
        self,
        query: str,
        token_budget: int = 8000,
    ) -> PackedContext:
        """Search for relevant entities and pack into context sections."""
        results = self.search(query, limit=30)
        sections: list[ContextSection] = []
        for r in results:
            tok = estimate_tokens(r.entity.content)
            sections.append(
                ContextSection(
                    name=r.entity.name,
                    content=r.entity.content,
                    priority=r.score,
                    token_estimate=tok,
                    source=r.entity.source,
                )
            )
        packer = ContextPacker(token_budget=token_budget)
        return packer.pack(sections)

    def graph_rank(self, seed_ids: list[str]) -> dict[str, float]:
        """Random-walk-with-restart from seed_ids over stored edges."""
        edges = self._all_edges()
        rwr = RandomWalkWithRestart()
        return rwr.compute(seed_ids, edges)

    def blast_radius(
        self,
        entity_id: str,
        *,
        include_enrichment: bool = False,
    ) -> BlastResult:
        """Compute blast radius from an entity via stored edges."""
        authority_classes = (
            (EdgeAuthorityClass.canonical, EdgeAuthorityClass.enrichment)
            if include_enrichment
            else (EdgeAuthorityClass.canonical,)
        )
        edges = self._all_edges(authority_classes=authority_classes)
        br = BlastRadius()
        return br.compute(entity_id, edges)

    def stats(self) -> dict:
        """Entity counts by type, edge count, total entities."""
        counts: dict[str, int] = {}
        total = 0
        for et in EntityType:
            n = len(self._engine.list(et, limit=10000))
            counts[et.value] = n
            total += n

        edge_count = self._count_edges()
        return {
            "entity_counts": counts,
            "total_entities": total,
            "edge_count": edge_count,
        }

    # -- internal helpers --------------------------------------------------

    def _all_edges(
        self,
        *,
        authority_classes: tuple[EdgeAuthorityClass, ...] = (EdgeAuthorityClass.canonical,),
    ) -> list[Edge]:
        """Retrieve all edges from the database."""
        conn = self._engine._connect()
        from memory.crud import _row_to_edge
        rows = conn.execute(
            """
            SELECT *
            FROM memory_edges
            WHERE active = true
              AND authority_class = ANY($1::text[])
            """,
            [edge_class.value for edge_class in authority_classes],
        )
        return [_row_to_edge(r) for r in rows]

    def _count_edges(self) -> int:
        conn = self._engine._connect()
        row = conn.fetchval(
            """
            SELECT COUNT(*)
            FROM memory_edges
            WHERE active = true
              AND authority_class = 'canonical'
            """
        )
        return row or 0
