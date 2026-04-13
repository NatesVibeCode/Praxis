from __future__ import annotations

import math
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from memory.engine import MemoryEngine
from memory.retrieval_telemetry import RetrievalInstrumenter, TelemetryStore
from memory.types import Entity, EntityType
from memory.ports.vector import VectorFilter

if TYPE_CHECKING:
    from memory.ports.embedding import EmbeddingPort
    from storage.postgres.connection import SyncPostgresConnection


@dataclass(frozen=True)
class RetrievalResult:
    entity: Entity
    score: float
    found_via: str  # 'text' | 'graph' | 'vector' | 'text+graph' | 'text+vector' | 'graph+vector' | 'all'
    provenance: dict


class BM25Scorer:
    """Standard BM25 scoring."""

    def score(
        self,
        query_terms: list[str],
        document_terms: list[str],
        avg_doc_length: float,
        k1: float = 1.5,
        b: float = 0.75,
    ) -> float:
        if not query_terms or not document_terms or avg_doc_length <= 0:
            return 0.0

        doc_len = len(document_terms)
        doc_freq = Counter(document_terms)
        total = 0.0

        for term in query_terms:
            tf = doc_freq.get(term, 0)
            if tf == 0:
                continue
            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * (doc_len / avg_doc_length))
            total += numerator / denominator

        return total


class NoisyOrFusion:
    """Noisy-OR probabilistic fusion."""

    def fuse(self, scores: list[float]) -> float:
        if not scores:
            return 0.0
        product = 1.0
        for p in scores:
            product *= (1.0 - p)
        return 1.0 - product


class HybridRetriever:
    """Hybrid text + graph + vector retrieval over the memory engine."""

    def __init__(
        self,
        engine: MemoryEngine,
        alpha: float = 0.6,
        embedder: EmbeddingPort | None = None,
        conn: SyncPostgresConnection | None = None,
        vector_store: Any | None = None,
    ) -> None:
        self._engine = engine
        self._alpha = alpha
        self._embedder = embedder
        self._conn = conn
        if vector_store is not None:
            self._vector_store = vector_store
        elif embedder is not None and conn is not None:
            from storage.postgres.vector_store import PostgresVectorStore
            self._vector_store = PostgresVectorStore(conn, embedder)
        else:
            self._vector_store = None
        self._telemetry_store: TelemetryStore | None = None
        self._fuser = NoisyOrFusion()

    def _vector_search(
        self,
        query: str,
        entity_type: EntityType | None = None,
        limit: int = 20,
    ) -> dict[str, float]:
        """Run cosine similarity search against memory_entities embeddings."""
        if self._vector_store is None:
            return {}

        vector_query = self._vector_store.prepare(query)
        filters = (
            [VectorFilter("entity_type", entity_type.value)]
            if entity_type is not None
            else None
        )
        rows = vector_query.search(
            "memory_entities",
            select_columns=("id",),
            filters=filters,
            limit=limit * 2,
            score_alias="cosine_sim",
        )

        return {row["id"]: float(row["cosine_sim"]) for row in rows}

    @staticmethod
    def _found_via_label(in_text: bool, in_graph: bool, in_vector: bool) -> str:
        sources = []
        if in_text:
            sources.append("text")
        if in_graph:
            sources.append("graph")
        if in_vector:
            sources.append("vector")
        if len(sources) == 3:
            return "all"
        return "+".join(sources)

    def search(
        self,
        query: str,
        entity_type: EntityType | None = None,
        limit: int = 20,
    ) -> list[RetrievalResult]:
        if not query.strip():
            return []

        started_at = time.monotonic()

        # 1. Text search via engine FTS
        text_hits = self._engine.search(query, entity_type=entity_type, limit=limit)
        if not text_hits and self._embedder is None:
            self._record_telemetry(
                query=query,
                pattern_name="search",
                results=[],
                started_at=started_at,
            )
            return []

        # Normalize text scores: rank by position (first = best)
        text_scores: dict[str, float] = {}
        for i, ent in enumerate(text_hits):
            text_scores[ent.id] = 1.0 - (i / max(len(text_hits), 1))

        # Entity lookup by id
        entity_map: dict[str, Entity] = {e.id: e for e in text_hits}

        # 1.5. Vector search (only when embedder is available)
        vector_scores = self._vector_search(query, entity_type, limit)

        # Resolve vector-only entities
        for vid in vector_scores:
            if vid not in entity_map:
                resolved = self._resolve_entity(vid, entity_type)
                if resolved is not None:
                    entity_map[vid] = resolved

        # 2. Graph expansion: for each text hit, get neighbors and score by edge weight
        graph_scores: dict[str, float] = {}
        for ent in text_hits:
            edges = self._engine.get_edges(ent.id, direction="both")
            for edge in edges:
                neighbor_id = (
                    edge.target_id if edge.source_id == ent.id else edge.source_id
                )
                propagated = text_scores.get(ent.id, 0.0) * edge.weight
                if neighbor_id in graph_scores:
                    graph_scores[neighbor_id] = max(
                        graph_scores[neighbor_id], propagated
                    )
                else:
                    graph_scores[neighbor_id] = propagated

        # Resolve graph-only neighbor entities
        for nid in graph_scores:
            if nid not in entity_map:
                resolved = self._resolve_entity(nid, entity_type)
                if resolved is not None:
                    entity_map[nid] = resolved

        # 3. Merge scores
        all_ids = set(text_scores.keys()) | set(graph_scores.keys()) | set(vector_scores.keys())
        merged: list[RetrievalResult] = []

        use_fusion = self._vector_store is not None

        for eid in all_ids:
            if eid not in entity_map:
                continue

            t_score = text_scores.get(eid, 0.0)
            g_score = graph_scores.get(eid, 0.0)
            v_score = vector_scores.get(eid, 0.0)

            if use_fusion:
                # NoisyOrFusion over all available signals
                signals = []
                if eid in text_scores:
                    signals.append(t_score)
                if eid in graph_scores:
                    signals.append(g_score)
                if eid in vector_scores:
                    signals.append(v_score)
                blended = self._fuser.fuse(signals)
            else:
                # Legacy alpha blending (text + graph only)
                blended = self._alpha * t_score + (1.0 - self._alpha) * g_score

            in_text = eid in text_scores
            in_graph = eid in graph_scores
            in_vector = eid in vector_scores
            found_via = self._found_via_label(in_text, in_graph, in_vector)

            provenance = {
                "text_score": t_score,
                "graph_score": g_score,
            }
            if use_fusion:
                provenance["vector_score"] = v_score
                provenance["fusion"] = "noisy_or"
            else:
                provenance["alpha"] = self._alpha

            merged.append(
                RetrievalResult(
                    entity=entity_map[eid],
                    score=blended,
                    found_via=found_via,
                    provenance=provenance,
                )
            )

        # 4. Sort descending by score, apply limit
        merged.sort(key=lambda r: r.score, reverse=True)
        results = merged[:limit]
        self._record_telemetry(
            query=query,
            pattern_name="search",
            results=results,
            started_at=started_at,
        )
        return results

    def search_with_seeds(
        self,
        query: str,
        seed_ids: list[str],
        entity_type: EntityType | None = None,
        limit: int = 20,
    ) -> list[RetrievalResult]:
        if not query.strip():
            return []

        started_at = time.monotonic()

        # 1. Text search
        text_hits = self._engine.search(query, entity_type=entity_type, limit=limit)
        text_scores: dict[str, float] = {}
        entity_map: dict[str, Entity] = {}

        for i, ent in enumerate(text_hits):
            text_scores[ent.id] = 1.0 - (i / max(len(text_hits), 1))
            entity_map[ent.id] = ent

        # 1.5. Vector search (only when embedder is available)
        vector_scores = self._vector_search(query, entity_type, limit)

        for vid in vector_scores:
            if vid not in entity_map:
                resolved = self._resolve_entity(vid, entity_type)
                if resolved is not None:
                    entity_map[vid] = resolved

        # 2. Graph expansion from seeds instead of text hits
        graph_scores: dict[str, float] = {}
        for sid in seed_ids:
            edges = self._engine.get_edges(sid, direction="both")
            for edge in edges:
                neighbor_id = (
                    edge.target_id if edge.source_id == sid else edge.source_id
                )
                propagated = edge.weight
                if neighbor_id in graph_scores:
                    graph_scores[neighbor_id] = max(
                        graph_scores[neighbor_id], propagated
                    )
                else:
                    graph_scores[neighbor_id] = propagated

        # Resolve entities we haven't seen yet
        for nid in graph_scores:
            if nid not in entity_map:
                resolved = self._resolve_entity(nid, entity_type)
                if resolved is not None:
                    entity_map[nid] = resolved

        # 3. Merge
        all_ids = set(text_scores.keys()) | set(graph_scores.keys()) | set(vector_scores.keys())
        merged: list[RetrievalResult] = []

        use_fusion = self._vector_store is not None

        for eid in all_ids:
            if eid not in entity_map:
                continue

            t_score = text_scores.get(eid, 0.0)
            g_score = graph_scores.get(eid, 0.0)
            v_score = vector_scores.get(eid, 0.0)

            if use_fusion:
                signals = []
                if eid in text_scores:
                    signals.append(t_score)
                if eid in graph_scores:
                    signals.append(g_score)
                if eid in vector_scores:
                    signals.append(v_score)
                blended = self._fuser.fuse(signals)
            else:
                blended = self._alpha * t_score + (1.0 - self._alpha) * g_score

            in_text = eid in text_scores
            in_graph = eid in graph_scores
            in_vector = eid in vector_scores
            found_via = self._found_via_label(in_text, in_graph, in_vector)

            provenance = {
                "text_score": t_score,
                "graph_score": g_score,
            }
            if use_fusion:
                provenance["vector_score"] = v_score
                provenance["fusion"] = "noisy_or"
            else:
                provenance["alpha"] = self._alpha

            merged.append(
                RetrievalResult(
                    entity=entity_map[eid],
                    score=blended,
                    found_via=found_via,
                    provenance=provenance,
                )
            )

        merged.sort(key=lambda r: r.score, reverse=True)
        results = merged[:limit]
        self._record_telemetry(
            query=query,
            pattern_name="search_with_seeds",
            results=results,
            started_at=started_at,
        )
        return results

    def _record_telemetry(
        self,
        query: str,
        pattern_name: str,
        results: list[RetrievalResult],
        started_at: float,
    ) -> None:
        if self._conn is None:
            return

        try:
            store = self._telemetry_store
            if store is None:
                store = TelemetryStore(self._conn)
                self._telemetry_store = store

            RetrievalInstrumenter(store).instrument(
                query=query,
                pattern_name=pattern_name,
                results=results,
                latency_ms=(time.monotonic() - started_at) * 1000.0,
            )
        except Exception:
            pass

    def _resolve_entity(
        self, entity_id: str, filter_type: EntityType | None
    ) -> Entity | None:
        types = [filter_type] if filter_type else list(EntityType)
        for et in types:
            ent = self._engine.get(entity_id, et)
            if ent is not None:
                return ent
        return None


class RecencyDecay:
    """Exponential decay based on entity age."""

    def decay(
        self,
        score: float,
        entity_updated_at: datetime,
        half_life_days: float = 30.0,
    ) -> float:
        updated_at = (
            entity_updated_at
            if entity_updated_at.tzinfo is not None
            else entity_updated_at.replace(tzinfo=timezone.utc)
        )
        now = datetime.now(updated_at.tzinfo)
        age_days = (now - updated_at).total_seconds() / 86400.0
        if age_days <= 0:
            return score
        decay_factor = math.exp(-math.log(2) * age_days / half_life_days)
        return score * decay_factor
