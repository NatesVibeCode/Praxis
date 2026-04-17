from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta

import pytest

import memory.retrieval as retrieval_module
from memory.engine import MemoryEngine
from memory.ports.vector import VectorFilter
from memory.retrieval import (
    BM25Scorer,
    HybridRetriever,
    NoisyOrFusion,
    RecencyDecay,
    RetrievalResult,
)
from memory.types import Edge, Entity, EntityType, RelationType
from memory.types import EdgeAuthorityClass, EdgeProvenanceKind


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entity(
    id: str,
    name: str,
    content: str,
    entity_type: EntityType = EntityType.fact,
    **kwargs,
) -> Entity:
    now = datetime.utcnow()
    return Entity(
        id=id,
        entity_type=entity_type,
        name=name,
        content=content,
        metadata=kwargs.get("metadata", {}),
        created_at=kwargs.get("created_at", now),
        updated_at=kwargs.get("updated_at", now),
        source=kwargs.get("source", "test"),
        confidence=kwargs.get("confidence", 1.0),
    )


def _make_edge(src: str, tgt: str, weight: float = 0.8) -> Edge:
    return Edge(
        source_id=src,
        target_id=tgt,
        relation_type=RelationType.related_to,
        weight=weight,
        metadata={},
        created_at=datetime.utcnow(),
        authority_class=EdgeAuthorityClass.canonical,
        provenance_kind=EdgeProvenanceKind.legacy_unspecified,
    )


def test_memory_vector_filter_normalizes_not_equal_operator() -> None:
    assert VectorFilter("entity_type", "decision", operator="!=").normalized_operator() == "<>"


# ---------------------------------------------------------------------------
# BM25Scorer
# ---------------------------------------------------------------------------


class TestBM25Scorer:
    def test_known_inputs(self):
        scorer = BM25Scorer()
        query = ["python", "routing"]
        doc = ["python", "routing", "is", "great", "python"]
        avg_dl = 5.0
        score = scorer.score(query, doc, avg_dl)
        assert score > 0

    def test_no_overlap_yields_zero(self):
        scorer = BM25Scorer()
        assert scorer.score(["alpha"], ["beta", "gamma"], 3.0) == 0.0

    def test_empty_query_yields_zero(self):
        scorer = BM25Scorer()
        assert scorer.score([], ["a", "b"], 2.0) == 0.0

    def test_empty_doc_yields_zero(self):
        scorer = BM25Scorer()
        assert scorer.score(["a"], [], 2.0) == 0.0

    def test_higher_tf_higher_score(self):
        scorer = BM25Scorer()
        low = scorer.score(["x"], ["x", "y", "z"], 3.0)
        high = scorer.score(["x"], ["x", "x", "z"], 3.0)
        assert high > low

    def test_zero_avg_doc_length(self):
        scorer = BM25Scorer()
        assert scorer.score(["a"], ["a"], 0.0) == 0.0


# ---------------------------------------------------------------------------
# NoisyOrFusion
# ---------------------------------------------------------------------------


class TestNoisyOrFusion:
    def test_single_score_passthrough(self):
        fuser = NoisyOrFusion()
        assert fuser.fuse([0.5]) == pytest.approx(0.5)

    def test_multiple_scores(self):
        fuser = NoisyOrFusion()
        result = fuser.fuse([0.3, 0.4])
        expected = 1.0 - (0.7 * 0.6)
        assert result == pytest.approx(expected)

    def test_all_zeros(self):
        fuser = NoisyOrFusion()
        assert fuser.fuse([0.0, 0.0, 0.0]) == pytest.approx(0.0)

    def test_all_ones(self):
        fuser = NoisyOrFusion()
        assert fuser.fuse([1.0, 1.0]) == pytest.approx(1.0)

    def test_empty_list(self):
        fuser = NoisyOrFusion()
        assert fuser.fuse([]) == 0.0


# ---------------------------------------------------------------------------
# RecencyDecay
# ---------------------------------------------------------------------------


class TestRecencyDecay:
    def test_recent_entity_near_full_score(self):
        decay = RecencyDecay()
        now = datetime.utcnow()
        result = decay.decay(1.0, now, half_life_days=30.0)
        assert result == pytest.approx(1.0, abs=0.01)

    def test_old_entity_reduced_score(self):
        decay = RecencyDecay()
        old = datetime.utcnow() - timedelta(days=60)
        result = decay.decay(1.0, old, half_life_days=30.0)
        # After 2 half-lives, should be ~0.25
        assert result == pytest.approx(0.25, abs=0.05)

    def test_half_life_gives_half_score(self):
        decay = RecencyDecay()
        half_ago = datetime.utcnow() - timedelta(days=30)
        result = decay.decay(1.0, half_ago, half_life_days=30.0)
        assert result == pytest.approx(0.5, abs=0.05)

    def test_score_scales_linearly(self):
        decay = RecencyDecay()
        old = datetime.utcnow() - timedelta(days=30)
        r1 = decay.decay(1.0, old, half_life_days=30.0)
        r2 = decay.decay(2.0, old, half_life_days=30.0)
        assert r2 == pytest.approx(2.0 * r1, abs=0.01)


# ---------------------------------------------------------------------------
# HybridRetriever (integration with in-memory engine)
# ---------------------------------------------------------------------------


class InMemoryEngine:
    """Lightweight in-memory stub that mimics MemoryEngine for unit tests."""

    def __init__(self):
        self._entities: dict[str, Entity] = {}
        self._edges: list[Edge] = []

    def insert(self, entity: Entity) -> str:
        self._entities[entity.id] = entity
        return entity.id

    def get(self, entity_id: str, entity_type: EntityType) -> Entity | None:
        ent = self._entities.get(entity_id)
        if ent and ent.entity_type == entity_type:
            return ent
        return None

    def search(
        self, query: str, entity_type: EntityType | None = None, limit: int = 20
    ) -> list[Entity]:
        terms = query.lower().split()
        results = []
        for ent in self._entities.values():
            if entity_type and ent.entity_type != entity_type:
                continue
            text = f"{ent.name} {ent.content}".lower()
            if any(t in text for t in terms):
                results.append(ent)
        return results[:limit]

    def add_edge(self, edge: Edge) -> bool:
        self._edges.append(edge)
        return True

    def get_edges(
        self,
        entity_id: str,
        direction: str = "outgoing",
        authority_classes=None,
    ) -> list[Edge]:
        normalized_authority_classes = authority_classes or (EdgeAuthorityClass.canonical,)
        result = []
        for e in self._edges:
            if e.authority_class not in normalized_authority_classes:
                continue
            if direction == "outgoing" and e.source_id == entity_id:
                result.append(e)
            elif direction == "incoming" and e.target_id == entity_id:
                result.append(e)
            elif direction == "both" and (
                e.source_id == entity_id or e.target_id == entity_id
            ):
                result.append(e)
        return result


@pytest.fixture
def populated_engine():
    engine = InMemoryEngine()

    e1 = _make_entity("e1", "python routing", "python deterministic routing logic")
    e2 = _make_entity("e2", "graph database", "graph traversal and edges")
    e3 = _make_entity("e3", "python testing", "unit tests for python modules")
    # e4 is only reachable via graph, content unrelated to "python routing"
    e4 = _make_entity("e4", "deployment pipeline", "CI CD deployment pipeline steps")

    for e in [e1, e2, e3, e4]:
        engine.insert(e)

    # Edges: e1 -> e3 (strong), e1 -> e4 (medium), e2 -> e4 (weak)
    engine.add_edge(_make_edge("e1", "e3", weight=0.9))
    engine.add_edge(_make_edge("e1", "e4", weight=0.6))
    engine.add_edge(_make_edge("e2", "e4", weight=0.3))

    return engine


class TestHybridRetriever:
    def test_search_returns_results(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python")
        assert len(results) > 0
        assert all(isinstance(r, RetrievalResult) for r in results)

    def test_empty_search_returns_empty(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("   ")
        assert results == []

    def test_found_via_text(self, populated_engine):
        # alpha=1.0 means text only; graph neighbors get zero blended score
        retriever = HybridRetriever(populated_engine, alpha=1.0)
        results = retriever.search("python")
        text_results = [r for r in results if "text" in r.found_via]
        assert len(text_results) > 0

    def test_found_via_graph(self, populated_engine):
        # alpha=0.0 means graph only; text hits get zero from text component
        retriever = HybridRetriever(populated_engine, alpha=0.0)
        results = retriever.search("python")
        graph_results = [r for r in results if "graph" in r.found_via]
        assert len(graph_results) > 0

    def test_enrichment_edges_do_not_expand_graph_by_default(self, populated_engine):
        populated_engine.insert(
            _make_entity("e5", "hidden heuristic", "only reachable through enrichment")
        )
        populated_engine.add_edge(
            Edge(
                source_id="e1",
                target_id="e5",
                relation_type=RelationType.related_to,
                weight=0.95,
                metadata={"kind": "heuristic"},
                created_at=datetime.utcnow(),
                authority_class=EdgeAuthorityClass.enrichment,
                provenance_kind=EdgeProvenanceKind.heuristic_extraction,
            )
        )

        retriever = HybridRetriever(populated_engine, alpha=0.0)
        results = retriever.search("python")
        ids = {result.entity.id for result in results}
        assert "e5" not in ids

    def test_alpha_one_text_only(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=1.0)
        results = retriever.search("python")
        # Graph-only entities should have score 0 and be at the bottom
        for r in results:
            if r.found_via == "graph":
                assert r.score == pytest.approx(0.0)

    def test_alpha_zero_graph_only(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.0)
        results = retriever.search("python")
        # Text-only entities should have score 0
        for r in results:
            if r.found_via == "text":
                assert r.score == pytest.approx(0.0)

    def test_deduplication(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python")
        ids = [r.entity.id for r in results]
        assert len(ids) == len(set(ids))

    def test_results_sorted_descending(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python")
        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_limit_respected(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python", limit=1)
        assert len(results) <= 1

    def test_entity_type_filter(self, populated_engine):
        # Insert a person entity
        person = _make_entity(
            "p1", "python dev", "python developer", entity_type=EntityType.person
        )
        populated_engine.insert(person)

        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python", entity_type=EntityType.person)
        for r in results:
            assert r.entity.entity_type == EntityType.person

    def test_no_match_returns_empty(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("xyznonexistent12345")
        assert results == []

    def test_search_with_seeds(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.5)
        # Seed from e1 -- should find e3 and e4 via graph
        results = retriever.search_with_seeds("python", seed_ids=["e1"])
        assert len(results) > 0

    def test_provenance_populated(self, populated_engine):
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python")
        for r in results:
            assert "text_score" in r.provenance
            assert "graph_score" in r.provenance
            # Legacy mode (no embedder): alpha blending
            assert "alpha" in r.provenance
            assert "vector_score" not in r.provenance

    def test_graph_expansion_finds_neighbors(self, populated_engine):
        # e4 has content about "deployment", not "python"
        # But e4 is linked to e1 which matches "python"
        retriever = HybridRetriever(populated_engine, alpha=0.3)
        results = retriever.search("python")
        found_ids = {r.entity.id for r in results}
        # e4 should be reachable via graph from e1
        assert "e4" in found_ids


# ---------------------------------------------------------------------------
# HybridRetriever — vector + NoisyOrFusion path
# ---------------------------------------------------------------------------


class FakeEmbedder:
    """Stub embedder for unit tests."""

    def embed_one(self, text: str) -> list[float]:
        return [0.1] * 384

    @staticmethod
    def vector_literal(vec: list[float]) -> str:
        return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


class FakeConn:
    """Stub connection returning configurable vector search rows."""

    def __init__(self, rows: list[dict] | None = None):
        self._rows = rows or []

    def execute(self, query: str, *args) -> list:
        return self._rows


class TestHybridRetrieverVector:
    def test_vector_scores_included(self, populated_engine):
        rows = [{"id": "e1", "cosine_sim": 0.85}, {"id": "e2", "cosine_sim": 0.55}]
        retriever = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn(rows),
        )
        results = retriever.search("python")
        assert len(results) > 0
        # e1 should include vector signal (found_via may be 'all' if text+graph+vector)
        e1_result = next(r for r in results if r.entity.id == "e1")
        assert e1_result.found_via in ("vector", "text+vector", "graph+vector", "all")
        assert e1_result.provenance["vector_score"] == pytest.approx(0.85)
        assert e1_result.provenance["fusion"] == "noisy_or"

    def test_fusion_scores_higher_with_vector(self, populated_engine):
        """An entity found by text+graph+vector should score higher than text+graph alone."""
        rows = [{"id": "e1", "cosine_sim": 0.7}]
        retriever_with_vec = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn(rows),
        )
        retriever_no_vec = HybridRetriever(populated_engine, alpha=0.6)

        results_vec = retriever_with_vec.search("python")
        results_no_vec = retriever_no_vec.search("python")

        e1_vec = next(r for r in results_vec if r.entity.id == "e1")
        e1_no_vec = next(r for r in results_no_vec if r.entity.id == "e1")
        # NoisyOrFusion of 3 signals should be >= alpha blend of 2
        assert e1_vec.score >= e1_no_vec.score

    def test_vector_only_entity_discovered(self, populated_engine):
        """An entity found only by vector (not text or graph) should appear in results."""
        # e4 content is about deployment, not python — text search won't find it
        # Give it a high vector score
        rows = [{"id": "e4", "cosine_sim": 0.9}]
        retriever = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn(rows),
        )
        results = retriever.search("python")
        found_ids = {r.entity.id for r in results}
        assert "e4" in found_ids

    def test_found_via_all(self, populated_engine):
        """Entity found by all three signals should have found_via='all'."""
        # e1 matches "python" text, has graph edges (back-propagation from e3), and vector
        rows = [{"id": "e1", "cosine_sim": 0.6}]
        retriever = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn(rows),
        )
        results = retriever.search("python")
        e1_result = next(r for r in results if r.entity.id == "e1")
        # e1 is found by text, graph (e3 has a back-edge to e1), and vector
        assert e1_result.found_via == "all"

    def test_provenance_has_vector_and_fusion(self, populated_engine):
        rows = [{"id": "e2", "cosine_sim": 0.4}]
        retriever = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn(rows),
        )
        results = retriever.search("python")
        for r in results:
            assert "vector_score" in r.provenance
            assert "fusion" in r.provenance
            assert r.provenance["fusion"] == "noisy_or"
            # Legacy alpha should NOT be present
            assert "alpha" not in r.provenance

    def test_no_embedder_gives_legacy_behavior(self, populated_engine):
        """Without embedder, behavior is identical to original alpha blending."""
        retriever = HybridRetriever(populated_engine, alpha=0.6)
        results = retriever.search("python")
        for r in results:
            assert "alpha" in r.provenance
            assert "vector_score" not in r.provenance

    def test_search_with_seeds_vector(self, populated_engine):
        rows = [{"id": "e1", "cosine_sim": 0.75}]
        retriever = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn(rows),
        )
        results = retriever.search_with_seeds("python", seed_ids=["e1"])
        assert len(results) > 0
        e1_result = next((r for r in results if r.entity.id == "e1"), None)
        if e1_result:
            assert e1_result.provenance["vector_score"] == pytest.approx(0.75)

    def test_empty_vector_results_still_works(self, populated_engine):
        """Embedder present but returns no vector matches — should still work."""
        retriever = HybridRetriever(
            populated_engine,
            embedder=FakeEmbedder(),
            conn=FakeConn([]),
        )
        results = retriever.search("python")
        assert len(results) > 0


class TestHybridRetrieverTelemetry:
    def test_search_records_retrieval_telemetry(self, populated_engine, monkeypatch):
        recorded = []

        class RecordingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                recorded.append(metric)

        monkeypatch.setattr(retrieval_module, "TelemetryStore", RecordingTelemetryStore)

        retriever = HybridRetriever(
            populated_engine,
            alpha=0.6,
            conn=FakeConn([]),
        )
        results = retriever.search("python")

        assert len(recorded) == 1
        metric = recorded[0]
        assert metric.pattern_name == "search"
        assert metric.result_count == len(results)
        assert metric.query_fingerprint == hashlib.sha256("python".encode()).hexdigest()[
            :8
        ]
        assert metric.latency_ms >= 0.0

    def test_search_with_seeds_records_retrieval_telemetry(self, populated_engine, monkeypatch):
        recorded = []

        class RecordingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                recorded.append(metric)

        monkeypatch.setattr(retrieval_module, "TelemetryStore", RecordingTelemetryStore)

        retriever = HybridRetriever(
            populated_engine,
            alpha=0.5,
            conn=FakeConn([]),
        )
        results = retriever.search_with_seeds("python", seed_ids=["e1"])

        assert len(recorded) == 1
        metric = recorded[0]
        assert metric.pattern_name == "search_with_seeds"
        assert metric.result_count == len(results)
        assert metric.latency_ms >= 0.0

    def test_telemetry_failures_do_not_change_results(self, populated_engine, monkeypatch):
        baseline = HybridRetriever(
            populated_engine,
            alpha=0.6,
            conn=FakeConn([]),
        ).search("python")

        class FailingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                raise RuntimeError("telemetry is down")

        monkeypatch.setattr(retrieval_module, "TelemetryStore", FailingTelemetryStore)

        retriever = HybridRetriever(
            populated_engine,
            alpha=0.6,
            conn=FakeConn([]),
        )
        results = retriever.search("python")

        assert [r.entity.id for r in results] == [r.entity.id for r in baseline]
        assert [r.score for r in results] == pytest.approx([r.score for r in baseline])
