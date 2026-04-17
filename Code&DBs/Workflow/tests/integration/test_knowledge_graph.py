"""Integration tests for KnowledgeGraph facade and populate_from_codebase."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import memory.knowledge_graph as knowledge_graph_mod
from memory.knowledge_graph import KnowledgeGraph
from memory.populate import populate_from_codebase
from memory.types import EdgeAuthorityClass, Entity, EntityType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _InMemoryKnowledgeGraphEngine:
    """Small in-memory engine for facade-style KnowledgeGraph tests."""

    def __init__(self, conn=None, *, db_path: str | None = None, embedder=None) -> None:
        del conn, db_path, embedder
        self._entities: dict[str, Entity] = {}
        self._edges = []

    def insert(self, entity: Entity) -> str:
        self._entities[entity.id] = entity
        return entity.id

    def get(self, entity_id: str, entity_type: EntityType) -> Entity | None:
        entity = self._entities.get(entity_id)
        if entity is None or entity.entity_type is not entity_type:
            return None
        return entity

    def list(
        self,
        entity_type: EntityType,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Entity]:
        entities = [
            entity
            for entity in self._entities.values()
            if entity.entity_type is entity_type
        ]
        return entities[offset:offset + limit]

    def search(
        self,
        query: str,
        entity_type: EntityType | None = None,
        limit: int = 20,
    ) -> list[Entity]:
        terms = [term for term in query.lower().split() if term]
        results: list[Entity] = []
        for entity in self._entities.values():
            if entity_type is not None and entity.entity_type is not entity_type:
                continue
            haystack = f"{entity.name} {entity.content}".lower()
            if not terms or any(term in haystack for term in terms):
                results.append(entity)
        return results[:limit]

    def add_edge(self, edge) -> bool:
        self._edges.append(edge)
        return True

    def get_edges(
        self,
        entity_id: str,
        direction: str = "outgoing",
        authority_classes=None,
    ) -> list:
        allowed = tuple(
            authority_classes or (EdgeAuthorityClass.canonical,)
        )
        edges = [
            edge
            for edge in self._edges
            if edge.authority_class in allowed
        ]
        if direction == "outgoing":
            return [edge for edge in edges if edge.source_id == entity_id]
        if direction == "incoming":
            return [edge for edge in edges if edge.target_id == entity_id]
        return [
            edge
            for edge in edges
            if edge.source_id == entity_id or edge.target_id == entity_id
        ]


@pytest.fixture
def kg(monkeypatch: pytest.MonkeyPatch):
    """Unit-style KnowledgeGraph facade backed by an in-memory engine stub."""

    monkeypatch.setattr(
        knowledge_graph_mod,
        "MemoryEngine",
        _InMemoryKnowledgeGraphEngine,
    )
    monkeypatch.setattr(
        knowledge_graph_mod.KnowledgeGraph,
        "_all_edges",
        lambda self, authority_classes=(EdgeAuthorityClass.canonical,): [
            edge
            for edge in self._engine._edges
            if edge.authority_class in authority_classes
        ],
    )
    monkeypatch.setattr(
        knowledge_graph_mod.KnowledgeGraph,
        "_count_edges",
        lambda self: len(
            [
                edge
                for edge in self._engine._edges
                if edge.authority_class is EdgeAuthorityClass.canonical
            ]
        ),
    )
    return KnowledgeGraph(db_path=":memory:")


@pytest.fixture
def kg_with_data(kg):
    """KnowledgeGraph pre-loaded with a few entities and edges."""
    # Ingest two module entities and an edge between them
    kg.ingest(
        kind="extraction",
        content=json.dumps({
            "entities": [
                {
                    "id": "mod:engine",
                    "entity_type": "module",
                    "name": "engine",
                    "content": "Thread-safe memory graph backed by SQLite.",
                    "confidence": 0.9,
                },
                {
                    "id": "mod:retrieval",
                    "entity_type": "module",
                    "name": "retrieval",
                    "content": "Hybrid text and graph retrieval over the memory engine.",
                    "confidence": 0.9,
                },
            ],
            "edges": [
                {
                    "source_id": "mod:retrieval",
                    "target_id": "mod:engine",
                    "relation_type": "depends_on",
                    "weight": 0.8,
                    "authority_class": "canonical",
                },
            ],
        }),
        source="test",
    )

    # Ingest a document
    kg.ingest(
        kind="document",
        content="Build plan for the memory subsystem with graph algorithms.",
        source="test",
        metadata={"title": "memory_build_plan"},
    )
    return kg


# ---------------------------------------------------------------------------
# Test: ingest + search round-trip
# ---------------------------------------------------------------------------

class TestIngestAndSearch:
    def test_ingest_creates_entities(self, kg):
        result = kg.ingest(
            kind="document",
            content="Knowledge graph integration test content.",
            source="test_ingest",
            metadata={"title": "test_doc"},
        )
        assert result.accepted is True
        assert result.entities_created >= 1

    def test_search_finds_ingested(self, kg_with_data):
        results = kg_with_data.search("memory engine SQLite")
        assert len(results) > 0
        names = [r.entity.name for r in results]
        assert "engine" in names

    def test_search_with_entity_type_filter(self, kg_with_data):
        results = kg_with_data.search("memory", entity_type="module")
        for r in results:
            assert r.entity.entity_type.value == "module"


# ---------------------------------------------------------------------------
# Test: resolve
# ---------------------------------------------------------------------------

class TestResolve:
    def test_resolve_finds_entity(self, kg_with_data):
        match = kg_with_data.resolve("engine")
        assert match is not None
        assert match.entity_name == "engine"
        assert match.score > 0.5

    def test_resolve_fuzzy(self, kg_with_data):
        match = kg_with_data.resolve("engin")
        assert match is not None
        assert match.entity_name == "engine"

    def test_resolve_returns_none_for_garbage(self, kg):
        match = kg.resolve("zzzzxxxxyyyy")
        assert match is None


# ---------------------------------------------------------------------------
# Test: pack_context
# ---------------------------------------------------------------------------

class TestPackContext:
    def test_pack_returns_sections(self, kg_with_data):
        packed = kg_with_data.pack_context("memory engine")
        assert len(packed.sections) > 0
        assert packed.total_tokens > 0

    def test_pack_respects_budget(self, kg_with_data):
        packed = kg_with_data.pack_context("memory", token_budget=50)
        assert packed.total_tokens <= 50 or len(packed.sections) <= 1

    def test_pack_empty_query(self, kg):
        packed = kg.pack_context("")
        assert len(packed.sections) == 0


# ---------------------------------------------------------------------------
# Test: graph_rank
# ---------------------------------------------------------------------------

class TestGraphRank:
    def test_graph_rank_returns_scores(self, kg_with_data):
        scores = kg_with_data.graph_rank(["mod:engine"])
        assert len(scores) > 0
        assert "mod:engine" in scores
        assert scores["mod:engine"] > 0

    def test_graph_rank_connected(self, kg_with_data):
        scores = kg_with_data.graph_rank(["mod:retrieval"])
        # engine should appear since retrieval depends_on engine
        assert "mod:engine" in scores

    def test_graph_rank_empty_seeds(self, kg):
        scores = kg.graph_rank([])
        assert scores == {}


# ---------------------------------------------------------------------------
# Test: blast_radius
# ---------------------------------------------------------------------------

class TestBlastRadius:
    def test_blast_direct(self, kg_with_data):
        result = kg_with_data.blast_radius("mod:retrieval")
        # retrieval -> engine is a direct edge
        assert "mod:engine" in result.direct
        assert result.total_affected >= 1

    def test_blast_empty_graph(self, kg):
        result = kg.blast_radius("nonexistent")
        assert result.total_affected == 0

    def test_blast_indirect(self, kg):
        """Three nodes chained: a -> b -> c. Blast from a should find c as indirect."""
        kg.ingest(
            kind="extraction",
            content=json.dumps({
                "entities": [
                    {"id": "a", "entity_type": "module", "name": "a", "content": "node a"},
                    {"id": "b", "entity_type": "module", "name": "b", "content": "node b"},
                    {"id": "c", "entity_type": "module", "name": "c", "content": "node c"},
                ],
                "edges": [
                    {
                        "source_id": "a",
                        "target_id": "b",
                        "relation_type": "depends_on",
                        "weight": 1.0,
                        "authority_class": "canonical",
                    },
                    {
                        "source_id": "b",
                        "target_id": "c",
                        "relation_type": "depends_on",
                        "weight": 1.0,
                        "authority_class": "canonical",
                    },
                ],
            }),
            source="test",
        )
        result = kg.blast_radius("a")
        assert "b" in result.direct
        assert "c" in result.indirect
        assert result.total_affected == 2


# ---------------------------------------------------------------------------
# Test: stats
# ---------------------------------------------------------------------------

class TestStats:
    def test_stats_reflect_data(self, kg_with_data):
        s = kg_with_data.stats()
        assert s["total_entities"] >= 2
        assert s["edge_count"] >= 1
        assert "module" in s["entity_counts"]
        assert s["entity_counts"]["module"] >= 2

    def test_stats_empty(self, kg):
        s = kg.stats()
        assert s["total_entities"] == 0
        assert s["edge_count"] == 0


# ---------------------------------------------------------------------------
# Test: populate_from_codebase (real files)
# ---------------------------------------------------------------------------

class TestPopulateFromCodebase:
    def test_populate_creates_entities(self, kg):
        repo_root = str(Path(__file__).resolve().parents[4])
        result = populate_from_codebase(kg, repo_root)

        assert result["entities_created"] > 0, "Should create entities from real files"
        assert result["edges_created"] > 0, "Should create import edges"

        stats = kg.stats()
        assert stats["total_entities"] > 0
        assert stats["entity_counts"]["module"] > 0

        # Verify a known module was ingested
        search_results = kg.search("engine", entity_type="module")
        assert len(search_results) > 0

        # Verify documents from Build Plan
        assert stats["entity_counts"]["document"] > 0
