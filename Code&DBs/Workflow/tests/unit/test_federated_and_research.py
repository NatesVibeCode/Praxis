"""Tests for federated_retrieval and research_runtime modules."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

import memory.federated_retrieval as federated_retrieval_module
import memory.research_runtime as research_runtime_module
from memory.types import Entity, EntityType

from memory.federated_retrieval import (
    DomainNode,
    FederatedRetriever,
    PathPredictor,
    QueryIntent,
    RetrievalDomain,
)
from memory.research_runtime import (
    CitationHelper,
    ResearchExecutor,
    ResearchSession,
    SearchHit,
    SearchResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_engine() -> MemoryEngine:
    return _InMemoryEngine()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _seed_entity(engine: MemoryEngine, etype: EntityType, name: str, content: str) -> Entity:
    e = Entity(
        id=f"test-{name.replace(' ', '-').lower()}",
        entity_type=etype,
        name=name,
        content=content,
        metadata={},
        created_at=_now(),
        updated_at=_now(),
        source="test",
        confidence=0.9,
    )
    engine.insert(e)
    return e


def _fake_entity(eid: str, etype: EntityType, name: str, content: str) -> Entity:
    ts = _now()
    return Entity(
        id=eid,
        entity_type=etype,
        name=name,
        content=content,
        metadata={},
        created_at=ts,
        updated_at=ts,
        source="test",
        confidence=0.9,
    )


class _FakeSearchEngine:
    def __init__(self, entities: list[Entity]) -> None:
        self._entities = list(entities)
        self._conn = object()

    def _connect(self):
        return self._conn

    def search(self, query: str, entity_type: EntityType | None = None, limit: int = 20):
        tokens = [token for token in query.lower().split() if token]
        results: list[Entity] = []
        for entity in self._entities:
            if entity_type is not None and entity.entity_type != entity_type:
                continue
            haystack = f"{entity.name} {entity.content}".lower()
            if all(token in haystack for token in tokens):
                results.append(entity)
        return results[:limit]


class _InMemoryEngine:
    def __init__(self) -> None:
        self._entities: dict[str, Entity] = {}
        self._conn = object()

    def _connect(self):
        return self._conn

    def insert(self, entity: Entity) -> str:
        self._entities[entity.id] = entity
        return entity.id

    def search(self, query: str, entity_type: EntityType | None = None, limit: int = 20):
        tokens = [token for token in query.lower().split() if token]
        results: list[Entity] = []
        for entity in self._entities.values():
            if entity_type is not None and entity.entity_type != entity_type:
                continue
            haystack = f"{entity.name} {entity.content}".lower()
            if all(token in haystack for token in tokens):
                results.append(entity)
        return results[:limit]


# ===========================================================================
# Federated Retrieval — RetrievalDomain
# ===========================================================================

class TestRetrievalDomain:
    def test_enum_values(self):
        assert RetrievalDomain.PLANNING.value == "planning"
        assert RetrievalDomain.OPS.value == "ops"
        assert RetrievalDomain.RESEARCH.value == "research"
        assert RetrievalDomain.GENERAL.value == "general"

    def test_enum_members_count(self):
        assert len(RetrievalDomain) == 4


# ===========================================================================
# DomainNode
# ===========================================================================

class TestDomainNode:
    def test_frozen(self):
        node = DomainNode(
            domain=RetrievalDomain.OPS,
            entity_types=("module",),
            keywords=("deploy",),
            description="ops",
        )
        with pytest.raises(AttributeError):
            node.domain = RetrievalDomain.PLANNING  # type: ignore[misc]

    def test_fields(self):
        node = DomainNode(
            domain=RetrievalDomain.RESEARCH,
            entity_types=("fact", "lesson"),
            keywords=("research",),
            description="desc",
        )
        assert node.entity_types == ("fact", "lesson")
        assert node.keywords == ("research",)


# ===========================================================================
# QueryIntent
# ===========================================================================

class TestQueryIntent:
    def test_frozen(self):
        qi = QueryIntent(
            query="plan the roadmap",
            matched_domain=RetrievalDomain.PLANNING,
            confidence=0.4,
            matched_keywords=("plan", "roadmap"),
        )
        with pytest.raises(AttributeError):
            qi.confidence = 1.0  # type: ignore[misc]


# ===========================================================================
# FederatedRetriever.classify
# ===========================================================================

class TestClassify:
    def test_planning_keywords(self):
        engine = _make_engine()
        fr = FederatedRetriever(engine)
        intent = fr.classify("plan the roadmap for phase 2")
        assert intent.matched_domain == RetrievalDomain.PLANNING
        assert "plan" in intent.matched_keywords
        assert "roadmap" in intent.matched_keywords
        assert intent.confidence > 0

    def test_ops_keywords(self):
        engine = _make_engine()
        fr = FederatedRetriever(engine)
        intent = fr.classify("deploy and test the build")
        assert intent.matched_domain == RetrievalDomain.OPS

    def test_research_keywords(self):
        engine = _make_engine()
        fr = FederatedRetriever(engine)
        intent = fr.classify("research and analyze the study results")
        assert intent.matched_domain == RetrievalDomain.RESEARCH

    def test_general_fallback(self):
        engine = _make_engine()
        fr = FederatedRetriever(engine)
        intent = fr.classify("hello world nonsense")
        assert intent.matched_domain == RetrievalDomain.GENERAL
        assert intent.confidence == 0.0
        assert intent.matched_keywords == ()

    def test_custom_domains(self):
        engine = _make_engine()
        custom = [
            DomainNode(
                domain=RetrievalDomain.GENERAL,
                entity_types=("fact",),
                keywords=("custom", "special"),
                description="custom domain",
            )
        ]
        fr = FederatedRetriever(engine, domains=custom)
        intent = fr.classify("this is a custom query")
        assert intent.matched_domain == RetrievalDomain.GENERAL
        assert "custom" in intent.matched_keywords


# ===========================================================================
# FederatedRetriever.search / search_all_domains
# ===========================================================================

class TestFederatedSearch:
    def test_search_routes_to_domain(self):
        engine = _make_engine()
        _seed_entity(engine, EntityType.task, "Plan sprint", "plan next sprint milestone")
        _seed_entity(engine, EntityType.module, "Deploy module", "deploy ops module")
        fr = FederatedRetriever(engine)
        # "plan" + "milestone" route to PLANNING which searches task type
        results = fr.search("plan milestone", limit=10)
        # Should route to PLANNING, which includes task
        names = [e.name for e in results]
        assert "Plan sprint" in names

    def test_search_general_fallback(self):
        engine = _make_engine()
        _seed_entity(engine, EntityType.fact, "Random fact", "something random")
        fr = FederatedRetriever(engine)
        results = fr.search("random", limit=10)
        # General fallback — unfiltered search
        assert isinstance(results, list)

    def test_search_all_domains(self):
        engine = _make_engine()
        _seed_entity(engine, EntityType.task, "Sprint task", "sprint planning task")
        _seed_entity(engine, EntityType.module, "Build mod", "build module")
        _seed_entity(engine, EntityType.fact, "Research fact", "research finding")
        fr = FederatedRetriever(engine)
        all_results = fr.search_all_domains("sprint build research", limit_per_domain=5)
        assert "planning" in all_results
        assert "ops" in all_results
        assert "research" in all_results


class TestFederatedSearchTelemetry:
    def test_search_records_retrieval_telemetry(self, monkeypatch):
        recorded = []

        class RecordingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                recorded.append(metric)

        monkeypatch.setattr(federated_retrieval_module, "TelemetryStore", RecordingTelemetryStore)

        engine = _FakeSearchEngine([
            _fake_entity("task-1", EntityType.task, "Plan sprint", "plan next sprint milestone"),
            _fake_entity("mod-1", EntityType.module, "Deploy module", "deploy ops module"),
        ])
        fr = FederatedRetriever(engine)
        results = fr.search("plan milestone", limit=10)

        assert len(recorded) == 1
        metric = recorded[0]
        assert metric.pattern_name == "federated.search"
        assert metric.result_count == len(results)
        assert metric.latency_ms >= 0.0

    def test_search_telemetry_failure_does_not_change_results(self, monkeypatch):
        engine = _FakeSearchEngine([
            _fake_entity("task-1", EntityType.task, "Plan sprint", "plan next sprint milestone"),
        ])

        baseline = FederatedRetriever(engine).search("plan milestone", limit=10)

        class FailingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                raise RuntimeError("telemetry is down")

        monkeypatch.setattr(federated_retrieval_module, "TelemetryStore", FailingTelemetryStore)

        results = FederatedRetriever(engine).search("plan milestone", limit=10)

        assert [r.id for r in results] == [r.id for r in baseline]
        assert [r.name for r in results] == [r.name for r in baseline]

    def test_search_all_domains_records_retrieval_telemetry(self, monkeypatch):
        recorded = []

        class RecordingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                recorded.append(metric)

        monkeypatch.setattr(federated_retrieval_module, "TelemetryStore", RecordingTelemetryStore)

        engine = _FakeSearchEngine([
            _fake_entity("task-1", EntityType.task, "Sprint task", "sprint planning task"),
            _fake_entity("mod-1", EntityType.module, "Build mod", "build module"),
            _fake_entity("fact-1", EntityType.fact, "Research fact", "research finding"),
        ])
        fr = FederatedRetriever(engine)
        all_results = fr.search_all_domains("sprint build research", limit_per_domain=5)

        assert len(recorded) == 1
        metric = recorded[0]
        assert metric.pattern_name == "federated.search_all_domains"
        assert metric.result_count == sum(len(results) for results in all_results.values())
        assert metric.latency_ms >= 0.0


# ===========================================================================
# PathPredictor
# ===========================================================================

class TestPathPredictor:
    def test_predict_planning(self):
        pp = PathPredictor()
        types = pp.predict("plan the roadmap")
        assert "decision" in types or "task" in types

    def test_predict_with_recent(self):
        pp = PathPredictor()
        types = pp.predict("what next", recent_queries=["deploy the build"])
        # Recent context should pull in OPS types
        assert any(t in types for t in ("module", "tool", "pattern"))

    def test_predict_fallback(self):
        pp = PathPredictor()
        types = pp.predict("xyzzy gibberish")
        assert types == ["fact", "task", "document"]


# ===========================================================================
# SearchHit / SearchResult
# ===========================================================================

class TestSearchDataclasses:
    def test_search_hit_frozen(self):
        hit = SearchHit(title="T", url="http://x", snippet="s", source="src")
        with pytest.raises(AttributeError):
            hit.title = "new"  # type: ignore[misc]

    def test_search_result(self):
        hit = SearchHit(title="T", url="http://x", snippet="s", source="src")
        sr = SearchResult(query="q", hits=(hit,), total_results=1)
        assert sr.total_results == 1
        assert len(sr.hits) == 1


# ===========================================================================
# CitationHelper
# ===========================================================================

class TestCitationHelper:
    def test_format_citation(self):
        ch = CitationHelper()
        hit = SearchHit(title="My Article", url="http://example.com", snippet="", source="web")
        assert ch.format_citation(hit, 1) == "[1] My Article — http://example.com"

    def test_format_bibliography(self):
        ch = CitationHelper()
        hits = [
            SearchHit(title="A", url="http://a.com", snippet="", source="web"),
            SearchHit(title="B", url="http://b.com", snippet="", source="web"),
        ]
        bib = ch.format_bibliography(hits)
        assert "[1] A" in bib
        assert "[2] B" in bib
        assert bib.count("\n") == 1  # two lines, one newline


# ===========================================================================
# ResearchExecutor
# ===========================================================================

class TestResearchExecutor:
    def test_search_local_no_engine(self):
        ex = ResearchExecutor(engine=None)
        result = ex.search_local("anything")
        assert result.total_results == 0
        assert result.hits == ()

    def test_search_local_with_engine(self):
        engine = _make_engine()
        _seed_entity(engine, EntityType.fact, "Climate data", "temperature rising")
        ex = ResearchExecutor(engine=engine)
        result = ex.search_local("temperature")
        assert result.total_results >= 1
        assert result.hits[0].title == "Climate data"

    def test_search_local_records_retrieval_telemetry(self, monkeypatch):
        recorded = []

        class RecordingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                recorded.append(metric)

        monkeypatch.setattr(research_runtime_module, "TelemetryStore", RecordingTelemetryStore)

        engine = _FakeSearchEngine([
            _fake_entity("fact-1", EntityType.fact, "Climate data", "temperature rising"),
        ])
        ex = ResearchExecutor(engine=engine)
        result = ex.search_local("temperature")

        assert len(recorded) == 1
        metric = recorded[0]
        assert metric.pattern_name == "research.search_local"
        assert metric.result_count == result.total_results
        assert metric.latency_ms >= 0.0

    def test_search_local_telemetry_failure_does_not_change_results(self, monkeypatch):
        engine = _FakeSearchEngine([
            _fake_entity("fact-1", EntityType.fact, "Climate data", "temperature rising"),
        ])

        baseline = ResearchExecutor(engine=engine).search_local("temperature")

        class FailingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                raise RuntimeError("telemetry is down")

        monkeypatch.setattr(research_runtime_module, "TelemetryStore", FailingTelemetryStore)

        result = ResearchExecutor(engine=engine).search_local("temperature")

        assert result.total_results == baseline.total_results
        assert [hit.title for hit in result.hits] == [hit.title for hit in baseline.hits]

    def test_record_finding(self):
        engine = _make_engine()
        ex = ResearchExecutor(engine=engine)
        ex.record_finding("q1", "the sky is blue", "observation")
        hits = engine.search("sky blue", entity_type=EntityType.fact)
        assert len(hits) >= 1

    def test_compile_brief(self):
        ex = ResearchExecutor()
        brief = ex.compile_brief("climate", ["It is warming", "Ice caps melting"])
        assert "# Research Brief: climate" in brief
        assert "Finding 1" in brief
        assert "Finding 2" in brief


# ===========================================================================
# ResearchSession
# ===========================================================================

class TestResearchSession:
    def test_add_and_findings(self):
        ex = ResearchExecutor()
        session = ResearchSession(ex, "test topic")
        session.add_finding("finding A", "source A")
        session.add_finding("finding B", "source B")
        assert len(session.findings) == 2
        assert session.findings[0]["finding"] == "finding A"

    def test_compile(self):
        ex = ResearchExecutor()
        session = ResearchSession(ex, "test topic")
        session.add_finding("important result", "lab")
        brief = session.compile()
        assert "Research Brief" in brief
        assert "important result" in brief

    def test_save(self):
        ex = ResearchExecutor()
        session = ResearchSession(ex, "save topic")
        session.add_finding("data point", "experiment")
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            session.save(path)
            with open(path) as fh:
                data = json.load(fh)
            assert data["topic"] == "save topic"
            assert len(data["findings"]) == 1
            assert "compiled" in data
        finally:
            os.unlink(path)
