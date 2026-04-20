from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from memory.types import Entity, EntityType
from surfaces import _recall


class _FakeFederatedRetriever:
    calls: list[tuple[str, int]] = []

    def __init__(self, engine):
        self._engine = engine

    def search(self, query: str, limit: int = 20, *, record_telemetry: bool = True):
        del record_telemetry
        self.calls.append((query, limit))
        now = datetime.now(timezone.utc)
        entity = Entity(
            id="memory-hit-1",
            entity_type=EntityType.fact,
            name="Federated memory hit",
            content="Federated recall should surface this result.",
            metadata={},
            created_at=now,
            updated_at=now,
            source="memory",
            confidence=0.91,
        )
        return [
            SimpleNamespace(
                entity=entity,
                score=0.91,
                found_via="text",
                provenance={"fusion": "noisy_or"},
            )
        ]


class _FakeSubsystems:
    def __init__(self) -> None:
        self._memory_engine = object()
        self._knowledge_graph = SimpleNamespace(
            search=lambda query, entity_type=None, limit=20: []
        )

    def get_memory_engine(self):
        return self._memory_engine

    def get_knowledge_graph(self):
        return self._knowledge_graph


def test_search_recall_results_uses_federated_memory_search(monkeypatch):
    monkeypatch.setattr(_recall, "FederatedRetriever", _FakeFederatedRetriever)
    monkeypatch.setattr(_recall, "_search_operator_decisions", lambda *_args, **_kwargs: [])
    _FakeFederatedRetriever.calls = []

    results = _recall.search_recall_results(
        _FakeSubsystems(),
        query="federated memory",
        limit=10,
    )

    assert len(results) == 1
    assert results[0]["entity_id"] == "memory-hit-1"
    assert results[0]["name"] == "Federated memory hit"
    assert results[0]["source"] == "memory"
    assert results[0]["found_via"] == "text"
    assert _FakeFederatedRetriever.calls == [("federated memory", 10)]


def test_search_recall_results_filters_federated_results_by_entity_type(monkeypatch):
    monkeypatch.setattr(_recall, "FederatedRetriever", _FakeFederatedRetriever)
    monkeypatch.setattr(_recall, "_search_operator_decisions", lambda *_args, **_kwargs: [])
    _FakeFederatedRetriever.calls = []

    results = _recall.search_recall_results(
        _FakeSubsystems(),
        query="federated memory",
        entity_type="lesson",
        limit=10,
    )

    assert results == []
    assert _FakeFederatedRetriever.calls == [("federated memory", 10)]


def test_search_recall_results_surfaces_federated_engine_failure() -> None:
    class _BrokenSubsystems(_FakeSubsystems):
        def get_memory_engine(self):
            raise RuntimeError("memory offline")

    with pytest.raises(_recall.RecallAuthorityError, match="memory offline"):
        _recall.search_recall_results(
            _BrokenSubsystems(),
            query="federated memory",
            limit=10,
        )


def test_search_recall_results_surfaces_federated_search_failure(monkeypatch) -> None:
    class _BrokenRetriever:
        def __init__(self, engine):
            self._engine = engine

        def search(self, query: str, limit: int = 20, *, record_telemetry: bool = True):
            del query, limit, record_telemetry
            raise RuntimeError("vector index offline")

    monkeypatch.setattr(_recall, "FederatedRetriever", _BrokenRetriever)

    with pytest.raises(_recall.RecallAuthorityError, match="vector index offline"):
        _recall.search_recall_results(
            _FakeSubsystems(),
            query="federated memory",
            limit=10,
        )
