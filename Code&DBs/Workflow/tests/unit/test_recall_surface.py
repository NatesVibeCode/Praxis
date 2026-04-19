from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from memory.types import Entity, EntityType
from surfaces import _recall


class _FakeFederatedRetriever:
    calls: list[tuple[str, object, int]] = []

    def __init__(self, engine):
        self._engine = engine

    def search(self, query: str, entity_type=None, limit: int = 20):
        self.calls.append((query, entity_type, limit))
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
    assert _FakeFederatedRetriever.calls == [("federated memory", None, 10)]
