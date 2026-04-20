from __future__ import annotations

from types import SimpleNamespace

from surfaces.mcp.tools import knowledge


class _FakeEngine:
    def get(self, entity_id, _entity_type):
        if entity_id == "dep-1":
            return SimpleNamespace(
                name="Dependency One",
                source="catalog/dependencies",
                content="# Dependency One",
            )
        return None


class _FakeKnowledgeGraph:
    def __init__(self, calls: list[bool]) -> None:
        self._calls = calls
        self._engine = _FakeEngine()

    def blast_radius(self, entity_id: str, *, include_enrichment: bool = False):
        self._calls.append(include_enrichment)
        assert entity_id == "root-1"
        return {
            "direct": {"dep-1": 0.9},
            "indirect": {},
            "total_affected": 1,
        }


def test_praxis_graph_defaults_to_canonical_only(monkeypatch) -> None:
    blast_calls: list[bool] = []

    monkeypatch.setattr(
        knowledge._subs,
        "get_knowledge_graph",
        lambda: _FakeKnowledgeGraph(blast_calls),
    )
    monkeypatch.setattr(knowledge, "_serialize", lambda value: value)
    monkeypatch.setattr(knowledge, "_resolve_entity", lambda *_args, **_kwargs: object())

    result = knowledge.tool_praxis_graph({"entity_id": "root-1"})

    assert blast_calls == [False]
    assert result["authority"] == {
        "default_edges": "canonical_only",
        "enrichment_included": False,
    }
    assert result["direct_dependencies"] == [
        {"entity_id": "dep-1", "name": "Dependency One", "impact": 0.9}
    ]


def test_praxis_graph_can_opt_into_enrichment(monkeypatch) -> None:
    blast_calls: list[bool] = []

    monkeypatch.setattr(
        knowledge._subs,
        "get_knowledge_graph",
        lambda: _FakeKnowledgeGraph(blast_calls),
    )
    monkeypatch.setattr(knowledge, "_serialize", lambda value: value)
    monkeypatch.setattr(knowledge, "_resolve_entity", lambda *_args, **_kwargs: object())

    result = knowledge.tool_praxis_graph(
        {"entity_id": "root-1", "include_enrichment": "yes"}
    )

    assert blast_calls == [True]
    assert result["authority"] == {
        "default_edges": "canonical_plus_enrichment",
        "enrichment_included": True,
    }


def test_praxis_story_returns_readable_story_lines(monkeypatch) -> None:
    class _FakeEntity:
        def __init__(self, name: str) -> None:
            self.name = name
            self.source = "catalog/story"
            self.content = f"# {name}"

    class _FakeComposer:
        def __init__(self, engine) -> None:
            self.engine = engine
            self.calls: list[int] = []

        def compose(self, entity_id: str, *, max_lines: int = 5):
            self.calls.append(max_lines)
            assert entity_id == "root-1"
            return [
                SimpleNamespace(
                    entity_a="root-1",
                    entity_b="dep-1",
                    relation="depends_on",
                    narrative="root-1 depends on dep-1",
                    strength=0.9,
                )
            ]

    monkeypatch.setattr(knowledge, "StoryComposer", lambda engine: _FakeComposer(engine))
    monkeypatch.setattr(knowledge._subs, "get_memory_engine", lambda: object())
    monkeypatch.setattr(
        knowledge._subs,
        "get_knowledge_graph",
        lambda: _FakeKnowledgeGraph([]),
    )
    monkeypatch.setattr(
        knowledge,
        "_resolve_entity",
        lambda _kg, entity_id: _FakeEntity("Root Entity") if entity_id == "root-1" else _FakeEntity("Dependency Entity"),
    )
    monkeypatch.setattr(knowledge, "_resolve_default_entity_id", lambda _kg: "root-1")

    result = knowledge.tool_praxis_story({"entity_id": "root-1", "max_lines": 3})

    assert result["entity_id"] == "root-1"
    assert result["name"] == "Root Entity"
    assert result["count"] == 1
    assert result["story_lines"] == [
        {
            "entity_a": {"id": "root-1", "name": "Root Entity"},
            "entity_b": {"id": "dep-1", "name": "Dependency Entity"},
            "relation": "depends_on",
            "narrative": "Root Entity depends on Dependency Entity",
            "strength": 0.9,
        }
    ]
