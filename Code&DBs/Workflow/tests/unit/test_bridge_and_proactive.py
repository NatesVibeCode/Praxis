"""Tests for bridge_queries and proactive_context modules."""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

import memory.bridge_queries as bridge_queries_module
from memory.bridge_queries import (
    BridgeEnvelope,
    BridgeQueryEngine,
    ProfileView,
    StoryComposer,
    StoryLine,
)
from memory.types import Edge, Entity, EntityType, RelationType

# Direct file import to avoid runtime/__init__.py (needs Python 3.10+ slots)
_PROACTIVE_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, "runtime", "proactive_context.py"
)
_spec = importlib.util.spec_from_file_location("proactive_context", _PROACTIVE_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["proactive_context"] = _mod
_spec.loader.exec_module(_mod)

CapsuleAssembler = _mod.CapsuleAssembler
ObjectiveCapsule = _mod.ObjectiveCapsule
ProactiveContextEngine = _mod.ProactiveContextEngine
ProactiveItem = _mod.ProactiveItem
SoulPayload = _mod.SoulPayload
SoulPayloadBuilder = _mod.SoulPayloadBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _entity(eid: str, etype: EntityType, name: str, updated: datetime | None = None) -> Entity:
    ts = updated or _now()
    return Entity(
        id=eid,
        entity_type=etype,
        name=name,
        content=name,
        metadata={},
        created_at=ts,
        updated_at=ts,
        source="test",
        confidence=0.9,
    )


def _edge(src: str, tgt: str, rel: RelationType, weight: float = 0.8) -> Edge:
    return Edge(
        source_id=src,
        target_id=tgt,
        relation_type=rel,
        weight=weight,
        metadata={},
        created_at=_now(),
    )


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
        self._edges: list[Edge] = []
        self._conn = object()

    def _connect(self):
        return self._conn

    def insert(self, entity: Entity) -> str:
        self._entities[entity.id] = entity
        return entity.id

    def get(self, entity_id: str, entity_type: EntityType) -> Entity | None:
        entity = self._entities.get(entity_id)
        if entity and entity.entity_type == entity_type:
            return entity
        return None

    def list(self, entity_type: EntityType, limit: int = 100) -> list[Entity]:
        return [
            entity
            for entity in self._entities.values()
            if entity.entity_type == entity_type
        ][:limit]

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

    def add_edge(self, edge: Edge) -> bool:
        self._edges.append(edge)
        return True

    def get_edges(self, entity_id: str, direction: str = "outgoing") -> list[Edge]:
        result: list[Edge] = []
        for edge in self._edges:
            if direction == "outgoing" and edge.source_id == entity_id:
                result.append(edge)
            elif direction == "incoming" and edge.target_id == entity_id:
                result.append(edge)
            elif direction == "both" and (
                edge.source_id == entity_id or edge.target_id == entity_id
            ):
                result.append(edge)
        return result


@pytest.fixture
def engine():
    yield _InMemoryEngine()


# ===================================================================
# Module 1: BridgeQueryEngine
# ===================================================================

class TestProfileView:
    def test_enum_values(self):
        assert ProfileView.DEVELOPER.value == "developer"
        assert ProfileView.STRATEGIST.value == "strategist"
        assert ProfileView.OPERATOR.value == "operator"

    def test_all_profiles_exist(self):
        assert len(ProfileView) == 3


class TestBridgeEnvelope:
    def test_frozen(self):
        env = BridgeEnvelope(
            query="test", source_profile=ProfileView.DEVELOPER,
            target_profile=ProfileView.STRATEGIST,
            results=(), confidence=0.5, provenance="dev->strat",
        )
        with pytest.raises(AttributeError):
            env.query = "changed"

    def test_fields(self):
        env = BridgeEnvelope(
            query="q", source_profile=ProfileView.OPERATOR,
            target_profile=ProfileView.DEVELOPER,
            results=({"id": "1"},), confidence=0.7, provenance="op->dev",
        )
        assert env.query == "q"
        assert len(env.results) == 1


class TestBridgeQueryEngine:
    def test_cross_profile_search_developer_to_strategist(self, engine):
        engine.insert(_entity("t1", EntityType.task, "build feature"))
        engine.insert(_entity("d1", EntityType.decision, "use sqlite"))
        bqe = BridgeQueryEngine(engine)
        result = bqe.cross_profile_search(
            "build", ProfileView.DEVELOPER, ProfileView.STRATEGIST, limit=10,
        )
        assert isinstance(result, BridgeEnvelope)
        assert result.source_profile == ProfileView.DEVELOPER
        assert result.target_profile == ProfileView.STRATEGIST
        assert result.provenance == "developer->strategist"

    def test_cross_profile_search_returns_target_types(self, engine):
        engine.insert(_entity("m1", EntityType.module, "router module"))
        engine.insert(_entity("t1", EntityType.task, "router task"))
        bqe = BridgeQueryEngine(engine)
        # Searching DEVELOPER target should find module, not task
        result = bqe.cross_profile_search(
            "router", ProfileView.STRATEGIST, ProfileView.DEVELOPER, limit=10,
        )
        type_values = {r["entity_type"] for r in result.results}
        assert "module" in type_values or len(result.results) == 0
        # task is STRATEGIST, should not appear in DEVELOPER target
        assert "task" not in type_values

    def test_cross_profile_search_empty(self, engine):
        bqe = BridgeQueryEngine(engine)
        result = bqe.cross_profile_search(
            "nonexistent", ProfileView.DEVELOPER, ProfileView.OPERATOR,
        )
        assert result.results == ()
        assert result.confidence == 0.0

    def test_cross_profile_search_limit(self, engine):
        for i in range(5):
            engine.insert(_entity(f"f{i}", EntityType.fact, f"fact alpha {i}"))
        bqe = BridgeQueryEngine(engine)
        result = bqe.cross_profile_search(
            "fact", ProfileView.DEVELOPER, ProfileView.OPERATOR, limit=2,
        )
        assert len(result.results) <= 2

    def test_cross_profile_search_records_retrieval_telemetry(self, monkeypatch):
        recorded = []

        class RecordingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                recorded.append(metric)

        monkeypatch.setattr(bridge_queries_module, "TelemetryStore", RecordingTelemetryStore)

        engine = _FakeSearchEngine([
            _fake_entity("t1", EntityType.task, "build feature", "build feature"),
            _fake_entity("d1", EntityType.decision, "use sqlite", "use sqlite"),
        ])
        bqe = BridgeQueryEngine(engine)
        result = bqe.cross_profile_search(
            "build", ProfileView.DEVELOPER, ProfileView.STRATEGIST, limit=10,
        )

        assert len(recorded) == 1
        metric = recorded[0]
        assert metric.pattern_name == "bridge.cross_profile_search"
        assert metric.result_count == len(result.results)
        assert metric.latency_ms >= 0.0

    def test_cross_profile_search_telemetry_failure_does_not_change_results(self, monkeypatch):
        engine = _FakeSearchEngine([
            _fake_entity("t1", EntityType.task, "build feature", "build feature"),
            _fake_entity("d1", EntityType.decision, "use sqlite", "use sqlite"),
        ])

        baseline = BridgeQueryEngine(engine).cross_profile_search(
            "build", ProfileView.DEVELOPER, ProfileView.STRATEGIST, limit=10,
        )

        class FailingTelemetryStore:
            def __init__(self, conn):
                self.conn = conn

            def record(self, metric):
                raise RuntimeError("telemetry is down")

        monkeypatch.setattr(bridge_queries_module, "TelemetryStore", FailingTelemetryStore)

        result = BridgeQueryEngine(engine).cross_profile_search(
            "build", ProfileView.DEVELOPER, ProfileView.STRATEGIST, limit=10,
        )

        assert result.results == baseline.results
        assert result.confidence == baseline.confidence

    def test_explain_relationship_forward(self, engine):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        a_id, b_id = f"t_{pfx}_a", f"t_{pfx}_b"
        engine.insert(_entity(a_id, EntityType.module, f"mod_a_{pfx}"))
        engine.insert(_entity(b_id, EntityType.module, f"mod_b_{pfx}"))
        engine.add_edge(_edge(a_id, b_id, RelationType.depends_on, 0.9))
        bqe = BridgeQueryEngine(engine)
        story = bqe.explain_relationship(a_id, b_id)
        assert story is not None
        assert story.relation == "depends_on"
        assert "depends on" in story.narrative
        assert story.strength == pytest.approx(0.9, abs=0.01)

    def test_explain_relationship_reverse(self, engine):
        engine.insert(_entity("x", EntityType.tool, "tool_x"))
        engine.insert(_entity("y", EntityType.pattern, "pat_y"))
        engine.add_edge(_edge("y", "x", RelationType.implements, 0.7))
        bqe = BridgeQueryEngine(engine)
        # Ask from x's perspective; edge is y->x so reverse template
        story = bqe.explain_relationship("x", "y")
        assert story is not None
        assert "implemented by" in story.narrative

    def test_explain_relationship_none(self, engine):
        bqe = BridgeQueryEngine(engine)
        assert bqe.explain_relationship("nope_a", "nope_b") is None


# ===================================================================
# Module 1: StoryComposer
# ===================================================================

class TestStoryLine:
    def test_frozen(self):
        sl = StoryLine(entity_a="a", entity_b="b", relation="r", narrative="n", strength=0.5)
        with pytest.raises(AttributeError):
            sl.strength = 1.0


class TestStoryComposer:
    def test_compose_basic(self, engine):
        engine.insert(_entity("s1", EntityType.module, "service"))
        engine.insert(_entity("s2", EntityType.module, "database"))
        engine.add_edge(_edge("s1", "s2", RelationType.depends_on, 0.8))
        composer = StoryComposer(engine)
        lines = composer.compose("s1")
        assert len(lines) == 1
        assert lines[0].entity_a == "s1"
        assert lines[0].entity_b == "s2"
        assert "depends on" in lines[0].narrative

    def test_compose_sorted_by_weight(self, engine):
        engine.insert(_entity("c", EntityType.module, "core"))
        engine.insert(_entity("d", EntityType.module, "dep1"))
        engine.insert(_entity("e", EntityType.module, "dep2"))
        engine.add_edge(_edge("c", "d", RelationType.depends_on, 0.3))
        engine.add_edge(_edge("c", "e", RelationType.depends_on, 0.9))
        composer = StoryComposer(engine)
        lines = composer.compose("c")
        assert lines[0].strength >= lines[-1].strength

    def test_compose_max_lines(self, engine):
        engine.insert(_entity("h", EntityType.module, "hub"))
        for i in range(10):
            eid = f"spoke_{i}"
            engine.insert(_entity(eid, EntityType.module, eid))
            engine.add_edge(_edge("h", eid, RelationType.depends_on, 0.5))
        composer = StoryComposer(engine)
        lines = composer.compose("h", max_lines=3)
        assert len(lines) == 3

    def test_compose_empty(self, engine):
        engine.insert(_entity("lonely", EntityType.module, "lonely"))
        composer = StoryComposer(engine)
        assert composer.compose("lonely") == []


# ===================================================================
# Module 2: ProactiveContextEngine
# ===================================================================

class TestProactiveItem:
    def test_frozen(self):
        item = ProactiveItem(category="open_task", content="x", relevance=0.5, source_entity_id=None)
        with pytest.raises(AttributeError):
            item.category = "risk"


class TestProactiveContextEngine:
    def test_surface_no_engine(self):
        pce = ProactiveContextEngine(engine=None)
        assert pce.surface() == []

    def test_surface_open_tasks(self, engine):
        engine.insert(_entity("tk1", EntityType.task, "fix bug", _now()))
        pce = ProactiveContextEngine(engine)
        items = pce.surface(limit=10)
        cats = [i.category for i in items]
        assert "open_task" in cats

    def test_surface_recent_decisions(self, engine):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        engine.insert(_entity(f"dec1_{pfx}", EntityType.decision, f"pick sqlite {pfx}", _now() - timedelta(days=2)))
        pce = ProactiveContextEngine(engine)
        # Use a large limit to ensure decisions aren't pushed out by tasks
        items = pce.surface(limit=200)
        cats = [i.category for i in items]
        assert "recent_decision" in cats

    def test_surface_stale_entities(self, engine):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        old = _now() - timedelta(days=60)
        engine.insert(_entity(f"old1_{pfx}", EntityType.fact, f"old fact {pfx}", old))
        pce = ProactiveContextEngine(engine)
        # With production data, stale entities may be pushed out by higher-relevance items
        # Use a very large limit to ensure stale entities appear
        items = pce.surface(limit=1000)
        cats = [i.category for i in items]
        assert "stale_entity" in cats

    def test_surface_limit(self, engine):
        for i in range(10):
            engine.insert(_entity(f"tk{i}", EntityType.task, f"task {i}", _now()))
        pce = ProactiveContextEngine(engine)
        items = pce.surface(limit=3)
        assert len(items) <= 3


# ===================================================================
# Module 2: SoulPayload
# ===================================================================

class TestSoulPayload:
    def test_frozen(self):
        sp = SoulPayload(identity="x", personality_traits=("a",), communication_style="b", priorities=("c",))
        with pytest.raises(AttributeError):
            sp.identity = "y"


class TestSoulPayloadBuilder:
    def test_build_defaults(self):
        builder = SoulPayloadBuilder()
        payload = builder.build()
        assert payload.identity == "engineering assistant"
        assert "precise" in payload.personality_traits
        assert payload.communication_style == "direct and technical"

    def test_build_custom(self):
        builder = SoulPayloadBuilder()
        payload = builder.build({"identity": "ops bot", "priorities": ["uptime"]})
        assert payload.identity == "ops bot"
        assert payload.priorities == ("uptime",)

    def test_render(self):
        builder = SoulPayloadBuilder()
        payload = builder.build()
        text = builder.render(payload)
        assert "Identity: engineering assistant" in text
        assert "Traits:" in text
        assert "Priorities:" in text


# ===================================================================
# Module 2: ObjectiveCapsule & CapsuleAssembler
# ===================================================================

class TestObjectiveCapsule:
    def test_frozen(self):
        cap = ObjectiveCapsule(objective="x", context_files=(), constraints=(), success_criteria=(), token_estimate=0)
        with pytest.raises(AttributeError):
            cap.objective = "y"


class TestCapsuleAssembler:
    def test_assemble_basic(self, tmp_path):
        f = tmp_path / "code.py"
        f.write_text("print('hello')")
        asm = CapsuleAssembler()
        capsule = asm.assemble(
            objective="add logging",
            repo_root=str(tmp_path),
            relevant_files=["code.py"],
            constraints=["no external deps"],
            success_criteria=["tests pass"],
        )
        assert capsule.objective == "add logging"
        assert len(capsule.context_files) == 1
        assert capsule.constraints == ("no external deps",)
        assert capsule.token_estimate > 0

    def test_assemble_missing_file(self, tmp_path):
        asm = CapsuleAssembler()
        capsule = asm.assemble(
            objective="fix it",
            repo_root=str(tmp_path),
            relevant_files=["ghost.py"],
        )
        assert capsule.token_estimate >= 0

    def test_render(self):
        cap = ObjectiveCapsule(
            objective="build feature",
            context_files=("/a.py",),
            constraints=("no breaks",),
            success_criteria=("green tests",),
            token_estimate=500,
        )
        asm = CapsuleAssembler()
        text = asm.render(cap)
        assert "## Objective" in text
        assert "build feature" in text
        assert "/a.py" in text
        assert "no breaks" in text
        assert "green tests" in text
        assert "500" in text
