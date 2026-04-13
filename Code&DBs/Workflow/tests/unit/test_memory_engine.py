from __future__ import annotations

import uuid
from datetime import datetime, timezone

from _pg_test_conn import get_test_conn
from memory.engine import MemoryEngine
from memory.types import (
    ChangeSet,
    Edge,
    Entity,
    EntityType,
    RelationType,
)

# Unique prefix per test run to avoid collisions with production data
_RUN = uuid.uuid4().hex[:8]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uid(tag: str) -> str:
    """Create a unique test id to avoid collisions."""
    return f"t_{_RUN}_{tag}"


def _entity(
    id: str,
    name: str = "test",
    content: str = "test content",
    etype: EntityType = EntityType.fact,
    **kw,
) -> Entity:
    now = _now()
    defaults = dict(
        id=id,
        entity_type=etype,
        name=name,
        content=content,
        metadata={},
        created_at=now,
        updated_at=now,
        source="test",
        confidence=0.9,
    )
    defaults.update(kw)
    return Entity(**defaults)


# --- Lifecycle ---


def test_insert_get():
    eng = MemoryEngine(conn=get_test_conn())
    eid = _uid("ig1")
    e = _entity(eid, name="Alpha", content="alpha fact")
    eng.insert(e)
    got = eng.get(eid, EntityType.fact)
    assert got is not None
    assert got.name == "Alpha"
    assert got.content == "alpha fact"


def test_accepts_legacy_db_path_when_conn_present():
    eng = MemoryEngine(conn=get_test_conn(), db_path=":memory:")
    eid = _uid("legacy_ctor")
    eng.insert(_entity(eid, name="LegacyCtor"))
    got = eng.get(eid, EntityType.fact)
    assert got is not None
    assert got.name == "LegacyCtor"


def test_update():
    eng = MemoryEngine(conn=get_test_conn())
    eid = _uid("up1")
    eng.insert(_entity(eid, name="Old"))
    ok = eng.update(eid, EntityType.fact, name="New", updated_at=_now())
    assert ok
    got = eng.get(eid, EntityType.fact)
    assert got is not None
    assert got.name == "New"


def test_delete_soft():
    eng = MemoryEngine(conn=get_test_conn())
    eid = _uid("ds1")
    eng.insert(_entity(eid))
    assert eng.delete(eid, EntityType.fact)
    assert eng.get(eid, EntityType.fact) is not None  # row still exists


def test_soft_delete_excludes_from_list():
    eng = MemoryEngine(conn=get_test_conn())
    eid1 = _uid("sdl1")
    eid2 = _uid("sdl2")
    eng.insert(_entity(eid1, name="visible"))
    eng.insert(_entity(eid2, name="gone"))
    eng.delete(eid2, EntityType.fact)
    entities = eng.list(EntityType.fact)
    ids = [e.id for e in entities]
    assert eid1 in ids
    assert eid2 not in ids


def test_soft_delete_excludes_from_search():
    eng = MemoryEngine(conn=get_test_conn())
    eid1 = _uid("sds1")
    eid2 = _uid("sds2")
    keyword = f"unique_keyword_{_RUN}"
    eng.insert(_entity(eid1, name="searchable", content=keyword))
    eng.insert(_entity(eid2, name="hidden", content=keyword))
    eng.delete(eid2, EntityType.fact)
    results = eng.search(keyword)
    ids = [e.id for e in results]
    assert eid1 in ids
    assert eid2 not in ids


# --- FTS5 Search ---


def test_fts_search():
    eng = MemoryEngine(conn=get_test_conn())
    keyword = f"dbengine_{_RUN}"
    eid1 = _uid("fts1")
    eid2 = _uid("fts2")
    eng.insert(_entity(eid1, name="sqlite", content=f"{keyword} database engine"))
    eng.insert(_entity(eid2, name="python", content="programming language"))
    results = eng.search(keyword)
    assert len(results) >= 1
    assert any(r.id == eid1 for r in results)


def test_search_with_entity_type_filter():
    eng = MemoryEngine(conn=get_test_conn())
    keyword = f"overlap_{_RUN}"
    eid1 = _uid("stf1")
    eid2 = _uid("stf2")
    eng.insert(_entity(eid1, content=keyword, etype=EntityType.fact))
    eng.insert(_entity(eid2, content=keyword, etype=EntityType.decision))
    results = eng.search(keyword, entity_type=EntityType.decision)
    assert len(results) >= 1
    assert all(r.entity_type == EntityType.decision for r in results)
    assert any(r.id == eid2 for r in results)


# --- Edge CRUD ---


def _edge(src: str, tgt: str, rel: RelationType = RelationType.related_to) -> Edge:
    return Edge(
        source_id=src,
        target_id=tgt,
        relation_type=rel,
        weight=1.0,
        metadata={},
        created_at=_now(),
    )


def test_add_get_edges_outgoing():
    eng = MemoryEngine(conn=get_test_conn())
    a, b, c = _uid("eo_a"), _uid("eo_b"), _uid("eo_c")
    eng.add_edge(_edge(a, b))
    eng.add_edge(_edge(a, c))
    edges = eng.get_edges(a, direction="outgoing")
    assert len(edges) == 2
    targets = {e.target_id for e in edges}
    assert targets == {b, c}


def test_get_edges_incoming():
    eng = MemoryEngine(conn=get_test_conn())
    a, b, c = _uid("ei_a"), _uid("ei_b"), _uid("ei_c")
    eng.add_edge(_edge(a, b))
    eng.add_edge(_edge(c, b))
    edges = eng.get_edges(b, direction="incoming")
    assert len(edges) == 2


def test_remove_edge():
    eng = MemoryEngine(conn=get_test_conn())
    a, b = _uid("re_a"), _uid("re_b")
    eng.add_edge(_edge(a, b))
    assert eng.remove_edge(a, b, RelationType.related_to)
    edges = eng.get_edges(a)
    assert len(edges) == 0


# --- ChangeSet ---


def test_changeset_atomic():
    eng = MemoryEngine(conn=get_test_conn())
    c1, c2 = _uid("cs1"), _uid("cs2")
    cs = ChangeSet(
        inserts=[
            _entity(c1, name="first"),
            _entity(c2, name="second"),
        ],
        edges_add=[_edge(c1, c2, RelationType.depends_on)],
    )
    eng.apply_changeset(cs)
    assert eng.get(c1, EntityType.fact) is not None
    assert eng.get(c2, EntityType.fact) is not None
    edges = eng.get_edges(c1)
    assert len(edges) == 1
    assert edges[0].relation_type == RelationType.depends_on


# --- Neighbors ---


def test_neighbors_depth_1():
    eng = MemoryEngine(conn=get_test_conn())
    n1, n2, n3 = _uid("nb1"), _uid("nb2"), _uid("nb3")
    eng.insert(_entity(n1, name="center"))
    eng.insert(_entity(n2, name="neighbor"))
    eng.insert(_entity(n3, name="far"))
    eng.add_edge(_edge(n1, n2))
    eng.add_edge(_edge(n2, n3))
    neighbors = eng.neighbors(n1, EntityType.fact, depth=1)
    ids = {e.id for e in neighbors}
    assert n2 in ids
    assert n3 not in ids
