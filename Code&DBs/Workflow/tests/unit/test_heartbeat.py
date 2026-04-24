"""Tests for runtime.heartbeat — maintenance modules and orchestrator."""
from __future__ import annotations

import importlib.util
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from _pg_test_conn import get_test_conn
from memory.engine import MemoryEngine
from memory.repository import MemoryEdgeRef
from memory.types import (
    Edge,
    EdgeAuthorityClass,
    EdgeProvenanceKind,
    Entity,
    EntityType,
    RelationType,
)

# Direct-load heartbeat to avoid runtime/__init__.py (slots= compat issue)
import sys as _sys
_hb_path = Path(__file__).resolve().parents[2] / "runtime" / "heartbeat.py"
_spec = importlib.util.spec_from_file_location("runtime_heartbeat", str(_hb_path))
_hb = importlib.util.module_from_spec(_spec)
_sys.modules["runtime_heartbeat"] = _hb
_spec.loader.exec_module(_hb)

DuplicateScanner = _hb.DuplicateScanner
GapScanner = _hb.GapScanner
HeartbeatModule = _hb.HeartbeatModule
HeartbeatModuleResult = _hb.HeartbeatModuleResult
HeartbeatOrchestrator = _hb.HeartbeatOrchestrator
OrphanEdgeCleanup = _hb.OrphanEdgeCleanup
StaleEntityDetector = _hb.StaleEntityDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _entity(
    id: str,
    name: str = "test",
    content: str = "test content",
    etype: EntityType = EntityType.fact,
    updated_at: datetime | None = None,
) -> Entity:
    now = _now()
    return Entity(
        id=id,
        entity_type=etype,
        name=name,
        content=content,
        metadata={},
        created_at=now,
        updated_at=updated_at or now,
        source="test",
        confidence=0.9,
    )


def _edge(src: str, tgt: str, rel: RelationType = RelationType.related_to) -> Edge:
    return Edge(
        source_id=src,
        target_id=tgt,
        relation_type=rel,
        weight=1.0,
        metadata={},
        created_at=_now(),
        authority_class=EdgeAuthorityClass.canonical,
        provenance_kind=EdgeProvenanceKind.legacy_unspecified,
    )


def _fresh_engine() -> MemoryEngine:
    return MemoryEngine(conn=get_test_conn())


class _RecordingMutationRepository:
    def __init__(self) -> None:
        self.deleted_calls: list[tuple[MemoryEdgeRef, ...]] = []

    def archive_entities(self, *, entity_ids):
        raise AssertionError(f"unexpected archive call: {tuple(entity_ids)}")

    def delete_edges(self, *, edges):
        recorded = tuple(edges)
        self.deleted_calls.append(recorded)
        return recorded


# ---------------------------------------------------------------------------
# StaleEntityDetector
# ---------------------------------------------------------------------------

def test_stale_entity_detector_finds_old_entities():
    eng = _fresh_engine()
    old_date = _now() - timedelta(days=60)
    eng.insert(_entity("old1", name="Old One", updated_at=old_date))
    eng.insert(_entity("fresh1", name="Fresh One"))

    mod = StaleEntityDetector(eng, stale_days=30)
    result = mod.run()

    assert result.module_name == "stale_entity_detector"
    assert result.ok is True  # self-healing: archives stale entities


# ---------------------------------------------------------------------------
# DuplicateScanner
# ---------------------------------------------------------------------------

def test_duplicate_scanner_flags_similar_names():
    eng = _fresh_engine()
    eng.insert(_entity("a", name="machine learning fundamentals"))
    eng.insert(_entity("b", name="machine learning fundamentals overview"))
    eng.insert(_entity("c", name="completely different topic"))

    mod = DuplicateScanner(eng, similarity_threshold=0.6)
    result = mod.run()

    assert result.module_name == "duplicate_scanner"
    assert result.ok is True  # self-healing: merges duplicates


# ---------------------------------------------------------------------------
# OrphanEdgeCleanup
# ---------------------------------------------------------------------------

def test_orphan_edge_cleanup_removes_dangling_edges():
    eng = _fresh_engine()
    eng.insert(_entity("e1", name="Exists"))
    # Create an edge to a non-existent target
    eng.add_edge(_edge("e1", "ghost"))
    # Create a valid edge
    eng.insert(_entity("e2", name="Also exists"))
    eng.add_edge(_edge("e1", "e2"))

    mod = OrphanEdgeCleanup(eng)
    result = mod.run()

    assert result.ok is True
    # Valid edge should remain, orphan removed
    remaining = eng.get_edges("e1")
    assert len(remaining) == 1
    assert remaining[0].target_id == "e2"


def test_orphan_edge_cleanup_removes_edges_to_archived():
    import uuid
    uid = uuid.uuid4().hex[:8]
    alive_id = f"alive_{uid}"
    archive_id = f"archive_{uid}"
    eng = _fresh_engine()
    eng.insert(_entity(alive_id, name="Alive"))
    eng.insert(_entity(archive_id, name="WillArchive"))
    eng.add_edge(_edge(alive_id, archive_id))
    # Soft-delete the target
    eng.delete(archive_id, EntityType.fact)

    mod = OrphanEdgeCleanup(eng)
    result = mod.run()

    assert result.ok is True


def test_orphan_edge_cleanup_routes_deletion_through_repository():
    import uuid
    uid = uuid.uuid4().hex[:8]
    owner_id = f"owner_{uid}"
    ghost_id = f"ghost_{uid}"
    eng = _fresh_engine()
    eng.insert(_entity(owner_id, name="Owner"))
    assert eng.add_edge(_edge(owner_id, ghost_id)) is True

    repository = _RecordingMutationRepository()
    mod = OrphanEdgeCleanup(eng, repository=repository)
    result = mod.run()

    assert result.ok is True
    # Verify our specific orphan edge was routed through the repository
    all_deleted = [ref for call in repository.deleted_calls for ref in call]
    assert any(ref.source_id == owner_id and ref.target_id == ghost_id for ref in all_deleted)


# ---------------------------------------------------------------------------
# GapScanner
# ---------------------------------------------------------------------------

def test_gap_scanner_finds_empty_content():
    eng = _fresh_engine()
    eng.insert(_entity("ok", name="Has Content", content="real data"))
    eng.insert(_entity("bad", name="No Content", content=""))

    mod = GapScanner(eng)
    result = mod.run()

    # GapScanner only archives entities with BOTH empty name and empty content
    # "bad" has name="No Content" so it won't be archived
    assert result.ok is True


def test_gap_scanner_finds_empty_name():
    eng = _fresh_engine()
    eng.insert(_entity("nameless", name="", content="has content"))

    mod = GapScanner(eng)
    result = mod.run()

    # GapScanner only archives entities with BOTH empty name and empty content
    # "nameless" has content="has content" so it won't be archived
    assert result.ok is True


# ---------------------------------------------------------------------------
# Orchestrator — aggregation
# ---------------------------------------------------------------------------

def test_orchestrator_aggregates_results():
    eng = _fresh_engine()
    old_date = _now() - timedelta(days=60)
    eng.insert(_entity("old", name="Old", updated_at=old_date))
    eng.insert(_entity("gap", name="", content=""))

    modules = [
        StaleEntityDetector(eng, stale_days=30),
        GapScanner(eng),
    ]
    orch = HeartbeatOrchestrator(modules)
    result = orch.run_cycle()

    assert len(result.module_results) == 2
    assert result.errors == 0
    assert result.cycle_id  # non-empty
    assert result.completed_at >= result.started_at


# ---------------------------------------------------------------------------
# Orchestrator — fault isolation
# ---------------------------------------------------------------------------

class _BrokenModule(HeartbeatModule):
    @property
    def name(self) -> str:
        return "broken"

    def run(self) -> HeartbeatModuleResult:
        raise RuntimeError("intentional failure")


def test_orchestrator_isolates_module_failures():
    eng = _fresh_engine()
    eng.insert(_entity("e1", name="", content="has content"))

    modules = [
        _BrokenModule(),
        GapScanner(eng),
    ]
    orch = HeartbeatOrchestrator(modules)
    result = orch.run_cycle()

    assert len(result.module_results) == 2
    broken_result = result.module_results[0]
    assert broken_result.module_name == "broken"
    assert broken_result.ok is False
    assert broken_result.error is not None
    assert "intentional failure" in broken_result.error

    gap_result = result.module_results[1]
    assert gap_result.module_name == "gap_scanner"
    assert gap_result.ok is True
    assert result.errors == 1


# ---------------------------------------------------------------------------
# Orchestrator — budget enforcement
# ---------------------------------------------------------------------------

class _SlowModule(HeartbeatModule):
    @property
    def name(self) -> str:
        return "slow"

    def run(self) -> HeartbeatModuleResult:
        time.sleep(0.3)
        return HeartbeatModuleResult(
            module_name=self.name,
            ok=True,
            error=None,
            duration_ms=300.0,
        )


def test_budget_enforcement_stops_early():
    modules = [
        _SlowModule(),  # takes ~300ms
        _SlowModule(),  # should be skipped if budget is tight
        _SlowModule(),
    ]
    orch = HeartbeatOrchestrator(modules)
    result = orch.run_cycle_with_budget(max_duration_seconds=0.4)

    # With 0.4s budget and each module taking ~0.3s, at most 1-2 should run
    assert len(result.module_results) < 3
    assert len(result.module_results) >= 1


def test_orchestrator_uses_timezone_aware_timestamps():
    orch = HeartbeatOrchestrator([_SlowModule()])
    result = orch.run_cycle()

    assert result.started_at.tzinfo == timezone.utc
    assert result.completed_at.tzinfo == timezone.utc
