"""Tests for advanced heartbeat scanners and inbox/queue processing."""
from __future__ import annotations

import importlib.util
import json
import sys as _sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from _pg_test_conn import get_test_conn

from memory.engine import MemoryEngine
from memory.types import (
    Edge,
    EdgeAuthorityClass,
    EdgeProvenanceKind,
    Entity,
    EntityType,
    RelationType,
)

_RUN = uuid.uuid4().hex[:8]

# Direct-load modules to avoid runtime/__init__.py (slots= compat on 3.9)
_rt_dir = Path(__file__).resolve().parents[2] / "runtime"

def _direct_load(name: str, filename: str):
    existing = _sys.modules.get(name)
    if existing is not None:
        return existing
    path = _rt_dir / filename
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    _sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

# heartbeat.py must be loaded first (scanners depends on it)
_hb = _direct_load("runtime.heartbeat", "heartbeat.py")
_scanners = _direct_load("runtime.heartbeat_scanners", "heartbeat_scanners.py")

RelationshipIntegrityScanner = _scanners.RelationshipIntegrityScanner
SchemaConsistencyScanner = _scanners.SchemaConsistencyScanner
ContentQualityScanner = _scanners.ContentQualityScanner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entity(
    etype: EntityType = EntityType.fact,
    name: str = "test entity",
    content: str = "some content",
    metadata: dict | None = None,
    confidence: float = 0.9,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> Entity:
    now = datetime.now(timezone.utc)
    return Entity(
        id=uuid.uuid4().hex,
        entity_type=etype,
        name=name,
        content=content,
        metadata=metadata or {},
        created_at=created_at or now,
        updated_at=updated_at or now,
        source="test",
        confidence=confidence,
    )


def _finding_present(result: object, needle: str) -> bool:
    error = str(getattr(result, "error", "") or "")
    return needle in error


def _insert_raw_edge(engine: MemoryEngine, src: str, tgt: str,
                      rel: str = "related_to", weight: float = 0.5) -> None:
    """Insert an edge directly via SQL to allow invalid values."""
    conn = engine._connect()
    conn.execute(
        "INSERT INTO memory_edges (source_id, target_id, relation_type, weight, metadata, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6) "
        "ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET weight=$4",
        src, tgt, rel, weight, "{}", datetime.now(timezone.utc),
    )


def _insert_raw_entity(engine: MemoryEngine, eid: str, etype: EntityType,
                        name: str = "x", content: str = "c",
                        metadata: str = "{}",
                        confidence: float = 0.9,
                        created_at: datetime | None = None,
                        updated_at: datetime | None = None) -> None:
    """Insert entity directly via SQL to allow invalid values."""
    now_dt = datetime.now(timezone.utc)
    conn = engine._connect()
    conn.execute(
        "INSERT INTO memory_entities "
        "(id, entity_type, name, content, metadata, source, confidence, archived, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, false, $8, $9) "
        "ON CONFLICT (id) DO UPDATE SET "
        "entity_type=$2, name=$3, content=$4, metadata=$5, source=$6, "
        "confidence=$7, archived=false, created_at=$8, updated_at=$9",
        eid, etype.value, name, content, metadata, "test", confidence,
        created_at or now_dt, updated_at or now_dt,
    )


class _FakeScannerConn:
    def __init__(self, *, edge_rows=None, entity_rows=None) -> None:
        self._edge_rows = list(edge_rows or [])
        self._entity_rows = list(entity_rows or [])

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT source_id, target_id, relation_type, weight FROM memory_edges"):
            return list(self._edge_rows)
        if normalized.startswith("SELECT id, created_at, updated_at, confidence FROM memory_entities"):
            return list(self._entity_rows)
        if normalized.startswith("SELECT id, name, content, metadata FROM memory_entities WHERE entity_type = $1 AND NOT archived"):
            entity_type = args[0]
            return [
                row for row in self._entity_rows
                if row.get("entity_type") == entity_type
            ]
        return []


class _FakeScannerEngine:
    def __init__(self, *, edge_rows=None, entity_rows=None) -> None:
        self._conn = _FakeScannerConn(edge_rows=edge_rows, entity_rows=entity_rows)

    def _connect(self):
        return self._conn


# ===========================================================================
# RelationshipIntegrityScanner
# ===========================================================================

class TestRelationshipIntegrityScanner:

    def test_clean_edges_no_crash(self):
        engine = MemoryEngine(conn=get_test_conn())
        e1 = _make_entity()
        e2 = _make_entity()
        engine.insert(e1)
        engine.insert(e2)
        engine.add_edge(
            Edge(
                e1.id,
                e2.id,
                RelationType.related_to,
                0.5,
                {},
                datetime.now(timezone.utc),
                authority_class=EdgeAuthorityClass.canonical,
                provenance_kind=EdgeProvenanceKind.legacy_unspecified,
            )
        )
        scanner = RelationshipIntegrityScanner(engine)
        result = scanner.run()
        assert result.module_name == "relationship_integrity_scanner"
        assert not _finding_present(result, e1.id)
        assert not _finding_present(result, e2.id)

    def test_invalid_relation_type(self):
        src, tgt = f"t_{_RUN}_ir_a", f"t_{_RUN}_ir_b"
        engine = _FakeScannerEngine(
            edge_rows=[{
                "source_id": src,
                "target_id": tgt,
                "relation_type": "made_up_relation",
                "weight": 0.5,
            }]
        )
        scanner = RelationshipIntegrityScanner(engine)
        result = scanner.run()
        assert result.ok is False
        assert _finding_present(result, "invalid_relation")

    def test_weight_out_of_range(self):
        src, tgt = f"t_{_RUN}_wr_a", f"t_{_RUN}_wr_b"
        engine = _FakeScannerEngine(
            edge_rows=[{
                "source_id": src,
                "target_id": tgt,
                "relation_type": "related_to",
                "weight": 1.5,
            }]
        )
        scanner = RelationshipIntegrityScanner(engine)
        result = scanner.run()
        assert result.ok is False
        assert _finding_present(result, "bad_weight")

    def test_negative_weight(self):
        src, tgt = f"t_{_RUN}_nw_a", f"t_{_RUN}_nw_b"
        engine = _FakeScannerEngine(
            edge_rows=[{
                "source_id": src,
                "target_id": tgt,
                "relation_type": "related_to",
                "weight": -0.1,
            }]
        )
        result = RelationshipIntegrityScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "bad_weight")

    def test_self_referential_edge(self):
        nid = f"t_{_RUN}_self"
        engine = _FakeScannerEngine(
            edge_rows=[{
                "source_id": nid,
                "target_id": nid,
                "relation_type": "related_to",
                "weight": 0.5,
            }]
        )
        scanner = RelationshipIntegrityScanner(engine)
        result = scanner.run()
        assert result.ok is False
        assert _finding_present(result, "self_ref")

    def test_multiple_violations_single_edge(self):
        nid = f"t_{_RUN}_multi"
        engine = _FakeScannerEngine(
            edge_rows=[{
                "source_id": nid,
                "target_id": nid,
                "relation_type": "bogus",
                "weight": 9.0,
            }]
        )
        result = RelationshipIntegrityScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "self_ref")
        assert _finding_present(result, "invalid_relation")
        assert _finding_present(result, "bad_weight")


# ===========================================================================
# SchemaConsistencyScanner
# ===========================================================================

class TestSchemaConsistencyScanner:

    def test_clean_entities_runs(self):
        engine = MemoryEngine(conn=get_test_conn())
        entity = _make_entity()
        engine.insert(entity)
        result = SchemaConsistencyScanner(engine).run()
        assert result.module_name == "schema_consistency_scanner"
        assert not _finding_present(result, entity.id)

    def test_future_created_at(self):
        future = datetime.now(timezone.utc) + timedelta(days=10)
        eid = f"t_{_RUN}_fc1"
        engine = _FakeScannerEngine(
            entity_rows=[{
                "id": eid,
                "created_at": future,
                "updated_at": future,
                "confidence": 0.9,
            }]
        )
        result = SchemaConsistencyScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "future_created")

    def test_updated_before_created(self):
        created = datetime.now(timezone.utc)
        updated = datetime.now(timezone.utc) - timedelta(days=5)
        eid = f"t_{_RUN}_ubc1"
        engine = _FakeScannerEngine(
            entity_rows=[{
                "id": eid,
                "created_at": created,
                "updated_at": updated,
                "confidence": 0.9,
            }]
        )
        result = SchemaConsistencyScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "updated_before_created")

    def test_mixed_naive_and_aware_timestamps_are_normalized(self):
        created = datetime.now(timezone.utc)
        updated = (created - timedelta(days=5)).replace(tzinfo=None).isoformat()
        eid = f"t_{_RUN}_mixed"
        engine = _FakeScannerEngine(
            entity_rows=[{
                "id": eid,
                "created_at": created,
                "updated_at": updated,
                "confidence": 0.9,
            }]
        )
        result = SchemaConsistencyScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "updated_before_created")

    def test_bad_confidence(self):
        eid = f"t_{_RUN}_bc1"
        engine = _FakeScannerEngine(
            entity_rows=[{
                "id": eid,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "confidence": 1.5,
            }]
        )
        result = SchemaConsistencyScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "bad_confidence")

    def test_negative_confidence(self):
        eid = f"t_{_RUN}_nc1"
        engine = _FakeScannerEngine(
            entity_rows=[{
                "id": eid,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "confidence": -0.1,
            }]
        )
        result = SchemaConsistencyScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "bad_confidence")


# ===========================================================================
# ContentQualityScanner
# ===========================================================================

class TestContentQualityScanner:

    def test_clean_entities_runs(self):
        engine = MemoryEngine(conn=get_test_conn())
        entity = _make_entity(
            name=f"good-name-{uuid.uuid4().hex[:8]}",
            content=f"good-content-{uuid.uuid4().hex}",
        )
        engine.insert(entity)
        result = ContentQualityScanner(engine).run()
        assert result.module_name == "content_quality_scanner"
        assert not _finding_present(result, entity.id)

    def test_whitespace_only_content(self):
        engine = MemoryEngine(conn=get_test_conn())
        eid = f"t_{_RUN}_ws1"
        _insert_raw_entity(engine, eid, EntityType.fact, name="ok", content="   \t\n  ")
        result = ContentQualityScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "whitespace_content")

    def test_single_char_name(self):
        engine = MemoryEngine(conn=get_test_conn())
        eid = f"t_{_RUN}_sc1"
        _insert_raw_entity(engine, eid, EntityType.fact, name="X", content="valid")
        result = ContentQualityScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "short_name")

    def test_duplicate_content_same_type(self):
        engine = MemoryEngine(conn=get_test_conn())
        eid1 = f"t_{_RUN}_dup1"
        eid2 = f"t_{_RUN}_dup2"
        dup_content = f"identical_text_{_RUN}"
        _insert_raw_entity(engine, eid1, EntityType.fact, name="first", content=dup_content)
        _insert_raw_entity(engine, eid2, EntityType.fact, name="second", content=dup_content)
        result = ContentQualityScanner(engine).run()
        assert result.ok is False
        assert _finding_present(result, "dup_content")

    def test_invalid_json_metadata_rejected_by_db(self):
        """Postgres jsonb column rejects invalid JSON at the DB level."""
        import asyncpg
        engine = MemoryEngine(conn=get_test_conn())
        eid = f"t_{_RUN}_bjm1"
        with pytest.raises((asyncpg.exceptions.InvalidTextRepresentationError, Exception)):
            _insert_raw_entity(engine, eid, EntityType.fact, name="ok", content="ok",
                                metadata="not json{{{")

    def test_valid_json_metadata_no_finding_for_entity(self):
        engine = MemoryEngine(conn=get_test_conn())
        eid = f"t_{_RUN}_vjm1"
        _insert_raw_entity(engine, eid, EntityType.fact, name="ok", content="ok",
                            metadata='{"key": "val"}')
        result = ContentQualityScanner(engine).run()
        # Our specific entity should not show bad_metadata
        assert not _finding_present(result, f"bad_metadata:{eid}")
