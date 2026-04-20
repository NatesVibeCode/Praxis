"""Boundary tests for memory graph maintenance ownership seams."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import memory.graph_hygiene as graph_hygiene_module
from memory.graph_hygiene import GraphHygienist
from memory.repository import MemoryEdgeRef
from memory.types import (
    Edge,
    EdgeAuthorityClass,
    EdgeProvenanceKind,
    Entity,
    EntityType,
    RelationType,
)
from runtime.database_maintenance import DatabaseMaintenanceProcessor, MaintenanceIntent
from runtime.embedding_service import EmbeddingRuntimeAuthority
from storage.postgres.memory_graph_repository import PostgresMemoryGraphRepository

_HB_PATH = Path(__file__).resolve().parents[2] / "runtime" / "heartbeat.py"
_HB_SPEC = importlib.util.spec_from_file_location("runtime_heartbeat_boundary", str(_HB_PATH))
_HB_MODULE = importlib.util.module_from_spec(_HB_SPEC)
sys.modules["runtime_heartbeat_boundary"] = _HB_MODULE
assert _HB_SPEC.loader is not None
_HB_SPEC.loader.exec_module(_HB_MODULE)
OrphanEdgeCleanup = _HB_MODULE.OrphanEdgeCleanup


def _normalize_sql(query: str) -> str:
    return " ".join(query.split())


class _FakeConn:
    def __init__(
        self,
        *,
        execute_results: dict[str, list[dict[str, object]]] | None = None,
        fetchrow_results: dict[str, dict[str, object] | None] | None = None,
    ) -> None:
        self.execute_results = execute_results or {}
        self.fetchrow_results = fetchrow_results or {}
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        normalized = _normalize_sql(query)
        self.execute_calls.append((normalized, args))
        return list(self.execute_results.get(normalized, []))

    def fetchrow(self, query: str, *args):
        normalized = _normalize_sql(query)
        self.fetchrow_calls.append((normalized, args))
        return self.fetchrow_results.get(normalized)


class _FakeEngine:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def _connect(self) -> _FakeConn:
        return self._conn


class _RecordingRepository:
    def __init__(self) -> None:
        self.archived_calls: list[tuple[str, ...]] = []
        self.deleted_calls: list[tuple[MemoryEdgeRef, ...]] = []
        self.embedding_failed_ids: list[str] = []
        self.embedding_ready_calls: list[tuple[str, str]] = []
        self.maintenance_touch_ids: list[str] = []
        self.absorb_calls: list[tuple[str, tuple[str, ...]]] = []
        self.vector_neighbor_calls: list[tuple[str, tuple[tuple[str, object], ...], str, int]] = []

    def archive_entities(self, *, entity_ids):
        recorded = tuple(entity_ids)
        self.archived_calls.append(recorded)
        return recorded

    def delete_edges(self, *, edges):
        recorded = tuple(edges)
        self.deleted_calls.append(recorded)
        return recorded

    def mark_entity_embedding_failed(self, *, entity_id):
        recorded = str(entity_id)
        self.embedding_failed_ids.append(recorded)
        return True

    def mark_entity_embedding_ready(self, *, entity_id, embedding_model):
        recorded = (str(entity_id), str(embedding_model))
        self.embedding_ready_calls.append(recorded)
        return True

    def touch_entity_maintenance(self, *, entity_id):
        recorded = str(entity_id)
        self.maintenance_touch_ids.append(recorded)
        return True

    def absorb_exact_duplicate_entities(self, *, canonical_entity_id, duplicate_entity_ids):
        recorded = (str(canonical_entity_id), tuple(str(entity_id) for entity_id in duplicate_entity_ids))
        self.absorb_calls.append(recorded)
        return {
            "canonical_entity_id": recorded[0],
            "archived_ids": list(recorded[1]),
            "archived_count": len(recorded[1]),
            "rehomed_edge_rows": 2,
            "deleted_edge_rows": 1,
            "deleted_inferred_rows": 1,
            "deleted_neighbor_rows": 1,
            "deleted_pending_intents": 0,
        }

    def replace_vector_neighbor_projection(
        self,
        *,
        source_entity_id,
        neighbors,
        policy_key,
        embedding_version,
    ):
        recorded_neighbors = tuple(
            tuple(sorted((str(key), value) for key, value in dict(neighbor).items()))
            for neighbor in neighbors
        )
        recorded = (
            str(source_entity_id),
            recorded_neighbors,
            str(policy_key),
            int(embedding_version),
        )
        self.vector_neighbor_calls.append(recorded)
        return len(recorded_neighbors)


class _FakeVectorQuery:
    def __init__(self, results: list[dict[str, object]] | None = None) -> None:
        self._results = list(results or [])

    def search(self, *args, **kwargs):
        return list(self._results)


class _FakeVectorStore:
    def __init__(self, *, neighbors: list[dict[str, object]] | None = None) -> None:
        self.embedding_calls: list[tuple[str, str, str, str]] = []
        self.neighbors = list(neighbors or [])

    def set_embedding(self, table: str, key_column: str, entity_id: str, *, text: str) -> None:
        self.embedding_calls.append((table, key_column, entity_id, text))

    def prepare(self, text: str) -> _FakeVectorQuery:
        return _FakeVectorQuery(self.neighbors)


class _FakeEmbedder:
    authority = EmbeddingRuntimeAuthority()
    model_name = authority.model_name
    dimensions = authority.dimensions


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _entity(*, entity_id: str) -> Entity:
    now = _utc_now()
    return Entity(
        id=entity_id,
        entity_type=EntityType.fact,
        name="Entity Name",
        content="Entity Content",
        metadata={"topic": "memory"},
        created_at=now,
        updated_at=now,
        source="test",
        confidence=0.9,
    )


def _edge(*, source_id: str, target_id: str) -> Edge:
    return Edge(
        source_id=source_id,
        target_id=target_id,
        relation_type=RelationType.related_to,
        weight=0.7,
        metadata={"kind": "test"},
        created_at=_utc_now(),
        authority_class=EdgeAuthorityClass.canonical,
        provenance_kind=EdgeProvenanceKind.legacy_unspecified,
    )


def test_postgres_memory_graph_repository_upserts_entities_through_owned_insert():
    query = _normalize_sql(
        """
        INSERT INTO memory_entities
        (
            id,
            entity_type,
            name,
            content,
            metadata,
            source,
            confidence,
            archived,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, false, $8, $9)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            source = EXCLUDED.source,
            confidence = EXCLUDED.confidence,
            archived = false,
            updated_at = EXCLUDED.updated_at
        """
    )
    conn = _FakeConn()
    repository = PostgresMemoryGraphRepository(conn)
    entity = _entity(entity_id="entity-1")

    entity_id = repository.upsert_entity(entity=entity)

    assert entity_id == "entity-1"
    assert conn.execute_calls == [
        (
            query,
            (
                "entity-1",
                "fact",
                "Entity Name",
                "Entity Content",
                '{"topic":"memory"}',
                "test",
                0.9,
                entity.created_at,
                entity.updated_at,
            ),
        )
    ]


def test_postgres_memory_graph_repository_updates_entity_fields_through_owned_update():
    query = _normalize_sql(
        """
        UPDATE memory_entities
        SET name = $1, metadata = $2::jsonb, updated_at = $3
        WHERE id = $4
          AND entity_type = $5
          AND NOT archived
        RETURNING id
        """
    )
    updated_at = _utc_now()
    conn = _FakeConn(execute_results={query: [{"id": "entity-1"}]})
    repository = PostgresMemoryGraphRepository(conn)

    updated = repository.update_entity_fields(
        entity_id="entity-1",
        entity_type=EntityType.fact,
        fields={
            "name": "Updated Name",
            "metadata": {"state": "fresh"},
            "updated_at": updated_at,
            "ignored": "skip",
        },
    )

    assert updated is True
    assert conn.execute_calls == [
        (
            query,
            (
                "Updated Name",
                '{"state":"fresh"}',
                updated_at,
                "entity-1",
                "fact",
            ),
        )
    ]


def test_postgres_memory_graph_repository_archives_entity_through_owned_update():
    query = _normalize_sql(
        """
        UPDATE memory_entities
        SET archived = true
        WHERE id = $1
          AND entity_type = $2
          AND NOT archived
        RETURNING id
        """
    )
    conn = _FakeConn(execute_results={query: [{"id": "entity-1"}]})
    repository = PostgresMemoryGraphRepository(conn)

    updated = repository.archive_entity(
        entity_id="entity-1",
        entity_type=EntityType.fact,
    )

    assert updated is True
    assert conn.execute_calls == [(query, ("entity-1", "fact"))]


def test_postgres_memory_graph_repository_upserts_edges_through_owned_insert():
    query = _normalize_sql(
        """
        INSERT INTO memory_edges
        (
            source_id,
            target_id,
            relation_type,
            weight,
            metadata,
            created_at,
            authority_class,
            provenance_kind,
            provenance_ref,
            edge_origin,
            active,
            last_validated_at
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $8, true, now())
        ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET
            weight = EXCLUDED.weight,
            metadata = EXCLUDED.metadata,
            authority_class = EXCLUDED.authority_class,
            provenance_kind = EXCLUDED.provenance_kind,
            provenance_ref = EXCLUDED.provenance_ref,
            edge_origin = EXCLUDED.edge_origin,
            active = true,
            last_validated_at = now()
        """
    )
    conn = _FakeConn()
    repository = PostgresMemoryGraphRepository(conn)
    edge = _edge(source_id="entity-1", target_id="entity-2")

    created = repository.upsert_edge(edge=edge)

    assert created is True
    assert conn.execute_calls == [
        (
            query,
            (
                "entity-1",
                "entity-2",
                "related_to",
                0.7,
                '{"kind":"test"}',
                edge.created_at,
                "canonical",
                "legacy_unspecified",
                None,
            ),
        )
    ]


def test_postgres_memory_graph_repository_deletes_single_edge_through_owned_delete():
    query = _normalize_sql(
        """
        DELETE FROM memory_edges
        WHERE source_id = $1
          AND target_id = $2
          AND relation_type = $3
        RETURNING source_id
        """
    )
    conn = _FakeConn(execute_results={query: [{"source_id": "entity-1"}]})
    repository = PostgresMemoryGraphRepository(conn)

    deleted = repository.delete_edge(
        source_id="entity-1",
        target_id="entity-2",
        relation_type=RelationType.related_to,
    )

    assert deleted is True
    assert conn.execute_calls == [(query, ("entity-1", "entity-2", "related_to"))]


def test_postgres_memory_graph_repository_archives_entities_through_owned_update():
    query = _normalize_sql(
        """
        UPDATE memory_entities
        SET archived = true,
            needs_reembed = false,
            embedding_status = 'archived',
            last_maintained_at = now()
        WHERE id = ANY($1::text[])
          AND archived = false
        RETURNING id
        """
    )
    conn = _FakeConn(
        execute_results={
            query: [{"id": "stale-1"}, {"id": "stale-2"}],
        }
    )
    repository = PostgresMemoryGraphRepository(conn)

    archived = repository.archive_entities(
        entity_ids=["stale-1", "stale-1", "stale-2"],
    )

    assert archived == ("stale-1", "stale-2")
    assert conn.execute_calls == [
        (
            query,
            (["stale-1", "stale-2"],),
        )
    ]


def test_postgres_memory_graph_repository_deletes_edges_through_owned_delete():
    query = _normalize_sql(
        """
        WITH requested AS (
            SELECT *
            FROM unnest($1::text[], $2::text[], $3::text[])
                AS requested(source_id, target_id, relation_type)
        ),
        deleted AS (
            DELETE FROM memory_edges AS edge
            USING requested
            WHERE edge.source_id = requested.source_id
              AND edge.target_id = requested.target_id
              AND edge.relation_type = requested.relation_type
            RETURNING edge.source_id, edge.target_id, edge.relation_type
        )
        SELECT source_id, target_id, relation_type
        FROM deleted
        """
    )
    conn = _FakeConn(
        execute_results={
            query: [
                {"source_id": "a", "target_id": "b", "relation_type": "related_to"},
            ],
        }
    )
    repository = PostgresMemoryGraphRepository(conn)

    deleted = repository.delete_edges(
        edges=[
            MemoryEdgeRef(source_id="a", target_id="b", relation_type="related_to"),
            MemoryEdgeRef(source_id="a", target_id="b", relation_type="related_to"),
        ]
    )

    assert deleted == (
        MemoryEdgeRef(source_id="a", target_id="b", relation_type="related_to"),
    )
    assert conn.execute_calls == [
        (
            query,
            (["a"], ["b"], ["related_to"]),
        )
    ]


def test_postgres_memory_graph_repository_replaces_vector_neighbor_projection_through_owned_mutation():
    delete_neighbors_query = _normalize_sql(
        """
        DELETE FROM memory_vector_neighbors WHERE source_entity_id = $1
        """
    )
    delete_inferred_query = _normalize_sql(
        """
        DELETE FROM memory_inferred_edges
        WHERE source_id = $1
          AND inference_kind = 'vector_neighbor'
        """
    )
    vector_insert_query = _normalize_sql(
        """
        INSERT INTO memory_vector_neighbors (
            source_entity_id,
            target_entity_id,
            policy_key,
            similarity,
            rank,
            embedding_version,
            refreshed_at,
            active
        )
        VALUES ($1, $2, $3, $4, $5, $6, now(), true)
        ON CONFLICT (source_entity_id, target_entity_id, policy_key) DO UPDATE
        SET similarity = EXCLUDED.similarity,
            rank = EXCLUDED.rank,
            embedding_version = EXCLUDED.embedding_version,
            refreshed_at = now(),
            active = true
        """
    )
    inferred_insert_query = _normalize_sql(
        """
        INSERT INTO memory_inferred_edges (
            source_id,
            target_id,
            relation_type,
            inference_kind,
            confidence,
            metadata,
            evidence_count,
            embedding_version,
            created_at,
            refreshed_at,
            active
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, now(), now(), true)
        ON CONFLICT (source_id, target_id, relation_type, inference_kind) DO UPDATE
        SET confidence = EXCLUDED.confidence,
            metadata = EXCLUDED.metadata,
            embedding_version = EXCLUDED.embedding_version,
            evidence_count = EXCLUDED.evidence_count,
            refreshed_at = now(),
            active = true
        """
    )
    conn = _FakeConn()
    repository = PostgresMemoryGraphRepository(conn)

    count = repository.replace_vector_neighbor_projection(
        source_entity_id="entity-1",
        neighbors=[
            {"id": "entity-2", "similarity": 0.92},
            {"id": "entity-3", "similarity": 0.81},
        ],
        policy_key="memory_entity.vector_neighbors",
        embedding_version=4,
    )

    assert count == 2
    assert conn.execute_calls == [
        (delete_neighbors_query, ("entity-1",)),
        (delete_inferred_query, ("entity-1",)),
        (
            vector_insert_query,
            ("entity-1", "entity-2", "memory_entity.vector_neighbors", 0.92, 1, 4),
        ),
        (
            vector_insert_query,
            ("entity-1", "entity-3", "memory_entity.vector_neighbors", 0.81, 2, 4),
        ),
        (
            inferred_insert_query,
            (
                "entity-1",
                "entity-2",
                "semantic_neighbor",
                "vector_neighbor",
                0.92,
                '{"policy_key":"memory_entity.vector_neighbors","rank":1}',
                1,
                4,
            ),
        ),
        (
            inferred_insert_query,
            (
                "entity-1",
                "entity-3",
                "semantic_neighbor",
                "vector_neighbor",
                0.81,
                '{"policy_key":"memory_entity.vector_neighbors","rank":2}',
                1,
                4,
            ),
        ),
    ]


def test_postgres_memory_graph_repository_marks_entity_embedding_failed_through_owned_update():
    query = _normalize_sql(
        """
        UPDATE memory_entities
        SET embedding = NULL,
            needs_reembed = false,
            embedding_status = 'failed',
            last_maintained_at = now()
        WHERE id = $1
        RETURNING id
        """
    )
    conn = _FakeConn(execute_results={query: [{"id": "entity-1"}]})
    repository = PostgresMemoryGraphRepository(conn)

    updated = repository.mark_entity_embedding_failed(entity_id="entity-1")

    assert updated is True
    assert conn.execute_calls == [(query, ("entity-1",))]


def test_postgres_memory_graph_repository_marks_entity_embedding_ready_through_owned_update():
    query = _normalize_sql(
        """
        UPDATE memory_entities
        SET needs_reembed = false,
            embedding_status = 'ready',
            embedding_model = $1,
            embedded_at = now(),
            last_maintained_at = now()
        WHERE id = $2
        RETURNING id
        """
    )
    conn = _FakeConn(execute_results={query: [{"id": "entity-1"}]})
    repository = PostgresMemoryGraphRepository(conn)

    updated = repository.mark_entity_embedding_ready(
        entity_id="entity-1",
        embedding_model="all-MiniLM-L6-v2",
    )

    assert updated is True
    assert conn.execute_calls == [(query, ("all-MiniLM-L6-v2", "entity-1"))]


def test_postgres_memory_graph_repository_touches_entity_maintenance_through_owned_update():
    query = _normalize_sql(
        """
        UPDATE memory_entities
        SET last_maintained_at = now()
        WHERE id = $1
        RETURNING id
        """
    )
    conn = _FakeConn(execute_results={query: [{"id": "entity-1"}]})
    repository = PostgresMemoryGraphRepository(conn)

    updated = repository.touch_entity_maintenance(entity_id="entity-1")

    assert updated is True
    assert conn.execute_calls == [(query, ("entity-1",))]


def test_postgres_memory_graph_repository_absorbs_exact_duplicates_through_owned_function():
    query = _normalize_sql(
        """
        SELECT absorb_exact_duplicate_memory_entities(
            $1,
            $2::text[]
        ) AS outcome
        """
    )
    conn = _FakeConn(
        fetchrow_results={
            query: {
                "outcome": {
                    "canonical_entity_id": "entity-1",
                    "archived_ids": ["entity-2"],
                    "archived_count": 1,
                    "rehomed_edge_rows": 2,
                    "deleted_edge_rows": 1,
                    "deleted_inferred_rows": 1,
                    "deleted_neighbor_rows": 1,
                    "deleted_pending_intents": 0,
                }
            }
        }
    )
    repository = PostgresMemoryGraphRepository(conn)

    outcome = repository.absorb_exact_duplicate_entities(
        canonical_entity_id="entity-1",
        duplicate_entity_ids=["entity-2", "entity-2"],
    )

    assert outcome["canonical_entity_id"] == "entity-1"
    assert outcome["archived_ids"] == ["entity-2"]
    assert conn.fetchrow_calls == [
        (
            query,
            ("entity-1", ["entity-2"]),
        )
    ]


def test_graph_hygienist_routes_archival_through_repository_seam():
    select_query = _normalize_sql(
        "SELECT id FROM memory_entities WHERE archived = false AND updated_at < $1"
    )
    conn = _FakeConn(
        execute_results={
            select_query: [{"id": "stale-node"}],
        }
    )
    repository = _RecordingRepository()
    hygienist = GraphHygienist(
        _FakeEngine(conn),
        max_age_days=90,
        repository=repository,
    )

    archived_count = hygienist.archive_stale()

    assert archived_count == 1
    assert repository.archived_calls == [("stale-node",)]
    assert all(not call[0].startswith("UPDATE memory_entities") for call in conn.execute_calls)


def test_graph_hygienist_resolves_default_repository_through_memory_seam(monkeypatch):
    select_query = _normalize_sql(
        "SELECT id FROM memory_entities WHERE archived = false AND updated_at < $1"
    )
    conn = _FakeConn(
        execute_results={
            select_query: [{"id": "stale-node"}],
        }
    )
    engine = _FakeEngine(conn)
    repository = _RecordingRepository()
    resolved_engines: list[_FakeEngine] = []

    def _resolve_memory_graph_mutation_repository(passed_engine):
        resolved_engines.append(passed_engine)
        return repository

    monkeypatch.setattr(
        graph_hygiene_module,
        "resolve_memory_graph_mutation_repository",
        _resolve_memory_graph_mutation_repository,
    )

    archived_count = GraphHygienist(engine, max_age_days=90).archive_stale()

    assert archived_count == 1
    assert resolved_engines == [engine]
    assert repository.archived_calls == [("stale-node",)]


def test_orphan_edge_cleanup_routes_deletion_through_repository_seam():
    select_query = _normalize_sql(
        """
        SELECT e.source_id, e.target_id, e.relation_type
        FROM memory_edges e
        LEFT JOIN memory_entities s ON e.source_id = s.id AND s.archived = false
        LEFT JOIN memory_entities t ON e.target_id = t.id AND t.archived = false
        WHERE s.id IS NULL OR t.id IS NULL
        """
    )
    conn = _FakeConn(
        execute_results={
            select_query: [
                {
                    "source_id": "owner",
                    "target_id": "ghost",
                    "relation_type": "related_to",
                }
            ],
        }
    )
    repository = _RecordingRepository()
    module = OrphanEdgeCleanup(_FakeEngine(conn), repository=repository)

    result = module.run()

    assert result.ok is True
    assert repository.deleted_calls == [
        (MemoryEdgeRef(source_id="owner", target_id="ghost", relation_type="related_to"),)
    ]
    assert all(not call[0].startswith("DELETE FROM memory_edges") for call in conn.execute_calls)


def test_orphan_edge_cleanup_resolves_default_repository_through_memory_seam(monkeypatch):
    select_query = _normalize_sql(
        """
        SELECT e.source_id, e.target_id, e.relation_type
        FROM memory_edges e
        LEFT JOIN memory_entities s ON e.source_id = s.id AND s.archived = false
        LEFT JOIN memory_entities t ON e.target_id = t.id AND t.archived = false
        WHERE s.id IS NULL OR t.id IS NULL
        """
    )
    conn = _FakeConn(
        execute_results={
            select_query: [
                {
                    "source_id": "owner",
                    "target_id": "ghost",
                    "relation_type": "related_to",
                }
            ],
        }
    )
    engine = _FakeEngine(conn)
    repository = _RecordingRepository()
    resolved_engines: list[_FakeEngine] = []

    def _resolve_memory_graph_mutation_repository(passed_engine):
        resolved_engines.append(passed_engine)
        return repository

    monkeypatch.setattr(
        _HB_MODULE,
        "resolve_memory_graph_mutation_repository",
        _resolve_memory_graph_mutation_repository,
    )

    result = OrphanEdgeCleanup(engine).run()

    assert result.ok is True
    assert resolved_engines == [engine]
    assert repository.deleted_calls == [
        (MemoryEdgeRef(source_id="owner", target_id="ghost", relation_type="related_to"),)
    ]


def test_database_maintenance_embed_empty_routes_entity_state_through_repository_seam():
    select_query = _normalize_sql(
        """
        SELECT id, name, content, archived, source_hash, embedding_version
        FROM memory_entities
        WHERE id = $1
        """
    )
    conn = _FakeConn(
        fetchrow_results={
            select_query: {
                "id": "entity-1",
                "name": "   ",
                "content": "",
                "archived": False,
                "source_hash": "source-hash",
                "embedding_version": 3,
            }
        }
    )
    repository = _RecordingRepository()
    processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
    processor._memory_graph_repository = repository
    processor._vector_store = None

    result = processor._process_embed_entity(
        MaintenanceIntent(
            intent_id=1,
            intent_kind="embed_entity",
            subject_kind="memory_entity",
            subject_id="entity-1",
            policy_key="memory_entity.embed",
            fingerprint="embed_entity:test",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=3,
        )
    )

    assert result["status"] == "skipped"
    assert result["message"] == "entity_empty:entity-1"
    assert repository.embedding_failed_ids == ["entity-1"]
    assert all(not call[0].startswith("UPDATE memory_entities") for call in conn.execute_calls)


def test_database_maintenance_embed_ready_routes_entity_state_through_repository_seam():
    select_query = _normalize_sql(
        """
        SELECT id, name, content, archived, source_hash, embedding_version
        FROM memory_entities
        WHERE id = $1
        """
    )
    conn = _FakeConn(
        fetchrow_results={
            select_query: {
                "id": "entity-1",
                "name": "alpha",
                "content": "beta",
                "archived": False,
                "source_hash": "source-hash",
                "embedding_version": 3,
            }
        }
    )
    repository = _RecordingRepository()
    vector_store = _FakeVectorStore()
    processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
    processor._memory_graph_repository = repository
    processor._vector_store = vector_store
    processor._enqueue_intent = lambda **kwargs: None

    result = processor._process_embed_entity(
        MaintenanceIntent(
            intent_id=1,
            intent_kind="embed_entity",
            subject_kind="memory_entity",
            subject_id="entity-1",
            policy_key="memory_entity.embed",
            fingerprint="embed_entity:test",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=3,
        )
    )

    assert result["status"] == "completed"
    assert result["message"] == "embedded_entity:entity-1"
    assert vector_store.embedding_calls == [("memory_entities", "id", "entity-1", "alpha beta")]
    assert repository.embedding_ready_calls == [("entity-1", "all-MiniLM-L6-v2")]
    assert all(not call[0].startswith("UPDATE memory_entities") for call in conn.execute_calls)


def test_database_maintenance_refresh_neighbors_routes_touch_through_repository_seam():
    select_query = _normalize_sql(
        """
        SELECT id,
               name,
               content,
               embedding_version,
               archived
        FROM memory_entities
        WHERE id = $1
        """
    )
    conn = _FakeConn(
        fetchrow_results={
            select_query: {
                "id": "entity-1",
                "name": "alpha",
                "content": "beta",
                "embedding_version": 3,
                "archived": False,
            }
        }
    )
    repository = _RecordingRepository()
    processor = DatabaseMaintenanceProcessor(conn, embedder=None)
    processor._memory_graph_repository = repository
    processor._vector_store = _FakeVectorStore()

    result = processor._process_refresh_vector_neighbors(
        MaintenanceIntent(
            intent_id=1,
            intent_kind="refresh_vector_neighbors",
            subject_kind="memory_entity",
            subject_id="entity-1",
            policy_key="memory_entity.vector_neighbors",
            fingerprint="refresh_vector_neighbors:test",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=3,
        )
    )

    assert result["status"] == "completed"
    assert result["message"] == "vector_neighbors:entity-1:0"
    assert repository.vector_neighbor_calls == [
        (
            "entity-1",
            (),
            "memory_entity.vector_neighbors",
            3,
        )
    ]
    assert repository.maintenance_touch_ids == ["entity-1"]
    assert all(not call[0].startswith("UPDATE memory_entities") for call in conn.execute_calls)


def test_database_maintenance_archive_stale_routes_archival_through_repository_seam():
    select_query = _normalize_sql(
        """
        SELECT id
        FROM memory_entities
        WHERE archived = false
          AND updated_at < now() - make_interval(days => $1)
        """
    )
    conn = _FakeConn(
        execute_results={
            select_query: [{"id": "stale-1"}, {"id": "stale-2"}],
        }
    )
    repository = _RecordingRepository()
    processor = DatabaseMaintenanceProcessor(conn, embedder=None)
    processor._memory_graph_repository = repository
    processor._fetch_policy = lambda policy_key: {
        "policy_key": policy_key,
        "config": {"max_age_days": 45},
    }

    result = processor._process_archive_stale_entities(
        MaintenanceIntent(
            intent_id=1,
            intent_kind="archive_stale_entities",
            subject_kind="memory_entity",
            subject_id=None,
            policy_key="memory_entity.archive_stale",
            fingerprint="archive_stale_entities:test",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=3,
        )
    )

    assert result["status"] == "completed"
    assert result["message"] == "archived_stale:2"
    assert result["outcome"]["archived_ids"] == ["stale-1", "stale-2"]
    assert repository.archived_calls == [("stale-1", "stale-2")]
    assert all(not call[0].startswith("UPDATE memory_entities") for call in conn.execute_calls)


def test_database_maintenance_exact_duplicate_repair_routes_through_repository_seam():
    select_query = _normalize_sql(
        """
        SELECT entity_type,
               source_hash,
               array_agg(id ORDER BY created_at ASC, id ASC) AS entity_ids,
               COUNT(*) AS duplicate_total
        FROM memory_entities
        WHERE archived = false AND COALESCE(source_hash, '') <> ''
        GROUP BY entity_type, source_hash
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, MIN(created_at) ASC, entity_type ASC, source_hash ASC
        LIMIT $1
        """
    )
    conn = _FakeConn(
        execute_results={
            select_query: [
                {
                    "entity_type": "fact",
                    "source_hash": "dup-hash",
                    "entity_ids": ["entity-1", "entity-2"],
                    "duplicate_total": 2,
                }
            ],
        }
    )
    repository = _RecordingRepository()
    processor = DatabaseMaintenanceProcessor(conn, embedder=None)
    processor._memory_graph_repository = repository
    processor._fetch_policy = lambda policy_key: {
        "policy_key": policy_key,
        "config": {"group_limit": 5},
    }

    result = processor._process_archive_exact_duplicate_entities(
        MaintenanceIntent(
            intent_id=1,
            intent_kind="archive_exact_duplicate_entities",
            subject_kind="memory_entity",
            subject_id="policy:memory_entity.archive_exact_duplicates",
            policy_key="memory_entity.archive_exact_duplicates",
            fingerprint="archive_exact_duplicate_entities:test",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=3,
        )
    )

    assert result["status"] == "completed"
    assert result["message"] == "archived_exact_duplicates:1"
    assert repository.absorb_calls == [("entity-1", ("entity-2",))]
    assert all(not call[0].startswith("UPDATE memory_entities") for call in conn.execute_calls)


def test_graph_hygienist_preserves_timezone_aware_cutoff():
    hygienist = GraphHygienist(_FakeEngine(_FakeConn()))

    cutoff = hygienist._cutoff_iso()

    assert isinstance(cutoff, datetime)
    assert cutoff.tzinfo is timezone.utc
