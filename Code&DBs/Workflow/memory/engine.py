"""Memory graph engine backed by Postgres."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, List, Optional

from memory.crud import (
    add_edge,
    delete_entity,
    get_edges,
    get_entity,
    insert_entity,
    list_entities,
    remove_edge,
    search_entities,
    update_entity,
)
from memory.types import ChangeSet, Edge, EdgeAuthorityClass, Entity, EntityType, RelationType
if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection
    from runtime.embedding_service import EmbeddingService


class MemoryEngine:
    """Memory graph backed by Postgres.

    Accepts a SyncPostgresConnection (from ensure_postgres_available).
    All CRUD ops delegate to memory.crud which targets the memory_entities table.
    """

    def __init__(
        self,
        conn: "SyncPostgresConnection" = None,
        *,
        db_path: str | None = None,
        embedder: Optional["EmbeddingService"] = None,
        vector_store: object | None = None,
    ) -> None:
        if conn is None:
            legacy_hint = (
                f" (received legacy db_path={db_path!r}; SQLite is no longer supported)"
                if db_path
                else ""
            )
            raise ValueError(
                "MemoryEngine requires a SyncPostgresConnection — SQLite is no longer supported"
                f"{legacy_hint}"
            )
        self._conn = conn
        self._embedder = embedder
        if vector_store is not None:
            self._vector_store = vector_store
        elif embedder is not None:
            from storage.postgres.vector_store import PostgresVectorStore
            self._vector_store = PostgresVectorStore(conn, embedder)
        else:
            self._vector_store = None

    def _connect(self):
        """Return the shared Postgres connection (no per-call connections needed)."""
        return self._conn

    def _embed_and_update(self, entity_id: str, text: str) -> None:
        """Generate embedding for text and write it to the entity row."""
        vec = self._embedder.embed_one(text)
        if self._vector_store is None:
            raise RuntimeError("vector store unavailable for embedding write")
        self._vector_store.set_embedding(
            "memory_entities",
            "id",
            entity_id,
            embedding=vec,
        )

    def __enter__(self) -> "MemoryEngine":
        return self

    def __exit__(self, *exc) -> None:
        pass

    # --- Entity CRUD ---

    def insert(self, entity: Entity) -> str:
        insert_entity(self._conn, entity)
        if self._embedder:
            self._embed_and_update(entity.id, entity.name + ' ' + entity.content)
        return entity.id

    def get(self, entity_id: str, entity_type: EntityType) -> Optional[Entity]:
        return get_entity(self._conn, entity_id, entity_type)

    def update(self, entity_id: str, entity_type: EntityType, **fields) -> bool:
        result = update_entity(self._conn, entity_id, entity_type, **fields)
        if result and self._embedder and ('name' in fields or 'content' in fields):
            ent = get_entity(self._conn, entity_id, entity_type)
            if ent:
                self._embed_and_update(entity_id, ent.name + ' ' + ent.content)
        return result

    def delete(self, entity_id: str, entity_type: EntityType) -> bool:
        return delete_entity(self._conn, entity_id, entity_type)

    def list(
        self, entity_type: EntityType, limit: int = 100, offset: int = 0
    ) -> List[Entity]:
        return list_entities(self._conn, entity_type, limit, offset)

    # --- Search ---

    def search(
        self, query: str, entity_type: Optional[EntityType] = None, limit: int = 20
    ) -> List[Entity]:
        return search_entities(self._conn, query, entity_type, limit)

    # --- Edges ---

    def add_edge(self, edge: Edge) -> bool:
        return add_edge(self._conn, edge)

    def remove_edge(
        self, source_id: str, target_id: str, relation_type: RelationType
    ) -> bool:
        return remove_edge(self._conn, source_id, target_id, relation_type)

    def get_edges(
        self,
        entity_id: str,
        direction: str = "outgoing",
        authority_classes: Sequence[EdgeAuthorityClass] | None = None,
    ) -> List[Edge]:
        return get_edges(
            self._conn,
            entity_id,
            direction,
            authority_classes=authority_classes,
        )

    # --- Graph traversal ---

    def neighbors(
        self,
        entity_id: str,
        entity_type: EntityType,
        relation_types: Optional[List[RelationType]] = None,
        depth: int = 1,
        authority_classes: Sequence[EdgeAuthorityClass] | None = None,
    ) -> List[Entity]:
        visited: set = {entity_id}
        frontier: set = {entity_id}

        for _ in range(depth):
            next_frontier: set = set()
            for nid in frontier:
                edges = get_edges(
                    self._conn,
                    nid,
                    direction="both",
                    authority_classes=authority_classes,
                )
                for edge in edges:
                    if relation_types and edge.relation_type not in relation_types:
                        continue
                    peer = edge.target_id if edge.source_id == nid else edge.source_id
                    if peer not in visited:
                        visited.add(peer)
                        next_frontier.add(peer)
            frontier = next_frontier

        result: list = []
        for nid in visited - {entity_id}:
            for et in EntityType:
                ent = get_entity(self._conn, nid, et)
                if ent is not None:
                    result.append(ent)
                    break

        return result

    # --- Batch ---

    def apply_changeset(self, changeset: ChangeSet) -> None:
        all_upserts = list(changeset.inserts) + list(changeset.updates)
        for entity in changeset.inserts:
            insert_entity(self._conn, entity)
        for entity in changeset.updates:
            insert_entity(self._conn, entity)
        if self._embedder and all_upserts:
            texts = [e.name + ' ' + e.content for e in all_upserts]
            vecs = self._embedder.embed(texts)
            for entity, vec in zip(all_upserts, vecs):
                if self._vector_store is None:
                    raise RuntimeError("vector store unavailable for batch embedding write")
                self._vector_store.set_embedding(
                    "memory_entities",
                    "id",
                    entity.id,
                    embedding=vec,
                )
        for entity_id in changeset.deletes:
            for et in EntityType:
                delete_entity(self._conn, entity_id, et)
        for edge in changeset.edges_add:
            add_edge(self._conn, edge)
        for src, tgt, rel in changeset.edges_remove:
            remove_edge(self._conn, src, tgt, rel)
