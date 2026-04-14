"""Memory CRUD operations against Postgres (memory_entities, memory_edges)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from storage.postgres.memory_graph_repository import PostgresMemoryGraphRepository

from memory.types import Edge, Entity, EntityType, RelationType

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection

_TABLE = "memory_entities"
_EDGES = "memory_edges"


def _graph_repository(conn: "SyncPostgresConnection") -> PostgresMemoryGraphRepository:
    return PostgresMemoryGraphRepository(conn)


def _row_to_entity(row, entity_type: EntityType | None = None) -> Entity:
    et = entity_type or EntityType(row["entity_type"])
    metadata = row["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    elif metadata is None:
        metadata = {}
    created = row["created_at"]
    updated = row["updated_at"]
    return Entity(
        id=row["id"],
        entity_type=et,
        name=row["name"],
        content=row["content"],
        metadata=metadata if isinstance(metadata, dict) else dict(metadata),
        created_at=created if isinstance(created, datetime) else datetime.fromisoformat(created),
        updated_at=updated if isinstance(updated, datetime) else datetime.fromisoformat(updated),
        source=row["source"],
        confidence=row["confidence"],
    )


def _row_to_edge(row) -> Edge:
    metadata = row["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    elif metadata is None:
        metadata = {}
    created = row["created_at"]
    return Edge(
        source_id=row["source_id"],
        target_id=row["target_id"],
        relation_type=RelationType(row["relation_type"]),
        weight=row["weight"],
        metadata=metadata if isinstance(metadata, dict) else dict(metadata),
        created_at=created if isinstance(created, datetime) else datetime.fromisoformat(created),
    )


def insert_entity(conn: "SyncPostgresConnection", entity: Entity) -> str:
    return _graph_repository(conn).upsert_entity(entity=entity)


def get_entity(
    conn: "SyncPostgresConnection", entity_id: str, entity_type: EntityType
) -> Entity | None:
    row = conn.fetchrow(
        f"SELECT * FROM {_TABLE} WHERE id = $1 AND entity_type = $2",
        entity_id, entity_type.value,
    )
    if row is None:
        return None
    return _row_to_entity(row, entity_type)


def update_entity(
    conn: "SyncPostgresConnection",
    entity_id: str,
    entity_type: EntityType,
    **fields,
) -> bool:
    return _graph_repository(conn).update_entity_fields(
        entity_id=entity_id,
        entity_type=entity_type,
        fields=fields,
    )


def delete_entity(
    conn: "SyncPostgresConnection", entity_id: str, entity_type: EntityType
) -> bool:
    return _graph_repository(conn).archive_entity(
        entity_id=entity_id,
        entity_type=entity_type,
    )


def list_entities(
    conn: "SyncPostgresConnection",
    entity_type: EntityType,
    limit: int = 100,
    offset: int = 0,
) -> list[Entity]:
    rows = conn.execute(
        f"SELECT * FROM {_TABLE} WHERE entity_type = $1 AND NOT archived "
        "ORDER BY created_at DESC LIMIT $2 OFFSET $3",
        entity_type.value, limit, offset,
    )
    return [_row_to_entity(r, entity_type) for r in rows]


def search_entities(
    conn: "SyncPostgresConnection",
    query: str,
    entity_type: EntityType | None = None,
    limit: int = 20,
) -> list[Entity]:
    if entity_type:
        rows = conn.execute(
            f"SELECT * FROM {_TABLE} "
            "WHERE search_vector @@ plainto_tsquery('english', $1) "
            "AND entity_type = $2 AND NOT archived "
            "LIMIT $3",
            query, entity_type.value, limit,
        )
    else:
        rows = conn.execute(
            f"SELECT * FROM {_TABLE} "
            "WHERE search_vector @@ plainto_tsquery('english', $1) "
            "AND NOT archived "
            "LIMIT $2",
            query, limit,
        )
    return [_row_to_entity(r) for r in rows]


def add_edge(conn: "SyncPostgresConnection", edge: Edge) -> bool:
    return _graph_repository(conn).upsert_edge(edge=edge)


def remove_edge(
    conn: "SyncPostgresConnection",
    source_id: str,
    target_id: str,
    relation_type: RelationType,
) -> bool:
    return _graph_repository(conn).delete_edge(
        source_id=source_id,
        target_id=target_id,
        relation_type=relation_type,
    )


def get_edges(
    conn: "SyncPostgresConnection", entity_id: str, direction: str = "outgoing"
) -> list[Edge]:
    if direction == "outgoing":
        rows = conn.execute(
            f"SELECT * FROM {_EDGES} WHERE source_id = $1", entity_id,
        )
    elif direction == "incoming":
        rows = conn.execute(
            f"SELECT * FROM {_EDGES} WHERE target_id = $1", entity_id,
        )
    else:
        rows = conn.execute(
            f"SELECT * FROM {_EDGES} WHERE source_id = $1 OR target_id = $1",
            entity_id,
        )
    return [_row_to_edge(r) for r in rows]
