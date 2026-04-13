"""Memory CRUD operations against Postgres (memory_entities, memory_edges)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from memory.types import Edge, Entity, EntityType, RelationType

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection

_TABLE = "memory_entities"
_EDGES = "memory_edges"


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
    conn.execute(
        f"INSERT INTO {_TABLE} "
        "(id, entity_type, name, content, metadata, source, confidence, archived, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, false, $8, $9) "
        "ON CONFLICT (id) DO UPDATE SET "
        "name = EXCLUDED.name, content = EXCLUDED.content, metadata = EXCLUDED.metadata, "
        "source = EXCLUDED.source, confidence = EXCLUDED.confidence, "
        "archived = false, updated_at = EXCLUDED.updated_at",
        entity.id, entity.entity_type.value, entity.name, entity.content,
        json.dumps(entity.metadata), entity.source, entity.confidence,
        entity.created_at, entity.updated_at,
    )
    return entity.id


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
    if not fields:
        return False
    allowed = {"name", "content", "metadata", "source", "confidence", "updated_at"}
    parts = []
    values = []
    idx = 1
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k == "metadata":
            v = json.dumps(v)
            parts.append(f"{k} = ${idx}::jsonb")
        else:
            parts.append(f"{k} = ${idx}")
        values.append(v)
        idx += 1
    if not parts:
        return False
    values.append(entity_id)
    values.append(entity_type.value)
    rows = conn.execute(
        f"UPDATE {_TABLE} SET {', '.join(parts)} "
        f"WHERE id = ${idx} AND entity_type = ${idx + 1} AND NOT archived "
        "RETURNING id",
        *values,
    )
    return len(rows) > 0


def delete_entity(
    conn: "SyncPostgresConnection", entity_id: str, entity_type: EntityType
) -> bool:
    rows = conn.execute(
        f"UPDATE {_TABLE} SET archived = true "
        "WHERE id = $1 AND entity_type = $2 AND NOT archived RETURNING id",
        entity_id, entity_type.value,
    )
    return len(rows) > 0


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
    try:
        conn.execute(
            f"INSERT INTO {_EDGES} "
            "(source_id, target_id, relation_type, weight, metadata, created_at) "
            "VALUES ($1, $2, $3, $4, $5::jsonb, $6) "
            "ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET "
            "weight = EXCLUDED.weight, metadata = EXCLUDED.metadata",
            edge.source_id, edge.target_id, edge.relation_type.value,
            edge.weight, json.dumps(edge.metadata), edge.created_at,
        )
        return True
    except Exception:
        return False


def remove_edge(
    conn: "SyncPostgresConnection",
    source_id: str,
    target_id: str,
    relation_type: RelationType,
) -> bool:
    rows = conn.execute(
        f"DELETE FROM {_EDGES} WHERE source_id = $1 AND target_id = $2 AND relation_type = $3 RETURNING source_id",
        source_id, target_id, relation_type.value,
    )
    return len(rows) > 0


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
