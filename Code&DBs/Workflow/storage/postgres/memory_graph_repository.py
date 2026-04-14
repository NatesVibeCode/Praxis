"""Explicit sync Postgres repository for canonical memory-graph mutations."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from memory.types import Edge, Entity, EntityType, RelationType

from memory.repository import MemoryEdgeRef

from .validators import _encode_jsonb, _require_mapping, _require_text, _require_utc


def _normalize_entity_ids(entity_ids: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for index, entity_id in enumerate(entity_ids):
        text = _require_text(entity_id, field_name=f"entity_ids[{index}]")
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _normalize_edges(edges: Sequence[MemoryEdgeRef]) -> tuple[MemoryEdgeRef, ...]:
    normalized: list[MemoryEdgeRef] = []
    seen: set[tuple[str, str, str]] = set()
    for index, edge in enumerate(edges):
        source_id = _require_text(edge.source_id, field_name=f"edges[{index}].source_id")
        target_id = _require_text(edge.target_id, field_name=f"edges[{index}].target_id")
        relation_type = _require_text(
            edge.relation_type,
            field_name=f"edges[{index}].relation_type",
        )
        key = (source_id, target_id, relation_type)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            MemoryEdgeRef(
                source_id=source_id,
                target_id=target_id,
                relation_type=relation_type,
            )
        )
    return tuple(normalized)


def _normalize_entity_field_updates(fields: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    normalized_fields = _require_mapping(fields, field_name="fields")
    allowed = {"name", "content", "metadata", "source", "confidence", "updated_at"}
    updates: list[tuple[str, object]] = []
    for key, value in normalized_fields.items():
        if key not in allowed:
            continue
        if key == "metadata":
            value = _encode_jsonb(value, field_name="fields.metadata")
        elif key == "updated_at":
            value = _require_utc(value, field_name="fields.updated_at")
        updates.append((key, value))
    return tuple(updates)


class PostgresMemoryGraphRepository:
    """Owns canonical memory-graph mutations for maintenance flows."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def upsert_entity(self, *, entity: Entity) -> str:
        normalized_id = _require_text(entity.id, field_name="entity.id")
        self._conn.execute(
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
            """,
            normalized_id,
            entity.entity_type.value,
            entity.name,
            entity.content,
            _encode_jsonb(entity.metadata, field_name="entity.metadata"),
            entity.source,
            entity.confidence,
            _require_utc(entity.created_at, field_name="entity.created_at"),
            _require_utc(entity.updated_at, field_name="entity.updated_at"),
        )
        return normalized_id

    def update_entity_fields(
        self,
        *,
        entity_id: str,
        entity_type: EntityType,
        fields: Mapping[str, object],
    ) -> bool:
        normalized_entity_id = _require_text(entity_id, field_name="entity_id")
        assignments = _normalize_entity_field_updates(fields)
        if not assignments:
            return False
        parts: list[str] = []
        values: list[object] = []
        for index, (column, value) in enumerate(assignments, start=1):
            if column == "metadata":
                parts.append(f"{column} = ${index}::jsonb")
            else:
                parts.append(f"{column} = ${index}")
            values.append(value)
        values.extend((normalized_entity_id, entity_type.value))
        rows = self._conn.execute(
            f"""
            UPDATE memory_entities
            SET {', '.join(parts)}
            WHERE id = ${len(values) - 1}
              AND entity_type = ${len(values)}
              AND NOT archived
            RETURNING id
            """,
            *values,
        )
        return bool(rows)

    def archive_entity(self, *, entity_id: str, entity_type: EntityType) -> bool:
        normalized_entity_id = _require_text(entity_id, field_name="entity_id")
        rows = self._conn.execute(
            """
            UPDATE memory_entities
            SET archived = true
            WHERE id = $1
              AND entity_type = $2
              AND NOT archived
            RETURNING id
            """,
            normalized_entity_id,
            entity_type.value,
        )
        return bool(rows)

    def archive_entities(self, *, entity_ids: Sequence[str]) -> tuple[str, ...]:
        normalized_ids = _normalize_entity_ids(entity_ids)
        if not normalized_ids:
            return ()
        rows = self._conn.execute(
            """
            UPDATE memory_entities
            SET archived = true,
                needs_reembed = false,
                embedding_status = 'archived',
                last_maintained_at = now()
            WHERE id = ANY($1::text[])
              AND archived = false
            RETURNING id
            """,
            list(normalized_ids),
        )
        return tuple(str(row["id"]) for row in rows)

    def mark_entity_embedding_failed(self, *, entity_id: str) -> bool:
        normalized_entity_id = _require_text(entity_id, field_name="entity_id")
        rows = self._conn.execute(
            """
            UPDATE memory_entities
            SET embedding = NULL,
                needs_reembed = false,
                embedding_status = 'failed',
                last_maintained_at = now()
            WHERE id = $1
            RETURNING id
            """,
            normalized_entity_id,
        )
        return bool(rows)

    def mark_entity_embedding_ready(self, *, entity_id: str, embedding_model: str) -> bool:
        normalized_entity_id = _require_text(entity_id, field_name="entity_id")
        normalized_embedding_model = _require_text(
            embedding_model,
            field_name="embedding_model",
        )
        rows = self._conn.execute(
            """
            UPDATE memory_entities
            SET needs_reembed = false,
                embedding_status = 'ready',
                embedding_model = $1,
                embedded_at = now(),
                last_maintained_at = now()
            WHERE id = $2
            RETURNING id
            """,
            normalized_embedding_model,
            normalized_entity_id,
        )
        return bool(rows)

    def touch_entity_maintenance(self, *, entity_id: str) -> bool:
        normalized_entity_id = _require_text(entity_id, field_name="entity_id")
        rows = self._conn.execute(
            """
            UPDATE memory_entities
            SET last_maintained_at = now()
            WHERE id = $1
            RETURNING id
            """,
            normalized_entity_id,
        )
        return bool(rows)

    def absorb_exact_duplicate_entities(
        self,
        *,
        canonical_entity_id: str,
        duplicate_entity_ids: Sequence[str],
    ) -> dict[str, object]:
        normalized_canonical_id = _require_text(
            canonical_entity_id,
            field_name="canonical_entity_id",
        )
        normalized_duplicate_ids = tuple(
            entity_id
            for entity_id in _normalize_entity_ids(duplicate_entity_ids)
            if entity_id != normalized_canonical_id
        )
        if not normalized_duplicate_ids:
            return {
                "canonical_entity_id": normalized_canonical_id,
                "archived_ids": [],
                "archived_count": 0,
                "rehomed_edge_rows": 0,
                "deleted_edge_rows": 0,
                "deleted_inferred_rows": 0,
                "deleted_neighbor_rows": 0,
                "deleted_pending_intents": 0,
            }
        row = self._conn.fetchrow(
            """
            SELECT absorb_exact_duplicate_memory_entities(
                $1,
                $2::text[]
            ) AS outcome
            """,
            normalized_canonical_id,
            list(normalized_duplicate_ids),
        )
        if row is None:
            return {}
        outcome = row.get("outcome")
        if isinstance(outcome, dict):
            return dict(outcome)
        if isinstance(outcome, str):
            try:
                parsed = json.loads(outcome)
            except json.JSONDecodeError:
                return {}
            if isinstance(parsed, dict):
                return dict(parsed)
        return {}

    def upsert_edge(self, *, edge: Edge) -> bool:
        try:
            self._conn.execute(
                """
                INSERT INTO memory_edges
                (
                    source_id,
                    target_id,
                    relation_type,
                    weight,
                    metadata,
                    created_at
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET
                    weight = EXCLUDED.weight,
                    metadata = EXCLUDED.metadata
                """,
                _require_text(edge.source_id, field_name="edge.source_id"),
                _require_text(edge.target_id, field_name="edge.target_id"),
                edge.relation_type.value,
                edge.weight,
                _encode_jsonb(edge.metadata, field_name="edge.metadata"),
                _require_utc(edge.created_at, field_name="edge.created_at"),
            )
        except Exception:
            return False
        return True

    def delete_edge(
        self,
        *,
        source_id: str,
        target_id: str,
        relation_type: RelationType,
    ) -> bool:
        rows = self._conn.execute(
            """
            DELETE FROM memory_edges
            WHERE source_id = $1
              AND target_id = $2
              AND relation_type = $3
            RETURNING source_id
            """,
            _require_text(source_id, field_name="source_id"),
            _require_text(target_id, field_name="target_id"),
            relation_type.value,
        )
        return bool(rows)

    def delete_edges(self, *, edges: Sequence[MemoryEdgeRef]) -> tuple[MemoryEdgeRef, ...]:
        normalized_edges = _normalize_edges(edges)
        if not normalized_edges:
            return ()

        rows = self._conn.execute(
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
            """,
            [edge.source_id for edge in normalized_edges],
            [edge.target_id for edge in normalized_edges],
            [edge.relation_type for edge in normalized_edges],
        )
        return tuple(
            MemoryEdgeRef(
                source_id=str(row["source_id"]),
                target_id=str(row["target_id"]),
                relation_type=str(row["relation_type"]),
            )
            for row in rows
        )
