"""Sync Postgres repository for data dictionary schema snapshots."""
from __future__ import annotations

import json
from typing import Any, Iterable


def insert_snapshot(
    conn: Any,
    *,
    fingerprint: str,
    object_count: int,
    field_count: int,
    triggered_by: str = "heartbeat",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_schema_snapshots
            (fingerprint, object_count, field_count, triggered_by, metadata)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        RETURNING snapshot_id::text, taken_at, fingerprint,
                  object_count, field_count, triggered_by, metadata
        """,
        fingerprint, object_count, field_count, triggered_by,
        json.dumps(metadata or {}),
    )
    return dict(row)


def insert_snapshot_fields(
    conn: Any,
    *,
    snapshot_id: str,
    fields: Iterable[dict[str, Any]],
) -> int:
    rows = list(fields)
    if not rows:
        return 0
    object_kinds = [r["object_kind"] for r in rows]
    field_paths = [r.get("field_path") or "" for r in rows]
    field_kinds = [r.get("field_kind") or "text" for r in rows]
    requireds = [bool(r.get("required", False)) for r in rows]
    sources_lists = [list(r.get("sources") or []) for r in rows]
    conn.execute(
        """
        INSERT INTO data_dictionary_schema_snapshot_fields
            (snapshot_id, object_kind, field_path, field_kind, required, sources)
        SELECT $1::uuid, ok, fp, fk, req, src
        FROM UNNEST($2::text[], $3::text[], $4::text[], $5::boolean[], $6::text[][])
            AS t(ok, fp, fk, req, src)
        ON CONFLICT (snapshot_id, object_kind, field_path) DO NOTHING
        """,
        snapshot_id,
        object_kinds, field_paths, field_kinds, requireds, sources_lists,
    )
    return len(rows)


def fetch_latest_snapshot(conn: Any) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT snapshot_id::text, taken_at, fingerprint,
               object_count, field_count, triggered_by, metadata
        FROM data_dictionary_schema_snapshots
        ORDER BY taken_at DESC
        LIMIT 1
        """
    )
    return dict(row) if row else None


def fetch_snapshot_before(
    conn: Any, *, taken_at: Any,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT snapshot_id::text, taken_at, fingerprint,
               object_count, field_count, triggered_by, metadata
        FROM data_dictionary_schema_snapshots
        WHERE taken_at < $1
        ORDER BY taken_at DESC
        LIMIT 1
        """,
        taken_at,
    )
    return dict(row) if row else None


def fetch_snapshot_by_id(conn: Any, snapshot_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT snapshot_id::text, taken_at, fingerprint,
               object_count, field_count, triggered_by, metadata
        FROM data_dictionary_schema_snapshots
        WHERE snapshot_id = $1::uuid
        """,
        snapshot_id,
    )
    return dict(row) if row else None


def list_snapshots(conn: Any, *, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT snapshot_id::text, taken_at, fingerprint,
               object_count, field_count, triggered_by
        FROM data_dictionary_schema_snapshots
        ORDER BY taken_at DESC
        LIMIT $1
        """,
        max(1, min(500, int(limit or 50))),
    )
    return [dict(r) for r in rows]


def fetch_snapshot_fields(
    conn: Any, *, snapshot_id: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT object_kind, field_path, field_kind, required, sources
        FROM data_dictionary_schema_snapshot_fields
        WHERE snapshot_id = $1::uuid
        ORDER BY object_kind, field_path
        """,
        snapshot_id,
    )
    return [dict(r) for r in rows]


def prune_snapshots_older_than(conn: Any, *, days: int) -> int:
    rows = conn.execute(
        """
        DELETE FROM data_dictionary_schema_snapshots
        WHERE taken_at < (now() - ($1 || ' days')::interval)
        RETURNING snapshot_id
        """,
        str(int(days)),
    )
    return len(list(rows))


__all__ = [
    "fetch_latest_snapshot",
    "fetch_snapshot_before",
    "fetch_snapshot_by_id",
    "fetch_snapshot_fields",
    "insert_snapshot",
    "insert_snapshot_fields",
    "list_snapshots",
    "prune_snapshots_older_than",
]
