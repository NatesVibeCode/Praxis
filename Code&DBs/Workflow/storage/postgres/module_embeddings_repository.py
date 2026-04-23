"""Dedicated Postgres authority for module embedding index rows."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .validators import _require_text


class PostgresModuleEmbeddingsRepository:
    """Owns all persistence operations for ``module_embeddings``."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def fetch_module_ids_for_path(
        self,
        *,
        module_path: str,
        module_ids: Sequence[str],
    ) -> list[str]:
        rows = self._conn.execute(
            "SELECT module_id FROM module_embeddings "
            "WHERE module_path = $1 AND module_id <> ALL($2::text[])",
            _require_text(module_path, field_name="module_path"),
            list(module_ids),
        )
        return [str(row["module_id"]) for row in rows or []]

    def delete_module_id(self, *, module_id: str) -> None:
        self._conn.execute(
            "DELETE FROM module_embeddings WHERE module_id = $1",
            _require_text(module_id, field_name="module_id"),
        )

    def fetch_module_paths_like(self, *, like_pattern: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT DISTINCT module_path FROM module_embeddings WHERE module_path LIKE $1",
            _require_text(like_pattern, field_name="like_pattern"),
        )
        return [str(row["module_path"]) for row in rows or []]

    def delete_module_path(self, *, module_path: str) -> None:
        self._conn.execute(
            "DELETE FROM module_embeddings WHERE module_path = $1",
            _require_text(module_path, field_name="module_path"),
        )

    def fetch_source_hashes(self) -> dict[str, str]:
        rows = self._conn.execute("SELECT module_id, source_hash FROM module_embeddings")
        return {str(row["module_id"]): str(row["source_hash"]) for row in rows or []}

    def upsert_embedding(
        self,
        *,
        module_id: str,
        module_path: str,
        kind: str,
        name: str,
        docstring: str,
        signature: str,
        behavior_json: str,
        summary: str,
        source_hash: str,
        embedding_literal: str,
    ) -> None:
        self._conn.execute(
            """INSERT INTO module_embeddings
               (module_id, module_path, kind, name, docstring, signature,
                behavior, summary, source_hash, embedding, indexed_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10::vector, NOW())
               ON CONFLICT (module_id) DO UPDATE SET
                   module_path = EXCLUDED.module_path,
                   kind = EXCLUDED.kind,
                   name = EXCLUDED.name,
                   docstring = EXCLUDED.docstring,
                   signature = EXCLUDED.signature,
                   behavior = EXCLUDED.behavior,
                   summary = EXCLUDED.summary,
                   source_hash = EXCLUDED.source_hash,
                   embedding = EXCLUDED.embedding,
                   indexed_at = NOW()
            """,
            _require_text(module_id, field_name="module_id"),
            _require_text(module_path, field_name="module_path"),
            _require_text(kind, field_name="kind"),
            _require_text(name, field_name="name"),
            docstring,
            signature,
            _require_text(behavior_json, field_name="behavior_json"),
            summary,
            _require_text(source_hash, field_name="source_hash"),
            _require_text(embedding_literal, field_name="embedding_literal"),
        )

    def fetch_total(self) -> int:
        row = self._conn.fetchval("SELECT COUNT(*) FROM module_embeddings")
        return int(row or 0)

    def fetch_counts_by_kind(self) -> list[tuple[str, int]]:
        rows = self._conn.execute(
            "SELECT kind, COUNT(*) as cnt FROM module_embeddings GROUP BY kind ORDER BY cnt DESC"
        )
        return [(str(r["kind"]), int(r["cnt"])) for r in rows or []]

    def fetch_path_hash_rows(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT module_path, MIN(source_hash) AS source_hash "
            "FROM module_embeddings GROUP BY module_path"
        )
        return [dict(r) for r in rows or []]


__all__ = ["PostgresModuleEmbeddingsRepository"]
