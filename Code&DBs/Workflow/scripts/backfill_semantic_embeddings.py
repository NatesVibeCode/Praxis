"""One-shot backfill: embed every row in the semantic tables that is still NULL.

Targets the tables whose vector(384) columns exist but whose rows are
un-embedded. Runs the canonical EmbeddingService (same model everything else
uses) and writes via pgvector literals.

Run with:
    PYTHONPATH='Code&DBs/Workflow' python3 \\
        'Code&DBs/Workflow/scripts/backfill_semantic_embeddings.py'
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Any

import asyncpg

_WORKFLOW_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _WORKFLOW_ROOT not in sys.path:
    sys.path.insert(0, os.path.abspath(_WORKFLOW_ROOT))

from runtime._workflow_database import resolve_runtime_database_url
from runtime.embedding_service import EmbeddingService
from storage.postgres.vector_store import format_vector_literal


@dataclass(frozen=True, slots=True)
class Target:
    table: str
    key_column: str
    select_sql: str
    compose_text: Any

    def text_for(self, row: dict[str, Any]) -> str:
        return self.compose_text(row)


def _clip(text: str, limit: int = 8000) -> str:
    return (text or "").strip()[:limit]


def _join(*parts: Any) -> str:
    return " ".join(str(p).strip() for p in parts if p).strip()


TARGETS: tuple[Target, ...] = (
    Target(
        table="bugs",
        key_column="bug_id",
        select_sql=(
            "SELECT bug_id, title, description FROM bugs "
            "WHERE embedding IS NULL"
        ),
        compose_text=lambda r: _clip(_join(r["title"], r["description"])),
    ),
    Target(
        table="roadmap_items",
        key_column="roadmap_item_id",
        select_sql=(
            "SELECT roadmap_item_id, title, summary FROM roadmap_items "
            "WHERE embedding IS NULL"
        ),
        compose_text=lambda r: _clip(_join(r["title"], r["summary"])),
    ),
    Target(
        table="operator_decisions",
        key_column="operator_decision_id",
        select_sql=(
            "SELECT operator_decision_id, title, rationale "
            "FROM operator_decisions WHERE embedding IS NULL"
        ),
        compose_text=lambda r: _clip(_join(r["title"], r["rationale"])),
    ),
    Target(
        table="registry_workflows",
        key_column="id",
        select_sql=(
            "SELECT id, name, description FROM registry_workflows "
            "WHERE embedding IS NULL"
        ),
        compose_text=lambda r: _clip(_join(r["name"], r["description"])),
    ),
    Target(
        table="registry_ui_components",
        key_column="id",
        select_sql=(
            "SELECT id, name, description FROM registry_ui_components "
            "WHERE embedding IS NULL"
        ),
        compose_text=lambda r: _clip(_join(r["name"], r["description"])),
    ),
    Target(
        table="registry_calculations",
        key_column="id",
        select_sql=(
            "SELECT id, name, description FROM registry_calculations "
            "WHERE embedding IS NULL"
        ),
        compose_text=lambda r: _clip(_join(r["name"], r["description"])),
    ),
)


async def _backfill_target(
    conn: asyncpg.Connection,
    target: Target,
    embedder: EmbeddingService,
    batch_size: int = 32,
) -> tuple[int, int]:
    rows = await conn.fetch(target.select_sql)
    if not rows:
        return (0, 0)

    total = 0
    skipped = 0
    batch: list[tuple[str, str]] = []

    async def flush(pairs: list[tuple[str, str]]) -> None:
        nonlocal total
        if not pairs:
            return
        vectors = embedder.embed([text for _, text in pairs])
        for (key, _), vec in zip(pairs, vectors):
            literal = format_vector_literal(vec)
            await conn.execute(
                f"UPDATE {target.table} SET embedding = $1::vector "
                f"WHERE {target.key_column} = $2",
                literal,
                key,
            )
            total += 1

    for row in rows:
        text = target.text_for(dict(row))
        if not text:
            skipped += 1
            continue
        batch.append((row[target.key_column], text))
        if len(batch) >= batch_size:
            await flush(batch)
            batch = []
    await flush(batch)
    return (total, skipped)


async def main() -> int:
    dsn = resolve_runtime_database_url(required=True)
    if dsn is None:
        print("WORKFLOW_DATABASE_URL is not configured", file=sys.stderr)
        return 2

    embedder = EmbeddingService()
    conn = await asyncpg.connect(dsn)
    try:
        summary: list[tuple[str, int, int]] = []
        for target in TARGETS:
            embedded, skipped = await _backfill_target(conn, target, embedder)
            summary.append((target.table, embedded, skipped))
            print(f"{target.table:24s}  embedded={embedded:5d}  skipped={skipped}")
    finally:
        await conn.close()

    print()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
