"""Adapter wrapping storage.postgres.vector_store.PostgresVectorStore as VectorStorePort."""
from __future__ import annotations

from typing import Any, Sequence

from memory.ports.vector import VectorFilter


def _convert_filters(
    filters: Sequence[VectorFilter] | None,
) -> list[Any] | None:
    """Convert port VectorFilter instances to storage VectorFilter instances."""
    if not filters:
        return None
    from storage.postgres.vector_store import VectorFilter as PgVectorFilter

    return [PgVectorFilter(column=f.column, value=f.value, operator=f.operator) for f in filters]


class PostgresVectorAdapter:
    """Adapts PostgresVectorStore to the VectorStorePort protocol."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def search_text(
        self,
        table: str,
        text: str,
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None = None,
    ) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "select_columns": select_columns,
            "filters": _convert_filters(filters),
            "limit": limit,
        }
        if min_similarity is not None:
            kwargs["min_similarity"] = min_similarity
        return self._store.search_text(table, text, **kwargs)

    def search_vector(
        self,
        table: str,
        vector: Sequence[float],
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None = None,
    ) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "select_columns": select_columns,
            "filters": _convert_filters(filters),
            "limit": limit,
        }
        if min_similarity is not None:
            kwargs["min_similarity"] = min_similarity
        return self._store.search_vector(table, vector, **kwargs)
