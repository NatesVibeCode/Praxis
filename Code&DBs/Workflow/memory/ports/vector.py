"""Port for vector similarity search."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True, slots=True)
class VectorFilter:
    """Equality or inequality filter for vector queries."""

    column: str
    value: Any
    operator: str = "="

    def normalized_operator(self) -> str:
        """Return the SQL-safe operator expected by PostgresVectorStore."""
        op = self.operator.strip().upper()
        if op == "!=":
            return "<>"
        return op


@runtime_checkable
class VectorStorePort(Protocol):
    """Searches and stores vector embeddings."""

    def search_text(
        self,
        table: str,
        text: str,
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None = None,
    ) -> list[dict[str, Any]]: ...

    def search_vector(
        self,
        table: str,
        vector: Sequence[float],
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None = None,
    ) -> list[dict[str, Any]]: ...
