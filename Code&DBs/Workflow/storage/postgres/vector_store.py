"""Postgres vector-store adapter.

This module owns the pgvector-specific SQL, literal formatting, and
similarity thresholds so runtime callers can work in semantic terms.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .connection import SyncPostgresConnection


_DEFAULT_MIN_SIMILARITY = 0.3
_USE_DEFAULT_MIN_SIMILARITY = object()
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_OPERATORS = {"=", "!=", "<>", "<", "<=", ">", ">="}


def _require_identifier(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty identifier")
    normalized = value.strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"invalid {label}: {value!r}")
    return normalized


def decode_vector_value(raw: Any) -> tuple[float, ...]:
    """Normalize a pgvector value or sequence into a float tuple."""
    if raw is None:
        return ()
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ()
        if text[0] in "[{" and text[-1] in "]}":
            text = text[1:-1]
        if not text:
            return ()
        return tuple(float(part) for part in text.split(",") if part.strip())
    if isinstance(raw, Sequence):
        return tuple(float(part) for part in raw)
    if isinstance(raw, Iterable):
        return tuple(float(part) for part in raw)
    raise TypeError(f"unsupported vector value type: {type(raw).__name__}")


def cosine_similarity(left: Any, right: Any) -> float:
    """Compute cosine similarity in Python for already-fetched vectors."""
    left_vec = decode_vector_value(left)
    right_vec = decode_vector_value(right)
    if not left_vec or not right_vec or len(left_vec) != len(right_vec):
        return 0.0

    dot = sum(a * b for a, b in zip(left_vec, right_vec))
    left_norm = sum(a * a for a in left_vec) ** 0.5
    right_norm = sum(b * b for b in right_vec) ** 0.5
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


def format_vector_literal(vector: Sequence[float]) -> str:
    """Format a vector literal for pgvector writes and queries."""
    return "[" + ",".join(f"{float(value):.6f}" for value in vector) + "]"


@dataclass(frozen=True, slots=True)
class VectorFilter:
    """Simple equality or inequality filter for vector-store queries."""

    column: str
    value: Any
    operator: str = "="

    def normalized_operator(self) -> str:
        op = self.operator.strip().upper()
        if op == "!=":
            op = "<>"
        if op not in _ALLOWED_OPERATORS:
            raise ValueError(f"unsupported vector filter operator: {self.operator!r}")
        return op


@dataclass(frozen=True, slots=True)
class PreparedVectorQuery:
    """Prepared embedding query that can be reused across multiple tables."""

    store: "PostgresVectorStore"
    text: str
    vector: tuple[float, ...]

    def search(
        self,
        table: str,
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None | object = _USE_DEFAULT_MIN_SIMILARITY,
        score_alias: str = "similarity",
        embedding_column: str = "embedding",
    ) -> list[dict[str, Any]]:
        return self.store.search_vector(
            table,
            self.vector,
            select_columns=select_columns,
            filters=filters,
            limit=limit,
            min_similarity=min_similarity,
            score_alias=score_alias,
            embedding_column=embedding_column,
        )

    def set_embedding(
        self,
        table: str,
        key_column: str,
        key_value: Any,
        *,
        embedding_column: str = "embedding",
    ) -> None:
        self.store.set_embedding(
            table,
            key_column,
            key_value,
            embedding=self.vector,
            embedding_column=embedding_column,
        )


class PostgresVectorStore:
    """Adapter that hides pgvector SQL from runtime callers."""

    def __init__(
        self,
        conn: "SyncPostgresConnection",
        embedder: Any | None = None,
        *,
        default_min_similarity: float = _DEFAULT_MIN_SIMILARITY,
    ) -> None:
        self._conn = conn
        self._embedder = embedder
        self._default_min_similarity = default_min_similarity
        self._authority = getattr(embedder, "authority", None)
        self._validate_embedder_contract()

    def prepare(self, text: str) -> PreparedVectorQuery:
        if self._embedder is None:
            raise RuntimeError("vector_store.embedder_required")
        vector = self._encode_text(text)
        return PreparedVectorQuery(store=self, text=text, vector=vector)

    def search_text(
        self,
        table: str,
        text: str,
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None | object = _USE_DEFAULT_MIN_SIMILARITY,
        score_alias: str = "similarity",
        embedding_column: str = "embedding",
    ) -> list[dict[str, Any]]:
        return self.prepare(text).search(
            table,
            select_columns=select_columns,
            filters=filters,
            limit=limit,
            min_similarity=min_similarity,
            score_alias=score_alias,
            embedding_column=embedding_column,
        )

    def search_vector(
        self,
        table: str,
        vector: Sequence[float] | str,
        *,
        select_columns: Sequence[str] | None = None,
        filters: Sequence[VectorFilter] | None = None,
        limit: int = 20,
        min_similarity: float | None | object = _USE_DEFAULT_MIN_SIMILARITY,
        score_alias: str = "similarity",
        embedding_column: str = "embedding",
    ) -> list[dict[str, Any]]:
        table_name = _require_identifier(table, label="table")
        embedding_name = _require_identifier(
            embedding_column, label="embedding column"
        )
        score_name = _require_identifier(score_alias, label="score alias")

        vector_value = decode_vector_value(vector)
        self._validate_vector(vector_value)
        vector_literal = format_vector_literal(vector_value)

        if select_columns:
            select_sql = self._select_sql(select_columns)
        else:
            select_sql = "*"

        clauses = [f"{embedding_name} IS NOT NULL"]
        params: list[Any] = [vector_literal]

        if min_similarity is _USE_DEFAULT_MIN_SIMILARITY:
            min_similarity = self._default_min_similarity

        if min_similarity is not None:
            clauses.append(f"1 - ({embedding_name} <=> $1::vector) >= $2")
            params.append(min_similarity)
            next_index = 3
        else:
            next_index = 2

        if filters:
            for vector_filter in filters:
                column_name = _require_identifier(
                    vector_filter.column, label="filter column"
                )
                op = vector_filter.normalized_operator()
                clauses.append(f"{column_name} {op} ${next_index}")
                params.append(vector_filter.value)
                next_index += 1

        clauses_sql = " AND ".join(clauses)
        order_sql = f"ORDER BY {embedding_name} <=> $1::vector"
        limit_placeholder = f"${next_index}"
        params.append(limit)

        sql = (
            f"SELECT {select_sql}, 1 - ({embedding_name} <=> $1::vector) AS {score_name} "
            f"FROM {table_name} "
            f"WHERE {clauses_sql} "
            f"{order_sql} "
            f"LIMIT {limit_placeholder}"
        )

        rows = self._conn.execute(sql, *params)
        return [dict(row) for row in rows]

    def set_embedding(
        self,
        table: str,
        key_column: str,
        key_value: Any,
        *,
        text: str | None = None,
        embedding: Sequence[float] | str | None = None,
        embedding_column: str = "embedding",
    ) -> None:
        if embedding is None:
            if text is None:
                raise ValueError("either text or embedding must be provided")
            if self._embedder is None:
                raise RuntimeError("vector_store.embedder_required")
            embedding = self._encode_text(text)
        embedding_value = decode_vector_value(embedding)
        self._validate_vector(embedding_value)

        table_name = _require_identifier(table, label="table")
        key_name = _require_identifier(key_column, label="key column")
        embedding_name = _require_identifier(
            embedding_column, label="embedding column"
        )
        vector_literal = format_vector_literal(embedding_value)

        self._conn.execute(
            f"UPDATE {table_name} SET {embedding_name} = $1::vector WHERE {key_name} = $2",
            vector_literal,
            key_value,
        )

    def _encode_text(self, text: str) -> tuple[float, ...]:
        vector = decode_vector_value(self._embedder.embed_one(text))
        self._validate_vector(vector)
        return vector

    def _validate_vector(self, vector: Sequence[float]) -> None:
        authority = self._authority
        if authority is not None and hasattr(authority, "validate_embedding_vector"):
            authority.validate_embedding_vector(vector)

    def _validate_embedder_contract(self) -> None:
        authority = self._authority
        if authority is None or self._embedder is None:
            return

        model_name = self._candidate_model_name()
        if model_name is not None and hasattr(authority, "validate_embedder_model"):
            authority.validate_embedder_model(model_name)

        dimensions = self._candidate_dimensions()
        if dimensions is not None and hasattr(authority, "validate_embedder_dimensions"):
            authority.validate_embedder_dimensions(dimensions)

    def _candidate_model_name(self) -> str | None:
        model_name = getattr(self._embedder, "model_name", None)
        if isinstance(model_name, str) and model_name.strip():
            return model_name.strip()
        private_name = getattr(self._embedder, "_model_name", None)
        if isinstance(private_name, str) and private_name.strip():
            return private_name.strip()
        return None

    def _candidate_dimensions(self) -> int | None:
        dimensions = getattr(self._embedder, "dimensions", None)
        if isinstance(dimensions, int) and not isinstance(dimensions, bool):
            return dimensions
        private_dimensions = getattr(self._embedder, "DIMENSIONS", None)
        if isinstance(private_dimensions, int) and not isinstance(private_dimensions, bool):
            return private_dimensions
        return None

    @staticmethod
    def _select_sql(select_columns: Sequence[str]) -> str:
        columns: list[str] = []
        for column in select_columns:
            if column == "*":
                return "*"
            columns.append(_require_identifier(column, label="select column"))
        return ", ".join(columns) if columns else "*"

__all__ = [
    "PreparedVectorQuery",
    "PostgresVectorStore",
    "VectorFilter",
    "cosine_similarity",
    "decode_vector_value",
    "format_vector_literal",
]
