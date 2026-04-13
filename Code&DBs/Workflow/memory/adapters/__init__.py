"""Concrete adapters implementing memory ports with current backends."""
from __future__ import annotations

from .embedding import EmbeddingServiceAdapter
from .postgres_vector import PostgresVectorAdapter

__all__ = [
    "EmbeddingServiceAdapter",
    "PostgresVectorAdapter",
]
