"""Port for embedding generation."""
from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable


@runtime_checkable
class EmbeddingPort(Protocol):
    """Generates vector embeddings from text."""

    def embed(self, text: str) -> list[float]: ...

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]: ...
