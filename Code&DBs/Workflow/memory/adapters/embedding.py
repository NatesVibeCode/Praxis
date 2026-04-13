"""Adapter wrapping runtime.embedding_service.EmbeddingService as EmbeddingPort."""
from __future__ import annotations

from typing import Any, Sequence


class EmbeddingServiceAdapter:
    """Adapts EmbeddingService to the EmbeddingPort protocol."""

    def __init__(self, service: Any) -> None:
        self._service = service

    def embed(self, text: str) -> list[float]:
        return self._service.embed(text)

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        if hasattr(self._service, "embed_batch"):
            return self._service.embed_batch(texts)
        return [self._service.embed(t) for t in texts]
