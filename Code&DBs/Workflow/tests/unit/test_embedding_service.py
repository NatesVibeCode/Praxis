from __future__ import annotations

import sys
import types
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.embedding_service import EmbeddingService


class _FakeVector:
    def __init__(self, values: list[float]) -> None:
        self._values = values

    def tolist(self) -> list[float]:
        return list(self._values)


def test_embedding_service_reuses_process_wide_model_cache(monkeypatch) -> None:
    load_calls: list[str] = []

    class _FakeSentenceTransformer:
        def __init__(self, model_name: str) -> None:
            load_calls.append(model_name)
            self.model_name = model_name

        def encode(self, texts, show_progress_bar=False, convert_to_numpy=True):
            return [_FakeVector([0.0] * 384) for _ in texts]

    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        types.SimpleNamespace(SentenceTransformer=_FakeSentenceTransformer),
    )
    EmbeddingService._reset_shared_state_for_tests()

    try:
        first = EmbeddingService()
        second = EmbeddingService()

        assert first._get_model() is second._get_model()
        assert first.embed_one("alpha") == [0.0] * 384
        assert second.embed_one("beta") == [0.0] * 384
        assert load_calls == [first.model_name]
        assert EmbeddingService._is_model_cached(first.model_name) is True
    finally:
        EmbeddingService._reset_shared_state_for_tests()


def test_embedding_service_background_prewarm_populates_shared_cache(monkeypatch) -> None:
    load_calls: list[str] = []

    class _FakeSentenceTransformer:
        def __init__(self, model_name: str) -> None:
            load_calls.append(model_name)
            self.model_name = model_name

        def encode(self, texts, show_progress_bar=False, convert_to_numpy=True):
            return [_FakeVector([1.0] * 384) for _ in texts]

    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        types.SimpleNamespace(SentenceTransformer=_FakeSentenceTransformer),
    )
    EmbeddingService._reset_shared_state_for_tests()

    try:
        thread = EmbeddingService.start_background_prewarm()
        assert thread is not None
        thread.join(timeout=1.0)

        assert EmbeddingService._is_model_cached() is True
        service = EmbeddingService()
        assert service.embed_one("warm") == [1.0] * 384
        assert load_calls == [service.model_name]
        assert EmbeddingService.start_background_prewarm(service.model_name) is None
    finally:
        EmbeddingService._reset_shared_state_for_tests()
