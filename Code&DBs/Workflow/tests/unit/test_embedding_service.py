from __future__ import annotations

import contextlib
import io
import sys
import threading
import time
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
        def __init__(self, model_name: str, **kwargs) -> None:
            del kwargs
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
        def __init__(self, model_name: str, **kwargs) -> None:
            del kwargs
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


def test_embedding_service_serializes_shared_model_encode_calls(monkeypatch) -> None:
    class _BorrowCheckingSentenceTransformer:
        def __init__(self, model_name: str, **kwargs) -> None:
            del kwargs
            self.model_name = model_name
            self._active = False

        def encode(self, texts, show_progress_bar=False, convert_to_numpy=True):
            del show_progress_bar, convert_to_numpy
            if self._active:
                raise RuntimeError("Already borrowed")
            self._active = True
            try:
                time.sleep(0.05)
                return [_FakeVector([2.0] * 384) for _ in texts]
            finally:
                self._active = False

    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        types.SimpleNamespace(SentenceTransformer=_BorrowCheckingSentenceTransformer),
    )
    EmbeddingService._reset_shared_state_for_tests()

    try:
        first = EmbeddingService()
        second = EmbeddingService()
        barrier = threading.Barrier(2)
        results: list[list[float]] = []
        errors: list[Exception] = []

        def _runner(service: EmbeddingService, text: str) -> None:
            try:
                barrier.wait(timeout=1.0)
                results.append(service.embed_one(text))
            except Exception as exc:  # pragma: no cover - defensive capture
                errors.append(exc)

        threads = [
            threading.Thread(target=_runner, args=(first, "alpha")),
            threading.Thread(target=_runner, args=(second, "beta")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=1.0)

        assert errors == []
        assert results == [[2.0] * 384, [2.0] * 384]
    finally:
        EmbeddingService._reset_shared_state_for_tests()


def test_embedding_service_prefers_local_cache_and_suppresses_load_noise(monkeypatch) -> None:
    init_calls: list[dict[str, object]] = []

    class _NoisySentenceTransformer:
        def __init__(self, model_name: str, **kwargs) -> None:
            init_calls.append({"model_name": model_name, **kwargs})
            print("Loading weights: noisy", file=sys.stderr)
            self.model_name = model_name

        def encode(self, texts, show_progress_bar=False, convert_to_numpy=True):
            return [_FakeVector([3.0] * 384) for _ in texts]

    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        types.SimpleNamespace(SentenceTransformer=_NoisySentenceTransformer),
    )
    monkeypatch.delenv("HF_TOKEN", raising=False)
    EmbeddingService._reset_shared_state_for_tests()

    try:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            service = EmbeddingService()
            assert service.embed_one("quiet") == [3.0] * 384

        assert stderr.getvalue() == ""
        assert init_calls == [
            {
                "model_name": service.model_name,
                "local_files_only": True,
                "token": None,
            }
        ]
    finally:
        EmbeddingService._reset_shared_state_for_tests()


def test_embedding_service_retries_with_hub_access_after_local_cache_miss(monkeypatch) -> None:
    init_calls: list[dict[str, object]] = []

    class _FallbackSentenceTransformer:
        def __init__(self, model_name: str, **kwargs) -> None:
            init_calls.append({"model_name": model_name, **kwargs})
            if kwargs.get("local_files_only"):
                raise RuntimeError("local_files_only cache miss")
            self.model_name = model_name

        def encode(self, texts, show_progress_bar=False, convert_to_numpy=True):
            return [_FakeVector([4.0] * 384) for _ in texts]

    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        types.SimpleNamespace(SentenceTransformer=_FallbackSentenceTransformer),
    )
    monkeypatch.setenv("HF_TOKEN", "token-123")
    EmbeddingService._reset_shared_state_for_tests()

    try:
        service = EmbeddingService()
        assert service.embed_one("fallback") == [4.0] * 384
        assert init_calls == [
            {
                "model_name": service.model_name,
                "local_files_only": True,
                "token": "token-123",
            },
            {
                "model_name": service.model_name,
                "local_files_only": False,
                "token": "token-123",
            },
        ]
    finally:
        EmbeddingService._reset_shared_state_for_tests()
