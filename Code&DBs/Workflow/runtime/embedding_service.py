"""Shared embedding runtime authority for semantic retrieval tables."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import logging
import os
import sys
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from typing import Any, ClassVar
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


_DEFAULT_MODEL_NAME = "all-MiniLM-L6-v2"
_DEFAULT_DIMENSIONS = 384
_DEFAULT_REFRESH_TRIGGER_INTENT_KINDS = ("embed_entity",)
_DEFAULT_REFRESH_FOLLOW_ON_INTENT_KIND = "refresh_vector_neighbors"
_DEFAULT_REFRESH_FOLLOW_ON_BATCH_LIMIT = 25
_DEFAULT_MISSING_EMBEDDER_MODE = "skip"
_DEFAULT_MISSING_EMBEDDER_REASON = "embedder_unavailable"
_DEFAULT_BACKEND_MODE = "auto"
_DEFAULT_SERVICE_TIMEOUT_SECONDS = 10.0

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EmbeddingRuntimeAuthority:
    """Canonical embedding runtime contract.

    One object owns the embedding model choice, dimensions, refresh policy,
    and failure handling. Callers should pass this object around instead of
    copying the defaults into startup or maintenance code paths.
    """

    model_name: str = _DEFAULT_MODEL_NAME
    dimensions: int = _DEFAULT_DIMENSIONS
    refresh_follow_on_enabled: bool = True
    refresh_trigger_intent_kinds: tuple[str, ...] = _DEFAULT_REFRESH_TRIGGER_INTENT_KINDS
    refresh_follow_on_intent_kind: str = _DEFAULT_REFRESH_FOLLOW_ON_INTENT_KIND
    refresh_follow_on_batch_limit: int = _DEFAULT_REFRESH_FOLLOW_ON_BATCH_LIMIT
    missing_embedder_mode: str = _DEFAULT_MISSING_EMBEDDER_MODE
    missing_embedder_reason: str = _DEFAULT_MISSING_EMBEDDER_REASON

    def validate_embedder_model(self, model_name: str | None) -> None:
        if model_name is None:
            return
        normalized = model_name.strip()
        if normalized and normalized != self.model_name:
            raise RuntimeError(
                f"embedding_model_mismatch:{normalized}:{self.model_name}"
            )

    def validate_embedder_dimensions(self, dimensions: int | None) -> None:
        if dimensions is None:
            return
        if isinstance(dimensions, bool) or not isinstance(dimensions, int):
            raise RuntimeError(
                f"embedding_dimensions_invalid:{type(dimensions).__name__}:{self.dimensions}"
            )
        if dimensions != self.dimensions:
            raise RuntimeError(
                f"embedding_dimensions_mismatch:{dimensions}:{self.dimensions}"
            )

    def validate_embedding_vector(self, vector: Sequence[float]) -> None:
        if len(vector) != self.dimensions:
            raise RuntimeError(
                f"embedding_dimensions_mismatch:{len(vector)}:{self.dimensions}"
            )

    def should_drain_follow_on_refresh(self, claimed_intent_kinds: Iterable[str]) -> bool:
        if not self.refresh_follow_on_enabled:
            return False
        trigger_kinds = set(self.refresh_trigger_intent_kinds)
        return any(intent_kind in trigger_kinds for intent_kind in claimed_intent_kinds)

    def missing_embedder_outcome(self, *, subject_id: str | None) -> dict[str, Any]:
        if self.missing_embedder_mode == "raise":
            raise RuntimeError(
                f"{self.missing_embedder_reason}:{subject_id or 'unknown'}"
            )
        return {
            "status": "skipped",
            "message": f"{self.missing_embedder_reason}:{subject_id or 'unknown'}",
            "outcome": {"reason": self.missing_embedder_reason},
        }


_DEFAULT_EMBEDDING_RUNTIME_AUTHORITY = EmbeddingRuntimeAuthority()


def resolve_embedding_runtime_authority() -> EmbeddingRuntimeAuthority:
    """Return the canonical embedding runtime authority."""

    return _DEFAULT_EMBEDDING_RUNTIME_AUTHORITY


class EmbeddingService:
    """Shared embedding surface for semantic retrieval.

    The authority object owns the runtime contract; the service exposes the
    concrete backend behind that contract. Vector literal formatting stays in
    the storage adapter.
    """

    DIMENSIONS = _DEFAULT_DIMENSIONS
    _shared_models: ClassVar[dict[str, Any]] = {}
    _shared_model_events: ClassVar[dict[str, threading.Event]] = {}
    _shared_model_lock: ClassVar[threading.Lock] = threading.Lock()
    _shared_encode_locks: ClassVar[dict[str, threading.Lock]] = {}
    _prewarm_threads: ClassVar[dict[str, threading.Thread]] = {}
    _prewarm_lock: ClassVar[threading.Lock] = threading.Lock()
    _service_cached_models: ClassVar[set[str]] = set()
    _service_cache_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        model_name: str | None = None,
        *,
        authority: EmbeddingRuntimeAuthority | None = None,
        dimensions: int | None = None,
    ) -> None:
        runtime_authority = authority or resolve_embedding_runtime_authority()
        if model_name is not None and model_name.strip():
            runtime_authority = replace(
                runtime_authority,
                model_name=model_name.strip(),
            )
        if dimensions is not None:
            runtime_authority = replace(
                runtime_authority,
                dimensions=dimensions,
            )
        self._authority = runtime_authority
        self._model_name = runtime_authority.model_name
        self._model = None
        self._backend_kind, self._backend_reason = self.resolve_backend_status()
        self._service_url = self._resolve_service_url()

    @property
    def authority(self) -> EmbeddingRuntimeAuthority:
        return self._authority

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dimensions(self) -> int:
        return self._authority.dimensions

    @property
    def backend_kind(self) -> str:
        return self._backend_kind

    @classmethod
    def _normalize_model_name(cls, model_name: str | None) -> str:
        normalized = str(model_name or resolve_embedding_runtime_authority().model_name).strip()
        if not normalized:
            raise RuntimeError("embedding_model_name_required")
        return normalized

    @classmethod
    def _normalize_backend_mode(cls, raw_mode: str | None) -> str:
        normalized = str(raw_mode or _DEFAULT_BACKEND_MODE).strip().lower()
        if normalized in {"", "auto"}:
            return "auto"
        if normalized in {"local"}:
            return "local"
        if normalized in {"service", "http", "remote"}:
            return "service"
        if normalized in {"disabled", "none", "off"}:
            return "disabled"
        return normalized

    @classmethod
    def _resolve_service_url(cls) -> str:
        return str(os.environ.get("WORKFLOW_EMBEDDING_SERVICE_URL") or "").strip()

    @classmethod
    def _local_backend_available(cls) -> bool:
        return (
            "sentence_transformers" in sys.modules
            or importlib.util.find_spec("sentence_transformers") is not None
        )

    @classmethod
    def resolve_backend_status(cls) -> tuple[str, str | None]:
        mode = cls._normalize_backend_mode(os.environ.get("WORKFLOW_EMBEDDING_BACKEND"))
        service_url = cls._resolve_service_url()
        local_available = cls._local_backend_available()

        if mode == "disabled":
            return "disabled", "embedding_backend_disabled"
        if mode == "service":
            if service_url:
                return "service", None
            return "unavailable", "embedding_service_url_required"
        if mode == "local":
            if local_available:
                return "local", None
            return "unavailable", "sentence_transformers_unavailable"
        if mode == "auto":
            if service_url:
                return "service", None
            if local_available:
                return "local", None
            return "unavailable", "embedding_backend_unavailable"
        return "unavailable", f"embedding_backend_invalid:{mode}"

    @classmethod
    def backend_available(cls) -> bool:
        backend_kind, _reason = cls.resolve_backend_status()
        return backend_kind in {"local", "service"}

    @classmethod
    def backend_unavailable_reason(cls) -> str | None:
        _backend_kind, reason = cls.resolve_backend_status()
        return reason

    @classmethod
    def _service_timeout_seconds(cls) -> float:
        raw_value = str(os.environ.get("WORKFLOW_EMBEDDING_SERVICE_TIMEOUT_S") or "").strip()
        if not raw_value:
            return _DEFAULT_SERVICE_TIMEOUT_SECONDS
        try:
            return max(0.1, float(raw_value))
        except ValueError:
            return _DEFAULT_SERVICE_TIMEOUT_SECONDS

    @classmethod
    def _service_endpoint(cls, path: str) -> str:
        service_url = cls._resolve_service_url()
        if not service_url:
            raise RuntimeError("embedding_service_url_required")
        normalized_base = service_url.rstrip("/") + "/"
        return urllib_parse.urljoin(normalized_base, path.lstrip("/"))

    @classmethod
    def _service_request(cls, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = cls._service_endpoint(path)
        request = urllib_request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib_request.urlopen(request, timeout=cls._service_timeout_seconds()) as response:
                status_code = int(getattr(response, "status", 200))
                response_text = response.read().decode("utf-8", errors="replace")
        except urllib_error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(
                f"embedding_service_http_error:{exc.code}:{detail or exc.reason}"
            ) from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"embedding_service_unreachable:{exc.reason}") from exc
        if status_code >= 400:
            raise RuntimeError(f"embedding_service_http_error:{status_code}:{response_text.strip()}")
        try:
            payload = json.loads(response_text or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError("embedding_service_protocol_invalid:invalid_json") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("embedding_service_protocol_invalid:object_required")
        return payload

    @classmethod
    def _mark_service_model_cached(cls, model_name: str) -> None:
        normalized = cls._normalize_model_name(model_name)
        with cls._service_cache_lock:
            cls._service_cached_models.add(normalized)

    def _raise_backend_unavailable(self) -> None:
        raise RuntimeError(self._backend_reason or "embedding_backend_unavailable")

    @classmethod
    @contextlib.contextmanager
    def _quiet_model_load(cls, model_name: str):
        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()
        previous_env: dict[str, str | None] = {}
        quiet_env = {
            "HF_HUB_DISABLE_PROGRESS_BARS": "1",
            "TOKENIZERS_PARALLELISM": "false",
            "TRANSFORMERS_NO_ADVISORY_WARNINGS": "1",
        }

        for key, value in quiet_env.items():
            previous_env[key] = os.environ.get(key)
            os.environ[key] = value

        progress_enabled = False
        previous_verbosity = None
        try:
            from transformers.utils import logging as transformers_logging

            progress_enabled = transformers_logging.is_progress_bar_enabled()
            previous_verbosity = transformers_logging.get_verbosity()
            transformers_logging.disable_progress_bar()
            transformers_logging.set_verbosity_error()
        except Exception:
            transformers_logging = None
        try:
            with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
                yield
        finally:
            if transformers_logging is not None and previous_verbosity is not None:
                transformers_logging.set_verbosity(previous_verbosity)
                if progress_enabled:
                    transformers_logging.enable_progress_bar()
            for key, previous_value in previous_env.items():
                if previous_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous_value
            suppressed = "\n".join(
                part.strip()
                for part in (captured_stdout.getvalue(), captured_stderr.getvalue())
                if part.strip()
            ).strip()
            if suppressed:
                logger.debug(
                    "Suppressed sentence-transformer load output for %s: %s",
                    model_name,
                    suppressed.splitlines()[0],
                )

    @classmethod
    def _construct_model_for_name(cls, model_name: str, *, local_files_only: bool):
        from sentence_transformers import SentenceTransformer

        token = os.environ.get("HF_TOKEN", "").strip() or None
        with cls._quiet_model_load(model_name):
            return SentenceTransformer(
                model_name,
                local_files_only=local_files_only,
                token=token,
            )

    @classmethod
    def _load_model_for_name(cls, model_name: str):
        try:
            return cls._construct_model_for_name(
                model_name,
                local_files_only=True,
            )
        except Exception as exc:
            logger.debug(
                "Local embedding cache unavailable for %s; retrying with hub access: %s",
                model_name,
                exc,
            )
        return cls._construct_model_for_name(
            model_name,
            local_files_only=False,
        )

    @classmethod
    def _get_or_load_shared_model(cls, model_name: str):
        normalized = cls._normalize_model_name(model_name)
        while True:
            with cls._shared_model_lock:
                cached = cls._shared_models.get(normalized)
                if cached is not None:
                    return cached
                event = cls._shared_model_events.get(normalized)
                if event is None:
                    event = threading.Event()
                    cls._shared_model_events[normalized] = event
                    should_load = True
                else:
                    should_load = False

            if should_load:
                try:
                    model = cls._load_model_for_name(normalized)
                except Exception:
                    with cls._shared_model_lock:
                        cls._shared_model_events.pop(normalized, None)
                        event.set()
                    raise
                with cls._shared_model_lock:
                    cls._shared_models[normalized] = model
                    cls._shared_model_events.pop(normalized, None)
                    event.set()
                return model

            event.wait()

    @classmethod
    def prewarm_model(cls, model_name: str | None = None):
        normalized = cls._normalize_model_name(model_name)
        backend_kind, reason = cls.resolve_backend_status()
        if backend_kind == "local":
            return cls._get_or_load_shared_model(normalized)
        if backend_kind == "service":
            payload = cls._service_request(
                "/prewarm",
                {"model_name": normalized},
            )
            cls._mark_service_model_cached(normalized)
            return payload
        raise RuntimeError(reason or "embedding_backend_unavailable")

    @classmethod
    def start_background_prewarm(cls, model_name: str | None = None) -> threading.Thread | None:
        normalized = cls._normalize_model_name(model_name)
        backend_kind, _reason = cls.resolve_backend_status()
        if backend_kind in {"disabled", "unavailable"}:
            return None
        if cls._is_model_cached(normalized):
            return None
        with cls._prewarm_lock:
            existing = cls._prewarm_threads.get(normalized)
            if existing is not None and existing.is_alive():
                return existing

            def _runner() -> None:
                try:
                    cls.prewarm_model(normalized)
                    logger.debug("Embedding prewarm finished for %s", normalized)
                except Exception as exc:
                    logger.warning(
                        "Embedding prewarm failed for %s: %s",
                        normalized,
                        exc,
                        exc_info=True,
                    )
                finally:
                    with cls._prewarm_lock:
                        current = cls._prewarm_threads.get(normalized)
                        if current is thread:
                            cls._prewarm_threads.pop(normalized, None)

            thread = threading.Thread(
                target=_runner,
                daemon=True,
                name=f"embedding-prewarm-{normalized}",
            )
            cls._prewarm_threads[normalized] = thread
            thread.start()
            return thread

    @classmethod
    def _is_model_cached(cls, model_name: str | None = None) -> bool:
        normalized = cls._normalize_model_name(model_name)
        backend_kind, _reason = cls.resolve_backend_status()
        if backend_kind == "service":
            with cls._service_cache_lock:
                return normalized in cls._service_cached_models
        with cls._shared_model_lock:
            return normalized in cls._shared_models

    @classmethod
    def _reset_shared_state_for_tests(cls) -> None:
        with cls._shared_model_lock:
            cls._shared_models.clear()
            for event in cls._shared_model_events.values():
                event.set()
            cls._shared_model_events.clear()
            cls._shared_encode_locks.clear()
        with cls._prewarm_lock:
            cls._prewarm_threads.clear()
        with cls._service_cache_lock:
            cls._service_cached_models.clear()

    @classmethod
    def _get_encode_lock(cls, model_name: str | None = None) -> threading.Lock:
        normalized = cls._normalize_model_name(model_name)
        with cls._shared_model_lock:
            lock = cls._shared_encode_locks.get(normalized)
            if lock is None:
                lock = threading.Lock()
                cls._shared_encode_locks[normalized] = lock
            return lock

    def _get_model(self):
        if self._backend_kind != "local":
            self._raise_backend_unavailable()
        if self._model is None:
            self._model = self._get_or_load_shared_model(self._model_name)
        return self._model

    def _embed_via_service(self, texts: list[str]) -> list[list[float]]:
        payload = self._service_request(
            "/embed",
            {
                "model_name": self._model_name,
                "texts": list(texts),
            },
        )
        self._authority.validate_embedder_model(payload.get("model_name"))
        self._authority.validate_embedder_dimensions(payload.get("dimensions"))

        raw_vectors = payload.get("vectors")
        if not isinstance(raw_vectors, list):
            raise RuntimeError("embedding_service_protocol_invalid:vectors_required")

        vectors: list[list[float]] = []
        for raw_vector in raw_vectors:
            if not isinstance(raw_vector, Sequence) or isinstance(raw_vector, (str, bytes)):
                raise RuntimeError("embedding_service_protocol_invalid:vector_sequence_required")
            vector = [float(value) for value in raw_vector]
            self._authority.validate_embedding_vector(vector)
            vectors.append(vector)
        self._mark_service_model_cached(self._model_name)
        return vectors

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts. Returns list of authoritative-dimension vectors."""
        if not texts:
            return []
        if self._backend_kind == "service":
            return self._embed_via_service(texts)
        if self._backend_kind != "local":
            self._raise_backend_unavailable()
        encode_lock = self._get_encode_lock(self._model_name)
        with encode_lock:
            model = self._get_model()
            embeddings = model.encode(
                texts,
                show_progress_bar=False,
                convert_to_numpy=True,
            )
        vectors = [e.tolist() for e in embeddings]
        for vector in vectors:
            self._authority.validate_embedding_vector(vector)
        return vectors

    def embed_one(self, text: str) -> list[float]:
        """Embed a single text. Convenience wrapper."""
        return self.embed([text])[0]
