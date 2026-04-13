"""Compiler sublayer: semantic retrieval via IntentMatcher.

Handles embedder resolution, intent matching with timeout management,
and match result flattening.
"""

from __future__ import annotations

import os
import signal
import threading
from typing import Any

import runtime.compiler_components as _compiler_components

_COMPILER_EMBEDDER: Any | None = None
_COMPILER_EMBEDDER_ERROR: str | None = None


class IntentMatchTimeoutError(TimeoutError):
    """Raised when compiler semantic retrieval exceeds its latency budget."""


def compiler_intent_timeout_seconds() -> float:
    raw = os.environ.get("WORKFLOW_COMPILER_INTENT_TIMEOUT_S", "").strip()
    if not raw:
        embedder = _COMPILER_EMBEDDER
        if embedder is None:
            return 8.0
        is_model_cached = getattr(embedder.__class__, "_is_model_cached", None)
        model_name = getattr(embedder, "model_name", None)
        if callable(is_model_cached):
            try:
                if not is_model_cached(model_name):
                    return 8.0
            except Exception:
                pass
        return 2.5
    try:
        value = float(raw)
    except ValueError:
        return 2.5
    return max(0.0, value)


def resolve_compiler_embedder() -> tuple[Any | None, dict[str, str | None]]:
    global _COMPILER_EMBEDDER, _COMPILER_EMBEDDER_ERROR

    import logging

    logger = logging.getLogger(__name__)

    disable_flag = os.environ.get("WORKFLOW_COMPILER_DISABLE_EMBEDDINGS", "").strip().lower()
    if disable_flag in {"1", "true", "yes", "on"}:
        return None, {"mode": "degraded", "reason": "disabled_by_env"}

    if _COMPILER_EMBEDDER is not None:
        return _COMPILER_EMBEDDER, {"mode": "semantic", "reason": None}

    if _COMPILER_EMBEDDER_ERROR is not None:
        return None, {"mode": "degraded", "reason": _COMPILER_EMBEDDER_ERROR}

    from runtime.embedding_service import EmbeddingService

    try:
        _COMPILER_EMBEDDER = EmbeddingService()
    except Exception as exc:
        _COMPILER_EMBEDDER_ERROR = f"embedding_init_failed: {exc}"
        logger.warning("Compiler embedding service unavailable: %s", exc)
        return None, {"mode": "degraded", "reason": _COMPILER_EMBEDDER_ERROR}
    return _COMPILER_EMBEDDER, {"mode": "semantic", "reason": None}


def run_intent_match(matcher: Any, prose: str) -> tuple[Any, Any]:
    timeout_seconds = compiler_intent_timeout_seconds()

    def _execute() -> tuple[Any, Any]:
        match_result = matcher.match(prose, limit=15)
        match_plan = matcher.compose(prose, match_result)
        return match_result, match_plan

    if timeout_seconds <= 0 or threading.current_thread() is not threading.main_thread():
        return _execute()

    def _handle_timeout(signum, frame):  # type: ignore[unused-ignore]
        raise IntentMatchTimeoutError(f"semantic retrieval exceeded {timeout_seconds:.1f}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return _execute()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


def flatten_match_result(match_result: Any) -> list[dict[str, Any]]:
    return _compiler_components.flatten_match_result(match_result)


def composition_to_dict(plan: Any) -> dict[str, Any]:
    return _compiler_components.composition_to_dict(plan)
