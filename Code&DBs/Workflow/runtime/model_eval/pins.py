"""Pinned-route contract for Model Eval workers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .openrouter import BLOCKED_PROVIDER_SLUGS


MODEL_EVAL_WORKER_TASK_TYPE = "model_eval_worker"

FORBIDDEN_MODEL_EVAL_AGENT_SLUGS = {
    "chat",
    "auto/chat",
    "testing",
    "auto/testing",
    "testing/chat",
}


class PinnedModelEvalRouteError(ValueError):
    """Raised when a Model Eval worker route is not a concrete admitted pin."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _provider_root(value: str) -> str:
    return value.strip().lower().split("/", 1)[0]


def pinned_agent_from_model_config(model_config: Mapping[str, Any]) -> str:
    return _text(model_config.get("agent") or model_config.get("agent_slug"))


def pinned_candidate_ref_from_model_config(model_config: Mapping[str, Any]) -> str | None:
    value = _text(
        model_config.get("model_eval_candidate_ref")
        or model_config.get("candidate_ref")
        or model_config.get("config_id")
    )
    return value or None


def validate_pinned_agent_slug(agent_slug: Any) -> str:
    """Return a normalized slug or raise when eval would route implicitly."""

    normalized = _text(agent_slug)
    lowered = normalized.lower()
    if not normalized:
        raise PinnedModelEvalRouteError(
            "model_eval_worker requires a concrete agent slug like provider/model"
        )
    if lowered.startswith("auto/"):
        raise PinnedModelEvalRouteError("model_eval_worker forbids auto/* agent slugs")
    if lowered in FORBIDDEN_MODEL_EVAL_AGENT_SLUGS:
        raise PinnedModelEvalRouteError(
            "model_eval_worker forbids chat/testing agent aliases"
        )
    if "/" not in normalized:
        raise PinnedModelEvalRouteError(
            "model_eval_worker agent must be a concrete provider/model slug"
        )
    provider_slug, model_slug = normalized.split("/", 1)
    if not provider_slug.strip() or not model_slug.strip():
        raise PinnedModelEvalRouteError(
            "model_eval_worker agent must include both provider and model"
        )
    blocked = {_provider_root(item) for item in BLOCKED_PROVIDER_SLUGS}
    if _provider_root(provider_slug) in blocked:
        raise PinnedModelEvalRouteError(
            f"model_eval_worker provider {provider_slug!r} is blocked"
        )
    return normalized


def validate_model_eval_model_config(model_config: Mapping[str, Any]) -> str:
    """Validate a Model Eval candidate config and return its pinned agent."""

    if not isinstance(model_config, Mapping):
        raise PinnedModelEvalRouteError("model_config must be an object")
    agent_slug = validate_pinned_agent_slug(pinned_agent_from_model_config(model_config))
    model_slug = _text(model_config.get("model_slug"))
    if not model_slug:
        raise PinnedModelEvalRouteError("model_config.model_slug is required")
    pinned_model_slug = agent_slug.split("/", 1)[1]
    if pinned_model_slug != model_slug:
        raise PinnedModelEvalRouteError(
            "model_eval_worker agent model must match model_config.model_slug"
        )
    return agent_slug


__all__ = [
    "FORBIDDEN_MODEL_EVAL_AGENT_SLUGS",
    "MODEL_EVAL_WORKER_TASK_TYPE",
    "PinnedModelEvalRouteError",
    "pinned_agent_from_model_config",
    "pinned_candidate_ref_from_model_config",
    "validate_model_eval_model_config",
    "validate_pinned_agent_slug",
]
