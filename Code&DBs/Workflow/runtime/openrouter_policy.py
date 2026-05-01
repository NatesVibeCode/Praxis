"""OpenRouter request-level privacy routing policy.

OpenRouter is a broker: without request-level provider controls, a model can
be served by whatever endpoint the router selects or falls back to. Praxis
requires explicit endpoint routing for OpenRouter requests that carry operator
data so privacy posture is inspectable at the payload boundary.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any


OPENROUTER_NO_CHINA_POLICY_DECISION_REF = (
    "architecture-policy::provider-routing-privacy::openrouter-no-china-data-path"
)

BLOCKED_PROVIDER_SLUGS: tuple[str, ...] = (
    "alibaba",
    "alibaba-cloud",
    "baidu",
    "bytedance",
    "deepseek",
    "deepseek-ai",
    "kimi",
    "minimax",
    "moonshot",
    "moonshotai",
    "qwen",
    "seed",
    "siliconflow",
    "stepfun",
    "tencent",
    "xiaomi",
    "z-ai",
    "zhipu",
)

# Exact OpenRouter endpoint tags approved for Praxis request routing. These
# tags come from OpenRouter's model endpoint catalog and are intentionally
# narrow: if a model is not listed here, OpenRouter dispatch fails closed.
APPROVED_PROVIDER_ORDER_BY_MODEL: Mapping[str, tuple[str, ...]] = {
    "openai/gpt-5.4-nano": ("azure",),
    "openai/gpt-5.4-mini": ("azure",),
    "moonshotai/kimi-k2.6": ("parasail/int4",),
    "qwen/qwen3-coder": ("deepinfra/turbo",),
    "qwen/qwen3-32b": ("groq",),
    "openai/gpt-oss-20b": ("parasail/fp4",),
    "google/gemma-3-27b-it": ("deepinfra/fp8",),
    "meta-llama/llama-4-scout": ("groq",),
    "nvidia/nemotron-3-nano-30b-a3b": ("deepinfra/fp4",),
    "nvidia/nemotron-nano-9b-v2": ("deepinfra/bf16",),
    "z-ai/glm-5.1": ("deepinfra/fp4",),
    "minimax/minimax-m2.7": ("fireworks",),
    "deepseek/deepseek-v4-flash": ("deepinfra/fp4",),
}


class OpenRouterPolicyError(ValueError):
    """Raised when an OpenRouter request cannot prove its privacy route."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def normalize_model_slug(model_slug: str) -> str:
    normalized = str(model_slug or "").strip()
    if normalized.startswith("openrouter/"):
        normalized = normalized.split("/", 1)[1]
    return normalized


def strict_provider_policy(
    *,
    model_slug: str,
    provider_order: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Return the strict provider object required on OpenRouter requests."""

    normalized_model = normalize_model_slug(model_slug)
    approved = tuple(APPROVED_PROVIDER_ORDER_BY_MODEL.get(normalized_model, ()))
    ordered = _clean_provider_order(provider_order) or approved
    if not ordered:
        raise OpenRouterPolicyError(
            "openrouter_policy.no_approved_endpoint",
            "OpenRouter request has no approved non-China endpoint route",
            details={
                "model_slug": normalized_model,
                "decision_ref": OPENROUTER_NO_CHINA_POLICY_DECISION_REF,
            },
        )

    _validate_provider_order(
        model_slug=normalized_model,
        ordered=ordered,
        approved=approved,
    )
    return {
        "order": list(ordered),
        "allow_fallbacks": False,
        "require_parameters": True,
        "data_collection": "deny",
        "zdr": True,
        "ignore": list(BLOCKED_PROVIDER_SLUGS),
    }


def apply_strict_openrouter_policy(
    *,
    body: Mapping[str, Any],
    model_slug: str,
) -> dict[str, Any]:
    """Return a copy of ``body`` with OpenRouter privacy routing enforced."""

    shaped = deepcopy(dict(body))
    existing = shaped.get("provider")
    existing_provider = existing if isinstance(existing, Mapping) else {}
    requested_order = _provider_order_from_existing(existing_provider)
    shaped["provider"] = strict_provider_policy(
        model_slug=model_slug,
        provider_order=requested_order,
    )
    return shaped


def _clean_provider_order(provider_order: Sequence[str] | None) -> tuple[str, ...]:
    if not provider_order:
        return ()
    return tuple(
        item
        for item in (str(value).strip().lower() for value in provider_order)
        if item
    )


def _provider_order_from_existing(existing_provider: Mapping[str, Any]) -> tuple[str, ...]:
    for key in ("order", "only"):
        value = existing_provider.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return _clean_provider_order(value)
    return ()


def _base_provider_slug(provider_slug: str) -> str:
    return str(provider_slug or "").strip().lower().split("/", 1)[0]


def _validate_provider_order(
    *,
    model_slug: str,
    ordered: tuple[str, ...],
    approved: tuple[str, ...],
) -> None:
    blocked = sorted(
        provider
        for provider in ordered
        if provider in BLOCKED_PROVIDER_SLUGS
        or _base_provider_slug(provider) in BLOCKED_PROVIDER_SLUGS
    )
    if blocked:
        raise OpenRouterPolicyError(
            "openrouter_policy.blocked_provider_endpoint",
            "OpenRouter provider route includes a blocked China-affiliated endpoint",
            details={
                "model_slug": model_slug,
                "blocked_provider_order": blocked,
                "decision_ref": OPENROUTER_NO_CHINA_POLICY_DECISION_REF,
            },
        )

    if not approved:
        raise OpenRouterPolicyError(
            "openrouter_policy.no_approved_endpoint",
            "OpenRouter request has no approved non-China endpoint route",
            details={
                "model_slug": model_slug,
                "decision_ref": OPENROUTER_NO_CHINA_POLICY_DECISION_REF,
            },
        )

    unapproved = sorted(set(ordered) - set(approved))
    if unapproved:
        raise OpenRouterPolicyError(
            "openrouter_policy.unapproved_provider_endpoint",
            "OpenRouter provider route is not in the approved endpoint allowlist",
            details={
                "model_slug": model_slug,
                "unapproved_provider_order": unapproved,
                "approved_provider_order": list(approved),
                "decision_ref": OPENROUTER_NO_CHINA_POLICY_DECISION_REF,
            },
        )


__all__ = [
    "APPROVED_PROVIDER_ORDER_BY_MODEL",
    "BLOCKED_PROVIDER_SLUGS",
    "OPENROUTER_NO_CHINA_POLICY_DECISION_REF",
    "OpenRouterPolicyError",
    "apply_strict_openrouter_policy",
    "normalize_model_slug",
    "strict_provider_policy",
]
