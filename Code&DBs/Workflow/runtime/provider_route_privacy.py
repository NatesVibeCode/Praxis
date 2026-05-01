"""Machine-checkable privacy posture for provider routes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


DIRECT_KIMI_PRIVACY_DECISION_REF = (
    "architecture-policy::provider-routing-privacy::direct-kimi-provider-zdr-gate"
)
OPENROUTER_PRIVACY_DECISION_REF = (
    "architecture-policy::provider-routing-privacy::openrouter-no-china-data-path"
)

_API_PRIVACY_REQUIREMENTS = (
    "no prompt/output training without explicit opt-in",
    "zero retention or account-level zero-data-retention verified",
    "direct provider path or approved endpoint only",
    "no aggregator fallback unless explicitly approved",
    "provider/data-path country restrictions satisfied",
)


def provider_route_privacy_posture(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return route privacy posture and whether dispatch may proceed."""

    provider_slug = _text(row.get("provider_slug")).lower()
    model_slug = _text(row.get("model_slug"))
    transport_type = _text(row.get("transport_type")).upper()

    if transport_type != "API":
        return {
            "state": "not_required",
            "dispatch_allowed": True,
            "reason_code": "privacy.not_required_for_cli_transport",
            "requirements": [],
        }

    if provider_slug == "openrouter":
        return _openrouter_posture(model_slug)

    if _is_kimi_model(model_slug):
        if provider_slug == "fireworks":
            return {
                "state": "approved",
                "dispatch_allowed": True,
                "reason_code": "privacy.fireworks_zdr_documented",
                "decision_ref": DIRECT_KIMI_PRIVACY_DECISION_REF,
                "requirements": list(_API_PRIVACY_REQUIREMENTS),
                "controls": {
                    "training_use": "disabled_by_provider_default",
                    "retention": "zero_data_retention_documented",
                    "data_path": "direct_fireworks",
                    "aggregator_fallback": "none",
                },
            }
        if provider_slug == "together":
            return {
                "state": "blocked",
                "dispatch_allowed": False,
                "reason_code": "privacy.zdr_account_unverified",
                "decision_ref": DIRECT_KIMI_PRIVACY_DECISION_REF,
                "requirements": list(_API_PRIVACY_REQUIREMENTS),
                "missing": ["account_zero_data_retention_verification"],
            }

    return {
        "state": "blocked",
        "dispatch_allowed": False,
        "reason_code": "privacy.route_policy_missing",
        "requirements": list(_API_PRIVACY_REQUIREMENTS),
        "missing": ["machine_checkable_provider_privacy_policy"],
    }


def _openrouter_posture(model_slug: str) -> dict[str, Any]:
    try:
        from runtime.openrouter_policy import strict_provider_policy

        provider_policy = strict_provider_policy(model_slug=model_slug)
    except Exception as exc:  # noqa: BLE001 - preserve policy failure as route posture
        return {
            "state": "blocked",
            "dispatch_allowed": False,
            "reason_code": getattr(exc, "reason_code", "privacy.openrouter_policy_unproven"),
            "decision_ref": OPENROUTER_PRIVACY_DECISION_REF,
            "requirements": list(_API_PRIVACY_REQUIREMENTS),
            "error": str(exc),
        }
    return {
        "state": "approved",
        "dispatch_allowed": True,
        "reason_code": "privacy.openrouter_request_policy_enforced",
        "decision_ref": OPENROUTER_PRIVACY_DECISION_REF,
        "requirements": list(_API_PRIVACY_REQUIREMENTS),
        "controls": {
            "provider_order": list(provider_policy.get("order") or []),
            "allow_fallbacks": provider_policy.get("allow_fallbacks"),
            "require_parameters": provider_policy.get("require_parameters"),
            "data_collection": provider_policy.get("data_collection"),
            "zdr": provider_policy.get("zdr"),
            "ignored_provider_slugs": list(provider_policy.get("ignore") or []),
        },
    }


def _is_kimi_model(model_slug: str) -> bool:
    normalized = model_slug.lower()
    return (
        "kimi-k2" in normalized
        or "kimi_k2" in normalized
        or "kimi-k2p6" in normalized
        or "kimi-k2.6" in normalized
    )


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = [
    "DIRECT_KIMI_PRIVACY_DECISION_REF",
    "OPENROUTER_PRIVACY_DECISION_REF",
    "provider_route_privacy_posture",
]
