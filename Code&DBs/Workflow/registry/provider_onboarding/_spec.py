"""Provider onboarding spec normalization — dataclasses, parsing, and validation."""

from __future__ import annotations

import json
import re
import os
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from adapters import provider_registry as provider_registry_mod

__all__ = [
    "ProviderOnboardingModelSpec",
    "ProviderOnboardingSpec",
    "ProviderOnboardingStepResult",
    "ProviderOnboardingResult",
    "ProviderTransportAuthorityTemplate",
    "ProviderAuthorityTemplate",
    "normalize_provider_onboarding_spec",
    "load_provider_onboarding_spec_from_file",
]

_DEFAULT_CONTEXT_WINDOW = 128_000
_LEGACY_CAPABILITY_TAG_BY_ROUTE_TIER = {
    "high": "frontier",
    "medium": "mid",
    "low": "economy",
}
_PROBE_PROMPT = "Reply with exactly PROVIDER_WIZARD_OK."
_PROBE_EXPECTED_RESPONSE = "PROVIDER_WIZARD_OK"
_VALID_LATENCY_CLASSES = {"reasoning", "instant"}
_VALID_ROUTE_TIERS = {"high", "medium", "low"}
_VALID_TRANSPORTS = {"cli", "api"}
_VALID_CLI_PROMPT_MODES = {"stdin", "argv"}


@dataclass(frozen=True, slots=True)
class ProviderOnboardingModelSpec:
    """A provider/model row the wizard should resolve and write."""

    model_slug: str
    route_tier: str | None = None
    route_tier_rank: int | None = None
    latency_class: str | None = None
    latency_rank: int | None = None
    context_window: int | None = None
    reasoning_control: Mapping[str, Any] = field(default_factory=dict)
    task_affinities: Mapping[str, Any] = field(default_factory=dict)
    benchmark_profile: Mapping[str, Any] = field(default_factory=dict)
    capability_tags: tuple[str, ...] = ()
    default_parameters: Mapping[str, Any] = field(default_factory=dict)
    status: str = "active"


@dataclass(frozen=True, slots=True)
class ProviderOnboardingSpec:
    """Provider onboarding payload used by the CLI and HTTP wizard."""

    provider_slug: str
    provider_name: str | None = None
    selected_transport: str | None = None
    binary_name: str | None = None
    base_flags: tuple[str, ...] = ()
    output_format: str | None = None
    output_envelope_key: str | None = None
    default_timeout: int | None = None
    model_flag: str | None = None
    system_prompt_flag: str | None = None
    json_schema_flag: str | None = None
    forbidden_flags: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    default_model: str | None = None
    requested_models: tuple[str, ...] = ()
    api_endpoint: str | None = None
    api_protocol_family: str | None = None
    api_key_env_vars: tuple[str, ...] = ()
    provider_api_key: str | None = None
    adapter_economics: dict[str, dict[str, Any]] = field(default_factory=dict)
    benchmark_source_slug: str | None = None
    benchmark_api_key: str | None = None
    cli_prompt_mode: str | None = None
    default_context_window: int | None = None
    provider_docs_url: str | None = None
    transport_docs_url: str | None = None
    models: tuple[ProviderOnboardingModelSpec, ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderOnboardingStepResult:
    """One wizard step result."""

    step: str
    status: str
    summary: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProviderOnboardingResult:
    """Structured onboarding result returned to the CLI and API."""

    ok: bool
    provider_slug: str
    provider_name: str
    decision_ref: str
    dry_run: bool
    steps: tuple[ProviderOnboardingStepResult, ...]
    provider_report: dict[str, Any] = field(default_factory=dict)
    model_reports: tuple[dict[str, Any], ...] = ()
    benchmark_report: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProviderTransportAuthorityTemplate:
    """Static connection authority for one provider transport."""

    transport: str
    supported: bool
    docs_url: str | None = None
    connection_hint: str | None = None
    binary_name: str | None = None
    base_flags: tuple[str, ...] = ()
    output_format: str = "json"
    output_envelope_key: str = "result"
    default_timeout: int = 300
    model_flag: str | None = "--model"
    system_prompt_flag: str | None = None
    json_schema_flag: str | None = None
    forbidden_flags: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    default_model: str | None = None
    api_endpoint: str | None = None
    api_protocol_family: str | None = None
    api_key_env_vars: tuple[str, ...] = ()
    cli_prompt_modes: tuple[str, ...] = ()
    discovery_strategy: str | None = None
    prompt_probe_strategy: str | None = None
    unsupported_reason: str | None = None
    default_context_window: int | None = None


@dataclass(frozen=True, slots=True)
class ProviderAuthorityTemplate:
    """Static provider onboarding authority."""

    provider_slug: str
    provider_name: str
    docs_url: str | None = None
    benchmark_source_slug: str | None = None
    default_context_window: int | None = None
    adapter_economics: dict[str, dict[str, Any]] = field(default_factory=dict)
    transports: dict[str, ProviderTransportAuthorityTemplate] = field(default_factory=dict)


def _jsonb(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return dict(value)


def _normalize_unique(items: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return tuple(ordered)


def _json_array(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field_name} must be a JSON array")
    return _normalize_unique([str(item) for item in value])


def _normalize_model_spec(raw: Mapping[str, Any]) -> ProviderOnboardingModelSpec:
    model_slug = str(raw.get("model_slug") or "").strip()
    if not model_slug:
        raise ValueError("models[].model_slug is required")

    route_tier_raw = raw.get("route_tier")
    route_tier: str | None = None
    if route_tier_raw not in (None, ""):
        route_tier = str(route_tier_raw).strip().lower()
        if route_tier not in _VALID_ROUTE_TIERS:
            raise ValueError(f"models[{model_slug}].route_tier must be one of high, medium, low")

    route_tier_rank_raw = raw.get("route_tier_rank")
    route_tier_rank: int | None = None
    if route_tier_rank_raw not in (None, ""):
        try:
            route_tier_rank = int(route_tier_rank_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"models[{model_slug}].route_tier_rank must be an integer") from exc
        if route_tier_rank < 1:
            raise ValueError(f"models[{model_slug}].route_tier_rank must be >= 1")

    latency_class_raw = raw.get("latency_class")
    latency_class: str | None = None
    if latency_class_raw not in (None, ""):
        latency_class = str(latency_class_raw).strip().lower()
        if latency_class not in _VALID_LATENCY_CLASSES:
            raise ValueError(
                f"models[{model_slug}].latency_class must be one of reasoning, instant"
            )

    latency_rank_raw = raw.get("latency_rank")
    latency_rank: int | None = None
    if latency_rank_raw not in (None, ""):
        try:
            latency_rank = int(latency_rank_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"models[{model_slug}].latency_rank must be an integer") from exc
        if latency_rank < 1:
            raise ValueError(f"models[{model_slug}].latency_rank must be >= 1")

    context_window_raw = raw.get("context_window")
    context_window: int | None = None
    if context_window_raw not in (None, ""):
        try:
            context_window = int(context_window_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"models[{model_slug}].context_window must be an integer") from exc
        if context_window < 1024:
            raise ValueError(f"models[{model_slug}].context_window must be >= 1024")

    return ProviderOnboardingModelSpec(
        model_slug=model_slug,
        route_tier=route_tier,
        route_tier_rank=route_tier_rank,
        latency_class=latency_class,
        latency_rank=latency_rank,
        context_window=context_window,
        reasoning_control=_json_object(
            raw.get("reasoning_control"),
            field_name=f"models[{model_slug}].reasoning_control",
        ),
        task_affinities=_json_object(
            raw.get("task_affinities"),
            field_name=f"models[{model_slug}].task_affinities",
        ),
        benchmark_profile=_json_object(
            raw.get("benchmark_profile"),
            field_name=f"models[{model_slug}].benchmark_profile",
        ),
        capability_tags=_json_array(
            raw.get("capability_tags"),
            field_name=f"models[{model_slug}].capability_tags",
        ),
        default_parameters=_json_object(
            raw.get("default_parameters"),
            field_name=f"models[{model_slug}].default_parameters",
        ),
        status=str(raw.get("status") or "active").strip().lower() or "active",
    )


def normalize_provider_onboarding_spec(raw: Mapping[str, Any]) -> ProviderOnboardingSpec:
    provider_raw = raw.get("provider") if isinstance(raw.get("provider"), Mapping) else raw
    if not isinstance(provider_raw, Mapping):
        raise ValueError("provider is required and must be an object")

    provider_slug = str(
        provider_raw.get("provider_slug") or raw.get("provider_slug") or ""
    ).strip().lower()
    if not provider_slug:
        raise ValueError("provider.provider_slug is required")

    selected_transport = (
        str(
            provider_raw.get("selected_transport")
            or provider_raw.get("transport")
            or raw.get("selected_transport")
            or raw.get("transport")
            or ""
        ).strip().lower()
        or None
    )
    if selected_transport is not None and selected_transport not in _VALID_TRANSPORTS:
        raise ValueError("provider.selected_transport must be either cli or api")

    requested_models = _json_array(
        raw.get("requested_models") or provider_raw.get("requested_models"),
        field_name="requested_models",
    )

    models_raw = raw.get("models")
    models: tuple[ProviderOnboardingModelSpec, ...] = ()
    if models_raw is not None:
        if not isinstance(models_raw, list):
            raise ValueError("models must be a JSON array when provided")
        models = tuple(
            _normalize_model_spec(item)
            for item in models_raw
            if isinstance(item, Mapping)
        )
        if len(models) != len(models_raw):
            raise ValueError("models must contain only objects")

    adapter_economics_raw = provider_raw.get("adapter_economics")
    adapter_economics: dict[str, dict[str, Any]] = {}
    if adapter_economics_raw is not None:
        raw_economics = _json_object(
            adapter_economics_raw,
            field_name="provider.adapter_economics",
        )
        for adapter_type, value in raw_economics.items():
            adapter_economics[str(adapter_type)] = _json_object(
                value,
                field_name=f"provider.adapter_economics.{adapter_type}",
            )

    default_context_window_raw = (
        provider_raw.get("default_context_window") or raw.get("default_context_window")
    )
    default_context_window: int | None = None
    if default_context_window_raw not in (None, ""):
        try:
            default_context_window = int(default_context_window_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("default_context_window must be an integer") from exc
        if default_context_window < 1024:
            raise ValueError("default_context_window must be >= 1024")

    cli_prompt_mode_raw = (
        provider_raw.get("cli_prompt_mode")
        if provider_raw.get("cli_prompt_mode") not in (None, "")
        else raw.get("cli_prompt_mode")
    )
    cli_prompt_mode: str | None = None
    if cli_prompt_mode_raw not in (None, ""):
        cli_prompt_mode = str(cli_prompt_mode_raw).strip().lower()
        if cli_prompt_mode not in _VALID_CLI_PROMPT_MODES:
            raise ValueError("provider.cli_prompt_mode must be either stdin or argv")

    return ProviderOnboardingSpec(
        provider_slug=provider_slug,
        provider_name=(
            str(provider_raw.get("provider_name")).strip()
            if provider_raw.get("provider_name") not in (None, "")
            else None
        ),
        selected_transport=selected_transport,
        binary_name=(
            str(provider_raw.get("binary_name")).strip()
            if provider_raw.get("binary_name") not in (None, "")
            else None
        ),
        base_flags=_json_array(
            provider_raw.get("base_flags"),
            field_name="provider.base_flags",
        ),
        output_format=(
            str(provider_raw.get("output_format")).strip()
            if provider_raw.get("output_format") not in (None, "")
            else None
        ),
        output_envelope_key=(
            str(provider_raw.get("output_envelope_key")).strip()
            if provider_raw.get("output_envelope_key") not in (None, "")
            else None
        ),
        default_timeout=(
            int(provider_raw.get("default_timeout"))
            if provider_raw.get("default_timeout") not in (None, "")
            else None
        ),
        model_flag=(
            provider_raw.get("model_flag")
            if provider_raw.get("model_flag") is None
            else str(provider_raw.get("model_flag")).strip() or None
        ),
        system_prompt_flag=(
            provider_raw.get("system_prompt_flag")
            if provider_raw.get("system_prompt_flag") is None
            else str(provider_raw.get("system_prompt_flag")).strip() or None
        ),
        json_schema_flag=(
            provider_raw.get("json_schema_flag")
            if provider_raw.get("json_schema_flag") is None
            else str(provider_raw.get("json_schema_flag")).strip() or None
        ),
        forbidden_flags=_json_array(
            provider_raw.get("forbidden_flags"),
            field_name="provider.forbidden_flags",
        ),
        aliases=_json_array(
            provider_raw.get("aliases"),
            field_name="provider.aliases",
        ),
        default_model=(
            str(provider_raw.get("default_model") or raw.get("default_model")).strip()
            if (provider_raw.get("default_model") or raw.get("default_model")) not in (None, "")
            else None
        ),
        requested_models=requested_models,
        api_endpoint=(
            str(provider_raw.get("api_endpoint")).strip()
            if provider_raw.get("api_endpoint") not in (None, "")
            else None
        ),
        api_protocol_family=(
            str(provider_raw.get("api_protocol_family")).strip()
            if provider_raw.get("api_protocol_family") not in (None, "")
            else None
        ),
        api_key_env_vars=_json_array(
            provider_raw.get("api_key_env_vars"),
            field_name="provider.api_key_env_vars",
        ),
        provider_api_key=(
            str(provider_raw.get("provider_api_key") or raw.get("provider_api_key")).strip()
            if (provider_raw.get("provider_api_key") or raw.get("provider_api_key"))
            not in (None, "")
            else None
        ),
        adapter_economics=adapter_economics,
        benchmark_source_slug=(
            str(raw.get("benchmark_source_slug")).strip()
            if raw.get("benchmark_source_slug") not in (None, "")
            else None
        ),
        benchmark_api_key=(
            str(raw.get("benchmark_api_key")).strip()
            if raw.get("benchmark_api_key") not in (None, "")
            else None
        ),
        cli_prompt_mode=cli_prompt_mode,
        default_context_window=default_context_window,
        provider_docs_url=(
            str(provider_raw.get("provider_docs_url")).strip()
            if provider_raw.get("provider_docs_url") not in (None, "")
            else None
        ),
        transport_docs_url=(
            str(provider_raw.get("transport_docs_url")).strip()
            if provider_raw.get("transport_docs_url") not in (None, "")
            else None
        ),
        models=models,
    )


def load_provider_onboarding_spec_from_file(path: str | Path) -> ProviderOnboardingSpec:
    spec_path = Path(path)
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("provider onboarding spec must be a JSON object")
    return normalize_provider_onboarding_spec(payload)


def _slug_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")


def _normalized_slug(value: str) -> str:
    return _slug_token(value)


def _family_slug(value: str) -> str:
    tokens = [token for token in _normalized_slug(value).split("-") if token]
    if not tokens:
        return ""
    filtered = [
        token
        for token in tokens
        if not re.fullmatch(r"\d{3,8}", token)
    ]
    return "-".join(filtered or tokens)


def _candidate_ref(provider_slug: str, model_slug: str) -> str:
    return f"candidate.{provider_slug}.{model_slug}"


def _provider_ref(provider_slug: str) -> str:
    return f"provider.{provider_slug}"


def _model_profile_id(provider_slug: str, model_slug: str) -> str:
    return f"model_profile.provider-onboarding.{provider_slug}.{_slug_token(model_slug)}"


def _model_profile_name(provider_slug: str, model_slug: str) -> str:
    return f"{provider_slug}.{model_slug}"


def _binding_id(provider_slug: str, model_slug: str) -> str:
    return f"binding.provider-onboarding.{provider_slug}.{_slug_token(model_slug)}"


def _rule_id(source_slug: str, provider_slug: str, model_slug: str) -> str:
    return (
        "provider_model_market_match_rule."
        f"{source_slug}.{provider_slug}.{_slug_token(model_slug)}"
    )


def _priority_for(route_tier: str, route_tier_rank: int) -> int:
    base = {"high": 500, "medium": 700, "low": 900}.get(route_tier, 1000)
    return base + max(0, int(route_tier_rank) - 1)


def _balance_weight_for(route_tier: str) -> int:
    return {"high": 1, "medium": 2, "low": 3}.get(route_tier, 1)


def _find_binary(binary_name: str | None) -> str | None:
    normalized = (binary_name or "").strip()
    if not normalized:
        return None
    found = shutil.which(normalized)
    if found:
        return found
    return None


def _resolve_explicit_or_env_secret(
    *,
    explicit_value: str | None,
    env_vars: Sequence[str],
) -> tuple[str | None, str | None, str | None]:
    if isinstance(explicit_value, str) and explicit_value.strip():
        return explicit_value.strip(), "explicit", None
    for env_var in env_vars:
        value = os.environ.get(env_var, "").strip()
        if value:
            return value, f"env:{env_var}", env_var
    from adapters.keychain import keychain_get
    for env_var in env_vars:
        value = keychain_get(env_var)
        if value:
            return value, f"keychain:{env_var}", env_var
    return None, None, (env_vars[0] if env_vars else None)


def _provider_cli_profile_payload(spec: ProviderOnboardingSpec) -> dict[str, Any]:
    return {
        "provider_slug": spec.provider_slug,
        "binary_name": spec.binary_name,
        "base_flags": list(spec.base_flags),
        "model_flag": spec.model_flag,
        "system_prompt_flag": spec.system_prompt_flag,
        "json_schema_flag": spec.json_schema_flag,
        "output_format": spec.output_format,
        "output_envelope_key": spec.output_envelope_key,
        "forbidden_flags": list(spec.forbidden_flags),
        "default_timeout": spec.default_timeout,
        "aliases": list(spec.aliases),
        "status": "active",
        "default_model": spec.default_model,
        "api_endpoint": spec.api_endpoint,
        "api_protocol_family": spec.api_protocol_family,
        "api_key_env_vars": list(spec.api_key_env_vars),
        "adapter_economics": dict(spec.adapter_economics),
        "prompt_mode": spec.cli_prompt_mode or "stdin",
    }


def _capability_tags_for(
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
) -> tuple[str, ...]:
    legacy = _LEGACY_CAPABILITY_TAG_BY_ROUTE_TIER.get(str(model.route_tier or ""))
    tags = [
        legacy,
        model.latency_class,
        "provider-onboarding",
        spec.provider_slug,
        model.model_slug,
        *model.capability_tags,
    ]
    return _normalize_unique([str(tag) for tag in tags if tag])


def _cli_config_for(
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
) -> dict[str, Any]:
    if spec.selected_transport != "cli":
        return {}
    prompt_mode = spec.cli_prompt_mode or "stdin"
    cmd_template = [str(spec.binary_name or ""), *spec.base_flags]
    if spec.model_flag:
        cmd_template.extend([spec.model_flag, "{model}"])
    if prompt_mode == "argv":
        cmd_template.append("{prompt}")
    return {
        "prompt_mode": prompt_mode,
        "cmd_template": cmd_template,
        "envelope_key": spec.output_envelope_key,
        "output_format": spec.output_format,
        "provider_slug": spec.provider_slug,
        "model_slug": model.model_slug,
        "binary_name": spec.binary_name,
        "system_prompt_flag": spec.system_prompt_flag,
        "json_schema_flag": spec.json_schema_flag,
    }


def _adapter_type_for_transport(transport: str) -> str:
    normalized = str(transport or "").strip().lower()
    return "cli_llm" if normalized == "cli" else "llm_task"


def _transport_kind_for_transport(transport: str) -> str:
    normalized = str(transport or "").strip().lower()
    return "cli" if normalized == "cli" else "http"


def _execution_topology_for_transport(transport: str) -> str:
    normalized = str(transport or "").strip().lower()
    return "local_cli" if normalized == "cli" else "direct_http"


def _provider_transport_admission_id(provider_slug: str, adapter_type: str) -> str:
    return f"provider_transport_admission.{provider_slug}.{adapter_type}"


def _provider_transport_probe_receipt_id(
    provider_slug: str,
    adapter_type: str,
    decision_ref: str,
    index: int,
    step: str,
) -> str:
    return (
        "provider_transport_probe_receipt."
        f"{provider_slug}.{adapter_type}.{_slug_token(decision_ref)}.{index:02d}.{_slug_token(step)}"
    )


def _selected_lane_probe_contract(
    *,
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
    transport_step: ProviderOnboardingStepResult,
    model_step: ProviderOnboardingStepResult | None,
    capacity_step: ProviderOnboardingStepResult | None,
    selected_models: Sequence[ProviderOnboardingModelSpec],
    router_supported: bool | None,
) -> dict[str, Any]:
    return {
        "model_discovery_probe": {
            "strategy": transport_template.discovery_strategy,
            "requested_models": list(spec.requested_models),
            "selected_models": [model.model_slug for model in selected_models],
            "status": model_step.status if model_step is not None else "not_run",
        },
        "prompt_probe": {
            "strategy": transport_template.prompt_probe_strategy,
            "prompt_mode": (
                capacity_step.details.get("prompt_mode")
                if capacity_step is not None
                else spec.cli_prompt_mode
            ),
            "status": capacity_step.status if capacity_step is not None else "not_run",
        },
        "sandbox_probe": {
            "execution_topology": _execution_topology_for_transport(spec.selected_transport or ""),
            "workspace_boundary": "runtime_profile_workspace",
            "status": transport_step.status,
        },
        "router_probe": {
            "selected_transport_supported": router_supported,
        },
    }


def _utc_now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)


def _fallback_template(provider_slug: str) -> ProviderAuthorityTemplate | None:
    profile = provider_registry_mod.get_profile(provider_slug)
    if profile is None:
        return None
    return ProviderAuthorityTemplate(
        provider_slug=provider_slug,
        provider_name=provider_slug.title(),
        docs_url=None,
        benchmark_source_slug="artificial_analysis",
        default_context_window=_DEFAULT_CONTEXT_WINDOW,
        adapter_economics=dict(profile.adapter_economics or {}),
        transports={
            "cli": ProviderTransportAuthorityTemplate(
                transport="cli",
                supported=bool(profile.binary),
                docs_url=None,
                binary_name=profile.binary,
                base_flags=tuple(profile.base_flags),
                output_format=profile.output_format,
                output_envelope_key=profile.output_envelope_key,
                default_timeout=profile.default_timeout,
                model_flag=profile.model_flag,
                system_prompt_flag=profile.system_prompt_flag,
                json_schema_flag=profile.json_schema_flag,
                forbidden_flags=tuple(profile.forbidden_flags),
                aliases=tuple(profile.aliases),
                default_model=profile.default_model,
                api_key_env_vars=tuple(profile.api_key_env_vars),
                cli_prompt_modes=_normalize_unique(
                    [profile.prompt_mode or "", "stdin", "argv"]
                ),
                prompt_probe_strategy="cli_headless_prompt",
                default_context_window=_DEFAULT_CONTEXT_WINDOW,
            ),
            "api": ProviderTransportAuthorityTemplate(
                transport="api",
                supported=bool(profile.api_endpoint and profile.api_protocol_family),
                docs_url=None,
                api_endpoint=profile.api_endpoint,
                api_protocol_family=profile.api_protocol_family,
                api_key_env_vars=tuple(profile.api_key_env_vars),
                default_model=profile.default_model,
                discovery_strategy=_api_model_discovery_strategy_for(profile.api_protocol_family),
                prompt_probe_strategy=_api_prompt_probe_strategy_for(profile.api_protocol_family),
                unsupported_reason=(
                    None
                    if profile.api_endpoint and profile.api_protocol_family
                    else "This provider does not expose an admitted llm_task transport in the registry yet."
                ),
                default_context_window=_DEFAULT_CONTEXT_WINDOW,
            ),
        },
    )


def _provider_template(provider_slug: str) -> ProviderAuthorityTemplate:
    fallback = _fallback_template(provider_slug)
    if fallback is None:
        raise ValueError(f"no onboarding authority template is registered for {provider_slug}")
    return fallback


def _resolve_spec(
    spec: ProviderOnboardingSpec,
) -> tuple[
    ProviderOnboardingSpec,
    ProviderAuthorityTemplate,
    ProviderTransportAuthorityTemplate,
    ProviderOnboardingStepResult,
]:
    from dataclasses import replace

    template = _provider_template(spec.provider_slug)
    selected_transport = (spec.selected_transport or "").strip().lower()
    if not selected_transport:
        raise ValueError(
            f"provider.selected_transport is required for {spec.provider_slug}; choose cli or api"
        )
    transport_template = template.transports.get(selected_transport)
    if transport_template is None:
        raise ValueError(
            f"{spec.provider_slug} does not declare transport {selected_transport!r}; "
            f"choose from {sorted(template.transports)}"
        )

    resolved = replace(
        spec,
        provider_name=spec.provider_name or template.provider_name,
        selected_transport=selected_transport,
        binary_name=spec.binary_name or transport_template.binary_name,
        base_flags=spec.base_flags or transport_template.base_flags,
        output_format=spec.output_format or transport_template.output_format,
        output_envelope_key=spec.output_envelope_key or transport_template.output_envelope_key,
        default_timeout=int(spec.default_timeout or transport_template.default_timeout),
        model_flag=spec.model_flag if spec.model_flag is not None else transport_template.model_flag,
        system_prompt_flag=(
            spec.system_prompt_flag
            if spec.system_prompt_flag is not None
            else transport_template.system_prompt_flag
        ),
        json_schema_flag=(
            spec.json_schema_flag
            if spec.json_schema_flag is not None
            else transport_template.json_schema_flag
        ),
        forbidden_flags=(
            spec.forbidden_flags
            if spec.forbidden_flags
            else transport_template.forbidden_flags
        ),
        aliases=_normalize_unique([*transport_template.aliases, *spec.aliases]),
        default_model=spec.default_model or transport_template.default_model,
        api_endpoint=spec.api_endpoint or transport_template.api_endpoint,
        api_protocol_family=spec.api_protocol_family or transport_template.api_protocol_family,
        api_key_env_vars=spec.api_key_env_vars or transport_template.api_key_env_vars,
        adapter_economics=spec.adapter_economics or dict(template.adapter_economics),
        benchmark_source_slug=spec.benchmark_source_slug or template.benchmark_source_slug,
        default_context_window=(
            spec.default_context_window
            or transport_template.default_context_window
            or template.default_context_window
            or _DEFAULT_CONTEXT_WINDOW
        ),
        provider_docs_url=spec.provider_docs_url or template.docs_url,
        transport_docs_url=spec.transport_docs_url or transport_template.docs_url,
    )
    economics = resolved.adapter_economics.get(
        "cli_llm" if selected_transport == "cli" else "llm_task",
        {},
    )
    step = ProviderOnboardingStepResult(
        step="authority_lookup",
        status="succeeded",
        summary=f"Resolved {resolved.provider_slug} {selected_transport} authority and connection contract",
        details={
            "provider_slug": resolved.provider_slug,
            "provider_name": resolved.provider_name,
            "selected_transport": selected_transport,
            "provider_docs_url": resolved.provider_docs_url,
            "transport_docs_url": resolved.transport_docs_url,
            "supported_transports": sorted(template.transports),
            "binary_name": resolved.binary_name,
            "base_flags": list(resolved.base_flags),
            "default_model": resolved.default_model,
            "api_endpoint": resolved.api_endpoint,
            "api_protocol_family": resolved.api_protocol_family,
            "api_key_env_vars": list(resolved.api_key_env_vars),
            "cli_prompt_mode": resolved.cli_prompt_mode,
            "cli_prompt_probe_modes": (
                list(_candidate_cli_prompt_modes(resolved, transport_template))
                if selected_transport == "cli"
                else []
            ),
            "discovery_strategy": transport_template.discovery_strategy,
            "prompt_probe_strategy": transport_template.prompt_probe_strategy,
            "connection_hint": transport_template.connection_hint,
            "billing_mode": economics.get("billing_mode"),
            "budget_bucket": economics.get("budget_bucket"),
            "effective_marginal_cost": economics.get("effective_marginal_cost"),
            "default_context_window": resolved.default_context_window,
        },
    )
    return resolved, template, transport_template, step


def _run_command(
    cmd: Sequence[str],
    *,
    env: Mapping[str, str],
    input_text: str | None = None,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        input=input_text,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        env=dict(env),
    )


def _candidate_cli_prompt_modes(
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
) -> tuple[str, ...]:
    explicit = (spec.cli_prompt_mode or "").strip().lower()
    if explicit:
        return (explicit,)
    declared = tuple(
        mode.strip().lower()
        for mode in transport_template.cli_prompt_modes
        if mode and mode.strip().lower() in _VALID_CLI_PROMPT_MODES
    )
    if declared:
        return _normalize_unique(declared)
    return ("stdin", "argv")


def _api_model_discovery_strategy_for(protocol_family: str | None) -> str | None:
    normalized = (protocol_family or "").strip().lower()
    return {
        "openai_chat_completions": "openai_models_list",
        "anthropic_messages": "anthropic_models_list",
        "google_generate_content": "google_models_list",
    }.get(normalized)


def _api_prompt_probe_strategy_for(protocol_family: str | None) -> str | None:
    if _api_model_discovery_strategy_for(protocol_family):
        return "api_llm_request"
    return None


def _planned_step(step: str, summary: str, *, details: dict[str, Any] | None = None) -> ProviderOnboardingStepResult:
    return ProviderOnboardingStepResult(
        step=step,
        status="planned",
        summary=summary,
        details=details or {},
    )


def _skipped_step(step: str, summary: str) -> ProviderOnboardingStepResult:
    return ProviderOnboardingStepResult(
        step=step,
        status="skipped",
        summary=summary,
        details={},
    )
