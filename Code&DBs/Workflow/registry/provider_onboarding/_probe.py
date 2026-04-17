"""Provider onboarding transport and model probing — discovery, capacity checks."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from adapters.keychain import resolve_secret

from ._spec import (
    ProviderOnboardingModelSpec,
    ProviderOnboardingSpec,
    ProviderOnboardingStepResult,
    ProviderTransportAuthorityTemplate,
    _DEFAULT_CONTEXT_WINDOW,
    _PROBE_EXPECTED_RESPONSE,
    _PROBE_PROMPT,
    _VALID_CLI_PROMPT_MODES,
    _api_model_discovery_strategy_for,
    _api_prompt_probe_strategy_for,
    _candidate_cli_prompt_modes,
    _find_binary as _find_binary_default,
    _normalize_unique,
    _resolve_explicit_or_env_secret,
    _run_command as _run_command_default,
)

__all__ = [
    "_probe_transport",
    "_probe_models",
    "_probe_capacity",
    "_discover_api_models",
    "_http_get_json",
    "_extract_cli_response_text",
    "_build_cli_probe_command",
    "_resolved_api_endpoint",
    "_resolve_api_key_from_env",
    "_requested_or_overridden_models",
    "_infer_route_tier",
    "_infer_latency_class",
    "_default_task_affinities",
    "_resolve_models",
]


def _find_binary(binary_name):
    return _find_binary_default(binary_name)


def _run_command(cmd, *, env, input_text=None, timeout_seconds):
    return _run_command_default(cmd, env=env, input_text=input_text, timeout_seconds=timeout_seconds)


def _http_get_json(
    url: str,
    *,
    headers: Mapping[str, str],
    timeout_seconds: int,
) -> dict[str, Any]:
    return _http_get_json_impl(url, headers=headers, timeout_seconds=timeout_seconds)


def _http_get_json_impl(
    url: str,
    *,
    headers: Mapping[str, str],
    timeout_seconds: int,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers=dict(headers),
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {exc.code}: {error_body[:500]}") from exc
    except (urllib.error.URLError, OSError) as exc:
        raise RuntimeError(str(exc)) from exc

    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"invalid JSON response from {url}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"expected JSON object from {url}")
    return data


# ---------------------------------------------------------------------------
# Pure helpers (not monkeypatched)
# ---------------------------------------------------------------------------

def _resolved_api_endpoint(spec: ProviderOnboardingSpec, model_slug: str | None) -> str | None:
    endpoint = (spec.api_endpoint or "").strip()
    if not endpoint:
        return None
    if "{model}" not in endpoint:
        return endpoint
    model = (model_slug or spec.default_model or "").strip()
    if not model:
        return None
    return endpoint.format(model=model)


def _resolve_api_key_from_env(
    spec: ProviderOnboardingSpec,
    *,
    env: Mapping[str, str],
) -> str | None:
    for env_var in spec.api_key_env_vars:
        value = str(resolve_secret(env_var, env=dict(env)) or "").strip()
        if value:
            return value
    return None


def _extract_cli_response_text(
    *,
    stdout: str,
    output_format: str,
    envelope_key: str,
) -> str:
    text = stdout.strip()
    if not text:
        return ""
    if output_format == "json":
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(payload, dict):
            value = payload.get(envelope_key)
            if isinstance(value, str):
                return value
        return text
    if output_format == "ndjson":
        extracted = ""
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                value = payload.get(envelope_key)
                if isinstance(value, str):
                    extracted = value
        return extracted or text
    return text


def _build_cli_probe_command(
    *,
    spec: ProviderOnboardingSpec,
    binary_path: str,
    model_slug: str,
    prompt_mode: str,
    prompt_text: str,
) -> tuple[list[str], str | None]:
    cmd = [binary_path, *spec.base_flags]
    if spec.model_flag:
        cmd.extend([spec.model_flag, model_slug])
    if prompt_mode == "argv":
        cmd.append(prompt_text)
        return cmd, None
    return cmd, prompt_text


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _discover_api_models(
    spec: ProviderOnboardingSpec,
    *,
    env: Mapping[str, str],
    transport_details: Mapping[str, Any],
) -> tuple[str, ...]:
    return _discover_api_models_impl(spec, env=env, transport_details=transport_details)


def _discover_api_models_impl(
    spec: ProviderOnboardingSpec,
    *,
    env: Mapping[str, str],
    transport_details: Mapping[str, Any],
) -> tuple[str, ...]:
    strategy = str(
        transport_details.get("discovery_strategy")
        or _api_model_discovery_strategy_for(spec.api_protocol_family)
        or ""
    ).strip()
    if not strategy:
        return ()

    api_key = _resolve_api_key_from_env(spec, env=env)
    if not api_key:
        raise RuntimeError(f"No API key resolved for {spec.provider_slug}")

    timeout_seconds = min(int(spec.default_timeout or 60), 60)
    if strategy == "openai_models_list":
        # Derive models URL from the provider's own endpoint.
        # e.g. https://api.deepseek.com/v1/chat/completions → https://api.deepseek.com/v1/models
        models_url = "https://api.openai.com/v1/models"
        provider_endpoint = (spec.api_endpoint or "").strip()
        if provider_endpoint:
            base = provider_endpoint.split("/v1/")[0] if "/v1/" in provider_endpoint else provider_endpoint.rstrip("/")
            models_url = f"{base}/v1/models"
        data = _http_get_json(
            models_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout_seconds=timeout_seconds,
        )
        return _normalize_unique(
            [
                str(item.get("id") or "").strip()
                for item in data.get("data", [])
                if isinstance(item, dict)
            ]
        )

    if strategy == "anthropic_models_list":
        data = _http_get_json(
            "https://api.anthropic.com/v1/models",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            timeout_seconds=timeout_seconds,
        )
        return _normalize_unique(
            [
                str(item.get("id") or "").strip()
                for item in data.get("data", [])
                if isinstance(item, dict)
            ]
        )

    if strategy == "google_models_list":
        models: list[str] = []
        page_token = ""
        max_pages = 5
        for _ in range(max_pages):
            query = {"key": api_key, "pageSize": "1000"}
            if page_token:
                query["pageToken"] = page_token
            url = "https://generativelanguage.googleapis.com/v1beta/models?" + urllib.parse.urlencode(query)
            data = _http_get_json(url, headers={}, timeout_seconds=timeout_seconds)
            for item in data.get("models", []):
                if not isinstance(item, dict):
                    continue
                supported_methods = {
                    str(method).strip()
                    for method in item.get("supportedGenerationMethods", [])
                    if str(method).strip()
                }
                if supported_methods and "generateContent" not in supported_methods:
                    continue
                slug = (
                    str(item.get("baseModelId") or "").strip()
                    or str(item.get("name") or "").strip().split("/", 1)[-1]
                )
                if slug:
                    models.append(slug)
            page_token = str(data.get("nextPageToken") or "").strip()
            if not page_token:
                break
        return _normalize_unique(models)

    if strategy == "cursor_models_list":
        data = _http_get_json(
            "https://api.cursor.com/v0/models",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout_seconds=timeout_seconds,
        )
        return _normalize_unique(
            [
                str(model_slug).strip()
                for model_slug in data.get("models", [])
                if str(model_slug).strip()
            ]
        )

    raise RuntimeError(f"Unsupported API discovery strategy: {strategy}")


# ---------------------------------------------------------------------------
# Transport probe
# ---------------------------------------------------------------------------

def _probe_transport(
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
) -> tuple[ProviderOnboardingStepResult, dict[str, str]]:
    credential, credential_source, missing_env_var = _resolve_explicit_or_env_secret(
        explicit_value=spec.provider_api_key,
        env_vars=spec.api_key_env_vars,
    )
    env = dict(os.environ)
    if credential and spec.api_key_env_vars:
        env[spec.api_key_env_vars[0]] = credential

    if spec.selected_transport == "cli":
        binary_path = _find_binary(spec.binary_name)
        if not transport_template.supported:
            return (
                ProviderOnboardingStepResult(
                    step="transport_probe",
                    status="failed",
                    summary=(
                        transport_template.unsupported_reason
                        or f"{spec.provider_slug} does not declare a supported CLI transport"
                    ),
                    details={
                        "selected_transport": spec.selected_transport,
                        "binary_name": spec.binary_name,
                        "docs_url": spec.transport_docs_url,
                    },
                ),
                env,
            )
        if binary_path is None:
            return (
                ProviderOnboardingStepResult(
                    step="transport_probe",
                    status="failed",
                    summary=f"Could not find {spec.binary_name} on PATH",
                    details={
                        "selected_transport": spec.selected_transport,
                        "binary_name": spec.binary_name,
                        "docs_url": spec.transport_docs_url,
                    },
                ),
                env,
            )
        return (
            ProviderOnboardingStepResult(
                step="transport_probe",
                status="succeeded",
                summary=(
                    f"CLI transport is available via {binary_path}; "
                    "authentication will be proven by the live model probes"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "binary_path": binary_path,
                    "base_flags": list(spec.base_flags),
                    "credential_source": credential_source or "ambient_cli_session",
                    "required_env_vars": list(spec.api_key_env_vars),
                    "docs_url": spec.transport_docs_url,
                },
            ),
            env,
        )

    if not transport_template.supported:
        return (
            ProviderOnboardingStepResult(
                step="transport_probe",
                status="failed",
                summary=(
                    transport_template.unsupported_reason
                    or f"{spec.provider_slug} does not declare a supported API transport"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "api_endpoint": spec.api_endpoint,
                    "api_protocol_family": spec.api_protocol_family,
                    "docs_url": spec.transport_docs_url,
                },
            ),
            env,
        )
    if not spec.api_endpoint or not spec.api_protocol_family:
        return (
            ProviderOnboardingStepResult(
                step="transport_probe",
                status="failed",
                summary=(
                    f"{spec.provider_slug} API transport is missing endpoint or protocol metadata"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "api_endpoint": spec.api_endpoint,
                    "api_protocol_family": spec.api_protocol_family,
                },
            ),
            env,
        )
    if spec.api_key_env_vars and credential is None:
        return (
            ProviderOnboardingStepResult(
                step="transport_probe",
                status="failed",
                summary=(
                    f"No credential configured for {spec.provider_slug}. "
                    f"Go get a key from that site and set {missing_env_var} "
                    "or pass provider_api_key."
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "api_endpoint": spec.api_endpoint,
                    "api_protocol_family": spec.api_protocol_family,
                    "required_env_vars": list(spec.api_key_env_vars),
                    "docs_url": spec.transport_docs_url,
                },
            ),
            env,
        )
    return (
        ProviderOnboardingStepResult(
            step="transport_probe",
            status="succeeded",
            summary=f"API transport metadata is ready for {spec.provider_slug}",
            details={
                "selected_transport": spec.selected_transport,
                "api_endpoint": spec.api_endpoint,
                "api_protocol_family": spec.api_protocol_family,
                "credential_source": credential_source,
                "required_env_vars": list(spec.api_key_env_vars),
                "discovery_strategy": transport_template.discovery_strategy,
                "prompt_probe_strategy": transport_template.prompt_probe_strategy,
                "docs_url": spec.transport_docs_url,
            },
        ),
        env,
    )


# ---------------------------------------------------------------------------
# Model resolution helpers
# ---------------------------------------------------------------------------

def _requested_or_overridden_models(spec: ProviderOnboardingSpec) -> tuple[str, ...]:
    ordered = list(spec.requested_models)
    for model in spec.models:
        ordered.append(model.model_slug)
    if spec.default_model:
        ordered.insert(0, spec.default_model)
    return _normalize_unique(ordered)


def _infer_route_tier(provider_slug: str, model_slug: str, *, is_default: bool) -> str:
    normalized = model_slug.lower()
    if any(token in normalized for token in ("mini", "fast", "flash", "lite", "nano")):
        return "low"
    if is_default or any(token in normalized for token in ("thinking", "opus", "sonnet", "composer")):
        return "high"
    return "medium"


def _infer_latency_class(provider_slug: str, model_slug: str) -> str:
    normalized = model_slug.lower()
    if any(token in normalized for token in ("mini", "fast", "flash", "lite", "nano")):
        return "instant"
    return "reasoning"


def _default_task_affinities(
    provider_slug: str,
    model_slug: str,
    *,
    route_tier: str,
    latency_class: str,
) -> dict[str, Any]:
    if route_tier == "high":
        return {
            "primary": ["build", "review", "architecture"],
            "secondary": ["analysis", "research"],
            "specialized": [],
            "avoid": [],
        }
    if latency_class == "instant":
        return {
            "primary": ["build", "chat", "analysis"],
            "secondary": ["review", "wiring"],
            "specialized": [],
            "avoid": [],
        }
    return {
        "primary": ["build"],
        "secondary": ["review", "analysis"],
        "specialized": [],
        "avoid": [],
    }


def _resolve_models(
    spec: ProviderOnboardingSpec,
    *,
    selected_model_slugs: Sequence[str],
) -> tuple[ProviderOnboardingModelSpec, ...]:
    overrides = {model.model_slug: model for model in spec.models}
    default_context_window = int(spec.default_context_window or _DEFAULT_CONTEXT_WINDOW)
    ranked: list[ProviderOnboardingModelSpec] = []
    route_counts: defaultdict[str, int] = defaultdict(int)
    latency_counts: defaultdict[str, int] = defaultdict(int)
    for model_slug in selected_model_slugs:
        override = overrides.get(model_slug)
        is_default = model_slug == spec.default_model
        route_tier = (
            override.route_tier
            if override is not None and override.route_tier
            else _infer_route_tier(spec.provider_slug, model_slug, is_default=is_default)
        )
        latency_class = (
            override.latency_class
            if override is not None and override.latency_class
            else _infer_latency_class(spec.provider_slug, model_slug)
        )
        route_counts[route_tier] += 1
        latency_counts[latency_class] += 1
        context_window = (
            override.context_window
            if override is not None and override.context_window is not None
            else default_context_window
        )
        default_parameters = {
            "context_window": context_window,
            "provider_slug": spec.provider_slug,
            "model_slug": model_slug,
            "selected_transport": spec.selected_transport,
        }
        if override is not None:
            default_parameters.update(dict(override.default_parameters))
        ranked.append(
            ProviderOnboardingModelSpec(
                model_slug=model_slug,
                route_tier=route_tier,
                route_tier_rank=(
                    override.route_tier_rank
                    if override is not None and override.route_tier_rank is not None
                    else route_counts[route_tier]
                ),
                latency_class=latency_class,
                latency_rank=(
                    override.latency_rank
                    if override is not None and override.latency_rank is not None
                    else latency_counts[latency_class]
                ),
                context_window=context_window,
                reasoning_control=(
                    dict(override.reasoning_control) if override is not None else {}
                ),
                task_affinities=(
                    dict(override.task_affinities)
                    if override is not None and override.task_affinities
                    else _default_task_affinities(
                        spec.provider_slug,
                        model_slug,
                        route_tier=route_tier,
                        latency_class=latency_class,
                    )
                ),
                benchmark_profile=(
                    dict(override.benchmark_profile) if override is not None else {}
                ),
                capability_tags=(
                    tuple(override.capability_tags) if override is not None else ()
                ),
                default_parameters=default_parameters,
                status=(override.status if override is not None else "active"),
            )
        )
    return tuple(ranked)


# ---------------------------------------------------------------------------
# Model probe
# ---------------------------------------------------------------------------

def _probe_models(
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
    *,
    env: Mapping[str, str],
    transport_details: Mapping[str, Any],
) -> tuple[ProviderOnboardingStepResult, tuple[ProviderOnboardingModelSpec, ...]]:
    requested_or_overridden = _requested_or_overridden_models(spec)
    discovered_models: tuple[str, ...] = ()

    if spec.selected_transport == "api":
        try:
            discovered_models = _discover_api_models(
                spec,
                env=env,
                transport_details=transport_details,
            )
        except Exception as exc:
            return (
                ProviderOnboardingStepResult(
                    step="model_probe",
                    status="failed",
                    summary=f"Model discovery failed for {spec.provider_slug}: {exc}",
                    details={
                        "selected_transport": spec.selected_transport,
                        "api_endpoint": spec.api_endpoint,
                        "api_protocol_family": spec.api_protocol_family,
                        "discovery_strategy": transport_details.get("discovery_strategy"),
                    },
                ),
                (),
            )
    elif requested_or_overridden:
        discovered_models = requested_or_overridden
    elif spec.default_model:
        discovered_models = (spec.default_model,)

    if not discovered_models:
        return (
            ProviderOnboardingStepResult(
                step="model_probe",
                status="failed",
                summary=(
                    f"No models could be discovered for {spec.provider_slug}; "
                    "pass requested_models or model overrides."
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "discovery_strategy": transport_template.discovery_strategy,
                },
            ),
            (),
        )

    selected_model_slugs = discovered_models
    missing_requested = [
        model_slug
        for model_slug in requested_or_overridden
        if model_slug not in discovered_models
    ]
    if requested_or_overridden and not missing_requested:
        selected_model_slugs = tuple(
            model_slug for model_slug in requested_or_overridden if model_slug in discovered_models
        )

    if missing_requested:
        return (
            ProviderOnboardingStepResult(
                step="model_probe",
                status="failed",
                summary=(
                    f"Requested models are not available for {spec.provider_slug}: "
                    + ", ".join(missing_requested)
                ),
                details={
                    "requested_models": list(requested_or_overridden),
                    "discovered_models": list(discovered_models),
                },
            ),
            (),
        )

    default_model = spec.default_model
    if not default_model or default_model not in selected_model_slugs:
        default_model = selected_model_slugs[0]
        spec = replace(spec, default_model=default_model)

    resolved_models = _resolve_models(spec, selected_model_slugs=selected_model_slugs)
    return (
        ProviderOnboardingStepResult(
            step="model_probe",
            status="succeeded",
            summary=(
                f"Discovered {len(discovered_models)} model(s) and selected "
                f"{len(resolved_models)} for onboarding"
            ),
            details={
                "selected_transport": spec.selected_transport,
                "discovered_models": list(discovered_models),
                "selected_models": [model.model_slug for model in resolved_models],
                "default_model": spec.default_model,
                "requested_models": list(requested_or_overridden),
            },
        ),
        resolved_models,
    )


# ---------------------------------------------------------------------------
# Capacity probe
# ---------------------------------------------------------------------------

def _probe_capacity(
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
    *,
    env: Mapping[str, str],
    transport_details: Mapping[str, Any],
    models: Sequence[ProviderOnboardingModelSpec],
) -> ProviderOnboardingStepResult:
    adapter_type = "cli_llm" if spec.selected_transport == "cli" else "llm_task"
    economics = dict(spec.adapter_economics.get(adapter_type, {}))
    default_model = spec.default_model or (models[0].model_slug if models else None)

    if not default_model:
        return ProviderOnboardingStepResult(
            step="capacity_probe",
            status="failed",
            summary="No default model is available for the entitlement probe",
            details={"selected_transport": spec.selected_transport},
        )

    if spec.selected_transport != "cli":
        probe_strategy = str(
            transport_template.prompt_probe_strategy
            or _api_prompt_probe_strategy_for(spec.api_protocol_family)
            or ""
        ).strip()
        if probe_strategy == "api_model_discovery_auth_probe":
            return ProviderOnboardingStepResult(
                step="capacity_probe",
                status="succeeded",
                summary=(
                    f"API auth and model discovery succeeded for {spec.provider_slug}; "
                    "repo-bound execution will be validated at workflow runtime"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "default_model": default_model,
                    "available_model_count": len(models),
                    "api_endpoint": spec.api_endpoint,
                    "api_protocol_family": spec.api_protocol_family,
                    "billing_mode": economics.get("billing_mode"),
                    "budget_bucket": economics.get("budget_bucket"),
                    "effective_marginal_cost": economics.get("effective_marginal_cost"),
                    "probe_strategy": probe_strategy,
                },
            )
        if probe_strategy != "api_llm_request":
            return ProviderOnboardingStepResult(
                step="capacity_probe",
                status="warning",
                summary=(
                    f"API capacity probing is not implemented for {spec.provider_slug} "
                    f"({spec.api_protocol_family or 'unknown protocol'})"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "api_endpoint": spec.api_endpoint,
                    "api_protocol_family": spec.api_protocol_family,
                    "probe_strategy": transport_template.prompt_probe_strategy,
                },
            )

        api_key = _resolve_api_key_from_env(spec, env=env)
        if not api_key:
            return ProviderOnboardingStepResult(
                step="capacity_probe",
                status="failed",
                summary=f"No API key resolved for {spec.provider_slug} capacity probe",
                details={
                    "selected_transport": spec.selected_transport,
                    "required_env_vars": list(spec.api_key_env_vars),
                },
            )

        endpoint_uri = _resolved_api_endpoint(spec, default_model)
        if not endpoint_uri:
            return ProviderOnboardingStepResult(
                step="capacity_probe",
                status="failed",
                summary=(
                    f"No resolved API endpoint is available for "
                    f"{spec.provider_slug}/{default_model}"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "api_endpoint": spec.api_endpoint,
                    "default_model": default_model,
                },
            )

        from adapters.llm_client import LLMClientError, LLMRequest, call_llm

        try:
            response = call_llm(
                LLMRequest(
                    endpoint_uri=endpoint_uri,
                    api_key=api_key,
                    provider_slug=spec.provider_slug,
                    model_slug=default_model,
                    messages=({"role": "user", "content": _PROBE_PROMPT},),
                    max_tokens=32,
                    temperature=0.0,
                    protocol_family=spec.api_protocol_family,
                    timeout_seconds=min(int(spec.default_timeout or 60), 60),
                    retry_attempts=0,
                    retry_backoff_seconds=(),
                    retryable_status_codes=(),
                )
            )
        except LLMClientError as exc:
            return ProviderOnboardingStepResult(
                step="capacity_probe",
                status="failed",
                summary=(
                    f"API prompt probe failed for {spec.provider_slug}/{default_model}: "
                    f"{exc.reason_code}"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "default_model": default_model,
                    "api_endpoint": endpoint_uri,
                    "api_protocol_family": spec.api_protocol_family,
                    "error": str(exc),
                    "billing_mode": economics.get("billing_mode"),
                    "budget_bucket": economics.get("budget_bucket"),
                },
            )

        if _PROBE_EXPECTED_RESPONSE not in response.content:
            return ProviderOnboardingStepResult(
                step="capacity_probe",
                status="failed",
                summary=(
                    f"API prompt probe did not complete successfully for "
                    f"{spec.provider_slug}/{default_model}"
                ),
                details={
                    "selected_transport": spec.selected_transport,
                    "default_model": default_model,
                    "api_endpoint": endpoint_uri,
                    "api_protocol_family": spec.api_protocol_family,
                    "response_excerpt": response.content[:200],
                    "status_code": response.status_code,
                    "billing_mode": economics.get("billing_mode"),
                    "budget_bucket": economics.get("budget_bucket"),
                },
            )

        return ProviderOnboardingStepResult(
            step="capacity_probe",
            status="succeeded",
            summary=(
                f"API prompt probe succeeded for {spec.provider_slug}/{default_model}; "
                f"{economics.get('billing_mode') or 'billing'} via {economics.get('budget_bucket') or 'unknown bucket'}"
            ),
            details={
                "selected_transport": spec.selected_transport,
                "default_model": default_model,
                "available_model_count": len(models),
                "api_endpoint": endpoint_uri,
                "api_protocol_family": spec.api_protocol_family,
                "status_code": response.status_code,
                "latency_ms": response.latency_ms,
                "billing_mode": economics.get("billing_mode"),
                "budget_bucket": economics.get("budget_bucket"),
                "effective_marginal_cost": economics.get("effective_marginal_cost"),
                "response_excerpt": response.content[:200],
            },
        )

    binary_path = str(transport_details.get("binary_path") or "")
    attempts: list[dict[str, Any]] = []
    for prompt_mode in _candidate_cli_prompt_modes(spec, transport_template):
        cmd, input_text = _build_cli_probe_command(
            spec=spec,
            binary_path=binary_path,
            model_slug=default_model,
            prompt_mode=prompt_mode,
            prompt_text=_PROBE_PROMPT,
        )
        try:
            proc = _run_command(
                cmd,
                env=env,
                input_text=input_text,
                timeout_seconds=min(int(spec.default_timeout), 60),
            )
        except Exception as exc:
            attempts.append(
                {
                    "prompt_mode": prompt_mode,
                    "command": cmd,
                    "success": False,
                    "error": str(exc),
                }
            )
            continue

        response_text = _extract_cli_response_text(
            stdout=proc.stdout or "",
            output_format=spec.output_format,
            envelope_key=spec.output_envelope_key,
        )
        succeeded = proc.returncode == 0 and _PROBE_EXPECTED_RESPONSE in response_text
        attempts.append(
            {
                "prompt_mode": prompt_mode,
                "command": cmd,
                "input_channel": prompt_mode,
                "success": succeeded,
                "returncode": proc.returncode,
                "stdout_excerpt": (proc.stdout or "")[:400],
                "stderr_excerpt": (proc.stderr or "")[:400],
                "response_excerpt": response_text[:200],
            }
        )
        if not succeeded:
            continue
        return ProviderOnboardingStepResult(
            step="capacity_probe",
            status="succeeded",
            summary=(
                f"Prompt probe succeeded for {spec.provider_slug}/{default_model} via {prompt_mode}; "
                f"{economics.get('billing_mode') or 'billing'} via {economics.get('budget_bucket') or 'unknown bucket'}"
            ),
            details={
                "selected_transport": spec.selected_transport,
                "default_model": default_model,
                "available_model_count": len(models),
                "billing_mode": economics.get("billing_mode"),
                "budget_bucket": economics.get("budget_bucket"),
                "effective_marginal_cost": economics.get("effective_marginal_cost"),
                "prompt_mode": prompt_mode,
                "response_excerpt": response_text[:200],
                "attempts": attempts,
            },
        )

    return ProviderOnboardingStepResult(
        step="capacity_probe",
        status="failed",
        summary=(
            f"Prompt probe did not complete successfully for "
            f"{spec.provider_slug}/{default_model}"
        ),
        details={
            "selected_transport": spec.selected_transport,
            "default_model": default_model,
            "billing_mode": economics.get("billing_mode"),
            "budget_bucket": economics.get("budget_bucket"),
            "attempts": attempts,
        },
    )
