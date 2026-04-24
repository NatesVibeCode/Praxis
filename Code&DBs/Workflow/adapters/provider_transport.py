"""Provider transport helpers.

This module stays on the adapter side of the boundary:

- adapter contract derivation
- endpoint / binary / command resolution
- provider-owned MCP CLI argument template resolution
- transport capability checks and profile validation

It intentionally does not own DB loading or mutable registry state.
"""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .provider_types import ProviderAdapterContract, ProviderCLIProfile

KNOWN_LLM_ADAPTER_TYPES = frozenset({"cli_llm", "llm_task"})


class AdapterEconomicsAuthorityError(RuntimeError):
    """Raised when provider adapter_economics authority is missing or sparse.

    Used for structural gaps (unknown provider, no row for adapter_type, and
    missing authority fields on a row that exists). Distinct from
    ``RuntimeError`` so callers that specifically want to treat sparse
    authority as a routing-surface error — not a generic unexpected-exception
    — can catch it narrowly.
    """


# Fields that the adapter_economics row MUST set. Sparse rows (missing or
# non-bool values) are rejected here instead of silently defaulting so that
# both provider_transport.resolve_adapter_economics and
# runtime.routing_economics.resolve_route_economics read one shared
# answer — closes BUG-8DAA5468.
_REQUIRED_ECONOMICS_BOOL_FIELDS: tuple[str, ...] = (
    "prefer_prepaid",
    "allow_payg_fallback",
)


@dataclass(frozen=True, slots=True)
class AdapterEconomicsContract:
    """Typed, validated economics for one (provider, adapter_type) pair.

    Single source of truth for the economics fields consumed by route
    selection (``runtime.routing_economics``), health surfaces
    (``runtime.health``), and operator observability. Sparse rows that omit
    authority fields (``prefer_prepaid``, ``allow_payg_fallback``) are
    rejected at construction so downstream consumers never need to default —
    the authority speaks or we fail closed.

    Previously both ``resolve_adapter_economics`` and
    ``resolve_route_economics`` ran ``bool(raw.get(field, False))`` against
    the same rows. Duplicated defaulting meant any future layer that
    changed its default would silently disagree with its sibling — exactly
    the BUG-8DAA5468 failure mode. Centralizing the contract eliminates the
    possibility.
    """

    provider_slug: str
    adapter_type: str
    billing_mode: str
    budget_bucket: str
    effective_marginal_cost: float
    prefer_prepaid: bool
    allow_payg_fallback: bool

    @classmethod
    def from_raw(
        cls,
        *,
        provider_slug: str,
        adapter_type: str,
        raw: Mapping[str, Any],
    ) -> "AdapterEconomicsContract":
        """Validate and construct from a raw adapter_economics row.

        Raises :class:`AdapterEconomicsAuthorityError` when any required
        field is missing or has an unusable type. This is the only
        construction path — consumers must never build the contract from
        hand-rolled dicts.
        """
        missing_core: list[str] = []
        for core_field in ("billing_mode", "budget_bucket", "effective_marginal_cost"):
            if core_field not in raw or raw[core_field] is None:
                missing_core.append(core_field)
        if missing_core:
            raise AdapterEconomicsAuthorityError(
                f"adapter_economics for {provider_slug}/{adapter_type} "
                f"missing required fields: {sorted(missing_core)}"
            )

        missing_bools: list[str] = []
        non_bool_fields: list[str] = []
        for bool_field in _REQUIRED_ECONOMICS_BOOL_FIELDS:
            if bool_field not in raw or raw[bool_field] is None:
                missing_bools.append(bool_field)
            elif not isinstance(raw[bool_field], bool):
                non_bool_fields.append(bool_field)
        if missing_bools:
            raise AdapterEconomicsAuthorityError(
                f"adapter_economics for {provider_slug}/{adapter_type} "
                f"must set {sorted(missing_bools)}; sparse rows are rejected "
                "so paid-lane fallback policy cannot silently flip. "
                "Closes BUG-8DAA5468."
            )
        if non_bool_fields:
            raise AdapterEconomicsAuthorityError(
                f"adapter_economics for {provider_slug}/{adapter_type} "
                f"fields {sorted(non_bool_fields)} must be bool"
            )

        return cls(
            provider_slug=provider_slug,
            adapter_type=adapter_type,
            billing_mode=str(raw["billing_mode"]),
            budget_bucket=str(raw["budget_bucket"]),
            effective_marginal_cost=float(raw["effective_marginal_cost"]),
            prefer_prepaid=bool(raw["prefer_prepaid"]),
            allow_payg_fallback=bool(raw["allow_payg_fallback"]),
        )

    def as_dict(self) -> dict[str, Any]:
        """Serialize to the legacy dict surface for consumers mid-migration.

        New code should read typed attributes directly; this shape exists
        only so that surfaces already returning a dict (JSON-ish payloads)
        don't have to change shape.
        """
        return {
            "billing_mode": self.billing_mode,
            "budget_bucket": self.budget_bucket,
            "effective_marginal_cost": self.effective_marginal_cost,
            "prefer_prepaid": self.prefer_prepaid,
            "allow_payg_fallback": self.allow_payg_fallback,
        }


BUILTIN_PROVIDER_PROFILES: tuple[ProviderCLIProfile, ...] = (
    ProviderCLIProfile(
        provider_slug="anthropic",
        binary="claude",
        prompt_mode="stdin",
        default_model="claude-sonnet-4-6",
        # Anthropic is CLI-only (subscription, OAuth). Direct API endpoint and
        # API key env var intentionally omitted per
        # decision.2026-04-20.anthropic-cli-only-restored (migration 181).
        # Nate has no ANTHROPIC_API_KEY; Claude is reached either via the
        # `claude` binary (OAuth) or via OpenRouter (openrouter/anthropic/*).
        api_endpoint=None,
        api_protocol_family=None,
        api_key_env_vars=(),
        adapter_economics={
            "cli_llm": {
                "billing_mode": "subscription_included",
                "budget_bucket": "anthropic_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": False,
            },
        },
        lane_policies={
            "cli_llm": {
                "admitted_by_policy": True,
                "execution_topology": "local_cli",
                "transport_kind": "cli",
                "policy_reason": "Admitted local CLI lane.",
            },
        },
        base_flags=("-p", "--output-format", "json"),
        model_flag="--model",
        system_prompt_flag="--system-prompt",
        json_schema_flag="--json-schema",
        output_format="json",
        output_envelope_key="result",
        forbidden_flags=(
            "--dangerously-skip-permissions",
            "--allow-dangerously-skip-permissions",
            "--add-dir",
        ),
        default_timeout=300,
    ),
    ProviderCLIProfile(
        provider_slug="openai",
        binary="codex",
        prompt_mode="stdin",
        default_model="gpt-4.1",
        api_endpoint="https://api.openai.com/v1/chat/completions",
        api_protocol_family="openai_chat_completions",
        api_key_env_vars=(),
        adapter_economics={
            "cli_llm": {
                "billing_mode": "subscription_included",
                "budget_bucket": "openai_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": True,
            },
            "llm_task": {
                "billing_mode": "metered_api",
                "budget_bucket": "openai_api_payg",
                "effective_marginal_cost": 1.0,
                "prefer_prepaid": False,
                "allow_payg_fallback": True,
            },
        },
        lane_policies={
            "cli_llm": {
                "admitted_by_policy": True,
                "execution_topology": "local_cli",
                "transport_kind": "cli",
                "policy_reason": "Admitted local CLI lane.",
            },
            "llm_task": {
                "admitted_by_policy": True,
                "execution_topology": "direct_http",
                "transport_kind": "http",
                "policy_reason": "Admitted direct HTTP lane.",
            },
        },
        base_flags=("exec", "-", "--json"),
        model_flag="--model",
        system_prompt_flag=None,
        json_schema_flag=None,
        output_format="ndjson",
        output_envelope_key="text",
        forbidden_flags=("--full-auto",),
        default_timeout=300,
    ),
    ProviderCLIProfile(
        provider_slug="cursor",
        binary="cursor-api",
        prompt_mode="stdin",
        default_model="composer-2",
        api_endpoint="https://api.cursor.com/v0/agents",
        api_protocol_family="cursor_background_agent",
        api_key_env_vars=(),
        adapter_economics={
            "llm_task": {
                "billing_mode": "subscription_included",
                "budget_bucket": "cursor_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": False,
            },
        },
        lane_policies={
            "llm_task": {
                "admitted_by_policy": True,
                "execution_topology": "repo_agent_http",
                "transport_kind": "http",
                "policy_reason": "Admitted Cursor background-agent API lane.",
            },
        },
        base_flags=(),
        model_flag=None,
        system_prompt_flag=None,
        json_schema_flag=None,
        output_format="text",
        output_envelope_key="text",
        forbidden_flags=(),
        default_timeout=900,
    ),
    ProviderCLIProfile(
        provider_slug="cursor_local",
        binary="cursor-agent",
        prompt_mode="stdin",
        default_model="composer-2",
        api_endpoint="",
        api_protocol_family="",
        api_key_env_vars=(),
        adapter_economics={
            "cli_llm": {
                "billing_mode": "subscription_included",
                "budget_bucket": "cursor_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": False,
            },
        },
        lane_policies={
            "cli_llm": {
                "admitted_by_policy": True,
                "execution_topology": "local_cli",
                "transport_kind": "cli",
                "policy_reason": "Admitted local Cursor Agent CLI lane.",
            },
        },
        base_flags=(
            "-p",
            "--output-format",
            "json",
            "--mode",
            "ask",
            "-f",
            "--sandbox",
            "disabled",
        ),
        model_flag="--model",
        system_prompt_flag=None,
        json_schema_flag=None,
        output_format="json",
        output_envelope_key="result",
        forbidden_flags=("--cloud", "--workspace", "-w", "--worktree"),
        default_timeout=900,
        aliases=("cursor-cli",),
    ),
    ProviderCLIProfile(
        provider_slug="google",
        binary="gemini",
        prompt_mode="stdin",
        default_model="gemini-2.5-flash",
        api_endpoint="https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
        api_protocol_family="google_generate_content",
        api_key_env_vars=(),
        adapter_economics={
            "cli_llm": {
                "billing_mode": "subscription_included",
                "budget_bucket": "google_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": True,
            },
            "llm_task": {
                "billing_mode": "metered_api",
                "budget_bucket": "google_api_payg",
                "effective_marginal_cost": 1.0,
                "prefer_prepaid": False,
                "allow_payg_fallback": True,
            },
        },
        lane_policies={
            "cli_llm": {
                "admitted_by_policy": True,
                "execution_topology": "local_cli",
                "transport_kind": "cli",
                "policy_reason": "Admitted local CLI lane.",
            },
            "llm_task": {
                "admitted_by_policy": True,
                "execution_topology": "direct_http",
                "transport_kind": "http",
                "policy_reason": "Admitted direct HTTP lane.",
            },
        },
        base_flags=("-p", ".", "-o", "json"),
        model_flag="--model",
        system_prompt_flag=None,
        json_schema_flag=None,
        output_format="json",
        output_envelope_key="response",
        forbidden_flags=(
            "--approval-mode",
            "--yolo",
            "-y",
        ),
        default_timeout=600,
        mcp_config_style="gemini_project_settings",
        mcp_args_template=["--allowed-mcp-server-names", "dag-workflow"],
        aliases=("gemini-cli",),
    ),
)


def _cfg(adapter_config: Mapping[str, Any], key: str, default: Any) -> Any:
    value = adapter_config.get(key)
    return value if value is not None else default


def _http_timeout_seconds(adapter_config: Mapping[str, Any]) -> int:
    value = _cfg(adapter_config, "llm_http.timeout_seconds", None)
    if isinstance(value, (int, float)):
        return int(value)
    return int(os.environ.get("PRAXIS_HTTP_TIMEOUT", "120"))


def _http_retry_attempts(adapter_config: Mapping[str, Any]) -> int:
    value = _cfg(adapter_config, "llm_http.retry_attempts", None)
    if isinstance(value, (int, float)):
        return max(0, int(value))
    return max(0, int(os.environ.get("PRAXIS_HTTP_RETRIES", "2")))


def _http_retry_backoff_seconds(adapter_config: Mapping[str, Any]) -> tuple[int, ...]:
    value = _cfg(adapter_config, "llm_http.retry_backoff_seconds", None)
    if isinstance(value, list):
        return tuple(int(item) for item in value)
    return (2, 5)


def _http_retryable_status_codes(adapter_config: Mapping[str, Any]) -> tuple[int, ...]:
    value = _cfg(adapter_config, "llm_http.retryable_status_codes", None)
    if isinstance(value, list):
        return tuple(int(item) for item in value)
    return (408, 429, 500, 502, 503, 504)


def _cli_prompt_envelope(profile: ProviderCLIProfile) -> dict[str, Any]:
    prompt_mode = (profile.prompt_mode or "stdin").strip().lower() or "stdin"
    return {
        "kind": "argv_prompt" if prompt_mode == "argv" else "stdin_prompt",
        "prompt_channel": prompt_mode,
        "system_prompt_channel": "cli_flag" if profile.system_prompt_flag else prompt_mode,
        "system_prompt_flag": profile.system_prompt_flag,
        "protocol_family": None,
    }


def _http_prompt_envelope(profile: ProviderCLIProfile) -> dict[str, Any]:
    return {
        "kind": profile.api_protocol_family or "native_http",
        "prompt_channel": "messages",
        "system_prompt_channel": {
            "anthropic_messages": "top_level_system",
            "google_generate_content": "systemInstruction",
            "openai_chat_completions": "messages.system",
        }.get(profile.api_protocol_family or "", "messages.system"),
        "protocol_family": profile.api_protocol_family,
    }


def _cli_tool_policy(profile: ProviderCLIProfile) -> dict[str, Any]:
    return {
        "supports_tools": True,
        "mode": "cli_json_schema",
        "json_schema_flag": profile.json_schema_flag,
        "forbidden_flags": list(profile.forbidden_flags),
    }


def _http_tool_policy(profile: ProviderCLIProfile) -> dict[str, Any]:
    return {
        "supports_tools": True,
        "mode": "native_api",
        "request_field": "tools",
        "response_field": "tool_calls",
        "protocol_family": profile.api_protocol_family,
    }


def _cli_structured_output(profile: ProviderCLIProfile) -> dict[str, Any]:
    return {
        "kind": profile.output_format,
        "envelope_key": profile.output_envelope_key,
        "source": "stdout",
    }


def _http_structured_output(profile: ProviderCLIProfile) -> dict[str, Any]:
    return {
        "kind": "api_response",
        "envelope_key": "content",
        "source": "response_body",
        "protocol_family": profile.api_protocol_family,
    }


def _cli_telemetry() -> dict[str, Any]:
    return {
        "latency_source": "process_runtime",
        "usage_source": "stdout_envelope",
        "status_source": "exit_code",
    }


def _http_telemetry() -> dict[str, Any]:
    return {
        "latency_source": "llm_response.latency_ms",
        "usage_source": "llm_response.usage",
        "status_source": "llm_response.status_code",
        "tool_call_source": "llm_response.tool_calls",
    }


def _cli_retry_policy() -> dict[str, Any]:
    return {
        "retry_attempts": 0,
        "backoff_seconds": [],
        "retryable_status_codes": [],
    }


def _http_retry_policy(adapter_config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "retry_attempts": _http_retry_attempts(adapter_config),
        "backoff_seconds": list(_http_retry_backoff_seconds(adapter_config)),
        "retryable_status_codes": list(_http_retryable_status_codes(adapter_config)),
    }


def resolve_lane_policy(
    provider_slug: str,
    adapter_type: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> dict[str, Any] | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None
    normalized_adapter_type = (adapter_type or "").strip().lower()
    if not normalized_adapter_type:
        return None
    lane_policies = profile.lane_policies or {}
    policy = lane_policies.get(normalized_adapter_type)
    if not isinstance(policy, dict):
        return None
    return dict(policy)


def _cli_failure_mapping(failure_mappings: Mapping[str, dict[str, str]]) -> dict[str, str]:
    defaults = {
        "cli_adapter.timeout": "cli_adapter.timeout",
        "cli_adapter.nonzero_exit": "cli_adapter.nonzero_exit",
        "cli_adapter.exec_error": "cli_adapter.exec_error",
    }
    cached = failure_mappings.get("cli")
    if cached:
        defaults.update(dict(cached))
    return defaults


def _http_failure_mapping(failure_mappings: Mapping[str, dict[str, str]]) -> dict[str, str]:
    defaults = {
        "llm_client.http_error": "adapter.http_error",
        "llm_client.network_error": "adapter.network_error",
        "llm_client.timeout": "adapter.timeout",
        "llm_client.response_parse_error": "adapter.response_parse_error",
        "http_transport.http_error": "adapter.http_error",
        "http_transport.network_error": "adapter.network_error",
        "http_transport.timeout": "adapter.timeout",
        "http_transport.response_parse_error": "adapter.response_parse_error",
    }
    cached = failure_mappings.get("http")
    if cached:
        defaults.update(dict(cached))
    return defaults


def _cli_adapter_contract(
    profile: ProviderCLIProfile,
    *,
    failure_mappings: Mapping[str, dict[str, str]],
) -> ProviderAdapterContract:
    return ProviderAdapterContract(
        provider_slug=profile.provider_slug,
        adapter_type="cli_llm",
        transport_kind="cli",
        execution_kind="subprocess",
        failure_namespace="cli_adapter",
        prompt_envelope=_cli_prompt_envelope(profile),
        tool_policy=_cli_tool_policy(profile),
        structured_output=_cli_structured_output(profile),
        timeout_seconds=profile.default_timeout,
        telemetry=_cli_telemetry(),
        retry_policy=_cli_retry_policy(),
        failure_mapping=_cli_failure_mapping(failure_mappings),
        readiness={
            "binary": profile.binary,
            "base_flags": list(profile.base_flags),
            "model_flag": profile.model_flag,
            "system_prompt_flag": profile.system_prompt_flag,
            "json_schema_flag": profile.json_schema_flag,
            "output_format": profile.output_format,
            "output_envelope_key": profile.output_envelope_key,
            "default_timeout": profile.default_timeout,
        },
        retryable_failure_codes=(
            "cli_adapter.timeout",
            "cli_adapter.nonzero_exit",
            "cli_adapter.exec_error",
        ),
        failover_failure_codes=(
            "cli_adapter.timeout",
            "cli_adapter.nonzero_exit",
            "cli_adapter.exec_error",
        ),
    )


def _llm_task_adapter_contract(
    profile: ProviderCLIProfile,
    *,
    adapter_config: Mapping[str, Any],
    failure_mappings: Mapping[str, dict[str, str]],
) -> ProviderAdapterContract:
    return ProviderAdapterContract(
        provider_slug=profile.provider_slug,
        adapter_type="llm_task",
        transport_kind="http",
        execution_kind="request",
        failure_namespace="adapter",
        prompt_envelope=_http_prompt_envelope(profile),
        tool_policy=_http_tool_policy(profile),
        structured_output=_http_structured_output(profile),
        timeout_seconds=_http_timeout_seconds(adapter_config),
        telemetry=_http_telemetry(),
        retry_policy=_http_retry_policy(adapter_config),
        failure_mapping=_http_failure_mapping(failure_mappings),
        readiness={
            "api_endpoint": profile.api_endpoint,
            "api_protocol_family": profile.api_protocol_family,
            "api_key_env_vars": list(profile.api_key_env_vars),
            "default_timeout": _http_timeout_seconds(adapter_config),
        },
        retryable_failure_codes=(
            "adapter.timeout",
            "adapter.http_error",
            "adapter.network_error",
            "adapter.response_parse_error",
        ),
        failover_failure_codes=(
            "adapter.timeout",
            "adapter.http_error",
            "adapter.network_error",
        ),
    )


def default_llm_adapter_type(
    profiles: Mapping[str, ProviderCLIProfile],
    *,
    adapter_config: Mapping[str, Any],
    failure_mappings: Mapping[str, dict[str, str]],
) -> str:
    if not profiles:
        raise RuntimeError("provider execution registry has no authoritative provider profiles")
    for candidate in ("cli_llm", "llm_task"):
        if any(
            resolve_adapter_contract(
                provider_slug,
                candidate,
                profiles=profiles,
                adapter_config=adapter_config,
                failure_mappings=failure_mappings,
            )
            is not None
            for provider_slug in profiles
        ):
            return candidate
    raise RuntimeError("provider execution registry has no supported LLM adapter types")


def default_adapter_type_for_provider(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> str | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None

    lane_policies = profile.lane_policies or {}
    admitted = [
        adapter_type
        for adapter_type in ("cli_llm", "llm_task")
        if isinstance(lane_policies.get(adapter_type), dict)
        and bool(lane_policies[adapter_type].get("admitted_by_policy"))
    ]
    if len(admitted) == 1:
        return admitted[0]
    if len(admitted) > 1:
        return "cli_llm" if "cli_llm" in admitted else admitted[0]

    if resolve_lane_policy(provider_slug, "cli_llm", profiles=profiles):
        return "cli_llm"
    if resolve_lane_policy(provider_slug, "llm_task", profiles=profiles):
        return "llm_task"
    return None


def default_model_for_provider(
    provider_slug: str,
    profiles: Mapping[str, ProviderCLIProfile],
) -> str | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None
    model = (profile.default_model or "").strip()
    return model or None


def resolve_adapter_economics_contract(
    provider_slug: str,
    adapter_type: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> AdapterEconomicsContract:
    """Return the typed economics contract for one (provider, adapter_type).

    This is the single construction path for economics authority. Both the
    legacy :func:`resolve_adapter_economics` dict surface and
    :func:`runtime.routing_economics.resolve_route_economics` now derive from
    this contract, which closes BUG-8DAA5468 (authority split defaulting
    ``allow_payg_fallback`` in two places).
    """
    profile = profiles.get(provider_slug)
    if profile is None:
        raise AdapterEconomicsAuthorityError(
            f"provider execution registry has no profile for {provider_slug!r}"
        )

    lane_policy = resolve_lane_policy(provider_slug, adapter_type, profiles=profiles)
    if not lane_policy or not bool(lane_policy.get("admitted_by_policy")):
        raise AdapterEconomicsAuthorityError(
            f"provider execution registry adapter not admitted by policy for {provider_slug}/{adapter_type}"
        )

    adapter_defaults = profile.adapter_economics or {}
    if adapter_type not in adapter_defaults:
        raise AdapterEconomicsAuthorityError(
            f"provider execution registry missing authoritative adapter_economics for {provider_slug}/{adapter_type}"
        )
    return AdapterEconomicsContract.from_raw(
        provider_slug=provider_slug,
        adapter_type=adapter_type,
        raw=adapter_defaults[adapter_type],
    )


def resolve_adapter_economics(
    provider_slug: str,
    adapter_type: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> dict[str, Any]:
    """Legacy dict surface — delegates to the contract constructor.

    Retained for consumers that still interoperate with raw dicts (JSON
    payloads, observability surfaces). Internally resolves the same typed
    :class:`AdapterEconomicsContract`, so the sparse-row rejection in
    :meth:`AdapterEconomicsContract.from_raw` is the single authority gate.
    """
    contract = resolve_adapter_economics_contract(
        provider_slug, adapter_type, profiles=profiles
    )
    return contract.as_dict()


def resolve_api_endpoint(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
    model_slug: str | None = None,
    logger: logging.Logger | None = None,
) -> str | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None
    endpoint = (profile.api_endpoint or "").strip()
    if endpoint and "{model}" in endpoint:
        model = (model_slug or default_model_for_provider(provider_slug, profiles) or "").strip()
        if not model:
            return None
        try:
            endpoint = endpoint.format(model=model)
        except (KeyError, ValueError) as exc:
            if logger is not None:
                logger.warning(
                    "provider execution registry: endpoint template error for %s: %s",
                    provider_slug,
                    exc,
                )
            return None
    return endpoint or None


def resolve_api_protocol_family(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> str | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None
    family = (profile.api_protocol_family or "").strip()
    return family or None


def resolve_api_key_env_vars(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> tuple[str, ...]:
    profile = profiles.get(provider_slug)
    if profile is None:
        return ()
    return tuple(env_var for env_var in profile.api_key_env_vars if env_var)


def resolve_mcp_args_template(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> list[str]:
    """Return the provider-owned MCP CLI argument template."""

    profile = profiles.get(provider_slug)
    if profile is None:
        return []
    template = profile.mcp_args_template
    if not isinstance(template, list):
        return []
    return [str(item) for item in template if item]


def resolve_adapter_contract(
    provider_slug: str,
    adapter_type: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
    adapter_config: Mapping[str, Any],
    failure_mappings: Mapping[str, dict[str, str]],
) -> ProviderAdapterContract | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None

    normalized_adapter_type = (adapter_type or "").strip().lower()
    lane_policy = resolve_lane_policy(
        provider_slug,
        normalized_adapter_type,
        profiles=profiles,
    )
    if not lane_policy or not bool(lane_policy.get("admitted_by_policy")):
        return None
    if normalized_adapter_type == "cli_llm":
        return _cli_adapter_contract(profile, failure_mappings=failure_mappings)
    if normalized_adapter_type == "llm_task":
        return _llm_task_adapter_contract(
            profile,
            adapter_config=adapter_config,
            failure_mappings=failure_mappings,
        )
    return None


def resolve_binary(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
) -> str | None:
    profile = profiles.get(provider_slug)
    if profile is None:
        return None
    binary = shutil.which(profile.binary)
    if binary:
        return binary
    return None


def supports_adapter(
    provider_slug: str,
    adapter_type: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
    adapter_config: Mapping[str, Any],
    failure_mappings: Mapping[str, dict[str, str]],
) -> bool:
    contract = resolve_adapter_contract(
        provider_slug,
        adapter_type,
        profiles=profiles,
        adapter_config=adapter_config,
        failure_mappings=failure_mappings,
    )
    if contract is None:
        return False
    if contract.transport_kind == "cli":
        return resolve_binary(provider_slug, profiles=profiles) is not None
    if contract.transport_kind == "http":
        return (
            resolve_api_endpoint(provider_slug, profiles=profiles) is not None
            and resolve_api_protocol_family(provider_slug, profiles=profiles) is not None
            and bool(resolve_api_key_env_vars(provider_slug, profiles=profiles))
        )
    return False


def supports_model_adapter(
    provider_slug: str,
    model_slug: str,
    adapter_type: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
    adapter_config: Mapping[str, Any],
    failure_mappings: Mapping[str, dict[str, str]],
) -> bool:
    normalized_model_slug = (model_slug or "").strip()
    if not normalized_model_slug:
        return False
    if not supports_adapter(
        provider_slug,
        adapter_type,
        profiles=profiles,
        adapter_config=adapter_config,
        failure_mappings=failure_mappings,
    ):
        return False

    contract = resolve_adapter_contract(
        provider_slug,
        adapter_type,
        profiles=profiles,
        adapter_config=adapter_config,
        failure_mappings=failure_mappings,
    )
    if contract is None:
        return False
    if contract.transport_kind == "http":
        return (
            resolve_api_endpoint(
                provider_slug,
                profiles=profiles,
                model_slug=normalized_model_slug,
            )
            is not None
        )
    return True


def build_command(
    provider_slug: str,
    *,
    profiles: Mapping[str, ProviderCLIProfile],
    model: str | None = None,
    binary_override: str | None = None,
    system_prompt: str | None = None,
    json_schema: str | None = None,
) -> list[str]:
    profile = profiles.get(provider_slug)
    if profile is None:
        raise ValueError(
            f"unknown provider {provider_slug!r}; registered: {sorted(profiles)}"
        )

    binary = binary_override or resolve_binary(provider_slug, profiles=profiles)
    if not binary:
        raise FileNotFoundError(f"{profile.binary} not found on PATH")

    command = [binary, *profile.base_flags]
    if model and profile.model_flag:
        command.extend([profile.model_flag, model])
    if system_prompt and profile.system_prompt_flag:
        command.extend([profile.system_prompt_flag, system_prompt])
    if json_schema and profile.json_schema_flag:
        command.extend([profile.json_schema_flag, json_schema])

    for forbidden in profile.forbidden_flags:
        if forbidden in command:
            raise RuntimeError(
                f"SAFETY: forbidden flag {forbidden!r} in command for {provider_slug}."
            )

    return command


def validate_profiles(
    profiles: Mapping[str, ProviderCLIProfile],
    *,
    adapter_config: Mapping[str, Any],
    failure_mappings: Mapping[str, dict[str, str]],
    adapter_types: Sequence[str] = tuple(KNOWN_LLM_ADAPTER_TYPES),
) -> dict[str, dict[str, Any]]:
    report: dict[str, dict[str, Any]] = {}
    for slug, profile in profiles.items():
        binary = resolve_binary(slug, profiles=profiles)
        adapter_contracts: dict[str, dict[str, Any]] = {}
        for adapter_type in adapter_types:
            contract = resolve_adapter_contract(
                slug,
                adapter_type,
                profiles=profiles,
                adapter_config=adapter_config,
                failure_mappings=failure_mappings,
            )
            if contract is not None:
                adapter_contracts[adapter_type] = contract.to_contract()
        report[slug] = {
            "binary": profile.binary,
            "binary_found": binary is not None,
            "binary_path": binary,
            "default_model": profile.default_model,
            "prompt_mode": profile.prompt_mode,
            "api_endpoint": profile.api_endpoint,
            "api_protocol_family": profile.api_protocol_family,
            "api_key_env_vars": list(profile.api_key_env_vars),
            "api_supported": supports_adapter(
                slug,
                "llm_task",
                profiles=profiles,
                adapter_config=adapter_config,
                failure_mappings=failure_mappings,
            ),
            "lane_policies": dict(profile.lane_policies or {}),
            "adapter_economics": {
                adapter_type: resolve_adapter_economics(
                    slug,
                    adapter_type,
                    profiles=profiles,
                )
                for adapter_type in adapter_types
                if supports_adapter(
                    slug,
                    adapter_type,
                    profiles=profiles,
                    adapter_config=adapter_config,
                    failure_mappings=failure_mappings,
                )
            },
            "adapter_contracts": adapter_contracts,
            "flags_safe": not any(
                flag in profile.base_flags
                for flag in profile.forbidden_flags
            ),
        }
    return report
