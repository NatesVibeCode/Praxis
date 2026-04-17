"""Provider execution registry compatibility facade.

The canonical DB-backed provider execution authority lives in
``registry.provider_execution_registry``. This module preserves the adapter
import path while avoiding eager imports that would create registry bootstrap
cycles.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

from .provider_types import ProviderAdapterContract, ProviderCLIProfile

__all__ = [
    "ProviderAdapterContract",
    "ProviderCLIProfile",
    "ProviderRegistryError",
    "ProviderRegistryLoadError",
    "ProviderRegistrySchemaError",
    "ProviderRegistryDataError",
    "ProviderRegistryLoadTimeout",
    "RegistryLoadStatus",
    "reload_from_db",
    "registry_health",
    "resolve_adapter_config",
    "get_profile",
    "get_all_profiles",
    "registered_providers",
    "resolve_provider_from_alias",
    "default_provider_slug",
    "default_llm_adapter_type",
    "default_adapter_type_for_provider",
    "default_model_for_provider",
    "resolve_adapter_economics",
    "resolve_api_endpoint",
    "resolve_api_protocol_family",
    "resolve_api_key_env_vars",
    "resolve_mcp_args_template",
    "resolve_lane_policy",
    "resolve_adapter_contract",
    "supports_adapter",
    "supports_model_adapter",
    "resolve_binary",
    "build_command",
    "validate_profiles",
    "transport_support_report",
]

_LAZY_PUBLIC_NAMES = {
    "ProviderRegistryError",
    "ProviderRegistryLoadError",
    "ProviderRegistrySchemaError",
    "ProviderRegistryDataError",
    "ProviderRegistryLoadTimeout",
    "RegistryLoadStatus",
}


def _execution_registry() -> ModuleType:
    return importlib.import_module("registry.provider_execution_registry")


def __getattr__(name: str) -> Any:
    if name in _LAZY_PUBLIC_NAMES:
        return getattr(_execution_registry(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def reload_from_db() -> None:
    _execution_registry().reload_from_db()


def registry_health() -> dict[str, Any]:
    return _execution_registry().registry_health()


def resolve_adapter_config(key: str, default: Any = None) -> Any:
    return _execution_registry().resolve_adapter_config(key, default)


def get_profile(provider_slug: str) -> ProviderCLIProfile | None:
    return _execution_registry().get_profile(provider_slug)


def get_all_profiles() -> dict[str, ProviderCLIProfile]:
    return _execution_registry().get_all_profiles()


def registered_providers() -> list[str]:
    return _execution_registry().registered_providers()


def resolve_provider_from_alias(alias: str) -> str | None:
    return _execution_registry().resolve_provider_from_alias(alias)


def default_provider_slug() -> str:
    return _execution_registry().default_provider_slug()


def default_llm_adapter_type() -> str:
    return _execution_registry().default_llm_adapter_type()


def default_adapter_type_for_provider(provider_slug: str) -> str | None:
    return _execution_registry().default_adapter_type_for_provider(provider_slug)


def default_model_for_provider(provider_slug: str) -> str | None:
    return _execution_registry().default_model_for_provider(provider_slug)


def resolve_adapter_economics(provider_slug: str, adapter_type: str) -> dict[str, Any]:
    return _execution_registry().resolve_adapter_economics(provider_slug, adapter_type)


def resolve_api_endpoint(provider_slug: str, model_slug: str | None = None) -> str | None:
    return _execution_registry().resolve_api_endpoint(provider_slug, model_slug=model_slug)


def resolve_api_protocol_family(provider_slug: str) -> str | None:
    return _execution_registry().resolve_api_protocol_family(provider_slug)


def resolve_api_key_env_vars(provider_slug: str) -> tuple[str, ...]:
    return _execution_registry().resolve_api_key_env_vars(provider_slug)


def resolve_mcp_args_template(provider_slug: str) -> list[str]:
    return _execution_registry().resolve_mcp_args_template(provider_slug)


def resolve_lane_policy(provider_slug: str, adapter_type: str) -> dict[str, Any] | None:
    return _execution_registry().resolve_lane_policy(provider_slug, adapter_type)


def resolve_adapter_contract(
    provider_slug: str,
    adapter_type: str,
) -> ProviderAdapterContract | None:
    return _execution_registry().resolve_adapter_contract(provider_slug, adapter_type)


def supports_adapter(provider_slug: str, adapter_type: str) -> bool:
    return _execution_registry().supports_adapter(provider_slug, adapter_type)


def supports_model_adapter(provider_slug: str, model_slug: str, adapter_type: str) -> bool:
    return _execution_registry().supports_model_adapter(provider_slug, model_slug, adapter_type)


def resolve_binary(provider_slug: str) -> str | None:
    return _execution_registry().resolve_binary(provider_slug)


def build_command(
    provider_slug: str,
    model: str | None = None,
    *,
    binary_override: str | None = None,
    system_prompt: str | None = None,
    json_schema: str | None = None,
) -> list[str]:
    return _execution_registry().build_command(
        provider_slug,
        model=model,
        binary_override=binary_override,
        system_prompt=system_prompt,
        json_schema=json_schema,
    )


def validate_profiles() -> dict[str, dict[str, Any]]:
    return _execution_registry().validate_profiles()


def transport_support_report(
    *,
    health_mod: Any,
    pg: Any,
    provider_filter: str | None = None,
    model_filter: str | None = None,
    runtime_profile_ref: str = "praxis",
    jobs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble the operator-facing transport support report from canonical authority."""

    from authority.transport_eligibility import load_transport_eligibility_authority
    from storage.postgres import PostgresTransportEligibilityRepository

    authority = load_transport_eligibility_authority(
        repository=PostgresTransportEligibilityRepository(pg),
        health_mod=health_mod,
        pg=pg,
        provider_filter=provider_filter,
        model_filter=model_filter,
        runtime_profile_ref=runtime_profile_ref,
        jobs=jobs,
        provider_registry_mod=_execution_registry(),
    )
    return authority.to_json()
