from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict

from runtime._workflow_database import resolve_runtime_database_url


class ProviderOnboardingCommand(BaseModel):
    model_config = ConfigDict(extra="allow")

    spec: dict[str, Any] | None = None
    provider_slug: str | None = None
    transport: str | None = None
    models: tuple[str, ...] = ()
    api_key_env_var: str | None = None
    dry_run: bool = False


def _resolved_database_url(subsystems: Any) -> str:
    env = getattr(subsystems, "_postgres_env", None)
    if callable(env):
        source = env()
        resolved = resolve_runtime_database_url(env=source, required=False)
        if resolved:
            return resolved
    raise RuntimeError("WORKFLOW_DATABASE_URL is required for provider onboarding")


def handle_provider_onboarding(
    command: ProviderOnboardingCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from registry.provider_onboarding import execute_provider_onboarding

    raw_spec: Mapping[str, Any] | None
    if command.spec is not None:
        raw_spec = command.spec
    elif command.model_extra:
        raw_spec = dict(command.model_extra)
        if command.provider_slug:
            raw_spec.setdefault("provider_slug", command.provider_slug)
        if command.transport:
            raw_spec.setdefault("selected_transport", command.transport)
        if command.models:
            raw_spec.setdefault("requested_models", list(command.models))
        if command.api_key_env_var:
            raw_spec.setdefault("api_key_env_vars", [command.api_key_env_var])
    else:
        raw_spec = None

    return execute_provider_onboarding(
        database_url=_resolved_database_url(subsystems),
        spec=raw_spec,
        provider_slug=command.provider_slug,
        transport=command.transport,
        models=command.models,
        api_key_env_var=command.api_key_env_var,
        dry_run=command.dry_run,
    )


__all__ = ["ProviderOnboardingCommand", "handle_provider_onboarding"]
