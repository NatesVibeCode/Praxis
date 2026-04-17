"""Shared provider onboarding wizard facade with explicit exports."""

from __future__ import annotations

import asyncpg  # noqa: F401  — tests monkeypatch provider_onboarding.asyncpg

from adapters import provider_registry as provider_registry_mod  # noqa: F401
from registry.provider_onboarding_repository import (
    _apply_benchmark_plan,
    _record_provider_transport_probe_receipts,
    _upsert_model_profile,
    _upsert_model_profile_binding,
    _upsert_provider_cli_profile,
    _upsert_provider_model_candidate,
    _upsert_provider_transport_admission,
)

from ._benchmark import _probe_benchmark
from ._execute import execute_provider_onboarding
from ._probe import (
    _discover_api_models,
    _http_get_json,
    _probe_capacity,
    _probe_models,
    _probe_transport,
)
from ._report import _run_provider_onboarding, _verification_report, run_provider_onboarding
from ._spec import (
    ProviderAuthorityTemplate,
    ProviderOnboardingModelSpec,
    ProviderOnboardingResult,
    ProviderOnboardingSpec,
    ProviderOnboardingStepResult,
    ProviderTransportAuthorityTemplate,
    _find_binary,
    _provider_template,
    _resolve_spec,
    _run_command,
    load_provider_onboarding_spec_from_file,
    normalize_provider_onboarding_spec,
)

__all__ = [
    "ProviderAuthorityTemplate",
    "ProviderOnboardingModelSpec",
    "ProviderOnboardingResult",
    "ProviderOnboardingSpec",
    "ProviderOnboardingStepResult",
    "ProviderTransportAuthorityTemplate",
    "_apply_benchmark_plan",
    "_discover_api_models",
    "execute_provider_onboarding",
    "_find_binary",
    "_http_get_json",
    "_probe_benchmark",
    "_probe_capacity",
    "_probe_models",
    "_probe_transport",
    "_provider_template",
    "_record_provider_transport_probe_receipts",
    "_resolve_spec",
    "_run_command",
    "_run_provider_onboarding",
    "_upsert_model_profile",
    "_upsert_model_profile_binding",
    "_upsert_provider_cli_profile",
    "_upsert_provider_model_candidate",
    "_upsert_provider_transport_admission",
    "_verification_report",
    "asyncpg",
    "load_provider_onboarding_spec_from_file",
    "normalize_provider_onboarding_spec",
    "provider_registry_mod",
    "run_provider_onboarding",
]
