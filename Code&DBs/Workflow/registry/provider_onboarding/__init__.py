"""Shared provider onboarding wizard — thin facade over pipeline-stage modules."""

from __future__ import annotations

import asyncpg  # noqa: F401  — tests monkeypatch provider_onboarding.asyncpg

from adapters import provider_registry as provider_registry_mod  # noqa: F401
from registry.provider_onboarding_repository import *  # noqa: F401,F403

from ._spec import *  # noqa: F401,F403
from ._probe import *  # noqa: F401,F403
from ._benchmark import *  # noqa: F401,F403
from ._report import *  # noqa: F401,F403

# Explicit re-exports so static analysers and documented importers see them.
from ._spec import (  # noqa: F811
    ProviderAuthorityTemplate,
    ProviderOnboardingModelSpec,
    ProviderOnboardingResult,
    ProviderOnboardingSpec,
    ProviderOnboardingStepResult,
    ProviderTransportAuthorityTemplate,
    normalize_provider_onboarding_spec,
    load_provider_onboarding_spec_from_file,
    _provider_template,
    _resolve_spec,
    _find_binary,
    _run_command,
)
from ._probe import (  # noqa: F811
    _discover_api_models,
    _http_get_json,
    _probe_transport,
    _probe_models,
    _probe_capacity,
)
from ._benchmark import (  # noqa: F811
    _probe_benchmark,
)
from registry.provider_onboarding_repository import (  # noqa: F811
    _apply_benchmark_plan,
    _upsert_provider_cli_profile,
    _upsert_provider_transport_admission,
    _record_provider_transport_probe_receipts,
    _upsert_model_profile,
    _upsert_provider_model_candidate,
    _upsert_model_profile_binding,
)
from ._report import (  # noqa: F811
    run_provider_onboarding,
    _run_provider_onboarding,
    _verification_report,
)
