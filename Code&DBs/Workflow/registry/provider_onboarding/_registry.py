"""Compatibility shim for provider onboarding registry helpers.

The canonical onboarding registry authority lives in
``registry.provider_onboarding_repository``. This module remains only to
preserve older imports while runtime callers are migrated.
"""

from __future__ import annotations

from registry.provider_onboarding_repository import *  # noqa: F401,F403

from registry.provider_onboarding_repository import (
    _apply_benchmark_plan,
    _record_provider_transport_probe_receipts,
    _upsert_match_rule,
    _upsert_model_profile,
    _upsert_model_profile_binding,
    _upsert_provider_cli_profile,
    _upsert_provider_model_candidate,
    _upsert_provider_transport_admission,
)

__all__ = [
    "_upsert_provider_cli_profile",
    "_upsert_provider_transport_admission",
    "_record_provider_transport_probe_receipts",
    "_upsert_model_profile",
    "_upsert_provider_model_candidate",
    "_upsert_model_profile_binding",
    "_upsert_match_rule",
    "_apply_benchmark_plan",
]
