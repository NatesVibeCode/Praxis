from __future__ import annotations

from pathlib import Path

import pytest

from runtime import instance as native_instance
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    NativeInstanceResolutionError,
    native_instance_contract,
)
from storage.postgres import PostgresConfigurationError


def _missing_postgres(*, env=None):
    raise PostgresConfigurationError(
        "postgres.configuration_missing",
        "missing test authority",
    )


def _repo_local_env() -> dict[str, str]:
    repo_root = Path(__file__).resolve().parents[4]
    return {
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(repo_root / "config" / "runtime_profiles.json"),
        PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
    }


def test_native_instance_contract_requires_db_authority_by_default(monkeypatch) -> None:
    monkeypatch.setattr(native_instance, "ensure_postgres_available", _missing_postgres)

    with pytest.raises(NativeInstanceResolutionError) as exc_info:
        native_instance_contract(env=_repo_local_env())

    assert exc_info.value.reason_code == "native_instance.authority_unavailable"


def test_native_instance_contract_explicit_degraded_fallback(monkeypatch) -> None:
    monkeypatch.setattr(native_instance, "ensure_postgres_available", _missing_postgres)

    contract = native_instance_contract(
        env=_repo_local_env(),
        allow_authority_fallback=True,
    )

    assert contract["praxis_runtime_profile"] == "praxis"
    assert contract["praxis_instance_name"] == "praxis"
    assert contract["authority_state"] == "degraded"
    assert contract["authority_reason_code"] == "native_instance.authority_unavailable"
    assert contract["runtime_profiles_config"].endswith("/config/runtime_profiles.json")
    assert contract["repo_root"].endswith("/Praxis")
    assert contract["workdir"].endswith("/Praxis")
