from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest

import runtime.instance as native_instance
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    NativeInstanceResolutionError,
    native_instance_contract,
    resolve_native_instance,
)


def _native_profile(**overrides):
    values = {
        "runtime_profile_ref": "praxis",
        "workspace_ref": "praxis",
        "sandbox_profile_ref": "sandbox_profile.praxis.default",
        "model_profile_id": "model_profile.praxis.default",
        "provider_policy_id": "provider_policy.praxis.default",
        "provider_name": "openai",
        "provider_names": ("openai",),
        "allowed_models": ("gpt-5.4",),
        "repo_root": str(Path(__file__).resolve().parents[4]),
        "workdir": str(Path(__file__).resolve().parents[4]),
        "instance_name": "praxis",
        "receipts_dir": str(Path(__file__).resolve().parents[4] / "artifacts" / "runtime_receipts"),
        "topology_dir": str(Path(__file__).resolve().parents[4] / "artifacts" / "runtime_topology"),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_native_instance_rejects_noncanonical_runtime_profile_assertion_path(tmp_path: Path) -> None:
    rogue_path = tmp_path / "rogue" / "runtime_profiles.json"

    with pytest.raises(NativeInstanceResolutionError) as exc_info:
        resolve_native_instance(
            env={PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(rogue_path)},
        )

    assert exc_info.value.reason_code == "native_instance.config_boundary"


def test_native_instance_runtime_profile_env_can_only_assert_db_default(
    monkeypatch,
) -> None:
    monkeypatch.setattr(native_instance, "ensure_postgres_available", lambda env=None: object())
    monkeypatch.setattr(
        native_instance,
        "resolve_native_runtime_profile_config",
        lambda conn=None: _native_profile(),
    )

    with pytest.raises(NativeInstanceResolutionError) as exc_info:
        resolve_native_instance(
            env={
                PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(Path(__file__).resolve().parents[4] / "config" / "runtime_profiles.json"),
                PRAXIS_RUNTIME_PROFILE_ENV: "alt-project",
            },
        )

    assert exc_info.value.reason_code == "native_instance.boundary_mismatch"
    assert exc_info.value.details == {
        "environment_variable": PRAXIS_RUNTIME_PROFILE_ENV,
        "expected": "praxis",
        "actual": "alt-project",
    }


def test_native_instance_resolves_contract_from_db_authority(monkeypatch) -> None:
    monkeypatch.setattr(native_instance, "ensure_postgres_available", lambda env=None: object())
    monkeypatch.setattr(
        native_instance,
        "resolve_native_runtime_profile_config",
        lambda conn=None: _native_profile(),
    )

    instance = resolve_native_instance(
        env={
            PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(Path(__file__).resolve().parents[4] / "config" / "runtime_profiles.json"),
            PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
        },
    )

    assert instance.runtime_profile_ref == "praxis"
    assert instance.instance_name == "praxis"
    assert instance.runtime_profiles_config.endswith("/config/runtime_profiles.json")


def test_native_instance_contract_falls_back_to_repo_local_defaults_without_db_authority() -> None:
    contract = native_instance_contract(
        env={
            PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(Path(__file__).resolve().parents[4] / "config" / "runtime_profiles.json"),
            PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
        }
    )

    assert contract["praxis_runtime_profile"] == "praxis"
    assert contract["praxis_instance_name"] == "praxis"
    assert contract["runtime_profiles_config"].endswith("/config/runtime_profiles.json")
    assert contract["repo_root"].endswith("/Praxis")
    assert contract["workdir"].endswith("/Praxis")
