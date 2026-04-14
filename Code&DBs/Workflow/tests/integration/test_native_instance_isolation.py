from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    NativeInstanceResolutionError,
    resolve_native_instance,
)


def _runtime_profile(
    *,
    instance_name: str,
    workdir: str = ".",
) -> dict[str, str]:
    return {
        "instance_name": instance_name,
        "repo_root": ".",
        "workdir": workdir,
        "receipts_dir": "artifacts/runtime_receipts",
        "topology_dir": "artifacts/runtime_topology",
    }


def _write_runtime_profiles_config(
    tmp_path: Path,
    payload: dict[str, object],
) -> Path:
    repo_root = tmp_path / "dag-repo"
    (repo_root / "config").mkdir(parents=True)
    (repo_root / "artifacts" / "runtime_receipts").mkdir(parents=True)
    (repo_root / "artifacts" / "runtime_topology").mkdir(parents=True)
    config_path = repo_root / "config" / "runtime_profiles.json"
    config_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return config_path


def test_native_instance_rejects_legacy_runtime_profile_grammar(tmp_path: Path) -> None:
    config_path = _write_runtime_profiles_config(
        tmp_path,
        {
            "schema_version": 1,
            "default_profile": "praxis-project",
            "profiles": [
                {
                    "profile": "praxis-project",
                    **_runtime_profile(instance_name="praxis-project"),
                }
            ],
        },
    )

    with pytest.raises(NativeInstanceResolutionError) as exc_info:
        resolve_native_instance(
            env={PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(config_path)},
        )

    assert exc_info.value.reason_code == "native_instance.config_invalid"
    assert exc_info.value.details == {"field": "default_profile"}


def test_native_instance_runtime_profile_env_can_only_assert_checked_in_default(
    tmp_path: Path,
) -> None:
    config_path = _write_runtime_profiles_config(
        tmp_path,
        {
            "schema_version": 1,
            "default_runtime_profile": "praxis",
            "runtime_profiles": {
                "praxis": _runtime_profile(instance_name="praxis"),
                "alt-project": _runtime_profile(
                    instance_name="alt-project",
                    workdir="artifacts",
                ),
            },
        },
    )

    with pytest.raises(NativeInstanceResolutionError) as exc_info:
        resolve_native_instance(
            env={
                PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(config_path),
                PRAXIS_RUNTIME_PROFILE_ENV: "alt-project",
            },
        )

    assert exc_info.value.reason_code == "native_instance.boundary_mismatch"
    assert exc_info.value.details == {
        "environment_variable": PRAXIS_RUNTIME_PROFILE_ENV,
        "expected": "praxis",
        "actual": "alt-project",
    }
