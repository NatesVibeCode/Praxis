"""Repo-local native workflow instance resolution.

This module owns the repo-local instance contract for native workflow operations:

- one checked-in runtime profile config is the authority
- receipts, topology, and workdir boundaries stay inside the repo
- environment values may assert the contract but do not invent fallback state
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
import json
import os
from pathlib import Path
from typing import Any, Final

from runtime._helpers import _fail as _shared_fail

PRAXIS_INSTANCE_NAME_ENV: Final[str] = "PRAXIS_INSTANCE_NAME"
PRAXIS_RECEIPTS_DIR_ENV: Final[str] = "PRAXIS_RECEIPTS_DIR"
PRAXIS_RUNTIME_PROFILE_ENV: Final[str] = "PRAXIS_RUNTIME_PROFILE"
PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: Final[str] = "PRAXIS_RUNTIME_PROFILES_CONFIG"
PRAXIS_TOPOLOGY_DIR_ENV: Final[str] = "PRAXIS_TOPOLOGY_DIR"
_CONFIG_FILENAME: Final[str] = "runtime_profiles.json"
_CONFIG_DIRNAME: Final[str] = "config"
_SCHEMA_VERSION: Final[int] = 1


class NativeInstanceResolutionError(RuntimeError):
    """Raised when the native workflow instance cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class NativeWorkflowInstance:
    """Resolved repo-local native workflow instance contract."""

    instance_name: str
    runtime_profile_ref: str
    repo_root: str
    workdir: str
    receipts_dir: str
    topology_dir: str
    runtime_profiles_config: str

    def to_contract(self) -> dict[str, str]:
        return {
            "praxis_instance_name": self.instance_name,
            "praxis_receipts_dir": self.receipts_dir,
            "praxis_runtime_profile": self.runtime_profile_ref,
            "praxis_topology_dir": self.topology_dir,
            "repo_root": self.repo_root,
            "runtime_profiles_config": self.runtime_profiles_config,
            "workdir": self.workdir,
        }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_config_path() -> Path:
    return _repo_root() / _CONFIG_DIRNAME / _CONFIG_FILENAME


_fail = partial(_shared_fail, error_type=NativeInstanceResolutionError)


def _require_mapping(
    value: object,
    *,
    field_name: str,
    reason_code: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _fail(
            reason_code,
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_text(
    value: object,
    *,
    field_name: str,
    reason_code: str,
) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            reason_code,
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_integer(
    value: object,
    *,
    field_name: str,
    reason_code: str,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(
            reason_code,
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _resolve_config_path(
    *,
    config_path: str | Path | None,
    env: Mapping[str, str],
) -> Path:
    raw_path = config_path if config_path is not None else env.get(
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
        str(_default_config_path()),
    )
    if not isinstance(raw_path, (str, Path)):
        raise _fail(
            "native_instance.config_invalid",
            "runtime profiles config path must be a string or Path",
            details={"value_type": type(raw_path).__name__},
        )
    raw_text = str(raw_path).strip()
    if not raw_text:
        raise _fail(
            "native_instance.config_invalid",
            "runtime profiles config path must be non-empty",
            details={"environment_variable": PRAXIS_RUNTIME_PROFILES_CONFIG_ENV},
        )

    path = Path(raw_text).expanduser()
    if not path.is_absolute():
        path = _repo_root() / path
    resolved_path = path.resolve()
    if not resolved_path.exists():
        raise _fail(
            "native_instance.config_missing",
            "runtime profiles config file does not exist",
            details={"path": str(resolved_path)},
        )
    if resolved_path.name != _CONFIG_FILENAME or resolved_path.parent.name != _CONFIG_DIRNAME:
        raise _fail(
            "native_instance.config_boundary",
            "runtime profiles config must be the canonical config/runtime_profiles.json file",
            details={"path": str(resolved_path)},
        )
    return resolved_path


def _config_repo_root(config_path: Path) -> Path:
    repo_root = config_path.parent.parent.resolve()
    if not repo_root.exists() or not repo_root.is_dir():
        raise _fail(
            "native_instance.boundary_violation",
            "runtime profiles config must live inside an existing repo root",
            details={"config_path": str(config_path), "repo_root": str(repo_root)},
        )
    return repo_root


def _load_runtime_profiles(config_path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise _fail(
            "native_instance.config_unreadable",
            "failed to read runtime profiles config",
            details={"path": str(config_path)},
        ) from exc
    except json.JSONDecodeError as exc:
        raise _fail(
            "native_instance.config_invalid",
            "runtime profiles config must contain valid JSON",
            details={"path": str(config_path), "lineno": exc.lineno, "colno": exc.colno},
        ) from exc
    return _require_mapping(
        payload,
        field_name="runtime_profiles_document",
        reason_code="native_instance.config_invalid",
    )


def _require_schema_version(document: Mapping[str, object]) -> None:
    schema_version = _require_integer(
        document.get("schema_version"),
        field_name="schema_version",
        reason_code="native_instance.config_invalid",
    )
    if schema_version != _SCHEMA_VERSION:
        raise _fail(
            "native_instance.config_invalid",
            "runtime profiles config schema_version is not supported",
            details={
                "field": "schema_version",
                "expected": _SCHEMA_VERSION,
                "actual": schema_version,
            },
        )


def _default_runtime_profile_ref(document: Mapping[str, object]) -> str:
    if "default_profile" in document:
        raise _fail(
            "native_instance.config_invalid",
            "runtime profiles config must use default_runtime_profile; default_profile is not supported",
            details={"field": "default_profile"},
        )
    field_name = "default_runtime_profile"
    return _require_text(
        document.get(field_name),
        field_name=field_name,
        reason_code="native_instance.config_invalid",
    )


def _resolve_profile_payload(
    document: Mapping[str, object],
    *,
    runtime_profile_ref: str,
    config_path: Path,
) -> tuple[Mapping[str, object], str]:
    if "profiles" in document:
        raise _fail(
            "native_instance.config_invalid",
            "runtime profiles config must use runtime_profiles; profiles is not supported",
            details={"field": "profiles"},
        )

    runtime_profiles = _require_mapping(
        document.get("runtime_profiles"),
        field_name="runtime_profiles",
        reason_code="native_instance.config_invalid",
    )
    profile_payload = runtime_profiles.get(runtime_profile_ref)
    if profile_payload is None:
        raise _fail(
            "native_instance.profile_unknown",
            "runtime profile is not defined in the repo-local config",
            details={
                "runtime_profile_ref": runtime_profile_ref,
                "config_path": str(config_path),
            },
        )
    return (
        _require_mapping(
            profile_payload,
            field_name=f"runtime_profiles.{runtime_profile_ref}",
            reason_code="native_instance.profile_invalid",
        ),
        f"runtime_profiles.{runtime_profile_ref}",
    )


def _resolve_repo_path(
    raw_value: object,
    *,
    field_name: str,
    repo_root: Path,
) -> Path:
    path_text = _require_text(
        raw_value,
        field_name=field_name,
        reason_code="native_instance.profile_invalid",
    )
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(repo_root)
    except ValueError as exc:
        raise _fail(
            "native_instance.boundary_violation",
            f"{field_name} must stay inside the workflow repo boundary",
            details={
                "field": field_name,
                "path": str(resolved_path),
                "repo_root": str(repo_root),
            },
        ) from exc
    return resolved_path


def _assert_existing_directory(path: Path, *, field_name: str) -> None:
    if not path.exists() or not path.is_dir():
        raise _fail(
            "native_instance.boundary_violation",
            f"{field_name} must point at an existing directory",
            details={"field": field_name, "path": str(path)},
        )


def _assert_expected_text(
    env: Mapping[str, str],
    *,
    env_name: str,
    actual_value: str,
) -> None:
    expected_value = env.get(env_name)
    if expected_value is None:
        return
    if expected_value.strip() != actual_value:
        raise _fail(
            "native_instance.boundary_mismatch",
            f"{env_name} does not match the repo-local native instance contract",
            details={
                "environment_variable": env_name,
                "expected": actual_value,
                "actual": expected_value,
            },
        )


def _assert_expected_path(
    env: Mapping[str, str],
    *,
    env_name: str,
    actual_path: Path,
) -> None:
    expected_value = env.get(env_name)
    if expected_value is None:
        return
    expected_path = Path(expected_value).expanduser().resolve()
    if expected_path != actual_path:
        raise _fail(
            "native_instance.boundary_mismatch",
            f"{env_name} does not match the repo-local native instance contract",
            details={
                "environment_variable": env_name,
                "expected": str(actual_path),
                "actual": str(expected_path),
            },
        )


def resolve_native_instance(
    *,
    env: Mapping[str, str] | None = None,
    config_path: str | Path | None = None,
) -> NativeWorkflowInstance:
    """Resolve the checked-in repo-local native workflow instance contract."""

    source = env if env is not None else os.environ
    resolved_config_path = _resolve_config_path(config_path=config_path, env=source)
    config_repo_root = _config_repo_root(resolved_config_path)
    document = _load_runtime_profiles(resolved_config_path)
    _require_schema_version(document)

    default_runtime_profile = _default_runtime_profile_ref(document)
    _assert_expected_text(
        source,
        env_name=PRAXIS_RUNTIME_PROFILE_ENV,
        actual_value=default_runtime_profile,
    )
    runtime_profile_ref = default_runtime_profile

    profile, profile_field_prefix = _resolve_profile_payload(
        document,
        runtime_profile_ref=runtime_profile_ref,
        config_path=resolved_config_path,
    )

    repo_root = _resolve_repo_path(
        profile.get("repo_root"),
        field_name=f"{profile_field_prefix}.repo_root",
        repo_root=config_repo_root,
    )
    if repo_root != config_repo_root:
        raise _fail(
            "native_instance.boundary_violation",
            "repo-local runtime profile must resolve back to this workflow repo root",
            details={
                "runtime_profile_ref": runtime_profile_ref,
                "repo_root": str(repo_root),
                "config_repo_root": str(config_repo_root),
            },
        )

    workdir = _resolve_repo_path(
        profile.get("workdir"),
        field_name=f"{profile_field_prefix}.workdir",
        repo_root=repo_root,
    )
    _assert_existing_directory(repo_root, field_name="repo_root")
    _assert_existing_directory(workdir, field_name="workdir")

    receipts_dir = _resolve_repo_path(
        profile.get("receipts_dir"),
        field_name=f"{profile_field_prefix}.receipts_dir",
        repo_root=repo_root,
    )
    topology_dir = _resolve_repo_path(
        profile.get("topology_dir"),
        field_name=f"{profile_field_prefix}.topology_dir",
        repo_root=repo_root,
    )
    instance_name = _require_text(
        profile.get("instance_name"),
        field_name=f"{profile_field_prefix}.instance_name",
        reason_code="native_instance.profile_invalid",
    )

    _assert_expected_text(
        source,
        env_name=PRAXIS_INSTANCE_NAME_ENV,
        actual_value=instance_name,
    )
    _assert_expected_path(
        source,
        env_name=PRAXIS_RECEIPTS_DIR_ENV,
        actual_path=receipts_dir,
    )
    _assert_expected_path(
        source,
        env_name=PRAXIS_TOPOLOGY_DIR_ENV,
        actual_path=topology_dir,
    )

    return NativeWorkflowInstance(
        instance_name=instance_name,
        runtime_profile_ref=runtime_profile_ref,
        repo_root=str(repo_root),
        workdir=str(workdir),
        receipts_dir=str(receipts_dir),
        topology_dir=str(topology_dir),
        runtime_profiles_config=str(resolved_config_path),
    )


def native_instance_contract(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return the resolved repo-local native workflow instance contract."""

    return resolve_native_instance(env=env).to_contract()


__all__ = [
    "PRAXIS_INSTANCE_NAME_ENV",
    "PRAXIS_RECEIPTS_DIR_ENV",
    "PRAXIS_RUNTIME_PROFILE_ENV",
    "PRAXIS_RUNTIME_PROFILES_CONFIG_ENV",
    "PRAXIS_TOPOLOGY_DIR_ENV",
    "NativeWorkflowInstance",
    "NativeInstanceResolutionError",
    "native_instance_contract",
    "resolve_native_instance",
]
