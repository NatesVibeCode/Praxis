"""Repo-local native workflow instance resolution.

This module owns the repo-local instance contract for native workflow operations:

- Postgres native-runtime authority is the canonical source of truth
- receipts, topology, and workdir boundaries stay inside the repo
- environment values may assert the contract but do not invent fallback state
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
import os
from pathlib import Path
from typing import Any, Final

from registry.native_runtime_profile_sync import (
    NativeRuntimeProfileSyncError,
    resolve_native_runtime_profile_config,
)
from runtime._helpers import _fail as _shared_fail
from storage.postgres import PostgresConfigurationError, ensure_postgres_available

PRAXIS_INSTANCE_NAME_ENV: Final[str] = "PRAXIS_INSTANCE_NAME"
PRAXIS_RECEIPTS_DIR_ENV: Final[str] = "PRAXIS_RECEIPTS_DIR"
PRAXIS_RUNTIME_PROFILE_ENV: Final[str] = "PRAXIS_RUNTIME_PROFILE"
PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: Final[str] = "PRAXIS_RUNTIME_PROFILES_CONFIG"
PRAXIS_TOPOLOGY_DIR_ENV: Final[str] = "PRAXIS_TOPOLOGY_DIR"
_CONFIG_FILENAME: Final[str] = "runtime_profiles.json"
_CONFIG_DIRNAME: Final[str] = "config"


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


def _resolve_config_path(
    *,
    config_path: str | Path | None,
    env: Mapping[str, str],
) -> Path:
    canonical_path = _default_config_path().resolve()
    raw_path = config_path if config_path is not None else env.get(
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
        str(canonical_path),
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
    asserted_path = Path(raw_text).expanduser()
    if not asserted_path.is_absolute():
        asserted_path = (_repo_root() / asserted_path).resolve()
    if asserted_path != canonical_path:
        raise _fail(
            "native_instance.config_boundary",
            "runtime profiles config must be the canonical config/runtime_profiles.json file",
            details={"path": str(asserted_path), "expected": str(canonical_path)},
        )
    return canonical_path


def _assert_existing_directory(path: Path, *, field_name: str) -> None:
    if not path.exists() or not path.is_dir():
        raise _fail(
            "native_instance.boundary_violation",
            f"{field_name} must point at an existing directory",
            details={"field": field_name, "path": str(path)},
        )


def _assert_under_repo(path: Path, *, repo_root: Path, field_name: str) -> None:
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise _fail(
            "native_instance.boundary_violation",
            f"{field_name} must stay inside the workflow repo boundary",
            details={
                "field": field_name,
                "path": str(path),
                "repo_root": str(repo_root),
            },
        ) from exc


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
    """Resolve the DB-native repo-local workflow instance contract."""

    source = env if env is not None else os.environ
    resolved_config_path = _resolve_config_path(config_path=config_path, env=source)
    try:
        conn = ensure_postgres_available(env=source)
    except PostgresConfigurationError as exc:
        raise _fail(
            "native_instance.authority_unavailable",
            "native instance authority requires explicit Postgres access",
            details={
                "reason_code": exc.reason_code,
                "message": str(exc),
            },
        ) from exc

    try:
        config = resolve_native_runtime_profile_config(conn=conn)
    except NativeRuntimeProfileSyncError as exc:
        raise _fail(
            "native_instance.profile_unknown",
            "native runtime profile authority is missing or invalid",
            details={"message": str(exc)},
        ) from exc

    repo_root = Path(config.repo_root).resolve()
    workdir = Path(config.workdir).resolve()
    receipts_dir = Path(config.receipts_dir).resolve()
    topology_dir = Path(config.topology_dir).resolve()
    actual_repo_root = _repo_root().resolve()

    if repo_root != actual_repo_root:
        raise _fail(
            "native_instance.boundary_violation",
            "repo-local native runtime profile must resolve back to this workflow repo root",
            details={
                "runtime_profile_ref": config.runtime_profile_ref,
                "repo_root": str(repo_root),
                "config_repo_root": str(actual_repo_root),
            },
        )

    _assert_existing_directory(repo_root, field_name="repo_root")
    _assert_existing_directory(workdir, field_name="workdir")
    _assert_under_repo(workdir, repo_root=repo_root, field_name="workdir")
    _assert_under_repo(receipts_dir, repo_root=repo_root, field_name="receipts_dir")
    _assert_under_repo(topology_dir, repo_root=repo_root, field_name="topology_dir")

    _assert_expected_text(
        source,
        env_name=PRAXIS_RUNTIME_PROFILE_ENV,
        actual_value=config.runtime_profile_ref,
    )
    _assert_expected_text(
        source,
        env_name=PRAXIS_INSTANCE_NAME_ENV,
        actual_value=config.instance_name,
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
        instance_name=config.instance_name,
        runtime_profile_ref=config.runtime_profile_ref,
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
