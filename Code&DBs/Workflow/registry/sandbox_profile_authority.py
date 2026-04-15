"""DB-backed sandbox profile authority for runtime execution."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .domain import SandboxProfileAuthorityRecord
from .native_runtime_profile_sync import (
    is_native_runtime_profile_ref,
    sync_native_runtime_profile_authority,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


class SandboxProfileAuthorityError(RuntimeError):
    """Raised when sandbox authority cannot be resolved safely."""

    def __init__(self, reason_code: str, details: str) -> None:
        super().__init__(details)
        self.reason_code = reason_code
        self.details = details


def _require_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SandboxProfileAuthorityError(
            "registry.invalid_sandbox_profile",
            f"{field_name} must be a non-empty string",
        )
    return text


def _normalize_secret_allowlist(value: object) -> tuple[str, ...]:
    parsed: Any = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = [value]
    if not isinstance(parsed, list):
        return ()
    normalized: list[str] = []
    for entry in parsed:
        text = str(entry or "").strip()
        if text:
            normalized.append(text)
    return tuple(dict.fromkeys(normalized))


def _row_to_record(row: dict[str, object]) -> SandboxProfileAuthorityRecord:
    return SandboxProfileAuthorityRecord(
        sandbox_profile_ref=_require_text(
            row.get("sandbox_profile_ref"),
            field_name="sandbox_profile_ref",
        ),
        sandbox_provider=_require_text(
            row.get("sandbox_provider"),
            field_name="sandbox_provider",
        ),
        docker_image=str(row.get("docker_image") or "").strip() or None,
        docker_cpus=str(row.get("docker_cpus") or "").strip() or None,
        docker_memory=str(row.get("docker_memory") or "").strip() or None,
        network_policy=_require_text(
            row.get("network_policy"),
            field_name="network_policy",
        ),
        workspace_materialization=_require_text(
            row.get("workspace_materialization"),
            field_name="workspace_materialization",
        ),
        secret_allowlist=_normalize_secret_allowlist(row.get("secret_allowlist")),
        auth_mount_policy=_require_text(
            row.get("auth_mount_policy"),
            field_name="auth_mount_policy",
        ),
        timeout_profile=_require_text(
            row.get("timeout_profile"),
            field_name="timeout_profile",
        ),
    )


def load_runtime_sandbox_profile_authority(
    conn: "SyncPostgresConnection",
    *,
    runtime_profile_ref: str,
) -> SandboxProfileAuthorityRecord:
    normalized_runtime_profile_ref = _require_text(
        runtime_profile_ref,
        field_name="runtime_profile_ref",
    )
    if is_native_runtime_profile_ref(normalized_runtime_profile_ref):
        sync_native_runtime_profile_authority(conn, prune=False)

    rows = conn.execute(
        """
        SELECT
            runtime.runtime_profile_ref,
            runtime.sandbox_profile_ref,
            sandbox.sandbox_provider,
            sandbox.docker_image,
            sandbox.docker_cpus,
            sandbox.docker_memory,
            sandbox.network_policy,
            sandbox.workspace_materialization,
            sandbox.secret_allowlist,
            sandbox.auth_mount_policy,
            sandbox.timeout_profile
        FROM registry_runtime_profile_authority AS runtime
        LEFT JOIN registry_sandbox_profile_authority AS sandbox
          ON sandbox.sandbox_profile_ref = runtime.sandbox_profile_ref
        WHERE runtime.runtime_profile_ref = $1
        LIMIT 1
        """,
        normalized_runtime_profile_ref,
    )
    if not rows:
        raise SandboxProfileAuthorityError(
            "registry.runtime_profile_unknown",
            f"runtime profile {normalized_runtime_profile_ref!r} is missing sandbox authority",
        )

    row = dict(rows[0])
    sandbox_profile_ref = str(row.get("sandbox_profile_ref") or "").strip()
    if not sandbox_profile_ref:
        raise SandboxProfileAuthorityError(
            "registry.sandbox_profile_missing",
            f"runtime profile {normalized_runtime_profile_ref!r} did not declare a sandbox_profile_ref",
        )
    if not row.get("sandbox_provider"):
        raise SandboxProfileAuthorityError(
            "registry.sandbox_profile_missing",
            f"runtime profile {normalized_runtime_profile_ref!r} references missing sandbox profile {sandbox_profile_ref!r}",
        )
    return _row_to_record(row)


def sandbox_profile_execution_payload(
    record: SandboxProfileAuthorityRecord,
) -> dict[str, object]:
    return {
        "sandbox_profile_ref": record.sandbox_profile_ref,
        "sandbox_provider": record.sandbox_provider,
        "docker_image": record.docker_image,
        "docker_cpus": record.docker_cpus,
        "docker_memory": record.docker_memory,
        "network_policy": record.network_policy,
        "workspace_materialization": record.workspace_materialization,
        "secret_allowlist": list(record.secret_allowlist),
        "auth_mount_policy": record.auth_mount_policy,
        "timeout_profile": record.timeout_profile,
    }


__all__ = [
    "SandboxProfileAuthorityError",
    "load_runtime_sandbox_profile_authority",
    "sandbox_profile_execution_payload",
]
