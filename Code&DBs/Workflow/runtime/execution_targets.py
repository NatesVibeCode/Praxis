"""Execution target authority helpers.

This module gives runtime dispatch one vocabulary for *where* work runs.  The
older sandbox vocabulary remains as a compatibility projection, but it is not
the authority: execution target/profile are.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence


CONTROL_PLANE_API_TARGET = "execution_target.control_plane_api"
DOCKER_THIN_CLI_TARGET = "execution_target.docker_thin_cli"
DOCKER_EMPTY_TARGET = "execution_target.docker_empty"
DOCKER_FULL_TARGET = "execution_target.docker_full"
CLOUD_CONTAINER_TARGET = "execution_target.cloud_container"
PYTHON_BUNDLE_REMOTE_TARGET = "execution_target.python_bundle_remote"
EXISTING_ENDPOINT_TARGET = "execution_target.existing_endpoint"
NATIVE_TRUSTED_TARGET = "execution_target.native_trusted"
PROCESS_SANDBOX_TARGET = "execution_target.process_sandbox"

CONTROL_PLANE_API_PROFILE = "execution_profile.praxis.control_plane_api"
DOCKER_THIN_CLI_PROFILE = "execution_profile.praxis.docker_thin_cli"
DOCKER_EMPTY_PROFILE = "execution_profile.praxis.docker_empty"
DOCKER_FULL_PROFILE = "execution_profile.praxis.docker_full"
CLOUD_CONTAINER_PROFILE = "execution_profile.praxis.cloud_container"
PYTHON_BUNDLE_REMOTE_PROFILE = "execution_profile.praxis.python_bundle_remote"
EXISTING_ENDPOINT_PROFILE = "execution_profile.praxis.existing_endpoint"
NATIVE_TRUSTED_PROFILE = "execution_profile.praxis.native_trusted"
PROCESS_SANDBOX_PROFILE = "execution_profile.praxis.process_sandbox"


@dataclass(frozen=True, slots=True)
class ExecutionTarget:
    execution_target_ref: str
    execution_target_kind: str
    lane: str
    isolation_level: str
    packaging_kind: str
    supported_transports: tuple[str, ...]
    artifact_mode: str
    credential_mode: str
    health_probe: str
    resource_class: str
    admitted: bool
    disabled_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["supported_transports"] = list(self.supported_transports)
        return payload


@dataclass(frozen=True, slots=True)
class ExecutionProfile:
    execution_profile_ref: str
    execution_target_ref: str
    network_policy: str
    workspace_materialization: str
    timeout_profile: str
    resource_limits: Mapping[str, Any]
    fallback_policy: str
    sandbox_profile_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TargetResolution:
    execution_target_ref: str
    execution_target_kind: str
    execution_profile_ref: str
    isolation_level: str
    packaging_kind: str
    sandbox_provider: str
    target_resolution_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


EXECUTION_TARGETS: dict[str, ExecutionTarget] = {
    CONTROL_PLANE_API_TARGET: ExecutionTarget(
        execution_target_ref=CONTROL_PLANE_API_TARGET,
        execution_target_kind="control_plane_api",
        lane="control_plane",
        isolation_level="provider_api_boundary",
        packaging_kind="none",
        supported_transports=("API",),
        artifact_mode="provider_response",
        credential_mode="secret_authority_env",
        health_probe="provider_route_health",
        resource_class="external_api",
        admitted=True,
    ),
    DOCKER_THIN_CLI_TARGET: ExecutionTarget(
        execution_target_ref=DOCKER_THIN_CLI_TARGET,
        execution_target_kind="docker_thin_cli",
        lane="local_container",
        isolation_level="container",
        packaging_kind="thin_cli_image",
        supported_transports=("CLI",),
        artifact_mode="workspace_delta",
        credential_mode="provider_scoped_auth_mount",
        health_probe="docker_cli_smoke",
        resource_class="local_cpu_memory",
        admitted=True,
    ),
    DOCKER_EMPTY_TARGET: ExecutionTarget(
        execution_target_ref=DOCKER_EMPTY_TARGET,
        execution_target_kind="docker_empty",
        lane="local_container",
        isolation_level="container",
        packaging_kind="empty_container",
        supported_transports=("CLI", "MCP"),
        artifact_mode="workspace_delta",
        credential_mode="explicit_secret_allowlist",
        health_probe="docker_empty_probe",
        resource_class="local_cpu_memory",
        admitted=True,
    ),
    DOCKER_FULL_TARGET: ExecutionTarget(
        execution_target_ref=DOCKER_FULL_TARGET,
        execution_target_kind="docker_full",
        lane="local_container",
        isolation_level="container",
        packaging_kind="full_container",
        supported_transports=("CLI", "MCP"),
        artifact_mode="workspace_delta",
        credential_mode="explicit_secret_allowlist",
        health_probe="docker_full_probe",
        resource_class="local_cpu_memory",
        admitted=True,
    ),
    CLOUD_CONTAINER_TARGET: ExecutionTarget(
        execution_target_ref=CLOUD_CONTAINER_TARGET,
        execution_target_kind="cloud_container",
        lane="remote_container",
        isolation_level="remote_container",
        packaging_kind="cloud_container",
        supported_transports=("CLI", "MCP"),
        artifact_mode="workspace_delta",
        credential_mode="remote_secret_binding",
        health_probe="remote_container_probe",
        resource_class="remote_cpu_memory",
        admitted=True,
    ),
    PYTHON_BUNDLE_REMOTE_TARGET: ExecutionTarget(
        execution_target_ref=PYTHON_BUNDLE_REMOTE_TARGET,
        execution_target_kind="python_bundle_remote",
        lane="remote_bundle",
        isolation_level="remote_worker",
        packaging_kind="python_bundle",
        supported_transports=("API", "BUNDLE"),
        artifact_mode="bundle_mount",
        credential_mode="remote_secret_binding",
        health_probe="bundle_worker_probe",
        resource_class="remote_cpu_gpu",
        admitted=True,
    ),
    EXISTING_ENDPOINT_TARGET: ExecutionTarget(
        execution_target_ref=EXISTING_ENDPOINT_TARGET,
        execution_target_kind="existing_endpoint",
        lane="remote_endpoint",
        isolation_level="provider_endpoint_boundary",
        packaging_kind="existing_endpoint",
        supported_transports=("API", "HTTP"),
        artifact_mode="provider_response",
        credential_mode="endpoint_credential_binding",
        health_probe="endpoint_health_probe",
        resource_class="remote_cpu_gpu",
        admitted=True,
    ),
    NATIVE_TRUSTED_TARGET: ExecutionTarget(
        execution_target_ref=NATIVE_TRUSTED_TARGET,
        execution_target_kind="native_trusted",
        lane="host_process",
        isolation_level="none_trusted_dev",
        packaging_kind="host_process",
        supported_transports=("CLI", "PROCESS"),
        artifact_mode="host_workspace_delta",
        credential_mode="host_environment",
        health_probe="dev_only_operator_gate",
        resource_class="host_cpu_memory",
        admitted=True,
    ),
    PROCESS_SANDBOX_TARGET: ExecutionTarget(
        execution_target_ref=PROCESS_SANDBOX_TARGET,
        execution_target_kind="process_sandbox",
        lane="host_process",
        isolation_level="not_proven",
        packaging_kind="process_sandbox",
        supported_transports=("PROCESS",),
        artifact_mode="host_workspace_delta",
        credential_mode="host_environment",
        health_probe="blocked_until_isolation_proof",
        resource_class="host_cpu_memory",
        admitted=False,
        disabled_reason="process_sandbox.isolation_not_proven",
    ),
}

EXECUTION_PROFILES: dict[str, ExecutionProfile] = {
    CONTROL_PLANE_API_PROFILE: ExecutionProfile(
        execution_profile_ref=CONTROL_PLANE_API_PROFILE,
        execution_target_ref=CONTROL_PLANE_API_TARGET,
        network_policy="provider_api_only",
        workspace_materialization="none",
        timeout_profile="interactive_api",
        resource_limits={"max_tokens_policy": "caller_bound"},
        fallback_policy="route_failover_allowed",
    ),
    DOCKER_THIN_CLI_PROFILE: ExecutionProfile(
        execution_profile_ref=DOCKER_THIN_CLI_PROFILE,
        execution_target_ref=DOCKER_THIN_CLI_TARGET,
        network_policy="provider_api_plus_praxis_mcp",
        workspace_materialization="manifest_shard",
        timeout_profile="interactive_cli",
        resource_limits={"docker_memory": "500m", "docker_cpus": "2"},
        fallback_policy="none",
        sandbox_profile_ref="sandbox_profile.praxis.default",
    ),
    DOCKER_EMPTY_PROFILE: ExecutionProfile(
        execution_profile_ref=DOCKER_EMPTY_PROFILE,
        execution_target_ref=DOCKER_EMPTY_TARGET,
        network_policy="explicit",
        workspace_materialization="none",
        timeout_profile="bounded",
        resource_limits={"docker_memory": "500m", "docker_cpus": "2"},
        fallback_policy="none",
        sandbox_profile_ref="sandbox_profile.praxis.default",
    ),
    DOCKER_FULL_PROFILE: ExecutionProfile(
        execution_profile_ref=DOCKER_FULL_PROFILE,
        execution_target_ref=DOCKER_FULL_TARGET,
        network_policy="explicit",
        workspace_materialization="manifest_shard",
        timeout_profile="bounded",
        resource_limits={"docker_memory": "1g", "docker_cpus": "4"},
        fallback_policy="none",
        sandbox_profile_ref="sandbox_profile.praxis.default",
    ),
    CLOUD_CONTAINER_PROFILE: ExecutionProfile(
        execution_profile_ref=CLOUD_CONTAINER_PROFILE,
        execution_target_ref=CLOUD_CONTAINER_TARGET,
        network_policy="remote_worker_policy",
        workspace_materialization="snapshot_upload",
        timeout_profile="remote_bounded",
        resource_limits={"provider": "cloudflare_remote"},
        fallback_policy="none",
        sandbox_profile_ref="sandbox_profile.praxis.legacy_copy_debug",
    ),
    PYTHON_BUNDLE_REMOTE_PROFILE: ExecutionProfile(
        execution_profile_ref=PYTHON_BUNDLE_REMOTE_PROFILE,
        execution_target_ref=PYTHON_BUNDLE_REMOTE_TARGET,
        network_policy="remote_worker_policy",
        workspace_materialization="python_bundle",
        timeout_profile="remote_bounded",
        resource_limits={"artifact": "python_wheel_bundle"},
        fallback_policy="none",
    ),
    EXISTING_ENDPOINT_PROFILE: ExecutionProfile(
        execution_profile_ref=EXISTING_ENDPOINT_PROFILE,
        execution_target_ref=EXISTING_ENDPOINT_TARGET,
        network_policy="endpoint_policy",
        workspace_materialization="none",
        timeout_profile="interactive_api",
        resource_limits={"endpoint": "preprovisioned"},
        fallback_policy="route_failover_allowed",
    ),
    NATIVE_TRUSTED_PROFILE: ExecutionProfile(
        execution_profile_ref=NATIVE_TRUSTED_PROFILE,
        execution_target_ref=NATIVE_TRUSTED_TARGET,
        network_policy="host_default",
        workspace_materialization="host_workspace",
        timeout_profile="dev_only",
        resource_limits={"process": "host"},
        fallback_policy="none",
    ),
    PROCESS_SANDBOX_PROFILE: ExecutionProfile(
        execution_profile_ref=PROCESS_SANDBOX_PROFILE,
        execution_target_ref=PROCESS_SANDBOX_TARGET,
        network_policy="blocked",
        workspace_materialization="blocked",
        timeout_profile="blocked",
        resource_limits={"admitted": False},
        fallback_policy="none",
    ),
}


def execution_targets_list(*, include_disabled: bool = False) -> list[dict[str, Any]]:
    targets = [
        target.to_dict()
        for target in EXECUTION_TARGETS.values()
        if include_disabled or target.admitted
    ]
    return sorted(targets, key=lambda row: row["execution_target_ref"])


def execution_profiles_list(*, include_disabled: bool = False) -> list[dict[str, Any]]:
    target_refs = {
        row["execution_target_ref"]
        for row in execution_targets_list(include_disabled=include_disabled)
    }
    profiles = [
        profile.to_dict()
        for profile in EXECUTION_PROFILES.values()
        if profile.execution_target_ref in target_refs
    ]
    return sorted(profiles, key=lambda row: row["execution_profile_ref"])


def _safe_ref_fragment(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_.-]+", "_", text)
    text = text.strip("._-")
    return text or "unknown"


def candidate_ref_for(
    *,
    task_type: str,
    provider_slug: str,
    model_slug: str,
    transport_type: str,
) -> str:
    return ".".join(
        (
            "dispatch_option",
            _safe_ref_fragment(task_type),
            _safe_ref_fragment(transport_type),
            _safe_ref_fragment(provider_slug),
            _safe_ref_fragment(model_slug),
        )
    )


def _profile_for_target(target_ref: str) -> str:
    for profile in EXECUTION_PROFILES.values():
        if profile.execution_target_ref == target_ref:
            return profile.execution_profile_ref
    return PROCESS_SANDBOX_PROFILE


def resolve_target_for_transport(
    *,
    transport_type: str | None,
    sandbox_provider: str | None = None,
    workspace_materialization: str | None = None,
    explicit_target_ref: str | None = None,
    explicit_profile_ref: str | None = None,
    allow_disabled: bool = False,
) -> TargetResolution:
    reason = "transport_default"
    profile_ref = explicit_profile_ref
    target_ref = explicit_target_ref

    if profile_ref:
        profile = EXECUTION_PROFILES.get(profile_ref)
        if profile is None:
            raise ValueError(f"unknown execution_profile_ref: {profile_ref}")
        target_ref = profile.execution_target_ref
        reason = "explicit_profile"

    if target_ref:
        reason = "explicit_target" if reason == "transport_default" else reason
    else:
        transport = str(transport_type or "").strip().upper()
        provider = str(sandbox_provider or "").strip().lower()
        materialization = str(workspace_materialization or "").strip().lower()
        if (
            transport == "API"
            and provider not in {"docker_local", "cloudflare_remote"}
        ):
            target_ref = CONTROL_PLANE_API_TARGET
        elif provider == "cloudflare_remote" or transport == "MCP":
            target_ref = CLOUD_CONTAINER_TARGET
        elif transport == "CLI":
            target_ref = DOCKER_THIN_CLI_TARGET
        elif materialization == "none":
            target_ref = DOCKER_EMPTY_TARGET
        else:
            target_ref = DOCKER_FULL_TARGET

    target = EXECUTION_TARGETS.get(target_ref)
    if target is None:
        raise ValueError(f"unknown execution_target_ref: {target_ref}")
    if not target.admitted and not allow_disabled:
        raise ValueError(target.disabled_reason or f"{target_ref} is not admitted")

    if not profile_ref:
        profile_ref = _profile_for_target(target.execution_target_ref)

    sandbox_provider_compat = "control_plane"
    if target.execution_target_ref in {
        DOCKER_THIN_CLI_TARGET,
        DOCKER_EMPTY_TARGET,
        DOCKER_FULL_TARGET,
    }:
        sandbox_provider_compat = "docker_local"
    elif target.execution_target_ref == CLOUD_CONTAINER_TARGET:
        sandbox_provider_compat = "cloudflare_remote"
    elif target.execution_target_ref == NATIVE_TRUSTED_TARGET:
        sandbox_provider_compat = "native_trusted"
    elif target.execution_target_ref == PROCESS_SANDBOX_TARGET:
        sandbox_provider_compat = "process_sandbox"

    return TargetResolution(
        execution_target_ref=target.execution_target_ref,
        execution_target_kind=target.execution_target_kind,
        execution_profile_ref=profile_ref,
        isolation_level=target.isolation_level,
        packaging_kind=target.packaging_kind,
        sandbox_provider=sandbox_provider_compat,
        target_resolution_reason=reason,
    )


def resolution_for_payload(
    *,
    execution_transport: str | None,
    sandbox_provider: str | None,
    execution_mode: str | None = None,
    workspace_materialization: str | None = None,
) -> TargetResolution:
    transport = str(execution_transport or "").strip().upper()
    if str(execution_mode or "").strip().lower() == "control_plane":
        transport = "API"
    return resolve_target_for_transport(
        transport_type=transport,
        sandbox_provider=sandbox_provider,
        workspace_materialization=workspace_materialization,
    )


def enrich_execution_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(payload)
    if enriched.get("execution_target_ref"):
        return enriched
    resolution = resolution_for_payload(
        execution_transport=enriched.get("execution_transport"),
        sandbox_provider=enriched.get("sandbox_provider"),
        execution_mode=enriched.get("execution_mode"),
        workspace_materialization=enriched.get("workspace_materialization"),
    )
    enriched.update(resolution.to_dict())
    return enriched


def candidate_set_hash(candidates: Sequence[Mapping[str, Any]]) -> str:
    canonical_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        canonical_rows.append(
            {
                "candidate_ref": candidate.get("candidate_ref"),
                "provider_slug": candidate.get("provider_slug"),
                "model_slug": candidate.get("model_slug"),
                "transport_type": candidate.get("transport_type"),
                "execution_target_ref": candidate.get("execution_target_ref"),
                "execution_profile_ref": candidate.get("execution_profile_ref"),
                "permitted": bool(candidate.get("permitted")),
                "disabled_reason": candidate.get("disabled_reason"),
            }
        )
    encoded = json.dumps(
        canonical_rows,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def enrich_dispatch_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(candidate)
    task_type = str(payload.get("task_type") or "chat")
    transport_type = str(payload.get("transport_type") or "API").strip().upper()
    try:
        resolution = resolve_target_for_transport(transport_type=transport_type)
        disabled_reason = payload.get("disabled_reason")
    except ValueError as exc:
        resolution = resolve_target_for_transport(
            transport_type="PROCESS",
            explicit_target_ref=PROCESS_SANDBOX_TARGET,
            explicit_profile_ref=PROCESS_SANDBOX_PROFILE,
            allow_disabled=True,
        )
        disabled_reason = str(exc)
    payload.setdefault(
        "candidate_ref",
        candidate_ref_for(
            task_type=task_type,
            provider_slug=str(payload.get("provider_slug") or ""),
            model_slug=str(payload.get("model_slug") or ""),
            transport_type=transport_type,
        ),
    )
    payload.update(resolution.to_dict())
    payload["dispatch_pin"] = {
        "provider_slug": payload.get("provider_slug"),
        "model_slug": payload.get("model_slug"),
        "transport_type": transport_type,
        "execution_target_ref": payload.get("execution_target_ref"),
        "execution_profile_ref": payload.get("execution_profile_ref"),
    }
    if disabled_reason:
        payload["disabled_reason"] = disabled_reason
    return payload


def attach_candidate_set_hash(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    enriched = [dict(candidate) for candidate in candidates]
    digest = candidate_set_hash(enriched)
    for candidate in enriched:
        candidate["candidate_set_hash"] = digest
    return enriched


def selected_candidate_from_set(
    *,
    candidates: Sequence[Mapping[str, Any]],
    selected_candidate_ref: str | None,
    selected_provider_slug: str | None = None,
    selected_model_slug: str | None = None,
    selected_transport_type: str | None = None,
) -> dict[str, Any]:
    if selected_candidate_ref:
        for candidate in candidates:
            if candidate.get("candidate_ref") == selected_candidate_ref:
                return dict(candidate)
        raise ValueError("selected candidate was not present in candidate set")

    provider = str(selected_provider_slug or "").strip()
    model = str(selected_model_slug or "").strip()
    transport = str(selected_transport_type or "").strip().upper()
    for candidate in candidates:
        if (
            str(candidate.get("provider_slug") or "") == provider
            and str(candidate.get("model_slug") or "") == model
            and (not transport or str(candidate.get("transport_type") or "").upper() == transport)
        ):
            return dict(candidate)
    raise ValueError("selected provider/model was not present in candidate set")


__all__ = [
    "CONTROL_PLANE_API_PROFILE",
    "CONTROL_PLANE_API_TARGET",
    "DOCKER_THIN_CLI_PROFILE",
    "DOCKER_THIN_CLI_TARGET",
    "EXECUTION_PROFILES",
    "EXECUTION_TARGETS",
    "TargetResolution",
    "attach_candidate_set_hash",
    "candidate_ref_for",
    "candidate_set_hash",
    "enrich_dispatch_candidate",
    "enrich_execution_payload",
    "execution_profiles_list",
    "execution_targets_list",
    "resolve_target_for_transport",
    "resolution_for_payload",
    "selected_candidate_from_set",
]
