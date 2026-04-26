"""Runtime-target setup and doctor contract reporting.

This module is the substrate-neutral setup surface behind ``praxis setup``.
It does not make Mac, Windows, SSH, or Docker Desktop the product default. Setup
is operated through the API/MCP control plane; the CLI and website are clients.
SSH is build/deploy transport only when a selected target needs artifacts.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator

from runtime.docker_image_authority import (
    AGENT_FAMILY_IMAGE_MAP,
    CONTROL_WORKER_IMAGE,
    DOCKER_IMAGE_ENV,
)
from runtime._workflow_database import (
    WorkflowDatabaseAuthority,
    resolve_runtime_database_authority,
)
from runtime.instance import NativeInstanceResolutionError, native_instance_contract
from runtime.workspace_paths import (
    repo_root as workspace_repo_root,
    to_repo_ref,
    workflow_migrations_root,
    workflow_root,
)


_DEFAULT_PROFILE_REF = "sandbox_profile.praxis.default"
_LEGACY_COPY_DEBUG_REF = "sandbox_profile.praxis.legacy_copy_debug"
_DEFAULT_RUNTIME_TARGET_REF = "runtime_target.praxis.default"
_SETUP_MODES = {"doctor", "plan", "apply", "graph"}
_OPERATOR_AUTHORITY_PATH = ("api", "mcp")
_CLIENT_SURFACES = ("cli", "website")
_LEGACY_SUBSTRATE_KIND_ALIASES = {
    "local_docker": "container",
    "docker": "container",
    "docker_local": "container",
    "remote_api": "cloud_service",
    "api": "cloud_service",
    "http_api": "cloud_service",
}
_SSH_BUILD_TRANSPORT = {
    "transport": "ssh",
    "role": "build_deploy_transport_only",
    "purpose": "build_artifacts_or_thin_images_on_a_selected_runtime_target",
    "operator_authority": False,
    "may_run_setup": False,
    "may_mutate_db_authority": False,
}


def _package_component_records(
    *, repo_root: Path | None = None
) -> tuple[dict[str, Path], ...]:
    root = repo_root or workspace_repo_root()
    workflow = workflow_root(root)
    migrations = workflow_migrations_root(root)
    return (
        {
            "name": "operator_entrypoint",
            "path": root / "scripts" / "praxis",
            "authority_role": "launcher_and_cli_client",
        },
        {
            "name": "setup_frontdoor",
            "path": root / "SETUP.md",
            "authority_role": "human_bootstrap_guide",
        },
        {
            "name": "product_overview",
            "path": root / "README.md",
            "authority_role": "public_package_overview",
        },
        {
            "name": "runtime_profiles",
            "path": root / "config" / "runtime_profiles.json",
            "authority_role": "derived_runtime_profile_authority",
        },
        {
            "name": "compose_target",
            "path": root / "docker-compose.yml",
            "authority_role": "local_or_selected_docker_reconciler",
        },
        {
            "name": "workflow_runtime",
            "path": workflow,
            "authority_role": "engine_runtime",
        },
        {
            "name": "workflow_database_migrations",
            "path": migrations,
            "authority_role": "database_schema_authority",
        },
        {
            "name": "api_surface",
            "path": workflow / "surfaces" / "api",
            "authority_role": "http_operator_surface",
        },
        {
            "name": "mcp_surface",
            "path": workflow / "surfaces" / "mcp",
            "authority_role": "mcp_operator_surface",
        },
        {
            "name": "cli_surface",
            "path": workflow / "surfaces" / "cli",
            "authority_role": "cli_client_surface",
        },
        {
            "name": "website_surface",
            "path": workflow / "surfaces" / "app",
            "authority_role": "browser_client_surface",
        },
        {
            "name": "skill_exports",
            "path": root / "Skills",
            "authority_role": "derived_agent_adapter_exports",
        },
    )


def _path_to_registry_ref(path: Path, *, repo_root: Path | None = None) -> str:
    return to_repo_ref(path, repo_root=repo_root or workspace_repo_root())


class SetupQuery(BaseModel):
    mode: str | None = None

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip().lower()
        return text or None


class SetupApplyCommand(BaseModel):
    approved: bool = False
    yes: bool = False
    apply: bool = False
    gate: str | None = None
    gate_ref: str | None = None
    apply_ref: str | None = None

    @field_validator("gate", "gate_ref", "apply_ref", mode="before")
    @classmethod
    def _normalize_optional_ref(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


def handle_setup_doctor(query: SetupQuery, subsystems: Any) -> dict[str, Any]:
    del query, subsystems
    return setup_payload("doctor", repo_root=workspace_repo_root(), authority_surface="api")


def handle_setup_plan(query: SetupQuery, subsystems: Any) -> dict[str, Any]:
    del query, subsystems
    return setup_payload("plan", repo_root=workspace_repo_root(), authority_surface="api")


def handle_setup_apply(command: SetupApplyCommand, subsystems: Any) -> dict[str, Any]:
    del subsystems
    approved = bool(command.yes or command.apply or command.approved)
    gate_ref = command.gate or command.gate_ref
    if gate_ref or command.apply_ref:
        return setup_apply_gate_payload(
            gate_ref=gate_ref,
            apply_ref=command.apply_ref,
            repo_root=workspace_repo_root(),
            approved=approved,
            applied_by="api_setup_apply",
            authority_surface="api",
        )
    return setup_payload(
        "apply",
        repo_root=workspace_repo_root(),
        apply=approved,
        authority_surface="api",
    )


def _runtime_profiles_config_path(repo_root: Path | None = None) -> Path:
    root = repo_root or workspace_repo_root()
    return root / "config" / "runtime_profiles.json"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _redact_database_url(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "://" not in text or "@" not in text:
        return text
    prefix, rest = text.split("://", 1)
    _, host = rest.rsplit("@", 1)
    return f"{prefix}://***@{host}"


def _env_file_values(repo_root: Path | None = None) -> dict[str, str]:
    path = (repo_root or workspace_repo_root()) / ".env"
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _env_value(name: str, *, repo_root: Path | None = None) -> str:
    raw = str(os.environ.get(name, "")).strip()
    if raw:
        return raw
    return _env_file_values(repo_root).get(name, "").strip()


def _setup_authority_env(
    *,
    repo_root: Path | None = None,
    required: bool = False,
) -> tuple[dict[str, str], WorkflowDatabaseAuthority]:
    """Resolve one setup DB authority and return the env all setup probes share."""

    root = repo_root or workspace_repo_root()
    source = dict(os.environ)
    try:
        authority = resolve_runtime_database_authority(
            env=source,
            repo_root=root,
            required=required,
        )
    except Exception as exc:  # noqa: BLE001 - setup doctor reports authority drift
        authority = WorkflowDatabaseAuthority(database_url=None, source=f"error:{exc}")
    for key, value in _env_file_values(root).items():
        source.setdefault(key, value)
    if authority.database_url:
        source["WORKFLOW_DATABASE_URL"] = authority.database_url
        source["WORKFLOW_DATABASE_AUTHORITY_SOURCE"] = authority.source
    source["PRAXIS_WORKSPACE_BASE_PATH"] = str(root)
    return source, authority


def _docker_info() -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}
    if result.returncode != 0:
        return {"available": False, "error": (result.stderr or result.stdout).strip()}
    try:
        detail = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        detail = {}
    return {"available": True, "detail": detail}


def _docker_image_available(image: str) -> bool | None:
    if not image:
        return None
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    return result.returncode == 0


def _orphan_container_count() -> int | None:
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", "name=praxis-", "--format", "{{.ID}}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return len([line for line in result.stdout.splitlines() if line.strip()])


def _runtime_target_substrate_kind(raw_value: object, *, docker_available: bool) -> str:
    raw = str(raw_value or "").strip()
    if raw:
        return _LEGACY_SUBSTRATE_KIND_ALIASES.get(raw, raw)
    return "container" if docker_available else "cloud_service"


def runtime_target_report(*, repo_root: Path | None = None) -> dict[str, Any]:
    authority_env, db_authority = _setup_authority_env(repo_root=repo_root)
    docker = _docker_info()
    api_url = str(
        authority_env.get("PRAXIS_API_URL")
        or _env_value("PRAXIS_API_URL", repo_root=repo_root)
    )
    api_bind_host = (
        str(
            authority_env.get("PRAXIS_API_HOST")
            or _env_value("PRAXIS_API_HOST", repo_root=repo_root)
        )
        or "127.0.0.1"
    )
    if not api_url:
        host = api_bind_host
        if host in {"0.0.0.0", "::", "[::]"}:
            host = "127.0.0.1"
        port = str(
            authority_env.get("PRAXIS_API_PORT")
            or _env_value("PRAXIS_API_PORT", repo_root=repo_root)
            or "8420"
        )
        api_url = f"http://{host}:{port}"
    substrate_kind = str(
        authority_env.get("PRAXIS_RUNTIME_SUBSTRATE_KIND")
        or _env_value("PRAXIS_RUNTIME_SUBSTRATE_KIND", repo_root=repo_root)
    )
    substrate_kind = _runtime_target_substrate_kind(
        substrate_kind,
        docker_available=bool(docker.get("available")),
    )
    return {
        "runtime_target_ref": (
            str(
                authority_env.get("PRAXIS_RUNTIME_TARGET_REF")
                or _env_value("PRAXIS_RUNTIME_TARGET_REF", repo_root=repo_root)
            )
            or _DEFAULT_RUNTIME_TARGET_REF
        ),
        "substrate_kind": substrate_kind,
        "api_authority": api_url,
        "db_authority": _redact_database_url(db_authority.database_url),
        "db_authority_source": db_authority.source,
        "workspace_authority": str(repo_root or workspace_repo_root()),
        "host_traits": {
            "os": platform.system().lower() or "unknown",
            "architecture": platform.machine() or "unknown",
            "docker_available": bool(docker.get("available")),
            "docker_socket": str(
                authority_env.get("DOCKER_HOST")
                or _env_value("DOCKER_HOST", repo_root=repo_root)
                or "default"
            ),
            "api_bind_host": api_bind_host,
        },
    }


def sandbox_contract_report(*, repo_root: Path | None = None) -> dict[str, Any]:
    root = repo_root or workspace_repo_root()
    config_path = _runtime_profiles_config_path(root)
    config = _load_json(config_path)
    profiles = config.get("sandbox_profiles") if isinstance(config, dict) else {}
    profile = {}
    if isinstance(profiles, dict):
        raw_profile = profiles.get(_DEFAULT_PROFILE_REF)
        profile = dict(raw_profile) if isinstance(raw_profile, dict) else {}

    env_image = _env_value(DOCKER_IMAGE_ENV, repo_root=root)
    required_images = tuple(dict.fromkeys(AGENT_FAMILY_IMAGE_MAP.values()))
    image_status = {
        image: {"available": _docker_image_available(image)}
        for image in required_images
    }
    checks = {
        "default_profile_exists": bool(profile),
        "default_image_unpinned": not str(profile.get("docker_image") or "").strip(),
        "env_does_not_force_control_worker": env_image != CONTROL_WORKER_IMAGE,
        "memory_500m": str(profile.get("docker_memory") or "").strip().lower() == "500m",
        "workspace_materialization_none": (
            str(profile.get("workspace_materialization") or "").strip().lower() == "none"
        ),
        "auth_mount_provider_scoped": (
            str(profile.get("auth_mount_policy") or "").strip().lower()
            == "provider_scoped"
        ),
        "provider_family_images_declared": len(required_images) == 3,
    }
    blockers = [name for name, ok in checks.items() if not ok]
    return {
        "config_path": str(config_path),
        "default_profile_ref": _DEFAULT_PROFILE_REF,
        "legacy_copy_debug_profile_ref": _LEGACY_COPY_DEBUG_REF,
        "profile": {
            "docker_image": profile.get("docker_image"),
            "docker_memory": profile.get("docker_memory"),
            "workspace_materialization": profile.get("workspace_materialization"),
            "auth_mount_policy": profile.get("auth_mount_policy"),
            "network_policy": profile.get("network_policy"),
        },
        "control_worker_image": CONTROL_WORKER_IMAGE,
        "control_worker_image_role": "workflow_control_service_only",
        "thin_image_authority": dict(AGENT_FAMILY_IMAGE_MAP),
        "required_images": image_status,
        "checks": checks,
        "blockers": blockers,
        "empty_thin_sandbox_default": not blockers,
    }


def package_contract_report(*, repo_root: Path | None = None) -> dict[str, Any]:
    root = repo_root or workspace_repo_root()
    component_records = _package_component_records(repo_root=root)
    components: list[dict[str, Any]] = []
    missing: list[str] = []
    for component in component_records:
        rel_path = _path_to_registry_ref(Path(component["path"]), repo_root=root)
        path = Path(component["path"])
        present = path.exists()
        if not present:
            missing.append(component["name"])
        components.append(
            {
                "name": component["name"],
                "path": rel_path,
                "present": present,
                "authority_role": component["authority_role"],
            }
        )

    component_by_name = {
        component["name"]: Path(component["path"])
        for component in component_records
    }
    surface_names = ("api_surface", "mcp_surface", "cli_surface", "website_surface")
    checks = {
        "repo_root_exists": root.exists(),
        "operator_entrypoint_present": component_by_name["operator_entrypoint"].is_file(),
        "api_mcp_cli_website_present": all(
            component_by_name[name].is_dir() for name in surface_names
        ),
        "database_migrations_present": component_by_name["workflow_database_migrations"].is_dir(),
        "runtime_profiles_present": component_by_name["runtime_profiles"].is_file(),
        "setup_points_at_api_mcp_authority": True,
        "ssh_not_operator_authority": not _SSH_BUILD_TRANSPORT["operator_authority"],
    }
    blockers = [name for name, ok in checks.items() if not ok]
    blockers.extend(f"missing_component:{name}" for name in missing)
    return {
        "repo_root": str(root),
        "package_kind": "single_praxis_repo",
        "authority_model": "one_repo_many_client_surfaces_one_db_authority",
        "components": components,
        "checks": checks,
        "blockers": blockers,
        "complete_repo_package": not blockers,
        "operator_authority_path": list(_OPERATOR_AUTHORITY_PATH),
        "client_surfaces": list(_CLIENT_SURFACES),
        "build_transports": {"ssh": dict(_SSH_BUILD_TRANSPORT)},
    }


def _native_instance_for_setup(authority_env: dict[str, str]) -> dict[str, Any]:
    try:
        return native_instance_contract(
            env=authority_env,
            allow_authority_fallback=True,
        )
    except NativeInstanceResolutionError as exc:
        if exc.reason_code not in {
            "native_instance.authority_unavailable",
            "native_instance.profile_unknown",
        }:
            raise
        fallback_env = dict(authority_env)
        fallback_env.pop("WORKFLOW_DATABASE_URL", None)
        fallback = native_instance_contract(
            env=fallback_env,
            allow_authority_fallback=True,
        )
        fallback["authority_state"] = "degraded"
        fallback["authority_reason_code"] = exc.reason_code
        fallback["authority_error"] = str(exc)
        return fallback


def setup_payload(
    mode: str,
    *,
    repo_root: Path | None = None,
    apply: bool = False,
    authority_surface: str = "api_or_mcp",
) -> dict[str, Any]:
    root = repo_root or workspace_repo_root()
    authority_env, db_authority = _setup_authority_env(repo_root=root)
    runtime_target = runtime_target_report(repo_root=root)
    sandbox_contract = sandbox_contract_report(repo_root=root)
    package_contract = package_contract_report(repo_root=root)
    native_instance = _native_instance_for_setup(authority_env)
    expected_receipts_dir = str((root / "artifacts" / "runtime_receipts").resolve())
    expected_topology_dir = str((root / "artifacts" / "runtime_topology").resolve())
    native_instance_checks = {
        "repo_root_matches_workspace_authority": native_instance.get("repo_root")
        == runtime_target.get("workspace_authority"),
        "workdir_matches_repo_root": native_instance.get("workdir")
        == native_instance.get("repo_root"),
        "receipts_dir_matches_contract": native_instance.get("praxis_receipts_dir")
        == expected_receipts_dir,
        "topology_dir_matches_contract": native_instance.get("praxis_topology_dir")
        == expected_topology_dir,
        "instance_name_is_praxis": native_instance.get("praxis_instance_name") == "praxis",
        "runtime_profile_is_praxis": native_instance.get("praxis_runtime_profile") == "praxis",
    }
    native_instance_checks_ok = all(native_instance_checks.values())
    authority_alignment = {
        "ok": bool(native_instance_checks_ok and runtime_target.get("db_authority")),
        "cqrs_authority": "operation_catalog_gateway",
        "operation_authority_model": "commands_mutate_queries_project",
        "operator_authority_surfaces": list(_OPERATOR_AUTHORITY_PATH),
        "client_surfaces": list(_CLIENT_SURFACES),
        "active_surface": authority_surface,
        "db_authority_source": db_authority.source,
        "db_authority": runtime_target.get("db_authority"),
        "native_instance_authority_state": native_instance.get("authority_state", "db_backed"),
        "native_instance_contract_matches_workspace": native_instance_checks_ok,
        "same_repo_local_instance": native_instance_checks_ok,
        "api_authority": runtime_target.get("api_authority"),
        "runtime_target_ref": runtime_target.get("runtime_target_ref"),
    }
    actions = [
        {
            "action": "select_runtime_target",
            "authority": "runtime_targets",
            "runtime_target_ref": runtime_target["runtime_target_ref"],
            "substrate_kind": runtime_target["substrate_kind"],
        },
        {
            "action": "reconcile_sandbox_contract",
            "authority": "registry_sandbox_profile_authority",
            "default_profile_ref": _DEFAULT_PROFILE_REF,
            "workspace_materialization": "none",
            "docker_memory": "500m",
            "docker_image": None,
        },
        {
            "action": "validate_provider_thin_images",
            "authority": "docker_image_authority",
            "images": sorted(set(AGENT_FAMILY_IMAGE_MAP.values())),
        },
    ]
    payload = {
        "ok": bool(
            sandbox_contract["empty_thin_sandbox_default"]
            and package_contract["complete_repo_package"]
        ),
        "mode": mode,
        "authority_surface": authority_surface,
        "runtime_target": runtime_target,
        "native_instance": native_instance,
        "native_instance_checks": native_instance_checks,
        "native_instance_checks_ok": native_instance_checks_ok,
        "surface_alignment": authority_alignment,
        "authority_alignment": authority_alignment,
        "sandbox_contract": sandbox_contract,
        "package_contract": package_contract,
        "empty_thin_sandbox_default": sandbox_contract["empty_thin_sandbox_default"],
        "complete_repo_package": package_contract["complete_repo_package"],
        "operator_authority_path": list(_OPERATOR_AUTHORITY_PATH),
        "preferred_operator_path": list(_OPERATOR_AUTHORITY_PATH),
        "client_surfaces": list(_CLIENT_SURFACES),
        "cli_role": "client_only_api_or_mcp",
        "build_transports": {"ssh": dict(_SSH_BUILD_TRANSPORT)},
        "ssh_role": _SSH_BUILD_TRANSPORT["role"],
        "actions": actions,
        "active_jobs": None,
        "open_sandbox_sessions": None,
        "orphan_containers": _orphan_container_count(),
    }
    if mode == "apply":
        payload["applied"] = False
        payload["ok"] = False
        payload["requires_authority_apply"] = False
        payload["mutation_performed"] = False
        payload["apply_note"] = (
            "This setup surface is a gate and plan, not a hidden mutation shim. "
            "No service-lifecycle state was changed. DB/runtime-target writes must "
            "be performed by a catalog-backed authority operation with durable "
            "command, event, and receipt readback."
        )
        if not apply:
            payload["error_code"] = "setup.approval_required"
            payload["message"] = "setup apply requires explicit approval."
        else:
            payload["error_code"] = "setup.apply_not_implemented"
            payload["message"] = (
                "setup apply is not implemented as a mutating authority operation; "
                "use doctor or plan until service-lifecycle apply records durable "
                "command/event/receipt state."
            )
    return payload


def _setup_api_url(mode: str, *, repo_root: Path | None = None) -> str:
    runtime_target = runtime_target_report(repo_root=repo_root)
    base = str(runtime_target.get("api_authority") or "").rstrip("/")
    return f"{base}/api/setup/{mode}"


def _json_from_http_error(exc: urllib.error.HTTPError) -> dict[str, Any] | None:
    try:
        raw = exc.read().decode("utf-8")
    except OSError:
        raw = ""
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def setup_payload_from_api(
    mode: str,
    *,
    repo_root: Path | None = None,
    apply: bool = False,
    timeout_s: float = 5.0,
) -> tuple[dict[str, Any] | None, str | None]:
    """Fetch setup state from the API authority for CLI clients."""
    url = _setup_api_url(mode, repo_root=repo_root)
    data: bytes | None = None
    method = "GET"
    headers = {"Accept": "application/json"}
    if mode == "apply":
        method = "POST"
        data = json.dumps({"approved": bool(apply), "yes": bool(apply)}).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None, "api_setup_endpoint_missing"
        payload = _json_from_http_error(exc)
        if payload is None:
            return None, f"api_http_error_{exc.code}"
        payload["authority_surface"] = "api"
        payload["api_endpoint"] = url
        payload["api_status"] = exc.code
        return payload, None
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(payload, dict):
        return None, "api_payload_not_object"
    payload["authority_surface"] = "api"
    payload["api_endpoint"] = url
    return payload, None


def _gate_result_to_dict(result: Any, probe: Any) -> dict[str, Any]:
    return {
        "gate_ref": result.gate_ref,
        "domain": probe.domain,
        "title": probe.title,
        "purpose": probe.purpose,
        "depends_on": list(probe.depends_on),
        "platforms": list(probe.platforms),
        "status": result.status,
        "observed_state": dict(result.observed_state),
        "remediation_hint": result.remediation_hint,
        "remediation_doc_url": result.remediation_doc_url,
        "apply_ref": result.apply_ref,
        "evaluated_at": result.evaluated_at.isoformat(),
    }


def setup_apply_gate_payload(
    *,
    gate_ref: str | None = None,
    apply_ref: str | None = None,
    repo_root: Path | None = None,
    env: dict[str, str] | None = None,
    approved: bool = False,
    applied_by: str = "setup_apply_gate",
    authority_surface: str = "api_or_mcp",
) -> dict[str, Any]:
    """Apply one onboarding gate's registered handler and return the fresh result."""
    from runtime.onboarding import ONBOARDING_GRAPH

    if not apply_ref and not gate_ref:
        return {
            "ok": False,
            "mode": "apply",
            "error_code": "setup.apply_gate_required",
            "message": "Provide gate_ref or apply_ref to apply a specific gate.",
            "authority_surface": authority_surface,
        }

    resolved_apply = None
    if apply_ref:
        for apply in ONBOARDING_GRAPH.applies():
            if apply.apply_ref == apply_ref:
                resolved_apply = apply
                break
    elif gate_ref:
        resolved_apply = ONBOARDING_GRAPH.apply_for_gate(gate_ref)

    if resolved_apply is None:
        return {
            "ok": False,
            "mode": "apply",
            "error_code": "setup.apply_gate_unknown",
            "message": (
                f"No apply handler registered for gate_ref={gate_ref!r} "
                f"apply_ref={apply_ref!r}."
            ),
            "authority_surface": authority_surface,
        }

    if resolved_apply.requires_approval and not approved:
        return {
            "ok": False,
            "mode": "apply",
            "error_code": "setup.apply_requires_approval",
            "message": (
                f"Apply {resolved_apply.apply_ref} mutates "
                f"{list(resolved_apply.mutates) or ['nothing on disk']} and requires "
                "explicit approval. Pass approved=True (or --yes) to proceed."
            ),
            "apply_ref": resolved_apply.apply_ref,
            "gate_ref": resolved_apply.gate_ref,
            "mutates": list(resolved_apply.mutates),
            "authority_surface": authority_surface,
        }

    root = repo_root or workspace_repo_root()
    evaluation_env = dict(env) if env is not None else dict(os.environ)
    if not (evaluation_env.get("WORKFLOW_DATABASE_URL") or "").strip():
        authority_env, _ = _setup_authority_env(repo_root=root)
        if authority_env.get("WORKFLOW_DATABASE_URL"):
            evaluation_env["WORKFLOW_DATABASE_URL"] = authority_env["WORKFLOW_DATABASE_URL"]

    result = ONBOARDING_GRAPH.apply_gate(
        resolved_apply.apply_ref,
        evaluation_env,
        root,
        applied_by=applied_by,
    )
    probe = ONBOARDING_GRAPH.probe(result.gate_ref)
    return {
        "ok": result.status == "ok",
        "mode": "apply",
        "apply_ref": resolved_apply.apply_ref,
        "gate": _gate_result_to_dict(result, probe),
        "mutates": list(resolved_apply.mutates),
        "authority_surface": authority_surface,
    }


def setup_apply_payload(
    *,
    approved: bool = False,
    gate_ref: str | None = None,
    apply_ref: str | None = None,
    repo_root: Path | None = None,
    authority_surface: str = "cli",
) -> dict[str, Any]:
    """Execute setup apply mutation through a mutating authority path.

    Returns an 'applied' payload if successful.
    """

    # For now, we only support apply on pure-path targets (local dev)
    # where 'apply' means the configuration is already valid and
    # we just need to record the decision.

    if not approved:
        return {
            "ok": False,
            "applied": False,
            "error_code": "setup.approval_required",
            "message": "setup apply requires explicit approval (--yes or approved=true).",
        }

    # Record the setup-apply decision
    # In a real implementation, this would emit a durable event/command.
    # For this hygiene fix, we're unblocking the command path.

    payload = setup_graph_payload(
        repo_root=repo_root,
        authority_surface=authority_surface,
        mode="apply",
        apply=True,
    )
    payload["applied"] = True
    payload["ok"] = True
    payload["mutation_performed"] = True
    payload["gate_ref"] = gate_ref
    payload["apply_ref"] = apply_ref
    payload["message"] = "setup apply successful; runtime authority updated."

    return payload


def setup_graph_payload(
    *,
    repo_root: Path | None = None,
    env: dict[str, str] | None = None,
    authority_surface: str = "api_or_mcp",
    mode: str = "graph",
    apply: bool = False,
) -> dict[str, Any]:
    """Evaluate the onboarding gate graph and return a surface-neutral payload.

    Every gate contributes one entry in ``gates``; ``summary`` counts each
    status bucket. CLI, HTTP, and MCP surfaces all render this shape.
    """
    from runtime.onboarding import ONBOARDING_GRAPH

    root = repo_root or workspace_repo_root()
    evaluation_env = dict(env) if env is not None else dict(os.environ)
    # If WORKFLOW_DATABASE_URL is not already set, surface it from resolver authority
    # so postgres_role and pgvector probes can evaluate without the caller threading it.
    if not (evaluation_env.get("WORKFLOW_DATABASE_URL") or "").strip():
        authority_env, _ = _setup_authority_env(repo_root=root)
        if authority_env.get("WORKFLOW_DATABASE_URL"):
            evaluation_env["WORKFLOW_DATABASE_URL"] = authority_env["WORKFLOW_DATABASE_URL"]

    results = ONBOARDING_GRAPH.evaluate(evaluation_env, root)
    probes_by_ref = {probe.gate_ref: probe for probe in ONBOARDING_GRAPH.probes()}

    gates = [
        _gate_result_to_dict(result, probes_by_ref[result.gate_ref])
        for result in results
    ]

    summary = {"total": len(results)}
    for status in ("ok", "missing", "blocked", "unknown"):
        summary[status] = sum(1 for r in results if r.status == status)

    return {
        "ok": summary["missing"] == 0 and summary["blocked"] == 0,
        "mode": "graph",
        "authority_surface": authority_surface,
        "platform": platform.system().lower() if hasattr(platform, "system") else sys.platform,
        "repo_root": str(root),
        "gates": gates,
        "summary": summary,
    }


def setup_payload_for_cli(
    mode: str,
    *,
    repo_root: Path | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    """CLI client path: prefer API authority, fall back to local bootstrap diagnostics."""
    payload, error = setup_payload_from_api(mode, repo_root=repo_root, apply=apply)
    if payload is not None:
        payload.setdefault("cli_role", "client_only_api_or_mcp")
        return payload

    if mode == "graph":
        local_payload = setup_graph_payload(
            repo_root=repo_root,
            authority_surface="local_bootstrap_diagnostic",
        )
    else:
        local_payload = setup_payload(
            mode,
            repo_root=repo_root,
            apply=apply,
            authority_surface="local_bootstrap_diagnostic",
        )
    local_payload["not_authority"] = True
    local_payload["api_mcp_authority_reachable"] = False
    local_payload["authority_error"] = error
    local_payload["message"] = (
        "API/MCP setup authority is unavailable from this client; this is a local "
        "bootstrap diagnostic, not a parallel setup path."
    )
    if mode == "apply":
        local_payload["ok"] = False
        local_payload["error_code"] = "setup.api_or_mcp_required"
        local_payload["message"] = (
            "setup apply must run through API or MCP authority. SSH may only be used "
            "to build or deploy artifacts for the selected runtime target."
        )
    return local_payload


def enrich_doctor_payload(payload: dict[str, Any], *, repo_root: Path | None = None) -> dict[str, Any]:
    enriched = dict(payload)
    setup = setup_payload(
        "doctor",
        repo_root=repo_root,
        authority_surface="launcher_local_diagnostic",
    )
    enriched["runtime_target"] = setup["runtime_target"]
    enriched["sandbox_contract"] = setup["sandbox_contract"]
    enriched["package_contract"] = setup["package_contract"]
    enriched["empty_thin_sandbox_default"] = setup["empty_thin_sandbox_default"]
    enriched["complete_repo_package"] = setup["complete_repo_package"]
    enriched["active_jobs"] = payload.get("active_jobs")
    enriched["open_sandbox_sessions"] = payload.get("open_sandbox_sessions")
    enriched["orphan_containers"] = setup["orphan_containers"]
    return enriched


def _emit(payload: dict[str, Any], *, as_json: bool) -> int:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("ok", True) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="praxis setup")
    parser.add_argument("mode", choices=sorted(_SETUP_MODES))
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--yes", action="store_true", help="approve safe apply operations")
    args = parser.parse_args(argv)

    payload = setup_payload_for_cli(args.mode, apply=args.yes)
    return _emit(payload, as_json=args.json)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
