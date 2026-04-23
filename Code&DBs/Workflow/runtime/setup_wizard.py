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

from runtime.docker_image_authority import (
    AGENT_FAMILY_IMAGE_MAP,
    CONTROL_WORKER_IMAGE,
    DOCKER_IMAGE_ENV,
)
from runtime.instance import native_instance_contract
from runtime.workspace_paths import repo_root as workspace_repo_root


_DEFAULT_PROFILE_REF = "sandbox_profile.praxis.default"
_LEGACY_COPY_DEBUG_REF = "sandbox_profile.praxis.legacy_copy_debug"
_DEFAULT_RUNTIME_TARGET_REF = "runtime_target.praxis.default"
_SETUP_MODES = {"doctor", "plan", "apply"}
_OPERATOR_AUTHORITY_PATH = ("api", "mcp")
_CLIENT_SURFACES = ("cli", "website")
_SSH_BUILD_TRANSPORT = {
    "transport": "ssh",
    "role": "build_deploy_transport_only",
    "purpose": "build_artifacts_or_thin_images_on_a_selected_runtime_target",
    "operator_authority": False,
    "may_run_setup": False,
    "may_mutate_db_authority": False,
}
_PACKAGE_COMPONENTS: tuple[dict[str, str], ...] = (
    {
        "name": "operator_entrypoint",
        "path": "scripts/praxis",
        "authority_role": "launcher_and_cli_client",
    },
    {
        "name": "setup_frontdoor",
        "path": "SETUP.md",
        "authority_role": "human_bootstrap_guide",
    },
    {
        "name": "product_overview",
        "path": "README.md",
        "authority_role": "public_package_overview",
    },
    {
        "name": "runtime_profiles",
        "path": "config/runtime_profiles.json",
        "authority_role": "derived_runtime_profile_authority",
    },
    {
        "name": "compose_target",
        "path": "docker-compose.yml",
        "authority_role": "local_or_selected_docker_reconciler",
    },
    {
        "name": "workflow_runtime",
        "path": "Code&DBs/Workflow",
        "authority_role": "engine_runtime",
    },
    {
        "name": "workflow_database_migrations",
        "path": "Code&DBs/Databases/migrations/workflow",
        "authority_role": "database_schema_authority",
    },
    {
        "name": "api_surface",
        "path": "Code&DBs/Workflow/surfaces/api",
        "authority_role": "http_operator_surface",
    },
    {
        "name": "mcp_surface",
        "path": "Code&DBs/Workflow/surfaces/mcp",
        "authority_role": "mcp_operator_surface",
    },
    {
        "name": "cli_surface",
        "path": "Code&DBs/Workflow/surfaces/cli",
        "authority_role": "cli_client_surface",
    },
    {
        "name": "website_surface",
        "path": "Code&DBs/Workflow/surfaces/app",
        "authority_role": "browser_client_surface",
    },
    {
        "name": "skill_exports",
        "path": "Skills",
        "authority_role": "derived_agent_adapter_exports",
    },
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


def runtime_target_report(*, repo_root: Path | None = None) -> dict[str, Any]:
    docker = _docker_info()
    api_url = _env_value("PRAXIS_API_URL", repo_root=repo_root)
    api_bind_host = _env_value("PRAXIS_API_HOST", repo_root=repo_root) or "127.0.0.1"
    if not api_url:
        host = api_bind_host
        if host in {"0.0.0.0", "::", "[::]"}:
            host = "127.0.0.1"
        port = _env_value("PRAXIS_API_PORT", repo_root=repo_root) or "8420"
        api_url = f"http://{host}:{port}"
    substrate_kind = _env_value("PRAXIS_RUNTIME_SUBSTRATE_KIND", repo_root=repo_root)
    if not substrate_kind:
        substrate_kind = "local_docker" if docker.get("available") else "remote_api"
    return {
        "runtime_target_ref": (
            _env_value("PRAXIS_RUNTIME_TARGET_REF", repo_root=repo_root)
            or _DEFAULT_RUNTIME_TARGET_REF
        ),
        "substrate_kind": substrate_kind,
        "api_authority": api_url,
        "db_authority": _redact_database_url(
            _env_value("WORKFLOW_DATABASE_URL", repo_root=repo_root)
        ),
        "workspace_authority": str(repo_root or workspace_repo_root()),
        "host_traits": {
            "os": platform.system().lower() or "unknown",
            "architecture": platform.machine() or "unknown",
            "docker_available": bool(docker.get("available")),
            "docker_socket": _env_value("DOCKER_HOST", repo_root=repo_root) or "default",
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
    components: list[dict[str, Any]] = []
    missing: list[str] = []
    for component in _PACKAGE_COMPONENTS:
        rel_path = component["path"]
        path = root / rel_path
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

    checks = {
        "repo_root_exists": root.exists(),
        "operator_entrypoint_present": (root / "scripts" / "praxis").is_file(),
        "api_mcp_cli_website_present": all(
            (root / rel_path).exists()
            for rel_path in (
                "Code&DBs/Workflow/surfaces/api",
                "Code&DBs/Workflow/surfaces/mcp",
                "Code&DBs/Workflow/surfaces/cli",
                "Code&DBs/Workflow/surfaces/app",
            )
        ),
        "database_migrations_present": (
            root / "Code&DBs" / "Databases" / "migrations" / "workflow"
        ).is_dir(),
        "runtime_profiles_present": (root / "config" / "runtime_profiles.json").is_file(),
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


def setup_payload(
    mode: str,
    *,
    repo_root: Path | None = None,
    apply: bool = False,
    authority_surface: str = "api_or_mcp",
) -> dict[str, Any]:
    root = repo_root or workspace_repo_root()
    runtime_target = runtime_target_report(repo_root=root)
    sandbox_contract = sandbox_contract_report(repo_root=root)
    package_contract = package_contract_report(repo_root=root)
    native_instance = native_instance_contract(allow_authority_fallback=True)
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
        "native_instance_checks_ok": all(native_instance_checks.values()),
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
        payload["requires_authority_apply"] = True
        payload["apply_note"] = (
            "This setup surface is the gate and plan, not a hidden mutation shim. "
            "DB/runtime-target writes must be performed by migration or catalog-backed "
            "authority operations, then reconciled by the selected runtime target."
        )
        if not apply:
            payload["ok"] = False
            payload["error_code"] = "setup.approval_required"
            payload["message"] = "setup apply requires explicit approval."
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
