"""Launcher status and recovery helpers for the FastAPI surface."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from typing import Any

from ._shared import REPO_ROOT


_PREFERRED_LAUNCHER_PATH = REPO_ROOT / "scripts" / "praxis"
_COMPATIBILITY_LAUNCHER_PATH = REPO_ROOT / "scripts" / "praxis-ctl"
_FAST_FRONTDOOR_PROBE_ENV = {
    "PRAXIS_ALPHA_TIMEOUT_API_HEALTH_S": "0.5",
    "PRAXIS_ALPHA_TIMEOUT_WORKFLOW_ORIENT_S": "0.5",
    "PRAXIS_ALPHA_TIMEOUT_MCP_S": "0.5",
    "PRAXIS_ALPHA_TIMEOUT_UI_S": "0.5",
}
_STATUS_TIMEOUT_S = 12.0
_RECOVER_TIMEOUT_S = 20.0
_LAUNCH_TIMEOUT_S = 45.0


@dataclass(slots=True)
class LauncherCommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


class LauncherAuthorityError(RuntimeError):
    """Raised when the launcher command cannot complete an authority request."""


def _resolve_launcher_path() -> str:
    for candidate in (_PREFERRED_LAUNCHER_PATH, _COMPATIBILITY_LAUNCHER_PATH):
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return str(_PREFERRED_LAUNCHER_PATH)


def _frontdoor_launcher_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update(_FAST_FRONTDOOR_PROBE_ENV)
    return env


def _run_launcher_command(
    *args: str,
    extra_env: dict[str, str] | None = None,
    timeout_s: float | None = None,
) -> LauncherCommandResult:
    command = [_resolve_launcher_path(), *args]
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    try:
        completed = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
            env=env,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        joined = " ".join(command)
        raise LauncherAuthorityError(
            f"{joined} timed out after {timeout_s or exc.timeout} seconds"
        ) from exc
    return LauncherCommandResult(
        command=command,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _launcher_command_name() -> str:
    return os.path.basename(_resolve_launcher_path())


def _run_launcher_json(
    *args: str,
    allow_failure: bool = False,
    extra_env: dict[str, str] | None = None,
    timeout_s: float | None = None,
) -> dict[str, Any]:
    result = _run_launcher_command(*args, extra_env=extra_env, timeout_s=timeout_s)
    raw = result.stdout.strip()
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        if result.returncode != 0:
            raise LauncherAuthorityError(
                f"{_launcher_command_name()} {' '.join(args)} failed with exit code {result.returncode}: "
                f"{result.stderr.strip() or raw or 'no output'}"
            ) from exc
        raise LauncherAuthorityError(
            f"{_launcher_command_name()} {' '.join(args)} returned invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise LauncherAuthorityError(f"{_launcher_command_name()} {' '.join(args)} did not return a JSON object")
    if result.returncode != 0 and not allow_failure:
        raise LauncherAuthorityError(
            f"{_launcher_command_name()} {' '.join(args)} failed with exit code {result.returncode}: "
            f"{result.stderr.strip() or raw or 'no output'}"
        )
    return payload


def _platform_ready(doctor: dict[str, Any]) -> bool:
    # Core readiness: database + schema. Everything else is nice-to-have.
    required_flags = (
        "database_reachable",
        "schema_bootstrapped",
    )
    return all(bool(doctor.get(flag)) for flag in required_flags)


def _service_summary(services: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {"total": len(services)}
    for service in services:
        state = str(service.get("state") or "unknown")
        summary[state] = summary.get(state, 0) + 1
    return summary


def launcher_status_payload() -> dict[str, Any]:
    # Try the full status from the service manager (may fail or be slow)
    try:
        status = _run_launcher_json(
            "status",
            "--json",
            extra_env=_frontdoor_launcher_env(),
            timeout_s=_STATUS_TIMEOUT_S,
        )
    except Exception:
        status = {}

    doctor = status.get("doctor") or {}
    services = status.get("services", [])

    # Readiness: check DB directly from this process (no subprocess probes needed)
    db_ok = False
    schema_ok = False
    try:
        from storage.dev_postgres import local_postgres_health
        pg_health = local_postgres_health()
        db_ok = pg_health.get("reachable", False) if isinstance(pg_health, dict) else False
        schema_ok = pg_health.get("bootstrapped", False) if isinstance(pg_health, dict) else False
    except Exception:
        pass
    # Fallback to doctor if direct check fails
    if not db_ok:
        db_ok = bool(doctor.get("database_reachable"))
        schema_ok = bool(doctor.get("schema_bootstrapped"))

    ready = db_ok and schema_ok

    return {
        "ok": True,
        "brand": status.get("brand") or "Praxis Engine",
        "service_manager": status.get("service_manager") or "scripts/praxis",
        "compatibility_alias": status.get("compatibility_alias") or "scripts/praxis-ctl",
        "preferred_command": status.get("preferred_command") or "praxis",
        "ready": ready,
        "platform_state": "ready" if ready else "degraded",
        "launch_url": "http://127.0.0.1:8420/app",
        "dashboard_url": "http://127.0.0.1:8420/app",
        "api_docs_url": "http://127.0.0.1:8420/docs",
        "doctor": doctor,
        "dependency_truth": doctor.get("dependency_truth"),
        "services": services,
        "service_summary": _service_summary(services if isinstance(services, list) else []),
    }


def launcher_recover_payload(
    *,
    action: str,
    service: str | None = None,
    run_id: str | None = None,
    open_browser: bool = False,
) -> tuple[int, dict[str, Any]]:
    normalized_action = action.strip().lower()
    if normalized_action == "launch":
        args = ["launch", "--json"]
        if not open_browser:
            args.append("--no-open")
        result = _run_launcher_json(
            *args,
            allow_failure=True,
            extra_env=_frontdoor_launcher_env(),
            timeout_s=_LAUNCH_TIMEOUT_S,
        )
        doctor = result.get("doctor", {})
        services = _run_launcher_json(
            "status",
            "--json",
            extra_env=_frontdoor_launcher_env(),
            timeout_s=_STATUS_TIMEOUT_S,
        ).get("services", [])
        payload = {
            "ok": bool(result.get("ok")),
            "action": "launch",
            "result": result,
            "doctor": doctor,
            "dependency_truth": doctor.get("dependency_truth"),
            "services": services,
            "service_summary": _service_summary(services if isinstance(services, list) else []),
            "launch_url": result.get("launch_url"),
            "dashboard_url": result.get("dashboard_url"),
            "api_docs_url": result.get("api_docs_url"),
        }
        return (200 if payload["ok"] else 503), payload

    if normalized_action == "restart_all":
        command_result = _run_launcher_command(
            "restart",
            "all",
            extra_env=_frontdoor_launcher_env(),
            timeout_s=_RECOVER_TIMEOUT_S,
        )
        if command_result.returncode != 0:
            raise LauncherAuthorityError(command_result.stderr.strip() or command_result.stdout.strip())
    elif normalized_action == "restart_service":
        normalized_service = (service or "").strip()
        if not normalized_service:
            raise ValueError("service is required when action=restart_service")
        command_result = _run_launcher_command(
            "restart",
            normalized_service,
            extra_env=_frontdoor_launcher_env(),
            timeout_s=_RECOVER_TIMEOUT_S,
        )
        if command_result.returncode != 0:
            raise LauncherAuthorityError(command_result.stderr.strip() or command_result.stdout.strip())
    elif normalized_action == "repair_sync":
        args = ["repair-sync"]
        if run_id and run_id.strip():
            args.append(run_id.strip())
        command_result = _run_launcher_command(
            *args,
            extra_env=_frontdoor_launcher_env(),
            timeout_s=_RECOVER_TIMEOUT_S,
        )
        if command_result.returncode != 0:
            raise LauncherAuthorityError(command_result.stderr.strip() or command_result.stdout.strip())
    else:
        raise ValueError("action must be one of: launch, restart_all, restart_service, repair_sync")

    status = _run_launcher_json(
        "status",
        "--json",
        extra_env=_frontdoor_launcher_env(),
        timeout_s=_STATUS_TIMEOUT_S,
    )
    doctor = status.get("doctor")
    if not isinstance(doctor, dict):
        doctor = {
            "services_ready": True,
            "database_reachable": status.get("database_reachable"),
            "schema_bootstrapped": status.get("schema_bootstrapped"),
            "api_server_ready": status.get("api_server_ready"),
            "workflow_api_ready": status.get("workflow_api_ready"),
            "mcp_bridge_ready": status.get("mcp_bridge_ready"),
            "ui_ready": status.get("ui_ready"),
            "launch_url": status.get("launch_url"),
            "dashboard_url": status.get("dashboard_url"),
            "api_docs_url": status.get("api_docs_url"),
            "dependency_truth": status.get("dependency_truth"),
        }
    services = status.get("services", [])
    payload = {
        "ok": _platform_ready(doctor) if normalized_action != "repair_sync" else True,
        "action": normalized_action,
        "service": service,
        "run_id": run_id,
        "command": {
            "argv": command_result.command,
            "returncode": command_result.returncode,
            "stdout": command_result.stdout,
            "stderr": command_result.stderr,
        },
        "doctor": doctor,
        "dependency_truth": doctor.get("dependency_truth"),
        "services": services,
        "service_summary": _service_summary(services if isinstance(services, list) else []),
        "brand": "Praxis Engine",
        "service_manager": "scripts/praxis",
        "compatibility_alias": "scripts/praxis-ctl",
        "preferred_command": "praxis",
        "launch_url": doctor.get("launch_url"),
        "dashboard_url": doctor.get("dashboard_url"),
        "api_docs_url": doctor.get("api_docs_url"),
    }
    return (200 if payload["ok"] else 503), payload
