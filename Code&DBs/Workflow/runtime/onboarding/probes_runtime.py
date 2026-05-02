"""Runtime gate probes: API port, venv, launcher, API health."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
from collections.abc import Mapping
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from .graph import (
    GateProbe,
    GateResult,
    ONBOARDING_GRAPH,
    gate_result,
)


_LAUNCHER_MARKER = "Praxis runtime launcher. Managed by ./scripts/bootstrap."


_API_PORT_FREE = GateProbe(
    gate_ref="runtime.api_port_free",
    domain="runtime",
    title="PRAXIS_API_PORT is free or held by Praxis",
    purpose=(
        "scripts/bootstrap starts the REST API on this port. If another process "
        "holds it, the health probe times out silently. This gate distinguishes "
        "'port free' from 'port held by a running praxis' (both ok) vs 'port "
        "held by an unknown process' (blocked)."
    ),
    ok_cache_ttl_s=30,
)


_VENV = GateProbe(
    gate_ref="runtime.venv",
    domain="runtime",
    title=".venv present with Python 3.14",
    purpose=(
        "./scripts/bootstrap creates .venv and pip-installs requirements.runtime.txt "
        "there; native workflow code runs from .venv/bin/python."
    ),
    depends_on=("platform.python3_14",),
    ok_cache_ttl_s=300,
)


_LAUNCHER_INSTALLED = GateProbe(
    gate_ref="runtime.launcher_installed",
    domain="runtime",
    title="praxis launcher installed on PATH",
    purpose=(
        "The praxis CLI launcher is written by scripts/bootstrap into "
        "$PRAXIS_LOCAL_BIN_DIR. Without it, every documented 'praxis ...' "
        "command fails with 'command not found'."
    ),
    depends_on=("runtime.venv",),
    ok_cache_ttl_s=300,
)


_API_HEALTHY = GateProbe(
    gate_ref="runtime.api_healthy",
    domain="runtime",
    title="REST API responds on /api/health",
    purpose=(
        "Canvas, MCP bridges, and the workflow runner all depend on the HTTP API. "
        "If the API is down, those surfaces fail closed."
    ),
    depends_on=("runtime.api_port_free",),
    ok_cache_ttl_s=15,
)


_ENV_FILE = GateProbe(
    gate_ref="runtime.env_file",
    domain="runtime",
    title="Repo .env declares WORKFLOW_DATABASE_URL",
    purpose=(
        "scripts/bootstrap writes the resolved DSN into repo-local .env so "
        "later invocations do not need the env var exported in the shell."
    ),
    ok_cache_ttl_s=300,
)


def _resolve_api_host_port(env: Mapping[str, str]) -> tuple[str, int]:
    port = int((env.get("PRAXIS_API_PORT") or "8420").strip())
    host = (env.get("PRAXIS_API_HOST") or "127.0.0.1").strip()
    if host == "0.0.0.0":
        host = "127.0.0.1"
    return host, port


_PRAXIS_HEALTH_CHECKS = frozenset({"postgres", "worker", "workflow"})


def _probe_api_identity(host: str, port: int, timeout: float = 2.0) -> dict[str, object] | None:
    """Return praxis identity payload if the holder responds to /api/health, else None.

    Recognizes Praxis by either the X-Praxis-Api-Version header or by the
    /api/health payload shape (a ``checks`` list containing canonical Praxis
    subsystem names). The header is only emitted on public routes, so the
    payload-shape check is the primary identity signal on /api/health.
    """
    url = f"http://{host}:{port}/api/health"
    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=timeout) as response:
            header_version = response.headers.get("X-Praxis-Api-Version")
            body = response.read(8192).decode("utf-8", errors="replace")
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                payload = {}
    except (URLError, OSError, ValueError):
        return None
    is_praxis = bool(header_version)
    check_names: set[str] = set()
    if not is_praxis and isinstance(payload, dict):
        checks = payload.get("checks")
        if isinstance(checks, list):
            check_names = {
                str(c.get("name", "")).strip()
                for c in checks
                if isinstance(c, dict)
            }
            is_praxis = bool(check_names & _PRAXIS_HEALTH_CHECKS)
        if not is_praxis and payload.get("service") == "praxis":
            is_praxis = True
    if not is_praxis:
        return None
    return {
        "api_version_header": header_version,
        "health_checks_observed": sorted(check_names) if check_names else None,
    }


def probe_api_port_free(env: Mapping[str, str], repo_root: Path) -> GateResult:
    host, port = _resolve_api_host_port(env)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
    except OSError as bind_exc:
        sock.close()
        identity = _probe_api_identity(host, port)
        if identity is not None:
            return gate_result(
                _API_PORT_FREE,
                status="ok",
                observed_state={
                    "host": host,
                    "port": port,
                    "port_free": False,
                    "holder": "praxis_api",
                    **identity,
                },
            )
        return gate_result(
            _API_PORT_FREE,
            status="blocked",
            observed_state={
                "host": host,
                "port": port,
                "port_free": False,
                "holder": "unknown",
                "bind_error": str(bind_exc),
            },
            remediation_hint=(
                f"Port {port} is held by a non-praxis process. Either stop it "
                f"(lsof -iTCP:{port} -sTCP:LISTEN) or set PRAXIS_API_PORT to a free port."
            ),
        )
    sock.close()
    return gate_result(
        _API_PORT_FREE,
        status="ok",
        observed_state={"host": host, "port": port, "port_free": True, "holder": None},
    )


def probe_venv(env: Mapping[str, str], repo_root: Path) -> GateResult:
    venv_python = repo_root / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return gate_result(
            _VENV,
            status="missing",
            observed_state={"venv_python_exists": False, "venv_path": str(venv_python)},
            remediation_hint="Run ./scripts/bootstrap to create .venv and install dependencies",
        )
    try:
        completed = subprocess.run(
            [str(venv_python), "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            _VENV,
            status="blocked",
            observed_state={"venv_path": str(venv_python), "error": str(exc)},
            remediation_hint="Rebuild .venv: rm -rf .venv && ./scripts/bootstrap",
        )
    version = (completed.stdout or "").strip()
    if not version.startswith("3.14"):
        return gate_result(
            _VENV,
            status="blocked",
            observed_state={"venv_path": str(venv_python), "version": version},
            remediation_hint=(
                f".venv reports Python {version}; Praxis requires 3.14. "
                "Rebuild: rm -rf .venv && ./scripts/bootstrap"
            ),
        )
    return gate_result(
        _VENV,
        status="ok",
        observed_state={"venv_path": str(venv_python), "version": version},
    )


def probe_launcher_installed(env: Mapping[str, str], repo_root: Path) -> GateResult:
    launcher_path = shutil.which("praxis")
    if launcher_path is None:
        return gate_result(
            _LAUNCHER_INSTALLED,
            status="missing",
            observed_state={"praxis_on_path": False},
            remediation_hint=(
                'Set PRAXIS_LOCAL_BIN_DIR (e.g. "$HOME/.local/bin") and run '
                "./scripts/bootstrap. Then add that directory to PATH."
            ),
        )
    try:
        contents = Path(launcher_path).read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return gate_result(
            _LAUNCHER_INSTALLED,
            status="blocked",
            observed_state={"launcher_path": launcher_path, "error": str(exc)},
            remediation_hint="Cannot read launcher file; check filesystem permissions",
        )
    if _LAUNCHER_MARKER not in contents:
        return gate_result(
            _LAUNCHER_INSTALLED,
            status="blocked",
            observed_state={"launcher_path": launcher_path, "is_praxis_launcher": False},
            remediation_hint=(
                f"Binary at {launcher_path} is not a Praxis runtime launcher. "
                "Remove it and rerun ./scripts/bootstrap to install the managed launcher."
            ),
        )
    return gate_result(
        _LAUNCHER_INSTALLED,
        status="ok",
        observed_state={"launcher_path": launcher_path, "is_praxis_launcher": True},
    )


def probe_api_healthy(env: Mapping[str, str], repo_root: Path) -> GateResult:
    host, port = _resolve_api_host_port(env)
    identity = _probe_api_identity(host, port)
    if identity is None:
        return gate_result(
            _API_HEALTHY,
            status="missing",
            observed_state={"host": host, "port": port, "reachable": False},
            remediation_hint=(
                f"API not responding at http://{host}:{port}/api/health. "
                "Start it: ./scripts/bootstrap (or re-run after fixing the API log "
                "at artifacts/bootstrap/api.log)."
            ),
        )
    return gate_result(
        _API_HEALTHY,
        status="ok",
        observed_state={"host": host, "port": port, "reachable": True, **identity},
    )


def probe_env_file(env: Mapping[str, str], repo_root: Path) -> GateResult:
    env_path = repo_root / ".env"
    if not env_path.exists():
        return gate_result(
            _ENV_FILE,
            status="missing",
            observed_state={"env_path": str(env_path), "exists": False},
            remediation_hint=(
                "Repo .env is absent. ./scripts/bootstrap writes it on first run, "
                "or apply: praxis setup apply --gate runtime.env_file --yes"
            ),
            apply_ref="apply.runtime.env_file.write",
        )
    body = env_path.read_text(encoding="utf-8", errors="replace")
    has_database_url = any(
        line.strip().startswith("WORKFLOW_DATABASE_URL=") and line.strip() != "WORKFLOW_DATABASE_URL="
        for line in body.splitlines()
    )
    if not has_database_url:
        return gate_result(
            _ENV_FILE,
            status="blocked",
            observed_state={"env_path": str(env_path), "has_database_url": False},
            remediation_hint=(
                f"{env_path} exists but does not declare WORKFLOW_DATABASE_URL. "
                "Add the DSN line or rerun ./scripts/bootstrap."
            ),
        )
    return gate_result(
        _ENV_FILE,
        status="ok",
        observed_state={"env_path": str(env_path), "has_database_url": True},
    )


def register(graph=ONBOARDING_GRAPH) -> None:
    graph.register(_API_PORT_FREE, probe_api_port_free)
    graph.register(_VENV, probe_venv)
    graph.register(_LAUNCHER_INSTALLED, probe_launcher_installed)
    graph.register(_API_HEALTHY, probe_api_healthy)
    graph.register(_ENV_FILE, probe_env_file)
