"""Single-authority background supervisor for Praxis services.

This module turns the repo-local runtime into one launchd-visible background
item. Postgres, the API server, the workflow worker, and the scheduler all run
as child processes managed by this supervisor instead of as separate launchd
agents.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import plistlib
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from runtime._workflow_database import (
    WorkflowDatabaseAuthority,
    resolve_runtime_database_authority,
)
from runtime.workspace_paths import code_tree_root, log_path as _layout_log_path, to_repo_ref
from storage.postgres.validators import PostgresConfigurationError

LOG = logging.getLogger(__name__)

SERVICE_PATH = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
SUPERVISOR_LABEL = "com.praxis.engine"
SUPERVISOR_PROGRAM_NAME = "praxis"
SUPERVISOR_STDOUT = Path("/tmp/praxis-engine.log")
SUPERVISOR_STDERR = Path("/tmp/praxis-engine.err")

LEGACY_LAUNCHD_LABELS = (
    "com.praxis.postgres",
    "com.praxis.api-server",
    "com.praxis.workflow-api",
    "com.praxis.workflow-worker",
    "com.praxis.scheduler",
    "com.praxis.queue-worker",
    "com.praxis.postgres",
    "com.praxis.api-server",
    "com.praxis.workflow-api",
    "com.praxis.workflow-worker",
    "com.praxis.scheduler",
)


@dataclass(frozen=True, slots=True)
class ComponentSpec:
    key: str
    display_name: str
    compatibility_label: str
    log_path: str
    port: int | None = None
    waits_for_postgres: bool = False


COMPONENT_SPECS = (
    ComponentSpec(
        key="postgres",
        display_name="postgres",
        compatibility_label="com.praxis.postgres",
        log_path=to_repo_ref(_layout_log_path("postgres")),
        port=5432,
    ),
    ComponentSpec(
        key="api-server",
        display_name="api-server",
        compatibility_label="com.praxis.api-server",
        log_path="/tmp/praxis-api-server.err",
        port=8420,
        waits_for_postgres=True,
    ),
    ComponentSpec(
        key="workflow-worker",
        display_name="workflow-worker",
        compatibility_label="com.praxis.workflow-worker",
        log_path="/tmp/praxis-workflow-worker.err",
        waits_for_postgres=True,
    ),
    ComponentSpec(
        key="scheduler",
        display_name="scheduler",
        compatibility_label="com.praxis.scheduler",
        log_path="/tmp/praxis-scheduler.err",
        waits_for_postgres=True,
    ),
)

COMPONENT_ORDER = tuple(spec.key for spec in COMPONENT_SPECS)
COMPONENT_BY_KEY = {spec.key: spec for spec in COMPONENT_SPECS}
COMPAT_ALIASES = {
    "postgres": "postgres",
    "pg": "postgres",
    "api": "api-server",
    "api-server": "api-server",
    "server": "api-server",
    "praxis-server": "api-server",
    "workflow-api": "api-server",
    "worker": "workflow-worker",
    "workflow-worker": "workflow-worker",
    "queue-worker": "workflow-worker",
    "scheduler": "scheduler",
    "all": "all",
}

_STOP_REQUESTED = False

@dataclass(frozen=True, slots=True)
class SupervisorPaths:
    repo_root: Path
    workflow_dir: Path
    pgdata: Path
    pg_log: Path
    launch_agents_dir: Path
    launch_agent_plist: Path
    wrapper_program: Path
    state_dir: Path
    state_file: Path
    control_file: Path
    database_url: str
    database_authority_source: str
    python_bin: Path = field(default_factory=lambda: Path(shutil.which("python3", path=SERVICE_PATH) or "python3"))
    postgres_bin: Path = field(default_factory=lambda: Path(shutil.which("postgres", path=SERVICE_PATH) or "postgres"))
    pg_isready_bin: Path = field(default_factory=lambda: Path(shutil.which("pg_isready", path=SERVICE_PATH) or "pg_isready"))
    service_path: str = SERVICE_PATH

    @property
    def environment(self) -> dict[str, str]:
        return {
            "PATH": self.service_path,
            "PYTHONPATH": str(self.workflow_dir),
            "WORKFLOW_DATABASE_URL": self.database_url,
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": self.database_authority_source,
        }


@dataclass(slots=True)
class ManagedProcess:
    key: str
    pid: int
    process: subprocess.Popen[str] | None
    restart_token: int
    restart_count: int = 0
    started_at: str = field(default_factory=lambda: utc_now())


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_component(name: str) -> str:
    normalized = name.strip().lower()
    if normalized not in COMPAT_ALIASES:
        raise ValueError(f"unknown component: {name}")
    return COMPAT_ALIASES[normalized]


def database_name_from_url(database_url: str) -> str:
    authorityless = database_url.split("://", 1)[-1]
    path_part = authorityless.split("/", 1)[-1]
    if path_part == authorityless:
        return "praxis"
    database_name = path_part.split("?", 1)[0].split("#", 1)[0]
    return database_name or "praxis"

def discover_database_authority(repo_root: Path) -> WorkflowDatabaseAuthority:
    try:
        return resolve_runtime_database_authority(
            repo_root=repo_root,
            required=True,
        )
    except PostgresConfigurationError as exc:
        repo_env_path = repo_root / ".env"
        raise RuntimeError(
            "praxis_supervisor requires explicit WORKFLOW_DATABASE_URL authority "
            f"from process env, launchd, or {repo_env_path}"
        ) from exc


def discover_database_url(repo_root: Path) -> str:
    authority = discover_database_authority(repo_root)
    return str(authority.database_url or "")


def build_paths(repo_root: Path, database_url: str | None = None) -> SupervisorPaths:
    repo_root = repo_root.resolve()
    code_root = code_tree_root(repo_root)
    workflow_dir = code_root / "Workflow"
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    authority = (
        resolve_runtime_database_authority(database_url=database_url, required=True)
        if database_url is not None
        else discover_database_authority(repo_root)
    )
    resolved_db_url = str(authority.database_url or "")
    state_dir = repo_root / ".cache" / "praxis-supervisor"
    return SupervisorPaths(
        repo_root=repo_root,
        workflow_dir=workflow_dir,
        pgdata=code_root / "Databases" / "postgres-dev" / "data",
        pg_log=code_root / "Databases" / "postgres-dev" / "log" / "postgres.log",
        launch_agents_dir=launch_agents_dir,
        launch_agent_plist=launch_agents_dir / f"{SUPERVISOR_LABEL}.plist",
        wrapper_program=repo_root / "scripts" / SUPERVISOR_PROGRAM_NAME,
        state_dir=state_dir,
        state_file=state_dir / "state.json",
        control_file=state_dir / "control.json",
        database_url=resolved_db_url,
        database_authority_source=authority.source,
    )


def default_control_payload() -> dict[str, Any]:
    return {
        "desired": {key: True for key in COMPONENT_ORDER},
        "restart_tokens": {key: 0 for key in COMPONENT_ORDER},
        "updated_at": utc_now(),
    }


def read_json_file(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return json.loads(json.dumps(fallback))
    except json.JSONDecodeError:
        return json.loads(json.dumps(fallback))
    return payload if isinstance(payload, dict) else json.loads(json.dumps(fallback))


def write_json_file(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def load_control(paths: SupervisorPaths) -> dict[str, Any]:
    payload = read_json_file(paths.control_file, default_control_payload())
    desired = payload.get("desired")
    restart_tokens = payload.get("restart_tokens")
    if not isinstance(desired, dict):
        desired = {key: True for key in COMPONENT_ORDER}
    if not isinstance(restart_tokens, dict):
        restart_tokens = {key: 0 for key in COMPONENT_ORDER}
    normalized_payload = {
        "desired": {key: bool(desired.get(key, True)) for key in COMPONENT_ORDER},
        "restart_tokens": {key: int(restart_tokens.get(key, 0)) for key in COMPONENT_ORDER},
        "updated_at": payload.get("updated_at") or utc_now(),
    }
    return normalized_payload


def save_control(paths: SupervisorPaths, payload: Mapping[str, Any]) -> None:
    normalized = {
        "desired": {key: bool(payload["desired"].get(key, True)) for key in COMPONENT_ORDER},
        "restart_tokens": {
            key: int(payload["restart_tokens"].get(key, 0)) for key in COMPONENT_ORDER
        },
        "updated_at": utc_now(),
    }
    write_json_file(paths.control_file, normalized)


def apply_control_action(paths: SupervisorPaths, action: str, component: str) -> dict[str, Any]:
    payload = load_control(paths)
    targets = COMPONENT_ORDER if component == "all" else (normalize_component(component),)

    if action == "start":
        for target in targets:
            payload["desired"][target] = True
    elif action == "stop":
        for target in targets:
            payload["desired"][target] = False
    elif action == "restart":
        for target in targets:
            payload["desired"][target] = True
            payload["restart_tokens"][target] = int(payload["restart_tokens"].get(target, 0)) + 1
    elif action == "reset":
        payload = default_control_payload()
    else:
        raise ValueError(f"unsupported control action: {action}")

    save_control(paths, payload)
    supervisor_pid = current_supervisor_pid(paths)
    if supervisor_pid is not None:
        try:
            os.kill(supervisor_pid, signal.SIGHUP)
        except ProcessLookupError:
            pass
    return load_control(paths)


def render_launch_agent_plist(paths: SupervisorPaths) -> str:
    payload = {
        "Label": SUPERVISOR_LABEL,
        "Program": str(paths.wrapper_program),
        "ProgramArguments": [str(paths.wrapper_program), "agent-run"],
        "WorkingDirectory": str(paths.workflow_dir),
        "EnvironmentVariables": paths.environment,
        "KeepAlive": True,
        "RunAtLoad": True,
        "StandardOutPath": str(SUPERVISOR_STDOUT),
        "StandardErrorPath": str(SUPERVISOR_STDERR),
        "ProcessType": "Background",
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=False).decode("utf-8")


def _process_command(pid: int) -> str | None:
    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        return None
    command = completed.stdout.strip()
    return command or None


def current_supervisor_pid(paths: SupervisorPaths) -> int | None:
    state = read_json_file(paths.state_file, {})
    supervisor = state.get("supervisor")
    if not isinstance(supervisor, dict):
        return None
    raw_pid = supervisor.get("pid")
    if not isinstance(raw_pid, int):
        return None
    command = _process_command(raw_pid)
    if not command:
        return None
    if "runtime.praxis_supervisor" not in command:
        return None
    return raw_pid


def _find_matching_pid(spec: ComponentSpec, paths: SupervisorPaths) -> int | None:
    completed = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        return None

    for raw_line in completed.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pid_str, _, command = line.partition(" ")
        if not pid_str.isdigit():
            continue
        pid = int(pid_str)
        if pid == os.getpid():
            continue
        if spec.key == "postgres":
            if str(paths.postgres_bin) in command and str(paths.pgdata) in command:
                return pid
        elif spec.key == "api-server":
            if "surfaces.api.server" in command and "--port 8420" in command:
                return pid
        elif spec.key == "workflow-worker":
            if "runtime.workflow_worker" in command:
                return pid
        elif spec.key == "scheduler":
            if "runtime.praxis_supervisor" in command and "component scheduler" in command:
                return pid
    return None


def _pg_ready(paths: SupervisorPaths) -> bool:
    completed = subprocess.run(
        [
            str(paths.pg_isready_bin),
            "-h",
            "127.0.0.1",
            "-p",
            "5432",
            "-d",
            database_name_from_url(paths.database_url),
            "-q",
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    return completed.returncode == 0


def _component_log_path(spec: ComponentSpec, paths: SupervisorPaths) -> Path:
    if spec.key == "postgres":
        return paths.pg_log
    return Path(spec.log_path)


def _component_command(spec: ComponentSpec, paths: SupervisorPaths) -> list[str]:
    if spec.key == "postgres":
        return [str(paths.postgres_bin), "-D", str(paths.pgdata)]
    if spec.key == "api-server":
        return [
            str(paths.python_bin),
            "-m",
            "surfaces.api.server",
            "--host",
            "127.0.0.1",
            "--port",
            "8420",
        ]
    if spec.key == "workflow-worker":
        return [str(paths.python_bin), "-m", "runtime.workflow_worker"]
    if spec.key == "scheduler":
        return [
            str(paths.python_bin),
            "-m",
            "runtime.praxis_supervisor",
            "--repo-root",
            str(paths.repo_root),
            "component",
            "scheduler",
        ]
    raise KeyError(f"unknown component: {spec.key}")


def build_status_snapshot(
    paths: SupervisorPaths,
    *,
    launchd_loaded: bool | None = None,
    supervisor_pid: int | None = None,
    control_payload: Mapping[str, Any] | None = None,
    state_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    control = dict(control_payload or load_control(paths))
    state = dict(state_payload or read_json_file(paths.state_file, {}))
    persisted_supervisor = state.get("supervisor") if isinstance(state.get("supervisor"), dict) else {}
    launchd_loaded = bool(paths.launch_agent_plist.exists()) if launchd_loaded is None else launchd_loaded
    supervisor_pid = supervisor_pid or current_supervisor_pid(paths)

    services: list[dict[str, Any]] = []
    components_state = state.get("components") if isinstance(state.get("components"), dict) else {}

    for spec in COMPONENT_SPECS:
        record = components_state.get(spec.key) if isinstance(components_state.get(spec.key), dict) else {}
        desired = bool(control.get("desired", {}).get(spec.key, True))
        pid = record.get("pid") if isinstance(record.get("pid"), int) else None
        if pid is not None and _process_command(pid) is None:
            pid = None
        if pid is None:
            pid = _find_matching_pid(spec, paths)
        running = pid is not None

        raw_state = record.get("state") if isinstance(record.get("state"), str) else None
        if running:
            state_name = "running"
        elif not paths.launch_agent_plist.exists() or not launchd_loaded:
            state_name = "missing"
        elif not desired:
            state_name = "stopped"
        elif spec.waits_for_postgres and not _pg_ready(paths):
            state_name = "waiting_on_postgres"
        elif raw_state:
            state_name = raw_state
        elif record.get("last_exit_code") not in (None, 0):
            state_name = "crashed"
        else:
            state_name = "idle"

        services.append(
            {
                "label": spec.compatibility_label,
                "name": spec.display_name,
                "component": spec.key,
                "authority_label": SUPERVISOR_LABEL,
                "loaded": bool(paths.launch_agent_plist.exists() and launchd_loaded),
                "running": running,
                "pid": pid,
                "exit_code": record.get("last_exit_code"),
                "port": spec.port,
                "log_file": str(_component_log_path(spec, paths)),
                "state": state_name,
                "desired": desired,
            }
        )

    return {
        "supervisor": {
            "label": SUPERVISOR_LABEL,
            "loaded": bool(paths.launch_agent_plist.exists() and launchd_loaded),
            "pid": supervisor_pid,
            "state_file": str(paths.state_file),
            "control_file": str(paths.control_file),
            "program": str(paths.wrapper_program),
            "started_at": persisted_supervisor.get("started_at"),
        },
        "services": services,
    }


def snapshot_rows(snapshot: Mapping[str, Any]) -> str:
    lines = []
    for service in snapshot.get("services", []):
        lines.append(
            "\t".join(
                [
                    str(service.get("label", "")),
                    str(service.get("name", "")),
                    "true" if service.get("loaded") else "false",
                    "true" if service.get("running") else "false",
                    str(service.get("pid") or ""),
                    str(service.get("exit_code") if service.get("exit_code") is not None else ""),
                    str(service.get("port") or ""),
                    str(service.get("log_file") or ""),
                    str(service.get("state") or ""),
                ]
            )
        )
    return "\n".join(lines)


class PraxisSupervisor:
    def __init__(self, paths: SupervisorPaths) -> None:
        self.paths = paths
        self.children: dict[str, ManagedProcess] = {}
        self.component_state: dict[str, dict[str, Any]] = {
            key: {
                "state": "idle",
                "pid": None,
                "last_exit_code": None,
                "last_started_at": None,
                "restart_count": 0,
            }
            for key in COMPONENT_ORDER
        }
        self.control = load_control(paths)
        self.state_file = read_json_file(paths.state_file, {})
        self._restore_children_from_state()

    def _restore_children_from_state(self) -> None:
        components = self.state_file.get("components") if isinstance(self.state_file.get("components"), dict) else {}
        for spec in COMPONENT_SPECS:
            record = components.get(spec.key) if isinstance(components.get(spec.key), dict) else {}
            pid = record.get("pid") if isinstance(record.get("pid"), int) else None
            if pid is None:
                pid = _find_matching_pid(spec, self.paths)
            if pid is None:
                self.component_state[spec.key].update(
                    {
                        "state": record.get("state", "idle"),
                        "last_exit_code": record.get("last_exit_code"),
                        "restart_count": int(record.get("restart_count", 0) or 0),
                        "last_started_at": record.get("last_started_at"),
                    }
                )
                continue
            self.children[spec.key] = ManagedProcess(
                key=spec.key,
                pid=pid,
                process=None,
                restart_token=int(self.control["restart_tokens"].get(spec.key, 0)),
                restart_count=int(record.get("restart_count", 0) or 0),
                started_at=record.get("last_started_at") or utc_now(),
            )
            self.component_state[spec.key].update(
                {
                    "state": "running",
                    "pid": pid,
                    "last_exit_code": record.get("last_exit_code"),
                    "restart_count": int(record.get("restart_count", 0) or 0),
                    "last_started_at": record.get("last_started_at") or utc_now(),
                }
            )

    def _write_state(self) -> None:
        payload = {
            "supervisor": {
                "label": SUPERVISOR_LABEL,
                "pid": os.getpid(),
                "started_at": self.state_file.get("supervisor", {}).get("started_at") or utc_now(),
                "updated_at": utc_now(),
            },
            "components": self.component_state,
        }
        write_json_file(self.paths.state_file, payload)
        self.state_file = payload

    def _update_control(self) -> None:
        self.control = load_control(self.paths)

    def _start_component(self, spec: ComponentSpec) -> None:
        log_path = _component_log_path(spec, self.paths)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        command = _component_command(spec, self.paths)
        log_handle = open(log_path, "a", encoding="utf-8")
        # Wrap command so stderr/stdout pass through a filter that strips macOS
        # MallocStackLogging noise emitted by every Python subprocess fork.
        # `exec "$@"` preserves PID so signal handling stays intact.
        wrapped = [
            "bash",
            "-c",
            'exec "$@" > >(grep --line-buffered -v MallocStackLogging) 2>&1',
            "--",
        ] + list(command)
        process = subprocess.Popen(
            wrapped,
            cwd=self.paths.workflow_dir,
            env={**os.environ, **self.paths.environment},
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        managed = ManagedProcess(
            key=spec.key,
            pid=process.pid,
            process=process,
            restart_token=int(self.control["restart_tokens"].get(spec.key, 0)),
            restart_count=int(self.component_state[spec.key].get("restart_count", 0) or 0) + 1,
        )
        self.children[spec.key] = managed
        self.component_state[spec.key].update(
            {
                "state": "running",
                "pid": process.pid,
                "last_exit_code": None,
                "last_started_at": managed.started_at,
                "restart_count": managed.restart_count,
            }
        )
        LOG.info("started %s pid=%s", spec.key, process.pid)

    def _terminate_component(self, spec: ComponentSpec, *, reason: str) -> None:
        managed = self.children.get(spec.key)
        if managed is None:
            return
        pid = managed.pid
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        else:
            deadline = time.time() + 10.0
            while time.time() < deadline:
                if _process_command(pid) is None:
                    break
                time.sleep(0.2)
            else:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        self.children.pop(spec.key, None)
        desired = bool(self.control["desired"].get(spec.key, True))
        self.component_state[spec.key].update(
            {
                "pid": None,
                "state": "stopped" if not desired or reason == "shutdown" else "idle",
            }
        )
        LOG.info("terminated %s pid=%s reason=%s", spec.key, pid, reason)

    def _refresh_component(self, spec: ComponentSpec) -> None:
        managed = self.children.get(spec.key)
        if managed is None:
            return
        if managed.process is not None:
            return_code = managed.process.poll()
            if return_code is None:
                self.component_state[spec.key].update({"state": "running", "pid": managed.pid})
                return
            self.children.pop(spec.key, None)
            self.component_state[spec.key].update(
                {
                    "pid": None,
                    "state": "crashed" if self.control["desired"].get(spec.key, True) else "stopped",
                    "last_exit_code": return_code,
                    "restart_count": managed.restart_count,
                }
            )
            LOG.warning("%s exited with code %s", spec.key, return_code)
            return

        command = _process_command(managed.pid)
        if command is not None:
            self.component_state[spec.key].update({"state": "running", "pid": managed.pid})
            return
        self.children.pop(spec.key, None)
        self.component_state[spec.key].update(
            {
                "pid": None,
                "state": "idle" if self.control["desired"].get(spec.key, True) else "stopped",
            }
        )

    def _reconcile_component(self, spec: ComponentSpec) -> None:
        desired = bool(self.control["desired"].get(spec.key, True))
        self._refresh_component(spec)
        managed = self.children.get(spec.key)
        expected_restart_token = int(self.control["restart_tokens"].get(spec.key, 0))

        if managed is not None and managed.restart_token != expected_restart_token:
            self._terminate_component(spec, reason="restart")
            managed = None

        if not desired:
            if managed is not None:
                self._terminate_component(spec, reason="disabled")
            self.component_state[spec.key].update({"state": "stopped", "pid": None})
            return

        if spec.waits_for_postgres and not _pg_ready(self.paths):
            if managed is not None:
                self.component_state[spec.key].update({"state": "running", "pid": managed.pid})
            else:
                self.component_state[spec.key].update({"state": "waiting_on_postgres", "pid": None})
            return

        if managed is None:
            existing_pid = _find_matching_pid(spec, self.paths)
            if existing_pid is not None:
                self.children[spec.key] = ManagedProcess(
                    key=spec.key,
                    pid=existing_pid,
                    process=None,
                    restart_token=expected_restart_token,
                    restart_count=int(self.component_state[spec.key].get("restart_count", 0) or 0),
                    started_at=self.component_state[spec.key].get("last_started_at") or utc_now(),
                )
                self.component_state[spec.key].update({"state": "running", "pid": existing_pid})
                return
            self._start_component(spec)

    def run(self) -> int:
        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        self.component_state.setdefault("started_at", utc_now())
        self._write_state()

        while not _STOP_REQUESTED:
            self._update_control()
            for spec in COMPONENT_SPECS:
                self._reconcile_component(spec)
            self._write_state()
            time.sleep(1.0)

        for spec in reversed(COMPONENT_SPECS):
            self._terminate_component(spec, reason="shutdown")
        self._write_state()
        return 0


def _scheduler_component(paths: SupervisorPaths) -> int:
    global _STOP_REQUESTED
    _STOP_REQUESTED = False
    while not _STOP_REQUESTED:
        completed = subprocess.run(
            [str(paths.python_bin), "-m", "runtime.scheduler", "tick"],
            cwd=paths.workflow_dir,
            env={**os.environ, **paths.environment},
            capture_output=False,
            check=False,
            text=True,
        )
        if completed.returncode != 0:
            LOG.warning("scheduler tick exited with code %s", completed.returncode)
            if _STOP_REQUESTED:
                break
            time.sleep(5.0)
            continue
        deadline = time.time() + 60.0
        while time.time() < deadline:
            if _STOP_REQUESTED:
                return 0
            time.sleep(1.0)
    return 0


def _install_signal_handlers() -> None:
    def _mark_stop(signum, frame) -> None:  # type: ignore[override]
        del signum, frame
        global _STOP_REQUESTED
        _STOP_REQUESTED = True

    def _refresh_control(signum, frame) -> None:  # type: ignore[override]
        del signum, frame
        LOG.info("received control reload signal")

    signal.signal(signal.SIGTERM, _mark_stop)
    signal.signal(signal.SIGINT, _mark_stop)
    signal.signal(signal.SIGHUP, _refresh_control)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Praxis background supervisor")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[3],
        help="Absolute Praxis repo root",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("db-url", help="Print the resolved workflow database URL")
    subparsers.add_parser("render-plist", help="Render the single launchd plist")

    snapshot_parser = subparsers.add_parser("snapshot", help="Print supervisor snapshot JSON")
    snapshot_parser.add_argument("--launchd-loaded", choices=("true", "false"), default=None)
    snapshot_parser.add_argument("--supervisor-pid", type=int, default=None)

    rows_parser = subparsers.add_parser("rows", help="Print component rows as TSV")
    rows_parser.add_argument("--launchd-loaded", choices=("true", "false"), default=None)
    rows_parser.add_argument("--supervisor-pid", type=int, default=None)

    control_parser = subparsers.add_parser("control", help="Update desired component state")
    control_parser.add_argument("action", choices=("start", "stop", "restart", "reset"))
    control_parser.add_argument("component", nargs="?", default="all")

    subparsers.add_parser("run", help="Run the long-lived supervisor")

    component_parser = subparsers.add_parser("component", help="Run a managed child component")
    component_parser.add_argument("component", choices=("scheduler",))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    args = _parse_args(argv or sys.argv[1:])
    paths = build_paths(args.repo_root)

    if args.command == "db-url":
        print(paths.database_url)
        return 0

    if args.command == "render-plist":
        print(render_launch_agent_plist(paths))
        return 0

    if args.command == "snapshot":
        launchd_loaded = None if args.launchd_loaded is None else args.launchd_loaded == "true"
        print(
            json.dumps(
                build_status_snapshot(
                    paths,
                    launchd_loaded=launchd_loaded,
                    supervisor_pid=args.supervisor_pid,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "rows":
        launchd_loaded = None if args.launchd_loaded is None else args.launchd_loaded == "true"
        snapshot = build_status_snapshot(
            paths,
            launchd_loaded=launchd_loaded,
            supervisor_pid=args.supervisor_pid,
        )
        print(snapshot_rows(snapshot))
        return 0

    if args.command == "control":
        print(json.dumps(apply_control_action(paths, args.action, args.component), indent=2, sort_keys=True))
        return 0

    _install_signal_handlers()

    if args.command == "component":
        if args.component == "scheduler":
            return _scheduler_component(paths)
        raise ValueError(f"unsupported component runner: {args.component}")

    if args.command == "run":
        return PraxisSupervisor(paths).run()

    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
