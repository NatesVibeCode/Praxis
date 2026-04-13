"""Docker-based execution runner for sandboxed model dispatch.

Models run in ephemeral Docker containers with stdin/stdout only.
No volume mounts, no network (for build tasks), no filesystem access.

The graph pipes compiled context via stdin and captures structured
output from stdout. The model never touches the host filesystem.

This module no longer falls back to host execution when Docker is unavailable.
"""

from __future__ import annotations

import os
import shlex
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from runtime.docker_image_authority import DOCKER_IMAGE_ENV, resolve_docker_image

if TYPE_CHECKING:
    from .deterministic import DeterministicExecutionControl


_PRAXIS_DOCKER_MEMORY_ENV = "PRAXIS_DOCKER_MEMORY"
_PRAXIS_DOCKER_CPUS_ENV = "PRAXIS_DOCKER_CPUS"


def _docker_image() -> str:
    image, _metadata = resolve_docker_image(
        requested_image=None,
        image_exists=_has_docker_image,
    )
    return image


def _docker_memory() -> str:
    return os.environ.get(_PRAXIS_DOCKER_MEMORY_ENV, "").strip() or "4g"


def _docker_cpus() -> str:
    return os.environ.get(_PRAXIS_DOCKER_CPUS_ENV, "").strip() or "2"


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Result from running a model process (Docker or host)."""

    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool
    latency_ms: int
    execution_mode: str  # "docker" | "host"
    cancelled: bool = False


def _has_docker() -> bool:
    """Check if Docker is available and running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _has_docker_image(image: str) -> bool:
    """Check whether the configured Docker image is already available locally."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _kill_process_group(proc: subprocess.Popen) -> None:
    """Kill process and its entire process group."""
    if proc.poll() is not None:
        return
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
            proc.wait(timeout=3)
    except (ProcessLookupError, PermissionError):
        pass
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=3)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Strip parent session env vars to avoid nested session conflicts
# ---------------------------------------------------------------------------

_STRIP_ENV_KEYS = frozenset({
    "CLAUDECODE",
    "CLAUDE_CODE_HEADLESS",
    "CLAUDE_CODE_ENTRYPOINT",
})


def _build_clean_env() -> dict[str, str]:
    """Build a clean execution environment."""
    env = {k: v for k, v in os.environ.items() if k not in _STRIP_ENV_KEYS}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


# ---------------------------------------------------------------------------
# Docker execution
# ---------------------------------------------------------------------------

def run_in_docker(
    *,
    command: str,
    stdin_text: str,
    timeout: int = 300,
    network: bool = False,
    env: Mapping[str, str] | None = None,
    image: str | None = None,
    memory: str | None = None,
    cpus: str | None = None,
    execution_control: DeterministicExecutionControl | None = None,
) -> ExecutionResult:
    """Run a command in a Docker container with stdin/stdout only.

    Parameters
    ----------
    command:
        Shell command to run inside the container.
    stdin_text:
        Text to pipe via stdin (the compiled prompt).
    timeout:
        Max execution time in seconds.
    network:
        Allow network access (False for build tasks, True for research).
    image:
        Docker image to use. Defaults to PRAXIS_DOCKER_IMAGE env var.
    memory:
        Memory limit. Defaults to PRAXIS_DOCKER_MEMORY env var.
    cpus:
        CPU limit. Defaults to PRAXIS_DOCKER_CPUS env var.
    """
    docker_image, image_meta = resolve_docker_image(
        requested_image=image,
        image_exists=_has_docker_image,
    )
    docker_memory = memory or _docker_memory()
    docker_cpus = cpus or _docker_cpus()
    if not _has_docker_image(docker_image):
        detail = str(image_meta.get("build_error") or "").strip()
        raise RuntimeError(
            f"Docker image {docker_image!r} is unavailable. Build it or set "
            f"{DOCKER_IMAGE_ENV} before execution."
            + (f" {detail}" if detail else "")
        )

    docker_cmd = [
        "docker", "run",
        "--rm",             # Cleanup after exit
        "-i",               # Keep stdin open
        "--memory", docker_memory,
        "--cpus", docker_cpus,
    ]

    for key, value in sorted((env or {}).items()):
        docker_cmd.extend(["-e", f"{key}={value}"])

    if not network:
        docker_cmd.append("--network=none")

    docker_cmd.extend([docker_image, "bash", "-c", command])

    start_ns = time.monotonic_ns()
    timed_out = False
    cancelled = False

    if execution_control is not None and execution_control.cancel_requested():
        return ExecutionResult(
            stdout="",
            stderr="",
            exit_code=1,
            timed_out=False,
            cancelled=True,
            latency_ms=0,
            execution_mode="docker",
        )

    proc = subprocess.Popen(
        docker_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )

    cancel_requested = threading.Event()

    if execution_control is not None:
        def _interrupt() -> None:
            if proc.poll() is not None:
                return
            cancel_requested.set()
            _kill_process_group(proc)

        execution_control.register_interrupt(_interrupt)

    try:
        stdout, stderr = proc.communicate(
            input=stdin_text,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        cancelled = cancel_requested.is_set()
        timed_out = not cancelled
        _kill_process_group(proc)
        stdout, stderr = proc.communicate()

    latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    return ExecutionResult(
        stdout=stdout or "",
        stderr=stderr or "",
        exit_code=proc.returncode if proc.returncode is not None else 1,
        timed_out=timed_out,
        cancelled=cancel_requested.is_set() or cancelled,
        latency_ms=latency_ms,
        execution_mode="docker",
    )


# ---------------------------------------------------------------------------
# Host execution (fallback when Docker is unavailable)
# ---------------------------------------------------------------------------

def run_on_host(
    *,
    command: str,
    stdin_text: str,
    timeout: int = 300,
    env_overrides: Mapping[str, str] | None = None,
    workdir: str | None = None,
    execution_control: DeterministicExecutionControl | None = None,
) -> ExecutionResult:
    """Run a command on the host with stdin/stdout only. No filesystem access.

    This is the fallback when Docker is unavailable. The command is run
    via bash -c with process group isolation but WITHOUT any flags that
    grant filesystem access (no --dangerously-skip-permissions, no --full-auto,
    no --approval-mode yolo).
    """
    env = _build_clean_env()
    if env_overrides:
        env.update({str(key): str(value) for key, value in env_overrides.items()})
    start_ns = time.monotonic_ns()
    timed_out = False
    cancelled = False

    if execution_control is not None and execution_control.cancel_requested():
        return ExecutionResult(
            stdout="",
            stderr="",
            exit_code=1,
            timed_out=False,
            cancelled=True,
            latency_ms=0,
            execution_mode="host",
        )

    proc = subprocess.Popen(
        ["bash", "-c", command],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
        cwd=workdir,
    )

    cancel_requested = threading.Event()

    if execution_control is not None:
        def _interrupt() -> None:
            if proc.poll() is not None:
                return
            cancel_requested.set()
            _kill_process_group(proc)

        execution_control.register_interrupt(_interrupt)

    try:
        stdout, stderr = proc.communicate(
            input=stdin_text,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        cancelled = cancel_requested.is_set()
        timed_out = not cancelled
        _kill_process_group(proc)
        stdout, stderr = proc.communicate()

    latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    return ExecutionResult(
        stdout=stdout or "",
        stderr=stderr or "",
        exit_code=proc.returncode if proc.returncode is not None else 1,
        timed_out=timed_out,
        cancelled=cancel_requested.is_set() or cancelled,
        latency_ms=latency_ms,
        execution_mode="host",
    )


# ---------------------------------------------------------------------------
# Unified runner
# ---------------------------------------------------------------------------

def run_model(
    *,
    command: str,
    stdin_text: str,
    timeout: int = 300,
    network: bool = False,
    prefer_docker: bool = True,
    require_docker: bool = False,
    env: Mapping[str, str] | None = None,
    image: str | None = None,
    workdir: str | None = None,
    execution_control: DeterministicExecutionControl | None = None,
) -> ExecutionResult:
    """Run a model command via Docker only.

    Parameters
    ----------
    command:
        Shell command (e.g. "claude --print --model claude-sonnet-4-6").
    stdin_text:
        Compiled prompt text to pipe via stdin.
    timeout:
        Max execution time in seconds.
    network:
        Allow network access in Docker (for research tasks).
    prefer_docker:
        Reserved for compatibility; Docker remains the only supported execution mode.
    image:
        Override Docker image.
    require_docker:
        Reserved for compatibility; execution fails closed if Docker is unavailable.
    """
    del prefer_docker, require_docker, workdir

    if execution_control is not None and execution_control.cancel_requested():
        return ExecutionResult(
            stdout="",
            stderr="",
            exit_code=1,
            timed_out=False,
            latency_ms=0,
            execution_mode="docker",
            cancelled=True,
        )

    if _has_docker():
        return run_in_docker(
            command=command,
            stdin_text=stdin_text,
            timeout=timeout,
            network=network,
            env=env,
            image=image,
            execution_control=execution_control,
        )

    raise RuntimeError("Docker is required for workflow model execution but is unavailable.")
