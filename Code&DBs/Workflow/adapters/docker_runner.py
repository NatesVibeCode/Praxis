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

from registry.provider_execution_registry import resolve_api_key_env_vars
from runtime.docker_image_authority import DOCKER_IMAGE_ENV, resolve_docker_image
from runtime.sandbox_runtime import _cli_auth_volume_flags
from runtime.workflow.execution_policy import validate_auth_mount_policy

if TYPE_CHECKING:
    from .deterministic import DeterministicExecutionControl


_PRAXIS_DOCKER_MEMORY_ENV = "PRAXIS_DOCKER_MEMORY"
_PRAXIS_DOCKER_CPUS_ENV = "PRAXIS_DOCKER_CPUS"

_PROVIDER_AUTH_ENV_CANDIDATES: dict[str, tuple[str, ...]] = {
    "anthropic": ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"),
    "openai": ("OPENAI_API_KEY",),
    "google": ("GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_GENAI_API_KEY"),
    "cursor": ("CURSOR_API_KEY",),
    "cursor_local": ("CURSOR_API_KEY",),
}


def _cli_auth_env_forward(provider_slug: str | None) -> dict[str, str]:
    """Return the single provider auth env var to forward into the container."""
    normalized_provider = str(provider_slug or "").strip().lower()
    if not normalized_provider:
        return {}

    forwarded: dict[str, str] = {}
    candidates = _PROVIDER_AUTH_ENV_CANDIDATES.get(normalized_provider) or resolve_api_key_env_vars(normalized_provider)
    for key in candidates:
        value = os.environ.get(key)
        if value:
            forwarded[key] = value
            break
    return forwarded


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


def normalize_command_parts_for_docker(command_parts: list[str]) -> list[str]:
    """Normalize CLI arguments for Docker-sandboxed execution.

    Adds the flags each CLI requires to run non-interactively and write files
    without prompting. Called from both cli_llm and execution_backends so the
    same behavior applies regardless of dispatch path.
    """
    if not command_parts:
        return []

    normalized = list(command_parts)
    cmd0_name = os.path.basename(normalized[0]).strip().lower()

    if cmd0_name == "claude":
        # Claude CLI refuses file writes without `--permission-mode bypassPermissions`
        # when run non-interactively. Inject immediately after the binary so flag
        # ordering is stable and idempotent.
        if "--permission-mode" not in normalized:
            normalized = [normalized[0], "--permission-mode", "bypassPermissions", *normalized[1:]]
        return normalized

    if cmd0_name == "codex":
        try:
            exec_idx = [part.strip().lower() for part in normalized].index("exec")
        except ValueError:
            return normalized

        normalized = [part for part in normalized if part != "--full-auto"]
        if "--skip-git-repo-check" not in normalized:
            normalized.insert(exec_idx + 1, "--skip-git-repo-check")
        if "--dangerously-bypass-approvals-and-sandbox" not in normalized:
            normalized.insert(exec_idx + 1, "--dangerously-bypass-approvals-and-sandbox")
        return normalized

    return normalized


def normalize_shell_command_for_docker(command: str) -> str:
    """Normalize a shell command string for Docker-sandboxed execution."""
    parts = shlex.split(command)
    if not parts:
        return command
    return shlex.join(normalize_command_parts_for_docker(parts))


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
    provider_slug: str | None = None,
    auth_mount_policy: str = "provider_scoped",
    workdir: str | None = None,
    user: str | None = "1100:1100",
    docker_network: str | None = None,
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

    # Run as non-root so CLIs can use --permission-mode bypassPermissions.
    # HOME is set via env below to match the user's home directory.
    if user:
        docker_cmd.extend(["--user", user])

    # Narrow per-run workdir bind: exposes ONLY the job's artifact directory
    # (not /workspace, not the repo source). Agent file writes persist to the
    # host via this mount; nothing else is reachable.
    if workdir:
        resolved_workdir = os.path.abspath(workdir)
        docker_cmd.extend([
            "-v", f"{resolved_workdir}:/workdir",
            "--workdir", "/workdir",
        ])

    normalized_auth_policy = validate_auth_mount_policy(auth_mount_policy)
    if normalized_auth_policy != "none":
        docker_cmd.extend(
            _cli_auth_volume_flags(
                provider_slug=provider_slug if normalized_auth_policy == "provider_scoped" else None,
            ),
        )

    forwarded_auth_env = _cli_auth_env_forward(provider_slug)
    # HOME forced to /home/praxis-agent so CLIs resolve config/creds from the
    # mounted auth files (which target /home/praxis-agent/... by policy).
    default_home_env = {"HOME": "/home/praxis-agent"} if user else {}
    merged_env: dict[str, str] = {
        **default_home_env,
        **forwarded_auth_env,
        **(dict(env) if env else {}),
    }
    for key, value in sorted(merged_env.items()):
        docker_cmd.extend(["-e", f"{key}={value}"])

    if docker_network:
        docker_cmd.append(f"--network={docker_network}")
    elif not network:
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
    provider_slug: str | None = None,
    auth_mount_policy: str = "provider_scoped",
    docker_network: str | None = None,
    docker_user: str | None = "1100:1100",
    execution_control: DeterministicExecutionControl | None = None,
) -> ExecutionResult:
    """Run a model command via Docker or host execution.

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
        When False, execute on the host using the cleaned stdin/stdout runner.
    image:
        Override Docker image.
    require_docker:
        Reserved for compatibility; execution fails closed if Docker is unavailable.
    """
    del require_docker

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

    if not prefer_docker:
        return run_on_host(
            command=command,
            stdin_text=stdin_text,
            timeout=timeout,
            env_overrides=env,
            workdir=workdir,
            execution_control=execution_control,
        )

    command = normalize_shell_command_for_docker(command)

    if _has_docker():
        return run_in_docker(
            command=command,
            stdin_text=stdin_text,
            timeout=timeout,
            network=network,
            env=env,
            image=image,
            provider_slug=provider_slug,
            auth_mount_policy=auth_mount_policy,
            workdir=workdir,
            user=docker_user,
            docker_network=docker_network,
            execution_control=execution_control,
        )

    raise RuntimeError("Docker is required for workflow model execution but is unavailable.")
