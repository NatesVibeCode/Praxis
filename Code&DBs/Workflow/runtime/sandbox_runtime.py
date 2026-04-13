"""Provider-agnostic sandbox lifecycle runtime."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol


_DOCKER_IMAGE_ENV = "PRAXIS_DOCKER_IMAGE"
_DOCKER_MEMORY_ENV = "PRAXIS_DOCKER_MEMORY"
_DOCKER_CPUS_ENV = "PRAXIS_DOCKER_CPUS"
_CLOUDFLARE_SANDBOX_URL_ENV = "PRAXIS_CLOUDFLARE_SANDBOX_URL"
_CLOUDFLARE_SANDBOX_TOKEN_ENV = "PRAXIS_CLOUDFLARE_SANDBOX_TOKEN"
_IGNORED_MANIFEST_DIRS = frozenset({".git", "__pycache__", ".pytest_cache", ".mypy_cache"})


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    """Control-plane-owned workspace materialization input."""

    source_root: str
    materialization: str = "copy"


@dataclass(frozen=True, slots=True)
class SandboxSessionSpec:
    """Requested session metadata for one execution."""

    sandbox_session_id: str
    sandbox_group_id: str | None
    provider: str
    workdir: str
    network_policy: str
    workspace_materialization: str
    timeout_seconds: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SandboxSession:
    """Active sandbox session bound to the canonical session identity."""

    sandbox_session_id: str
    sandbox_group_id: str | None
    provider: str
    provider_session_id: str
    workspace_root: str
    network_policy: str
    workspace_materialization: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HydrationReceipt:
    """Result of preparing the workspace inside the sandbox."""

    sandbox_session_id: str
    workspace_root: str
    hydrated_files: int
    workspace_materialization: str


@dataclass(frozen=True, slots=True)
class ArtifactReceipt:
    """Changed artifact metadata collected after execution."""

    sandbox_session_id: str
    artifact_refs: tuple[str, ...]
    artifact_count: int


@dataclass(frozen=True, slots=True)
class TeardownReceipt:
    """Session teardown confirmation."""

    sandbox_session_id: str
    provider: str
    disposition: str


@dataclass(frozen=True, slots=True)
class SandboxExecRequest:
    """One command execution within an existing sandbox session."""

    command: str
    stdin_text: str
    env: dict[str, str]
    timeout_seconds: int
    execution_transport: str
    image: str | None = None


@dataclass(frozen=True, slots=True)
class SandboxExecutionResult:
    """Normalized execution envelope for all sandbox providers."""

    sandbox_session_id: str
    sandbox_group_id: str | None
    sandbox_provider: str
    execution_transport: str
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    artifact_refs: tuple[str, ...]
    started_at: str
    finished_at: str
    network_policy: str
    provider_latency_ms: int
    execution_mode: str
    workspace_root: str


class SandboxProviderAdapter(Protocol):
    """Lifecycle contract implemented by each sandbox provider."""

    provider_name: str
    execution_lane: str           # "local" or "remote"
    requires_artifact_sync: bool  # True if artifacts must be synced back (remote providers)

    def create_session(self, spec: SandboxSessionSpec) -> SandboxSession: ...

    def hydrate_workspace(
        self,
        session: SandboxSession,
        snapshot: WorkspaceSnapshot,
    ) -> HydrationReceipt: ...

    def exec(self, session: SandboxSession, request: SandboxExecRequest) -> SandboxExecutionResult: ...

    def collect_artifacts(
        self,
        session: SandboxSession,
        before_manifest: dict[str, tuple[int, int]],
    ) -> ArtifactReceipt: ...

    def destroy_session(self, session: SandboxSession, disposition: str) -> TeardownReceipt: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0



def _docker_image_available(image: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _docker_image() -> str:
    return os.environ.get(_DOCKER_IMAGE_ENV, "praxis-worker:latest")


def _docker_memory() -> str:
    return os.environ.get(_DOCKER_MEMORY_ENV, "4g")


def _docker_cpus() -> str:
    return os.environ.get(_DOCKER_CPUS_ENV, "2")


def _ensure_text(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def _workspace_manifest(root: str) -> dict[str, tuple[int, int]]:
    manifest: dict[str, tuple[int, int]] = {}
    root_path = Path(root)
    if not root_path.exists():
        return manifest
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in _IGNORED_MANIFEST_DIRS]
        for filename in filenames:
            absolute = Path(dirpath) / filename
            try:
                stat = absolute.stat()
            except OSError:
                continue
            relpath = absolute.relative_to(root_path).as_posix()
            manifest[relpath] = (stat.st_size, stat.st_mtime_ns)
    return manifest


def _hydrate_copy(source_root: str, destination_root: str) -> int:
    copied = 0
    source = Path(source_root)
    destination = Path(destination_root)
    destination.mkdir(parents=True, exist_ok=True)
    for dirpath, dirnames, filenames in os.walk(source_root):
        dirnames[:] = [name for name in dirnames if name not in _IGNORED_MANIFEST_DIRS]
        source_dir = Path(dirpath)
        relative_dir = source_dir.relative_to(source)
        target_dir = destination / relative_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        for filename in filenames:
            source_file = source_dir / filename
            target_file = target_dir / filename
            shutil.copy2(source_file, target_file)
            copied += 1
    return copied


def _write_text_artifact(path: str, content: str) -> None:
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(content, encoding="utf-8")


def _seatbelt_ancestor_metadata_rules(path: str) -> list[str]:
    rules: list[str] = []
    resolved = Path(path).resolve()
    for ancestor in (resolved, *resolved.parents):
        rules.append(f'(allow file-read-metadata (literal "{ancestor.as_posix()}"))')
    return rules


def _seatbelt_path_variants(path: str) -> tuple[str, ...]:
    candidates = {
        os.path.abspath(path),
        os.path.realpath(path),
    }
    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        cleaned = Path(candidate).as_posix()
        if cleaned not in seen:
            seen.add(cleaned)
            normalized.append(cleaned)
    return tuple(normalized)


def _seatbelt_exec_env(base_env: dict[str, str], workspace_root: str) -> dict[str, str]:
    env = dict(base_env)
    env.setdefault("HOME", workspace_root)
    env.setdefault("PWD", workspace_root)
    env.setdefault("TMPDIR", os.path.join(workspace_root, ".tmp"))
    Path(env["TMPDIR"]).mkdir(parents=True, exist_ok=True)
    return env


def _default_profile(
    *,
    workspace_root: str,
    network_policy: str,
) -> str:
    real_home = os.path.expanduser("~")
    system_paths = (
        "/System",
        "/usr",
        "/bin",
        "/sbin",
        "/dev",
        "/private/tmp",
        "/tmp",
        # Platform package manager paths (Homebrew on macOS, linuxbrew, etc.)
        *(p for p in ["/opt/homebrew", "/home/linuxbrew/.linuxbrew"] if os.path.isdir(p)),
        # CLI auth: claude OAuth tokens, codex auth, gemini config
        os.path.join(real_home, "Library/Application Support/Claude"),
        os.path.join(real_home, "Library/Application Support/Codex"),
        os.path.join(real_home, "Library/Keychains"),
        os.path.join(real_home, ".claude"),
        os.path.join(real_home, ".codex"),
        os.path.join(real_home, ".gemini"),
    )
    parts = [
        "(version 1)",
        '(deny default)',
        '(import "system.sb")',
        "(allow process*)",
        "(allow sysctl-read)",
        "(allow mach-lookup)",
    ]
    for workspace_variant in _seatbelt_path_variants(workspace_root):
        parts.append(f'(allow file-read* (subpath "{workspace_variant}"))')
        parts.append(f'(allow file-write* (subpath "{workspace_variant}"))')
        parts.extend(_seatbelt_ancestor_metadata_rules(workspace_variant))
    for path in system_paths:
        parts.append(f'(allow file-read* (subpath "{path}"))')
        parts.extend(_seatbelt_ancestor_metadata_rules(path))
    if network_policy != "disabled":
        parts.append("(allow network*)")
    return "\n".join(parts)


class SeatbeltLocalSandboxProvider:
    """macOS Seatbelt-backed sandbox provider."""

    provider_name = "seatbelt_local"
    execution_lane = "local"
    requires_artifact_sync = False

    def create_session(self, spec: SandboxSessionSpec) -> SandboxSession:
        if os.uname().sysname.lower() != "darwin":
            raise RuntimeError("seatbelt_local is only available on macOS")
        session_root = os.path.realpath(tempfile.mkdtemp(prefix="praxis-sandbox-"))
        workspace_root = os.path.join(session_root, "workspace")
        os.makedirs(workspace_root, exist_ok=True)
        return SandboxSession(
            sandbox_session_id=spec.sandbox_session_id,
            sandbox_group_id=spec.sandbox_group_id,
            provider=self.provider_name,
            provider_session_id=Path(session_root).name,
            workspace_root=workspace_root,
            network_policy=spec.network_policy,
            workspace_materialization=spec.workspace_materialization,
            metadata=dict(spec.metadata),
        )

    def hydrate_workspace(
        self,
        session: SandboxSession,
        snapshot: WorkspaceSnapshot,
    ) -> HydrationReceipt:
        copied = _hydrate_copy(snapshot.source_root, session.workspace_root)
        # Copy CLI auth config dirs so tools can authenticate in the sandbox
        real_home = os.path.expanduser("~")
        for config_dir in (".gemini", ".claude", ".codex"):
            src = os.path.join(real_home, config_dir)
            dst = os.path.join(session.workspace_root, config_dir)
            if os.path.isdir(src) and not os.path.exists(dst):
                shutil.copytree(src, dst, symlinks=True, ignore_dangling_symlinks=True,
                                ignore=shutil.ignore_patterns(
                                    "history", "tmp", "antigravity*",
                                    "debug", "plans", "projects", "file-history",
                                    "paste-cache", "backups", "sessions",
                                    "shell_snapshots", "skills", "worktrees",
                                ))
        # Strip MCP server configs from codex config.toml (they reference
        # host-side URLs that aren't reachable inside the sandbox)
        codex_config = os.path.join(session.workspace_root, ".codex", "config.toml")
        if os.path.isfile(codex_config):
            try:
                lines = Path(codex_config).read_text(encoding="utf-8").splitlines()
                filtered = []
                skip_section = False
                for line in lines:
                    if line.strip().startswith("[mcp_servers"):
                        skip_section = True
                        continue
                    if skip_section and line.strip().startswith("["):
                        skip_section = False
                    if not skip_section:
                        filtered.append(line)
                Path(codex_config).write_text("\n".join(filtered) + "\n", encoding="utf-8")
            except Exception:
                pass
        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=copied,
            workspace_materialization=snapshot.materialization,
        )

    def exec(self, session: SandboxSession, request: SandboxExecRequest) -> SandboxExecutionResult:
        profile_path = os.path.join(Path(session.workspace_root).parent, "seatbelt.sb")
        Path(profile_path).write_text(
            _default_profile(
                workspace_root=session.workspace_root,
                network_policy=session.network_policy,
            ),
            encoding="utf-8",
        )
        start = _utc_now()
        start_monotonic = time.monotonic_ns()
        proc = subprocess.Popen(
            [
                "/usr/bin/sandbox-exec",
                "-f",
                profile_path,
                "bash",
                "--noprofile",
                "--norc",
                "-c",
                request.command,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=session.workspace_root,
            env=_seatbelt_exec_env(request.env, session.workspace_root),
        )
        timed_out = False
        try:
            stdout, stderr = proc.communicate(
                input=request.stdin_text,
                timeout=request.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            timed_out = True
            proc.kill()
            stdout, stderr = proc.communicate()
        end = _utc_now()
        latency_ms = int((time.monotonic_ns() - start_monotonic) / 1_000_000)
        return SandboxExecutionResult(
            sandbox_session_id=session.sandbox_session_id,
            sandbox_group_id=session.sandbox_group_id,
            sandbox_provider=self.provider_name,
            execution_transport=request.execution_transport,
            exit_code=proc.returncode if proc.returncode is not None else 1,
            stdout=stdout or "",
            stderr=stderr or "",
            timed_out=timed_out,
            artifact_refs=(),
            started_at=start.isoformat(),
            finished_at=end.isoformat(),
            network_policy=session.network_policy,
            provider_latency_ms=latency_ms,
            execution_mode=self.provider_name,
            workspace_root=session.workspace_root,
        )

    def collect_artifacts(
        self,
        session: SandboxSession,
        before_manifest: dict[str, tuple[int, int]],
    ) -> ArtifactReceipt:
        after_manifest = _workspace_manifest(session.workspace_root)
        changed = sorted(
            path for path, metadata in after_manifest.items() if before_manifest.get(path) != metadata
        )
        return ArtifactReceipt(
            sandbox_session_id=session.sandbox_session_id,
            artifact_refs=tuple(changed),
            artifact_count=len(changed),
        )

    def destroy_session(self, session: SandboxSession, disposition: str) -> TeardownReceipt:
        shutil.rmtree(Path(session.workspace_root).parent, ignore_errors=True)
        return TeardownReceipt(
            sandbox_session_id=session.sandbox_session_id,
            provider=self.provider_name,
            disposition=disposition,
        )


class DockerLocalSandboxProvider:
    """Docker-backed sandbox provider with explicit workspace hydration."""

    provider_name = "docker_local"
    execution_lane = "local"
    requires_artifact_sync = False

    def create_session(self, spec: SandboxSessionSpec) -> SandboxSession:
        if not _docker_available():
            raise RuntimeError("Docker is required for docker_local sandbox execution but is unavailable.")
        session_root = os.path.realpath(tempfile.mkdtemp(prefix="praxis-docker-sandbox-"))
        workspace_root = os.path.join(session_root, "workspace")
        os.makedirs(workspace_root, exist_ok=True)
        return SandboxSession(
            sandbox_session_id=spec.sandbox_session_id,
            sandbox_group_id=spec.sandbox_group_id,
            provider=self.provider_name,
            provider_session_id=Path(session_root).name,
            workspace_root=workspace_root,
            network_policy=spec.network_policy,
            workspace_materialization=spec.workspace_materialization,
            metadata=dict(spec.metadata),
        )

    def hydrate_workspace(
        self,
        session: SandboxSession,
        snapshot: WorkspaceSnapshot,
    ) -> HydrationReceipt:
        copied = _hydrate_copy(snapshot.source_root, session.workspace_root)
        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=copied,
            workspace_materialization=snapshot.materialization,
        )

    def exec(self, session: SandboxSession, request: SandboxExecRequest) -> SandboxExecutionResult:
        docker_image = request.image or _docker_image()
        if not _docker_image_available(docker_image):
            raise RuntimeError(
                "docker_local requires image "
                f"{docker_image!r}. Build or configure {_DOCKER_IMAGE_ENV} before sandbox execution."
            )
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--memory",
            _docker_memory(),
            "--cpus",
            _docker_cpus(),
            "--workdir",
            "/workspace",
            "-v",
            f"{session.workspace_root}:/workspace",
        ]
        for key, value in sorted(request.env.items()):
            docker_cmd.extend(["-e", f"{key}={value}"])
        if session.network_policy == "disabled":
            docker_cmd.append("--network=none")
        docker_cmd.extend([docker_image, "bash", "-lc", request.command])
        start = _utc_now()
        start_monotonic = time.monotonic_ns()
        proc = subprocess.Popen(
            docker_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        timed_out = False
        try:
            stdout, stderr = proc.communicate(
                input=request.stdin_text,
                timeout=request.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            timed_out = True
            proc.kill()
            stdout, stderr = proc.communicate()
        end = _utc_now()
        latency_ms = int((time.monotonic_ns() - start_monotonic) / 1_000_000)
        return SandboxExecutionResult(
            sandbox_session_id=session.sandbox_session_id,
            sandbox_group_id=session.sandbox_group_id,
            sandbox_provider=self.provider_name,
            execution_transport=request.execution_transport,
            exit_code=proc.returncode if proc.returncode is not None else 1,
            stdout=stdout or "",
            stderr=stderr or "",
            timed_out=timed_out,
            artifact_refs=(),
            started_at=start.isoformat(),
            finished_at=end.isoformat(),
            network_policy=session.network_policy,
            provider_latency_ms=latency_ms,
            execution_mode=self.provider_name,
            workspace_root=session.workspace_root,
        )

    def collect_artifacts(
        self,
        session: SandboxSession,
        before_manifest: dict[str, tuple[int, int]],
    ) -> ArtifactReceipt:
        after_manifest = _workspace_manifest(session.workspace_root)
        changed = sorted(
            path for path, metadata in after_manifest.items() if before_manifest.get(path) != metadata
        )
        return ArtifactReceipt(
            sandbox_session_id=session.sandbox_session_id,
            artifact_refs=tuple(changed),
            artifact_count=len(changed),
        )

    def destroy_session(self, session: SandboxSession, disposition: str) -> TeardownReceipt:
        shutil.rmtree(Path(session.workspace_root).parent, ignore_errors=True)
        return TeardownReceipt(
            sandbox_session_id=session.sandbox_session_id,
            provider=self.provider_name,
            disposition=disposition,
        )


class CloudflareRemoteSandboxProvider:
    """Remote provider backed by a Cloudflare-hosted sandbox bridge."""

    provider_name = "cloudflare_remote"
    execution_lane = "remote"
    requires_artifact_sync = True

    def __init__(self) -> None:
        self._base_url = (os.environ.get(_CLOUDFLARE_SANDBOX_URL_ENV) or "").rstrip("/")
        self._token = os.environ.get(_CLOUDFLARE_SANDBOX_TOKEN_ENV) or ""

    def _request(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._base_url:
            raise RuntimeError(
                "cloudflare_remote requires "
                f"{_CLOUDFLARE_SANDBOX_URL_ENV}"
            )
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}{path}",
            data=body,
            headers={
                "Content-Type": "application/json",
                **(
                    {"Authorization": f"Bearer {self._token}"}
                    if self._token
                    else {}
                ),
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Cloudflare sandbox bridge request failed for {path}: {exc}") from exc
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            raise RuntimeError(f"Cloudflare sandbox bridge returned invalid payload for {path}")
        return data

    def create_session(self, spec: SandboxSessionSpec) -> SandboxSession:
        local_mirror = os.path.realpath(tempfile.mkdtemp(prefix="praxis-cloudflare-sandbox-"))
        response = self._request(
            "/sessions/create",
            {
                "sandbox_session_id": spec.sandbox_session_id,
                "sandbox_group_id": spec.sandbox_group_id,
                "network_policy": spec.network_policy,
                "workspace_materialization": spec.workspace_materialization,
                "timeout_seconds": spec.timeout_seconds,
                "metadata": spec.metadata,
            },
        )
        provider_session_id = str(response.get("provider_session_id") or "").strip()
        if not provider_session_id:
            raise RuntimeError("Cloudflare sandbox bridge did not return provider_session_id")
        return SandboxSession(
            sandbox_session_id=spec.sandbox_session_id,
            sandbox_group_id=spec.sandbox_group_id,
            provider=self.provider_name,
            provider_session_id=provider_session_id,
            workspace_root=local_mirror,
            network_policy=spec.network_policy,
            workspace_materialization=spec.workspace_materialization,
            metadata=dict(spec.metadata),
        )

    def hydrate_workspace(
        self,
        session: SandboxSession,
        snapshot: WorkspaceSnapshot,
    ) -> HydrationReceipt:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            archive.add(snapshot.source_root, arcname="workspace")
        payload = {
            "archive_base64": base64.b64encode(buffer.getvalue()).decode("ascii"),
            "workspace_materialization": snapshot.materialization,
        }
        response = self._request(f"/sessions/{session.provider_session_id}/hydrate", payload)
        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=int(response.get("hydrated_files") or 0),
            workspace_materialization=snapshot.materialization,
        )

    def exec(self, session: SandboxSession, request: SandboxExecRequest) -> SandboxExecutionResult:
        response = self._request(
            f"/sessions/{session.provider_session_id}/exec",
            {
                "command": request.command,
                "stdin_text": request.stdin_text,
                "env": request.env,
                "timeout_seconds": request.timeout_seconds,
                "execution_transport": request.execution_transport,
            },
        )
        return SandboxExecutionResult(
            sandbox_session_id=session.sandbox_session_id,
            sandbox_group_id=session.sandbox_group_id,
            sandbox_provider=self.provider_name,
            execution_transport=request.execution_transport,
            exit_code=int(response.get("exit_code") or 0),
            stdout=str(response.get("stdout") or ""),
            stderr=str(response.get("stderr") or ""),
            timed_out=bool(response.get("timed_out")),
            artifact_refs=tuple(str(path) for path in response.get("artifact_refs") or ()),
            started_at=str(response.get("started_at") or _iso_now()),
            finished_at=str(response.get("finished_at") or _iso_now()),
            network_policy=session.network_policy,
            provider_latency_ms=int(response.get("provider_latency_ms") or 0),
            execution_mode=self.provider_name,
            workspace_root=session.workspace_root,
        )

    def collect_artifacts(
        self,
        session: SandboxSession,
        before_manifest: dict[str, tuple[int, int]],
    ) -> ArtifactReceipt:
        del before_manifest
        response = self._request(
            f"/sessions/{session.provider_session_id}/artifacts",
            {"include_content": True},
        )
        artifact_refs = tuple(str(path) for path in response.get("artifact_refs") or ())
        artifacts_payload = response.get("artifacts")
        if isinstance(artifacts_payload, list):
            synced_refs: list[str] = []
            for artifact in artifacts_payload:
                if not isinstance(artifact, dict):
                    continue
                relpath = str(artifact.get("path") or "").strip()
                if not relpath:
                    continue
                absolute_path = os.path.join(session.workspace_root, relpath)
                if artifact.get("content_base64") is not None:
                    content = base64.b64decode(str(artifact["content_base64"]))
                    target = Path(absolute_path)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(content)
                elif artifact.get("content") is not None:
                    _write_text_artifact(absolute_path, str(artifact["content"]))
                synced_refs.append(relpath)
            if synced_refs:
                artifact_refs = tuple(synced_refs)
        return ArtifactReceipt(
            sandbox_session_id=session.sandbox_session_id,
            artifact_refs=artifact_refs,
            artifact_count=len(artifact_refs),
        )

    def destroy_session(self, session: SandboxSession, disposition: str) -> TeardownReceipt:
        try:
            self._request(
                f"/sessions/{session.provider_session_id}/destroy",
                {"disposition": disposition},
            )
        finally:
            shutil.rmtree(session.workspace_root, ignore_errors=True)
        return TeardownReceipt(
            sandbox_session_id=session.sandbox_session_id,
            provider=self.provider_name,
            disposition=disposition,
        )


class SandboxRuntime:
    """Lifecycle authority that normalizes provider execution."""

    def __init__(self) -> None:
        self._providers: dict[str, SandboxProviderAdapter] = {
            "seatbelt_local": SeatbeltLocalSandboxProvider(),
            "docker_local": DockerLocalSandboxProvider(),
            "cloudflare_remote": CloudflareRemoteSandboxProvider(),
        }

    def _provider(self, provider_name: str) -> SandboxProviderAdapter:
        provider = self._providers.get(provider_name)
        if provider is None:
            raise RuntimeError(f"Unknown sandbox provider: {provider_name}")
        return provider

    def execute_command(
        self,
        *,
        provider_name: str,
        sandbox_session_id: str,
        sandbox_group_id: str | None,
        workdir: str,
        command: str,
        stdin_text: str,
        env: dict[str, str],
        timeout_seconds: int,
        network_policy: str,
        workspace_materialization: str,
        execution_transport: str,
        image: str | None = None,
        metadata: dict[str, Any] | None = None,
        artifact_store: Any | None = None,
    ) -> SandboxExecutionResult:
        provider = self._provider(provider_name)
        session = provider.create_session(
            SandboxSessionSpec(
                sandbox_session_id=sandbox_session_id,
                sandbox_group_id=sandbox_group_id,
                provider=provider_name,
                workdir=workdir,
                network_policy=network_policy,
                workspace_materialization=workspace_materialization,
                timeout_seconds=timeout_seconds,
                metadata=dict(metadata or {}),
            )
        )
        disposition = "completed"
        try:
            provider.hydrate_workspace(
                session,
                WorkspaceSnapshot(
                    source_root=workdir,
                    materialization=workspace_materialization,
                ),
            )
            before_manifest = _workspace_manifest(session.workspace_root)
            result = provider.exec(
                session,
                SandboxExecRequest(
                    command=command,
                    stdin_text=stdin_text,
                    env=env,
                    timeout_seconds=timeout_seconds,
                    execution_transport=execution_transport,
                    image=image,
                ),
            )
            artifact_receipt = provider.collect_artifacts(session, before_manifest)
            artifact_refs = artifact_receipt.artifact_refs
            if artifact_store is not None:
                persisted_refs: list[str] = []
                missing_artifacts: list[str] = []
                for relpath in artifact_refs:
                    content = _ensure_text(os.path.join(session.workspace_root, relpath))
                    if content is None:
                        missing_artifacts.append(relpath)
                        continue
                    record = artifact_store.capture(relpath, content, sandbox_session_id)
                    persisted_refs.append(record.artifact_id)
                if getattr(provider, "requires_artifact_sync", False) and missing_artifacts:
                    raise RuntimeError(
                        f"{provider_name} returned artifact refs without synced content: "
                        + ", ".join(sorted(missing_artifacts))
                    )
                artifact_refs = tuple(persisted_refs or artifact_refs)
            return SandboxExecutionResult(
                sandbox_session_id=result.sandbox_session_id,
                sandbox_group_id=result.sandbox_group_id,
                sandbox_provider=result.sandbox_provider,
                execution_transport=result.execution_transport,
                exit_code=result.exit_code,
                stdout=result.stdout,
                stderr=result.stderr,
                timed_out=result.timed_out,
                artifact_refs=artifact_refs,
                started_at=result.started_at,
                finished_at=result.finished_at,
                network_policy=result.network_policy,
                provider_latency_ms=result.provider_latency_ms,
                execution_mode=result.execution_mode,
                workspace_root=result.workspace_root,
            )
        except Exception:
            disposition = "failed"
            raise
        finally:
            provider.destroy_session(session, disposition)


def derive_sandbox_identity(
    *,
    workdir: str,
    execution_bundle: dict[str, Any] | None,
    execution_transport: str,
) -> tuple[str, str | None]:
    bundle = execution_bundle if isinstance(execution_bundle, dict) else {}
    run_id = str(bundle.get("run_id") or "").strip()
    job_label = str(bundle.get("job_label") or "").strip()
    if run_id:
        suffix = job_label or execution_transport
        return f"sandbox_session:{run_id}:{suffix}", f"group:{run_id}"
    digest = hashlib.sha1(f"{workdir}:{execution_transport}:{time.time_ns()}".encode("utf-8")).hexdigest()[:12]
    return f"sandbox_session:adhoc:{digest}", None
