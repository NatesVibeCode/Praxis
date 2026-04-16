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
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from uuid import uuid4
from pathlib import PurePosixPath

from .docker_image_authority import DOCKER_IMAGE_ENV, resolve_docker_image
from runtime.workflow.execution_policy import validate_auth_mount_policy

_DOCKER_MEMORY_ENV = "PRAXIS_DOCKER_MEMORY"
_DOCKER_CPUS_ENV = "PRAXIS_DOCKER_CPUS"
_CLI_AUTH_HOME_ENV = "PRAXIS_CLI_AUTH_HOME"
_SNAPSHOT_CACHE_ROOT_ENV = "PRAXIS_SANDBOX_SNAPSHOT_CACHE_DIR"


def _parse_docker_mem_str(mem_str: str) -> int:
    """Parse a docker stats memory string like '234MiB' or '1.2GiB' to bytes."""
    mem_str = mem_str.strip()
    units = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3,
             "kb": 1000, "mb": 1000**2, "gb": 1000**3}
    lower = mem_str.lower()
    for suffix, factor in sorted(units.items(), key=lambda x: -len(x[0])):
        if lower.endswith(suffix):
            try:
                return int(float(lower[: -len(suffix)]) * factor)
            except ValueError:
                return 0
    try:
        return int(float(mem_str))
    except ValueError:
        return 0
_CLOUDFLARE_SANDBOX_URL_ENV = "PRAXIS_CLOUDFLARE_SANDBOX_URL"
_CLOUDFLARE_SANDBOX_TOKEN_ENV = "PRAXIS_CLOUDFLARE_SANDBOX_TOKEN"
_IGNORED_MANIFEST_DIRS = frozenset({".git", "__pycache__", ".pytest_cache", ".mypy_cache"})

# CLI auth files to mount read-only into Docker containers.
# Each entry: (provider slugs, host_path_relative_to_home, container_path).
_CLI_AUTH_MOUNTS: tuple[tuple[frozenset[str], str, str], ...] = (
    (frozenset({"openai"}), ".codex/auth.json", "/root/.codex/auth.json"),
    (frozenset({"anthropic"}), ".claude.json", "/root/.claude.json"),
    (frozenset({"google", "gemini"}), ".gemini/oauth_creds.json", "/root/.gemini/oauth_creds.json"),
    (frozenset({"google", "gemini"}), ".gemini/google_accounts.json", "/root/.gemini/google_accounts.json"),
    (frozenset({"google", "gemini"}), ".gemini/settings.json", "/root/.gemini/settings.json"),
)


def _cli_auth_home() -> str:
    configured = os.environ.get(_CLI_AUTH_HOME_ENV, "").strip()
    if configured:
        return configured
    return os.path.expanduser("~")


def _cli_auth_probe_homes() -> tuple[str, ...]:
    host_home = _cli_auth_home()
    probe_homes: list[str] = []
    for candidate in (host_home, os.path.expanduser("~")):
        normalized = str(candidate or "").strip()
        if normalized and normalized not in probe_homes:
            probe_homes.append(normalized)
    return tuple(probe_homes)


def _normalize_relative_path(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError(f"{field_name} must be a non-empty relative path")

    normalized_text = text.replace("\\", "/")
    path = PurePosixPath(normalized_text)
    if path.is_absolute():
        raise RuntimeError(f"{field_name} must stay inside the sandbox workspace boundary: {text}")

    parts: list[str] = []
    for part in path.parts:
        if part in ("", "."):
            continue
        if part == ".." or part.endswith(":"):
            raise RuntimeError(f"{field_name} must stay inside the sandbox workspace boundary: {text}")
        parts.append(part)

    if not parts:
        raise RuntimeError(f"{field_name} must be a non-empty relative path")
    return "/".join(parts)


def _normalize_relative_paths(
    values: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, Path)):
        raw_values = [values]
    elif isinstance(values, Sequence):
        raw_values = list(values)
    else:
        return ()

    normalized: list[str] = []
    for value in raw_values:
        normalized_path = _normalize_relative_path(value, field_name=field_name)
        if normalized_path not in normalized:
            normalized.append(normalized_path)
    return tuple(normalized)


def _scope_allows_path(path: str, write_scope: Sequence[str]) -> bool:
    normalized_path = _normalize_relative_path(path, field_name="artifact_ref")
    for scope_path in write_scope:
        normalized_scope = _normalize_relative_path(scope_path, field_name="write_scope")
        if normalized_path == normalized_scope:
            return True
        prefix = normalized_scope.rstrip("/")
        if prefix and normalized_path.startswith(prefix + "/"):
            return True
    return False


def _execution_write_scope(metadata: Mapping[str, Any] | None) -> tuple[str, ...] | None:
    if not isinstance(metadata, Mapping):
        return None
    execution_bundle = metadata.get("execution_bundle")
    if not isinstance(execution_bundle, Mapping):
        return None
    access_policy = execution_bundle.get("access_policy")
    if not isinstance(access_policy, Mapping):
        return None
    return _normalize_relative_paths(
        access_policy.get("write_scope"),
        field_name="execution_bundle.access_policy.write_scope",
    )


def _validated_artifact_refs(
    artifact_refs: Sequence[str],
    *,
    write_scope: tuple[str, ...] | None,
) -> tuple[str, ...]:
    normalized_refs = _normalize_relative_paths(artifact_refs, field_name="artifact_ref")
    if write_scope is None:
        return normalized_refs

    blocked_refs = [
        ref
        for ref in normalized_refs
        if not _scope_allows_path(ref, write_scope)
    ]
    if blocked_refs:
        raise RuntimeError(
            "sandbox produced artifacts outside declared write_scope: "
            + ", ".join(blocked_refs)
        )
    return normalized_refs


def _provider_slug(metadata: Mapping[str, Any] | None) -> str | None:
    if not isinstance(metadata, Mapping):
        return None
    raw = str(metadata.get("provider_slug") or "").strip().lower()
    return raw or None


def _cli_auth_volume_flags(*, provider_slug: str | None = None) -> list[str]:
    """Return docker -v flags for CLI auth files that exist on the host."""
    home = _cli_auth_home()
    probe_homes = _cli_auth_probe_homes()
    flags: list[str] = []
    normalized_provider = str(provider_slug or "").strip().lower()
    for providers, rel_path, container_path in _CLI_AUTH_MOUNTS:
        if normalized_provider and normalized_provider not in providers:
            continue
        host_path = os.path.join(home, rel_path)
        if any(os.path.isfile(os.path.join(probe_home, rel_path)) for probe_home in probe_homes):
            flags.extend(["-v", f"{host_path}:{container_path}:ro"])
    return flags


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    """Control-plane-owned workspace materialization input."""

    source_root: str
    materialization: str = "copy"
    workspace_snapshot_ref: str = ""
    overlay_files: tuple[dict[str, str], ...] = ()


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
    workspace_snapshot_ref: str = ""
    workspace_snapshot_cache_hit: bool = False


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
    workspace_snapshot_ref: str = ""
    workspace_snapshot_cache_hit: bool = False
    container_cpu_percent: float | None = None
    container_mem_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshotArchive:
    """Host-side cached archive for one workspace snapshot."""

    workspace_snapshot_ref: str
    archive_path: str
    hydrated_files: int
    cache_hit: bool


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
    image, _metadata = resolve_docker_image(
        requested_image=None,
        image_exists=_docker_image_available,
    )
    return image


def _docker_memory(metadata: Mapping[str, Any] | None = None) -> str:
    if isinstance(metadata, Mapping):
        configured = str(metadata.get("docker_memory") or "").strip()
        if configured:
            return configured
    return os.environ.get(_DOCKER_MEMORY_ENV, "4g")


def _docker_cpus(metadata: Mapping[str, Any] | None = None) -> str:
    if isinstance(metadata, Mapping):
        configured = str(metadata.get("docker_cpus") or "").strip()
        if configured:
            return configured
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


def _normalize_workspace_overlay_files(
    value: object | None,
) -> tuple[dict[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise RuntimeError("workspace_overlays must be a sequence of overlay records")
    normalized: list[dict[str, str]] = []
    seen_paths: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise RuntimeError(f"workspace_overlays[{index}] must be an object")
        relpath = _normalize_relative_path(
            item.get("relative_path"),
            field_name=f"workspace_overlays[{index}].relative_path",
        )
        content = item.get("content")
        if not isinstance(content, str):
            raise RuntimeError(f"workspace_overlays[{index}].content must be a string")
        if relpath in seen_paths:
            continue
        seen_paths.add(relpath)
        normalized.append(
            {
                "relative_path": relpath,
                "content": content,
            }
        )
    return tuple(normalized)


def _workspace_snapshot_ref(
    root: str,
    *,
    overlay_files: Sequence[Mapping[str, str]] | None = None,
) -> str:
    """Return a stable content-addressed ref for one hydrated workspace input."""
    entries: list[tuple[str, str]] = []
    root_path = Path(root)
    normalized_overlays = _normalize_workspace_overlay_files(overlay_files)
    overlay_paths = {overlay["relative_path"] for overlay in normalized_overlays}
    if root_path.exists():
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [name for name in dirnames if name not in _IGNORED_MANIFEST_DIRS]
            current_dir = Path(dirpath)
            for filename in filenames:
                absolute = current_dir / filename
                relpath = absolute.relative_to(root_path).as_posix()
                if relpath in overlay_paths:
                    continue
                try:
                    content_hash = hashlib.sha256(absolute.read_bytes()).hexdigest()
                except OSError as exc:
                    raise RuntimeError(
                        f"workspace snapshot fingerprint could not read {absolute}"
                    ) from exc
                entries.append((relpath, content_hash))
    for overlay in normalized_overlays:
        entries.append(
            (
                overlay["relative_path"],
                hashlib.sha256(overlay["content"].encode("utf-8")).hexdigest(),
            )
        )
    entries.sort(key=lambda item: item[0])
    canonical = json.dumps(entries, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"workspace_snapshot:{digest}"


def _workspace_snapshot_cache_root() -> str:
    configured = os.environ.get(_SNAPSHOT_CACHE_ROOT_ENV, "").strip()
    if configured:
        return os.path.realpath(configured)
    return os.path.realpath(os.path.join(tempfile.gettempdir(), "praxis-workspace-snapshots"))


def _workspace_snapshot_cache_dir(workspace_snapshot_ref: str) -> str:
    digest = hashlib.sha1(workspace_snapshot_ref.encode("utf-8")).hexdigest()[:16]
    return os.path.join(_workspace_snapshot_cache_root(), digest)


def _read_snapshot_archive_metadata(metadata_path: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_workspace_snapshot_archive(
    source_root: str,
    archive_path: str,
    *,
    overlay_files: Sequence[Mapping[str, str]] | None = None,
) -> int:
    source = Path(source_root)
    if not source.exists():
        raise RuntimeError(f"workspace snapshot source root is missing: {source_root}")

    archive_target = Path(archive_path)
    archive_target.parent.mkdir(parents=True, exist_ok=True)
    hydrated_files = 0
    normalized_overlays = _normalize_workspace_overlay_files(overlay_files)
    overlay_paths = {overlay["relative_path"] for overlay in normalized_overlays}
    with tarfile.open(archive_target, mode="w:gz") as archive:
        archive.add(source_root, arcname="workspace", recursive=False)
        for dirpath, dirnames, filenames in os.walk(source_root):
            dirnames[:] = sorted(name for name in dirnames if name not in _IGNORED_MANIFEST_DIRS)
            filenames = sorted(filenames)
            current_dir = Path(dirpath)
            relative_dir = current_dir.relative_to(source)
            if relative_dir != Path("."):
                archive.add(
                    str(current_dir),
                    arcname=str(PurePosixPath("workspace") / relative_dir.as_posix()),
                    recursive=False,
                )
            for filename in filenames:
                absolute = current_dir / filename
                relpath = absolute.relative_to(source).as_posix()
                if relpath in overlay_paths:
                    continue
                archive.add(
                    str(absolute),
                    arcname=str(PurePosixPath("workspace") / relpath),
                    recursive=False,
                )
                hydrated_files += 1
        for overlay in normalized_overlays:
            relative_path = overlay["relative_path"]
            content_bytes = overlay["content"].encode("utf-8")
            tar_info = tarfile.TarInfo(
                name=str(PurePosixPath("workspace") / relative_path)
            )
            tar_info.size = len(content_bytes)
            tar_info.mtime = int(time.time())
            tar_info.mode = 0o644
            archive.addfile(tar_info, io.BytesIO(content_bytes))
            hydrated_files += 1
    return hydrated_files


def _cached_workspace_snapshot_archive(snapshot: WorkspaceSnapshot) -> WorkspaceSnapshotArchive:
    snapshot_ref = str(
        getattr(snapshot, "workspace_snapshot_ref", "")
        or _workspace_snapshot_ref(
            snapshot.source_root,
            overlay_files=getattr(snapshot, "overlay_files", ()),
        )
    ).strip()
    if not snapshot_ref:
        raise RuntimeError("workspace_snapshot_ref must be resolved before hydration")

    cache_dir = Path(_workspace_snapshot_cache_dir(snapshot_ref))
    archive_path = cache_dir / "workspace.tar.gz"
    metadata_path = cache_dir / "metadata.json"

    metadata = _read_snapshot_archive_metadata(str(metadata_path))
    if archive_path.is_file() and metadata is not None:
        return WorkspaceSnapshotArchive(
            workspace_snapshot_ref=snapshot_ref,
            archive_path=str(archive_path),
            hydrated_files=int(metadata.get("hydrated_files") or 0),
            cache_hit=True,
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    temp_archive = cache_dir / f".{uuid4().hex}.tar.gz"
    temp_metadata = cache_dir / f".{uuid4().hex}.json"
    hydrated_files = _write_workspace_snapshot_archive(
        snapshot.source_root,
        str(temp_archive),
        overlay_files=getattr(snapshot, "overlay_files", ()),
    )
    temp_metadata.write_text(
        json.dumps(
            {
                "workspace_snapshot_ref": snapshot_ref,
                "hydrated_files": hydrated_files,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    os.replace(temp_archive, archive_path)
    os.replace(temp_metadata, metadata_path)
    return WorkspaceSnapshotArchive(
        workspace_snapshot_ref=snapshot_ref,
        archive_path=str(archive_path),
        hydrated_files=hydrated_files,
        cache_hit=False,
    )


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


def _dehydrate_copy(
    workspace_root: str, host_root: str, artifact_refs: Sequence[str]
) -> int:
    """Copy changed files from the sandbox workspace back to the host repo.

    Run after successful execution. Only paths listed in artifact_refs
    are copied — caller is responsible for filtering to write_scope.
    """
    copied = 0
    workspace = Path(workspace_root)
    host = Path(host_root)
    for relpath in artifact_refs:
        src = workspace / relpath
        if not src.is_file():
            continue
        dst = host / relpath
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1
    return copied


def _write_text_artifact(path: str, content: str) -> None:
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(content, encoding="utf-8")




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
        cached_snapshot = _cached_workspace_snapshot_archive(snapshot)
        workspace_root = Path(session.workspace_root)
        shutil.rmtree(workspace_root, ignore_errors=True)
        workspace_root.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(cached_snapshot.archive_path, mode="r:gz") as archive:
            archive.extractall(workspace_root.parent)
        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=cached_snapshot.hydrated_files,
            workspace_materialization=snapshot.materialization,
            workspace_snapshot_ref=cached_snapshot.workspace_snapshot_ref,
            workspace_snapshot_cache_hit=cached_snapshot.cache_hit,
        )

    def exec(self, session: SandboxSession, request: SandboxExecRequest) -> SandboxExecutionResult:
        docker_image, image_meta = resolve_docker_image(
            requested_image=request.image,
            image_exists=_docker_image_available,
        )
        if not _docker_image_available(docker_image):
            build_hint = f" Build or configure {DOCKER_IMAGE_ENV} before sandbox execution."
            if image_meta.get("source") == "default":
                build_hint = (
                    " Praxis auto-build also failed."
                    if image_meta.get("build_error")
                    else " Build or configure PRAXIS_DOCKER_IMAGE before sandbox execution."
                )
            detail = str(image_meta.get("build_error") or "").strip()
            raise RuntimeError(
                "docker_local requires image "
                f"{docker_image!r}.{build_hint}"
                + (f" {detail}" if detail else "")
            )
        container_name = f"praxis-{uuid4().hex[:12]}"
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--name", container_name,
            "--memory",
            _docker_memory(session.metadata),
            "--cpus",
            _docker_cpus(session.metadata),
            "--workdir",
            "/workspace",
            "-v",
            f"{session.workspace_root}:/workspace",
        ]
        auth_mount_policy = validate_auth_mount_policy(
            session.metadata.get("auth_mount_policy") or "provider_scoped"
        )
        if auth_mount_policy != "none":
            provider_slug = (
                _provider_slug(session.metadata)
                if auth_mount_policy == "provider_scoped"
                else None
            )
            docker_cmd.extend(
                _cli_auth_volume_flags(provider_slug=provider_slug)
            )
        for key, value in sorted(request.env.items()):
            docker_cmd.extend(["-e", f"{key}={value}"])
        if session.network_policy == "disabled":
            docker_cmd.append("--network=none")
        docker_cmd.extend([docker_image, "bash", "-lc", request.command])

        # Background thread: poll docker stats every 2s to capture peak CPU/memory.
        peak_cpu: list[float] = [0.0]
        peak_mem: list[int] = [0]
        stats_stop = threading.Event()

        def _poll_stats() -> None:
            while not stats_stop.is_set():
                try:
                    sr = subprocess.run(
                        ["docker", "stats", "--no-stream", "--format",
                         "{{.CPUPerc}}\t{{.MemUsage}}", container_name],
                        capture_output=True, text=True, timeout=5,
                    )
                    if sr.returncode == 0 and sr.stdout.strip():
                        cpu_part, _, mem_part = sr.stdout.strip().partition("\t")
                        cpu = float(cpu_part.rstrip("%") or 0)
                        mem = _parse_docker_mem_str(mem_part.split("/")[0])
                        if cpu > peak_cpu[0]:
                            peak_cpu[0] = cpu
                        if mem > peak_mem[0]:
                            peak_mem[0] = mem
                except Exception:
                    pass
                stats_stop.wait(timeout=2.0)

        stats_thread = threading.Thread(target=_poll_stats, daemon=True, name=f"docker-stats-{container_name}")
        stats_thread.start()

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
        finally:
            stats_stop.set()
            stats_thread.join(timeout=3.0)

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
            workspace_snapshot_ref="",
            workspace_snapshot_cache_hit=False,
            container_cpu_percent=peak_cpu[0] if peak_cpu[0] > 0 else None,
            container_mem_bytes=peak_mem[0] if peak_mem[0] > 0 else None,
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
        cached_snapshot = _cached_workspace_snapshot_archive(snapshot)
        payload = {
            "archive_base64": base64.b64encode(Path(cached_snapshot.archive_path).read_bytes()).decode("ascii"),
            "workspace_materialization": snapshot.materialization,
            "workspace_snapshot_ref": cached_snapshot.workspace_snapshot_ref,
        }
        response = self._request(f"/sessions/{session.provider_session_id}/hydrate", payload)
        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=int(response.get("hydrated_files") or cached_snapshot.hydrated_files),
            workspace_materialization=snapshot.materialization,
            workspace_snapshot_ref=str(
                response.get("workspace_snapshot_ref") or cached_snapshot.workspace_snapshot_ref
            ),
            workspace_snapshot_cache_hit=cached_snapshot.cache_hit,
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
            artifact_refs=_normalize_relative_paths(
                response.get("artifact_refs") or (),
                field_name="artifact_ref",
            ),
            started_at=str(response.get("started_at") or _iso_now()),
            finished_at=str(response.get("finished_at") or _iso_now()),
            network_policy=session.network_policy,
            provider_latency_ms=int(response.get("provider_latency_ms") or 0),
            execution_mode=self.provider_name,
            workspace_root=session.workspace_root,
            workspace_snapshot_ref="",
            workspace_snapshot_cache_hit=False,
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
        artifact_refs = _normalize_relative_paths(
            response.get("artifact_refs") or (),
            field_name="artifact_ref",
        )
        artifacts_payload = response.get("artifacts")
        if isinstance(artifacts_payload, list):
            synced_refs: list[str] = []
            for artifact in artifacts_payload:
                if not isinstance(artifact, dict):
                    continue
                raw_path = artifact.get("path")
                if raw_path is None:
                    continue
                relpath = _normalize_relative_path(raw_path, field_name="artifact.path")
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
        provider_metadata = dict(metadata or {})
        write_scope = _execution_write_scope(provider_metadata)
        session = provider.create_session(
            SandboxSessionSpec(
                sandbox_session_id=sandbox_session_id,
                sandbox_group_id=sandbox_group_id,
                provider=provider_name,
                workdir=workdir,
                network_policy=network_policy,
                workspace_materialization=workspace_materialization,
                timeout_seconds=timeout_seconds,
                metadata=provider_metadata,
            )
        )
        disposition = "completed"
        try:
            workspace_overlays = _normalize_workspace_overlay_files(
                provider_metadata.get("workspace_overlays")
            )
            workspace_snapshot_ref = _workspace_snapshot_ref(
                workdir,
                overlay_files=workspace_overlays,
            )
            hydration_receipt = provider.hydrate_workspace(
                session,
                WorkspaceSnapshot(
                    source_root=workdir,
                    materialization=workspace_materialization,
                    workspace_snapshot_ref=workspace_snapshot_ref,
                    overlay_files=workspace_overlays,
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
            artifact_refs = _validated_artifact_refs(
                artifact_receipt.artifact_refs,
                write_scope=write_scope,
            )
            # Dehydrate: copy changed files from sandbox back to host workdir.
            # Without this, agent-produced files exist only in the ephemeral
            # container workspace and never reach the host repo.
            if artifact_refs and getattr(provider, "execution_lane", "") == "local":
                _dehydrate_copy(session.workspace_root, workdir, artifact_refs)
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
                        f"{provider.provider_name} returned artifact refs without synced content: "
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
                workspace_snapshot_ref=(
                    hydration_receipt.workspace_snapshot_ref
                    or workspace_snapshot_ref
                    or result.workspace_snapshot_ref
                ),
                workspace_snapshot_cache_hit=(
                    hydration_receipt.workspace_snapshot_cache_hit
                    or result.workspace_snapshot_cache_hit
                ),
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
    identity_payload: Mapping[str, Any] | None = None,
) -> tuple[str, str | None]:
    bundle = execution_bundle if isinstance(execution_bundle, dict) else {}
    run_id = str(bundle.get("run_id") or "").strip()
    job_label = str(bundle.get("job_label") or "").strip()
    if run_id:
        suffix = job_label or execution_transport
        return f"sandbox_session:{run_id}:{suffix}", f"group:{run_id}"
    identity_seed: dict[str, Any] = {
        "workdir": os.path.realpath(workdir),
        "execution_transport": str(execution_transport or "").strip(),
        "execution_bundle": bundle,
    }
    if isinstance(identity_payload, Mapping):
        identity_seed["request"] = dict(identity_payload)
    elif identity_payload is not None:
        identity_seed["request"] = str(identity_payload)
    canonical_seed = json.dumps(
        identity_seed,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha1(canonical_seed.encode("utf-8")).hexdigest()[:16]
    return f"sandbox_session:adhoc:{digest}", None
