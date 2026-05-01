"""Provider-agnostic sandbox lifecycle runtime."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from posixpath import normpath
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from uuid import uuid4
from pathlib import PurePosixPath

from .docker_image_authority import DOCKER_IMAGE_ENV, resolve_docker_image


_STATS_THREAD_CLASS = threading.Thread
from runtime.workspace_paths import (
    container_auth_seed_dir,
    container_home,
    container_workspace_root,
)
from runtime.workflow.execution_policy import validate_auth_mount_policy

_DOCKER_MEMORY_ENV = "PRAXIS_DOCKER_MEMORY"
_DOCKER_CPUS_ENV = "PRAXIS_DOCKER_CPUS"
_CLI_AUTH_HOME_ENV = "PRAXIS_CLI_AUTH_HOME"
_SNAPSHOT_CACHE_ROOT_ENV = "PRAXIS_SANDBOX_SNAPSHOT_CACHE_DIR"
_ALLOW_LEGACY_WORKSPACE_COPY_ENV = "PRAXIS_ALLOW_LEGACY_WORKSPACE_COPY"


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
_SANDBOX_HOME = Path(os.environ.get("PRAXIS_SANDBOX_HOME") or container_home()).expanduser()
_CONTAINER_WORKSPACE_ROOT = str(container_workspace_root())
# Fallback-only ignore set, used when the source root isn't a git checkout.
# For git checkouts, `_workspace_file_entries` uses `git ls-files` to honor
# `.gitignore`, `.git/info/exclude`, and `core.excludesFile` — so anything a
# developer already told git to ignore (node_modules, .venv, postgres-dev/data,
# build outputs, etc.) is auto-excluded from workspace hydration. This set
# only covers the narrow case of non-git source roots; keep it minimal.
_IGNORED_MANIFEST_DIRS = frozenset({".git", "__pycache__", ".pytest_cache", ".mypy_cache"})
_EMPTY_WORKSPACE_MATERIALIZATION = "none"
_CLI_AGENT_USER = "praxis-agent"
_CLI_AGENT_UID = 1100
_CLI_AGENT_GID = 1100
_OPENAI_AUTH_SEED_PATH = str(container_auth_seed_dir() / "openai-auth.json")
_GOOGLE_AUTH_SEED_PATH = str(container_auth_seed_dir() / "google-gemini-oauth_creds.json")


@dataclass(frozen=True, slots=True)
class _CliAuthMountSpec:
    provider_slug: str
    host_relative_path: str
    container_path: str


@dataclass(frozen=True, slots=True)
class _CliAuthCatalog:
    mount_specs: tuple[_CliAuthMountSpec, ...]
    home_tmpfs_dirs: tuple[str, ...]


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


def _execution_write_scope_entries(metadata: Mapping[str, Any] | None) -> tuple[tuple[str, bool], ...]:
    if not isinstance(metadata, Mapping):
        return ()
    execution_bundle = metadata.get("execution_bundle")
    if not isinstance(execution_bundle, Mapping):
        return ()
    access_policy = execution_bundle.get("access_policy")
    if not isinstance(access_policy, Mapping):
        return ()
    raw_scope = access_policy.get("write_scope")
    if raw_scope is None:
        return ()
    raw_values = [raw_scope] if isinstance(raw_scope, (str, Path)) else raw_scope
    if not isinstance(raw_values, Sequence) or isinstance(raw_values, (str, bytes, bytearray)):
        return ()
    entries: list[tuple[str, bool]] = []
    for value in raw_values:
        raw_text = str(value or "").strip()
        if not raw_text:
            continue
        normalized_path = _normalize_relative_path(
            raw_text,
            field_name="execution_bundle.access_policy.write_scope",
        )
        is_directory = raw_text.endswith("/")
        entry = (normalized_path, is_directory)
        if entry not in entries:
            entries.append(entry)
    return tuple(entries)


def _chmod_workspace(path: str | Path, mode_expr: str) -> None:
    try:
        subprocess.run(
            ["chmod", "-R", mode_expr, str(path)],
            check=False,
            capture_output=True,
            timeout=30,
        )
    except Exception:
        pass


def _make_workspace_writable(workspace_root: str) -> None:
    _chmod_workspace(workspace_root, "a+rwX")


def _prepare_workspace_write_scope(
    workspace_root: str,
    write_scope_entries: Sequence[tuple[str, bool]],
) -> None:
    root = Path(workspace_root)
    if not root.exists():
        return
    for relpath, is_directory in write_scope_entries:
        target = root / relpath
        if is_directory or target.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch(exist_ok=True)

    _chmod_workspace(root, "a-w,a+rX")
    for relpath, is_directory in write_scope_entries:
        target = root / relpath
        if is_directory or target.is_dir():
            _chmod_workspace(target, "a+rwX")
        else:
            try:
                target.chmod(0o666)
            except OSError:
                pass


def _submission_required(metadata: Mapping[str, Any] | None) -> bool:
    # Determines whether this job runs under the sealed-submission contract.
    # Under that contract, the sandbox filesystem is ephemeral scratch and the
    # authoritative deliverable is the workflow_job_submissions row. Scope
    # drift is still captured as structured evidence and elevated to job
    # failure by the execution envelope.
    if not isinstance(metadata, Mapping):
        return False
    execution_bundle = metadata.get("execution_bundle")
    if not isinstance(execution_bundle, Mapping):
        return False
    contract = execution_bundle.get("completion_contract")
    if not isinstance(contract, Mapping):
        return False
    return bool(contract.get("submission_required"))


def _authority_binding_blocked_paths(
    metadata: Mapping[str, Any] | None,
) -> tuple[Mapping[str, Any], ...]:
    """Extract source-path entries from authority_binding.blocked_compat_units.

    Each entry in the result has the predecessor path the worker must NOT
    extend, plus the canonical successor info so the refusal message can
    point the agent at the correct write target. The resolver only adds
    `source_path`-kind predecessors to blocked_compat_units; that is
    exactly what filesystem write enforcement can act on.
    """

    if not isinstance(metadata, Mapping):
        return ()
    binding = metadata.get("authority_binding")
    if not isinstance(binding, Mapping):
        return ()
    blocked_raw = binding.get("blocked_compat_units")
    if not isinstance(blocked_raw, list):
        return ()
    out: list[Mapping[str, Any]] = []
    for entry in blocked_raw:
        if not isinstance(entry, Mapping):
            continue
        unit_kind = str(
            entry.get("predecessor_unit_kind") or entry.get("unit_kind") or ""
        ).strip().lower()
        unit_ref = str(
            entry.get("predecessor_unit_ref") or entry.get("unit_ref") or ""
        ).strip()
        if unit_kind != "source_path" or not unit_ref:
            continue
        out.append(
            {
                "blocked_path": unit_ref,
                "successor_unit_kind": entry.get("successor_unit_kind"),
                "successor_unit_ref": entry.get("successor_unit_ref"),
                "supersession_status": entry.get("supersession_status"),
                "obligation_summary": entry.get("obligation_summary"),
            }
        )
    return tuple(out)


def _validated_blocked_compat_refs(
    artifact_refs: Sequence[str],
    *,
    blocked_compat: tuple[Mapping[str, Any], ...],
    submission_required: bool,
) -> tuple[Mapping[str, Any], ...]:
    """Reject artifact refs that fall inside any blocked compat path.

    Drift records share the structured shape of write-scope drift so a
    single ingest path consumes both, distinguished by the `reason` field.
    Submission contract: drift captured but not raised. Legacy contract:
    raise so the worker stops before promoting predecessor extensions.
    """

    if not blocked_compat:
        return ()
    drift: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for raw_ref in artifact_refs or ():
        if raw_ref is None or not str(raw_ref).strip():
            continue
        ref = _normalize_relative_path(raw_ref, field_name="artifact_ref")
        if not ref or ref in seen:
            continue
        for entry in blocked_compat:
            blocked_path = _normalize_relative_path(
                str(entry["blocked_path"]),
                field_name="blocked_compat_path",
            )
            if not blocked_path:
                continue
            prefix = blocked_path.rstrip("/")
            hits = ref == blocked_path or (prefix and ref.startswith(prefix + "/"))
            if not hits:
                continue
            seen.add(ref)
            drift.append(
                {
                    "artifact_ref": ref,
                    "reason": "blocked_compat_path_extension",
                    "blocked_predecessor_path": blocked_path,
                    "canonical_successor": {
                        "unit_kind": entry.get("successor_unit_kind"),
                        "unit_ref": entry.get("successor_unit_ref"),
                    },
                    "supersession_status": entry.get("supersession_status"),
                    "obligation_summary": entry.get("obligation_summary"),
                    "guidance": "do_not_imitate__preserve_tested_invariants__write_to_canonical_successor",
                    "submission_required": submission_required,
                }
            )
            break
    if not drift:
        return ()
    if submission_required:
        return tuple(drift)
    blocked_refs = ", ".join(d["artifact_ref"] for d in drift)
    successors = ", ".join(
        f"{d['canonical_successor'].get('unit_kind')}:{d['canonical_successor'].get('unit_ref')}"
        for d in drift
    )
    raise RuntimeError(
        "sandbox attempted to extend blocked compat predecessor paths: "
        f"{blocked_refs} — write to canonical successor instead ({successors})."
    )


def _validated_artifact_refs(
    artifact_refs: Sequence[str],
    *,
    write_scope: tuple[str, ...] | None,
    submission_required: bool,
) -> tuple[tuple[str, ...], tuple[Mapping[str, Any], ...]]:
    """Normalize artifact refs and produce a structured drift record.

    Returns (normalized_refs, drift_records). For submission-contract jobs,
    drift is captured but not enforced — the sealed submission is the
    authoritative deliverable. For legacy jobs, drift raises as before.

    Drift records have a fixed shape so downstream ingesters (verifier
    worker, drift tracker, shard-suggestion feedback) can consume them
    without reparsing stderr:

        {
            "artifact_ref": "<relative path>",
            "reason": "outside_write_scope",
            "declared_write_scope": ("<path>", ...),
            "submission_required": True,
        }
    """
    normalized_refs = _normalize_relative_paths(artifact_refs, field_name="artifact_ref")

    # Short-circuit when no enforcement surface exists.
    if write_scope is None or len(write_scope) == 0:
        return normalized_refs, ()

    out_of_scope_refs = tuple(
        ref for ref in normalized_refs if not _scope_allows_path(ref, write_scope)
    )

    if not out_of_scope_refs:
        return normalized_refs, ()

    # Structured drift record — same shape regardless of enforcement mode.
    drift_records = tuple(
        {
            "artifact_ref": ref,
            "reason": "outside_write_scope",
            "declared_write_scope": tuple(write_scope),
            "submission_required": submission_required,
        }
        for ref in out_of_scope_refs
    )

    # Submission contract: drift is captured for the receipt path. The workflow
    # execution envelope marks the job failed instead of promoting the scratch
    # artifact.
    if submission_required:
        return normalized_refs, drift_records

    # Legacy contract: filesystem write_scope is authority; reject hard.
    raise RuntimeError(
        "sandbox produced artifacts outside declared write_scope: "
        + ", ".join(out_of_scope_refs)
    )


def _provider_slug(metadata: Mapping[str, Any] | None) -> str | None:
    if not isinstance(metadata, Mapping):
        return None
    raw = str(metadata.get("provider_slug") or "").strip().lower()
    return raw or None


def _jsonb_array(value: object) -> tuple[object, ...]:
    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return ()
    if not isinstance(parsed, Sequence) or isinstance(parsed, (str, bytes, bytearray)):
        return ()
    return tuple(parsed)


def _container_auth_mount_path(payload: Mapping[str, object]) -> str | None:
    seed_name = str(payload.get("container_seed_filename") or "").strip()
    if seed_name:
        return str(container_auth_seed_dir() / _normalize_relative_path(seed_name, field_name="container_seed_filename"))
    relative_path = str(payload.get("container_relative_path") or "").strip()
    if relative_path:
        return str(_SANDBOX_HOME / _normalize_relative_path(relative_path, field_name="container_relative_path"))
    return None


def _auth_catalog_from_rows(rows: Sequence[Mapping[str, object]]) -> _CliAuthCatalog:
    mount_specs: list[_CliAuthMountSpec] = []
    tmpfs_dirs: list[str] = []
    for row in rows:
        provider_slug = str(row.get("provider_slug") or "").strip().lower()
        if not provider_slug:
            continue
        for raw_dir in _jsonb_array(row.get("cli_home_tmpfs_dirs")):
            try:
                tmpfs_dir = _normalize_relative_path(raw_dir, field_name="cli_home_tmpfs_dirs")
            except RuntimeError:
                continue
            if tmpfs_dir not in tmpfs_dirs:
                tmpfs_dirs.append(tmpfs_dir)
        for raw_mount in _jsonb_array(row.get("auth_mounts")):
            if not isinstance(raw_mount, Mapping):
                continue
            try:
                host_relative_path = _normalize_relative_path(
                    raw_mount.get("host_relative_path"),
                    field_name="auth_mounts.host_relative_path",
                )
                container_path = _container_auth_mount_path(raw_mount)
            except RuntimeError:
                continue
            if not container_path:
                continue
            mount_specs.append(
                _CliAuthMountSpec(
                    provider_slug=provider_slug,
                    host_relative_path=host_relative_path,
                    container_path=container_path,
                )
            )
    return _CliAuthCatalog(
        mount_specs=tuple(mount_specs),
        home_tmpfs_dirs=tuple(tmpfs_dirs),
    )


def _load_cli_auth_catalog() -> _CliAuthCatalog:
    """Load provider CLI auth mount authority from Postgres.

    Do not process-cache this. Provider onboarding can add or repair CLI
    auth mounts while workers are already running; a stale in-process catalog
    makes the next sandbox launch omit real credentials even though the DB
    authority has been fixed.
    """
    try:
        from runtime._workflow_database import resolve_runtime_database_url
        from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

        database_url = resolve_runtime_database_url(required=False)
        if not database_url:
            return _CliAuthCatalog((), ())
        conn = SyncPostgresConnection(
            get_workflow_pool(env={"WORKFLOW_DATABASE_URL": str(database_url)})
        )
        rows = conn.execute(
            """
            SELECT
                provider_slug,
                probe_contract -> 'auth_mounts' AS auth_mounts,
                probe_contract -> 'cli_home_tmpfs_dirs' AS cli_home_tmpfs_dirs
            FROM provider_transport_admissions
            WHERE adapter_type = 'cli_llm'
              AND status = 'active'
              AND admitted_by_policy IS TRUE
              AND probe_contract ? 'auth_mounts'
            ORDER BY provider_slug
            """
        )
    except Exception:
        return _CliAuthCatalog((), ())
    return _auth_catalog_from_rows(tuple(dict(row) for row in rows or ()))


def _cli_auth_volume_flags(*, provider_slug: str | None = None) -> list[str]:
    """Return docker -v flags for CLI auth files that exist on the host."""
    home = _cli_auth_home()
    probe_homes = _cli_auth_probe_homes()
    flags: list[str] = []
    normalized_provider = str(provider_slug or "").strip().lower()
    for spec in _load_cli_auth_catalog().mount_specs:
        if normalized_provider and normalized_provider != spec.provider_slug:
            continue
        # Claude Code still rewrites ~/.claude.json at runtime even on builds
        # where docs describe it as deprecated. Mounting the host file :ro makes
        # the CLI back it up, lose the live path, and hang before auth
        # completes. Treat OAuth env/token or Linux-only credentials.json as the
        # Anthropic auth authority instead of the legacy config file.
        if spec.provider_slug == "anthropic" and spec.host_relative_path == ".claude.json":
            continue
        host_path = os.path.join(home, spec.host_relative_path)
        container_path = spec.container_path
        # Gemini rewrites oauth_creds.json during normal CLI startup. Mount the
        # host file as a read-only seed, then copy it into writable tmpfs during
        # bootstrap instead of overlaying the live target with a read-only file.
        if spec.provider_slug == "google" and spec.host_relative_path == ".gemini/oauth_creds.json":
            container_path = _GOOGLE_AUTH_SEED_PATH
        if any(os.path.isfile(os.path.join(probe_home, spec.host_relative_path)) for probe_home in probe_homes):
            flags.extend(["-v", f"{host_path}:{container_path}:ro"])
    return flags


def _cli_home_tmpfs_flags(*, uid: int = _CLI_AGENT_UID, gid: int = _CLI_AGENT_GID) -> list[str]:
    """Return writable CLI home dirs for non-root Docker CLI execution."""
    flags: list[str] = []
    for home_subdir in _load_cli_auth_catalog().home_tmpfs_dirs:
        flags.extend(
            [
                "--tmpfs",
                f"{_SANDBOX_HOME}/{home_subdir}:uid={uid},gid={gid},mode=755",
            ]
        )
    return flags


def _cli_requires_root_auth_bootstrap(
    *, provider_slug: str | None, auth_mount_policy: str, requested_user: str | None
) -> bool:
    """Some CLIs need root only long enough to copy host auth seeds."""
    normalized_provider = str(provider_slug or "").strip().lower()
    normalized_policy = str(auth_mount_policy or "").strip().lower()
    return (
        normalized_provider in {"openai", "google"}
        and normalized_policy != "none"
        and bool(requested_user)
    )


def _cli_auth_bootstrap_command(command: str, *, provider_slug: str | None) -> str:
    """Copy root-readable provider auth into the agent home, then drop privileges."""
    normalized_provider = str(provider_slug or "").strip().lower()
    if normalized_provider == "openai":
        seed_path = _OPENAI_AUTH_SEED_PATH
        auth_dir = str(_SANDBOX_HOME / ".codex")
        auth_target = str(_SANDBOX_HOME / ".codex" / "auth.json")
    elif normalized_provider == "google":
        seed_path = _GOOGLE_AUTH_SEED_PATH
        auth_dir = str(_SANDBOX_HOME / ".gemini")
        auth_target = str(_SANDBOX_HOME / ".gemini" / "oauth_creds.json")
    else:
        return command

    quoted_command = shlex.quote(command)
    fallback_command = shlex.quote(f"HOME={shlex.quote(str(_SANDBOX_HOME))} bash -lc {quoted_command}")
    return (
        "set -e; "
        f"if [ -f {shlex.quote(seed_path)} ]; then "
        f"mkdir -p {shlex.quote(auth_dir)}; "
        f"cp {shlex.quote(seed_path)} {shlex.quote(auth_target)}; "
        f"chown {_CLI_AGENT_UID}:{_CLI_AGENT_GID} "
        f"{shlex.quote(auth_dir)} {shlex.quote(auth_target)}; "
        f"chmod 600 {shlex.quote(auth_target)}; "
        "fi; "
        "if command -v setpriv >/dev/null 2>&1; then "
        f"exec setpriv --reuid={_CLI_AGENT_UID} --regid={_CLI_AGENT_GID} "
        f"--init-groups env HOME={shlex.quote(str(_SANDBOX_HOME))} bash -lc {quoted_command}; "
        "fi; "
        f"exec su -s /bin/bash {_CLI_AGENT_USER} -c {fallback_command}"
    )


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    """Control-plane-owned workspace materialization input.

    path_filter, when non-empty, scopes hydration to files whose workspace-
    relative path matches at least one entry (exact match or fnmatch-glob).
    This is the shard-materialization lever: when the bundle's access_policy
    declares a resolved_read_scope / write_scope / test_scope / blast_radius,
    execute_command unions them into path_filter so the sandbox only sees that
    shard — not the whole repo. Empty filter (default) preserves legacy
    full-workspace copy behavior.
    """

    source_root: str
    materialization: str = "copy"
    workspace_snapshot_ref: str = ""
    overlay_files: tuple[dict[str, str], ...] = ()
    path_filter: tuple[str, ...] = ()


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
    hydrated_paths: tuple[str, ...] = ()


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
    """One command execution within an existing sandbox session.

    agent_slug flows to resolve_docker_image so it can dispatch to the
    per-agent-family thin image when no explicit image override is set.
    """

    command: str
    stdin_text: str
    env: dict[str, str]
    timeout_seconds: int
    execution_transport: str
    image: str | None = None
    agent_slug: str | None = None


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
    # Scratch-drift record for submission-contract jobs. When an agent writes
    # files outside the declared write_scope, the sandbox reports the finding
    # here so the execution envelope can fail the job and the receipt can carry
    # the exact out-of-scope artifact evidence.
    artifact_scope_drift: tuple[Mapping[str, Any], ...] = ()
    workspace_manifest_audit: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshotArchive:
    """Host-side cached archive for one workspace snapshot."""

    workspace_snapshot_ref: str
    archive_path: str
    hydrated_files: int
    cache_hit: bool
    hydrated_paths: tuple[str, ...] = ()


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


_DOCKER_PROBE_RETRY_ATTEMPTS = 3
_DOCKER_PROBE_RETRY_DELAY_SECONDS = 2.0


def _docker_available() -> bool:
    last_returncode: int | None = None
    for attempt in range(_DOCKER_PROBE_RETRY_ATTEMPTS):
        try:
            result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        except FileNotFoundError:
            return False
        except subprocess.TimeoutExpired:
            last_returncode = None
        else:
            if result.returncode == 0:
                return True
            last_returncode = result.returncode
        if attempt < _DOCKER_PROBE_RETRY_ATTEMPTS - 1:
            time.sleep(_DOCKER_PROBE_RETRY_DELAY_SECONDS)
    return last_returncode == 0


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
    return os.environ.get(_DOCKER_MEMORY_ENV, "500m")


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
    for relpath, absolute in _workspace_file_entries(root):
        try:
            stat = absolute.stat()
        except OSError:
            continue
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


def _git_tracked_relpaths(root: str) -> list[str] | None:
    """Return relative POSIX paths for files git considers part of the workspace.

    Uses `git ls-files --cached --others --exclude-standard` — that's
    tracked files plus untracked files not matched by `.gitignore`,
    `.git/info/exclude`, or `core.excludesFile`. Returns None if the
    root isn't inside a git checkout (caller should fall back to
    `os.walk` + `_IGNORED_MANIFEST_DIRS`).
    """
    try:
        result = subprocess.run(
            ["git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    stdout = result.stdout or b""
    entries = stdout.split(b"\0")
    paths: list[str] = []
    for raw in entries:
        if not raw:
            continue
        try:
            decoded = raw.decode("utf-8")
        except UnicodeDecodeError:
            decoded = raw.decode("utf-8", errors="surrogateescape")
        paths.append(decoded)
    return paths


def _path_matches_filter(relpath: str, path_filter: Sequence[str]) -> bool:
    """Return True if relpath matches any entry in path_filter.

    Each filter entry is either an exact relative path or an fnmatch-style
    glob (e.g. "adapters/*.py", "runtime/**/execution_*.py"). Directory
    entries admit their descendants so scoped workflow packets can declare a
    shard directory without spelling every file. Empty filter means "no
    filter" — callers should short-circuit before calling.
    """
    import fnmatch
    relpath = PurePosixPath(str(relpath).replace("\\", "/").lstrip("./")).as_posix()
    for pattern in path_filter:
        pattern = PurePosixPath(str(pattern).strip().replace("\\", "/").lstrip("./")).as_posix()
        if not pattern:
            continue
        if pattern == relpath:
            return True
        if relpath.startswith(f"{pattern.rstrip('/')}/"):
            return True
        if fnmatch.fnmatchcase(relpath, pattern):
            return True
    return False


def _workspace_file_entries(
    root: str,
    *,
    path_filter: Sequence[str] = (),
) -> list[tuple[str, Path]]:
    """Enumerate (relpath, absolute_path) tuples for all files to hydrate.

    Honors `.gitignore` when `root` is a git checkout. Falls back to
    `os.walk` with `_IGNORED_MANIFEST_DIRS` otherwise. When path_filter is
    non-empty, results are restricted to entries matching at least one
    filter pattern (exact match or fnmatch-glob).
    """
    root_path = Path(root)
    if not root_path.exists():
        return []

    normalized_filter = tuple(str(p).strip() for p in path_filter if str(p).strip())
    filter_active = bool(normalized_filter)

    tracked = _git_tracked_relpaths(root)
    if tracked is not None:
        entries: list[tuple[str, Path]] = []
        for relpath in tracked:
            relpath = PurePosixPath(relpath).as_posix()
            if filter_active and not _path_matches_filter(relpath, normalized_filter):
                continue
            absolute = root_path / relpath
            # git ls-files may report paths for files that were deleted
            # but not yet staged; skip anything that no longer exists.
            if not absolute.is_file():
                continue
            entries.append((relpath, absolute))
        return entries

    # Fallback: non-git workspace. Use the minimal hardcoded ignore set.
    entries = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in _IGNORED_MANIFEST_DIRS]
        current_dir = Path(dirpath)
        for filename in filenames:
            absolute = current_dir / filename
            relpath = absolute.relative_to(root_path).as_posix()
            if filter_active and not _path_matches_filter(relpath, normalized_filter):
                continue
            entries.append((relpath, absolute))
    return entries


def _workspace_snapshot_hydrated_paths(
    root: str,
    *,
    overlay_files: Sequence[Mapping[str, str]] | None = None,
    path_filter: Sequence[str] = (),
) -> tuple[str, ...]:
    normalized_overlays = _normalize_workspace_overlay_files(overlay_files)
    overlay_paths = {overlay["relative_path"] for overlay in normalized_overlays}
    paths = {
        relpath
        for relpath, _absolute in _workspace_file_entries(root, path_filter=path_filter)
        if relpath not in overlay_paths
    }
    paths.update(overlay_paths)
    return tuple(sorted(paths))


def _workspace_snapshot_ref(
    root: str,
    *,
    overlay_files: Sequence[Mapping[str, str]] | None = None,
    path_filter: Sequence[str] = (),
) -> str:
    """Return a stable content-addressed ref for one hydrated workspace input.

    path_filter participates in the hash so different shards of the same
    source_root produce different cache refs (no collision on archive reuse).
    """
    normalized_overlays = _normalize_workspace_overlay_files(overlay_files)
    overlay_paths = {overlay["relative_path"] for overlay in normalized_overlays}
    normalized_filter = tuple(
        sorted({str(p).strip() for p in path_filter if str(p).strip()})
    )
    entries: list[tuple[str, str]] = []
    for relpath, absolute in _workspace_file_entries(root, path_filter=normalized_filter):
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
    canonical = json.dumps(
        {"files": entries, "path_filter": list(normalized_filter)},
        separators=(",", ":"),
        ensure_ascii=True,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"workspace_snapshot:{digest}"


def _uses_empty_workspace_materialization(value: object) -> bool:
    return str(value or "").strip().lower() == _EMPTY_WORKSPACE_MATERIALIZATION


def _env_flag_enabled(name: str) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _validate_workspace_materialization_allowed(
    value: object,
    metadata: Mapping[str, Any] | None,
) -> str:
    materialization = str(value or "").strip().lower() or _EMPTY_WORKSPACE_MATERIALIZATION
    if materialization == _EMPTY_WORKSPACE_MATERIALIZATION:
        return materialization
    if _execution_shard_paths(metadata):
        return materialization
    if _env_flag_enabled(_ALLOW_LEGACY_WORKSPACE_COPY_ENV):
        return materialization
    raise RuntimeError(
        "unscoped workspace materialization is disabled by default. "
        f"Set {_ALLOW_LEGACY_WORKSPACE_COPY_ENV}=1 only for an explicit operator "
        "debug run; normal model sandboxes must use workspace_materialization=none "
        "or declare a scoped access_policy shard through resolved_read_scope, "
        "declared_read_scope, write_scope, test_scope, or blast_radius."
    )


def _execution_scope_paths(
    metadata: Mapping[str, Any] | None,
    *,
    keys: Sequence[str],
) -> tuple[str, ...]:
    """Extract selected scoped path lists from the execution bundle."""
    if not isinstance(metadata, Mapping):
        return ()
    bundle = metadata.get("execution_bundle")
    if not isinstance(bundle, Mapping):
        return ()
    access_policy = bundle.get("access_policy")
    if not isinstance(access_policy, Mapping):
        return ()
    union: set[str] = set()
    for key in keys:
        value = access_policy.get(key)
        if not isinstance(value, (list, tuple)):
            continue
        for entry in value:
            text = str(entry or "").strip()
            if text:
                union.add(text)
    return tuple(sorted(union))


def _execution_shard_paths(metadata: Mapping[str, Any] | None) -> tuple[str, ...]:
    """Extract the sandbox shard path_filter from the execution bundle.

    The shard is the union of resolved_read_scope, write_scope, test_scope,
    and blast_radius from access_policy. Returns () when the bundle declares
    nothing — that preserves legacy full-workspace copy behavior for specs
    that haven't adopted scope declarations yet.
    """
    return _execution_scope_paths(
        metadata,
        keys=("resolved_read_scope", "declared_read_scope", "write_scope", "test_scope", "blast_radius"),
    )


def _missing_intended_manifest_paths(
    intended_paths: Sequence[str],
    hydrated_paths: Sequence[str],
) -> tuple[str, ...]:
    missing: list[str] = []
    for intended in intended_paths:
        text = str(intended or "").strip()
        if not text:
            continue
        if any(_path_matches_filter(hydrated, (text,)) for hydrated in hydrated_paths):
            continue
        missing.append(text)
    return tuple(sorted(set(missing)))


def _observed_file_read_refs_from_output(
    output_text: str,
    candidate_paths: Sequence[str],
) -> tuple[str, ...]:
    if not output_text:
        return ()
    observed: list[str] = []
    for path in candidate_paths:
        text = str(path or "").strip()
        if text and text in output_text and text not in observed:
            observed.append(text)
    return tuple(observed)


def _workspace_manifest_audit(
    *,
    metadata: Mapping[str, Any] | None,
    hydration_receipt: HydrationReceipt,
    result: SandboxExecutionResult | None = None,
) -> dict[str, Any]:
    intended_paths = _execution_shard_paths(metadata)
    intended_read_paths = _execution_scope_paths(
        metadata,
        keys=("resolved_read_scope", "declared_read_scope", "test_scope", "blast_radius"),
    )
    intended_write_paths = _execution_scope_paths(
        metadata,
        keys=("write_scope",),
    )
    hydrated_paths = tuple(str(path) for path in hydration_receipt.hydrated_paths)
    # Provider output path mentions are only a weak read hint, so do not
    # treat write-only scope entries as observed reads.
    observed_candidates = tuple(sorted(set((*intended_read_paths, *hydrated_paths))))
    output_text = ""
    if result is not None:
        output_text = "\n".join(
            text for text in (result.stdout, result.stderr) if isinstance(text, str)
        )
    return {
        "intended_manifest_paths": list(intended_paths),
        "intended_read_manifest_paths": list(intended_read_paths),
        "intended_write_manifest_paths": list(intended_write_paths),
        "hydrated_manifest_paths": list(hydrated_paths),
        "hydrated_file_count": int(hydration_receipt.hydrated_files),
        "missing_intended_paths": list(
            _missing_intended_manifest_paths(intended_read_paths, hydrated_paths)
        ),
        "observed_file_read_refs": list(
            _observed_file_read_refs_from_output(output_text, observed_candidates)
        ),
        "observed_file_read_mode": "provider_output_path_mentions",
        "workspace_materialization": hydration_receipt.workspace_materialization,
        "workspace_snapshot_ref": hydration_receipt.workspace_snapshot_ref,
        "workspace_snapshot_cache_hit": hydration_receipt.workspace_snapshot_cache_hit,
    }


def _workspace_snapshot_cache_root() -> str:
    configured = os.environ.get(_SNAPSHOT_CACHE_ROOT_ENV, "").strip()
    if configured:
        return os.path.realpath(configured)
    return os.path.realpath(os.path.join(tempfile.gettempdir(), "praxis-workspace-snapshots"))


def _workspace_snapshot_cache_dir(workspace_snapshot_ref: str) -> str:
    digest = hashlib.sha1(workspace_snapshot_ref.encode("utf-8")).hexdigest()[:16]
    return os.path.join(_workspace_snapshot_cache_root(), digest)


def _ensure_private_directory(path: Path) -> Path:
    existed = path.exists()
    path.mkdir(parents=True, exist_ok=True)
    is_snapshot_root = path.name == "praxis-workspace-snapshots"
    is_snapshot_digest = len(path.name) == 16 and all(
        ch in "0123456789abcdef" for ch in path.name
    )
    if not existed or is_snapshot_root or is_snapshot_digest:
        os.chmod(path, 0o700)
    return path


def _real_path_is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath(
            (os.path.realpath(path), os.path.realpath(root))
        ) == os.path.realpath(root)
    except ValueError:
        return False


def _validate_workspace_snapshot_archive_path(archive_path: Path) -> None:
    cache_root = _ensure_private_directory(Path(_workspace_snapshot_cache_root()))
    if not _real_path_is_within(archive_path, cache_root):
        raise RuntimeError(f"workspace snapshot archive escaped cache root: {archive_path}")


def _safe_workspace_archive_members(
    archive: tarfile.TarFile,
    destination_parent: Path,
) -> list[tarfile.TarInfo]:
    destination_root = destination_parent.resolve(strict=False)
    safe_members: list[tarfile.TarInfo] = []
    for member in archive.getmembers():
        normalized_name = normpath(member.name)
        if (
            normalized_name in {"", "."}
            or normalized_name.startswith("../")
            or normalized_name.startswith("/")
            or normalized_name != member.name
        ):
            raise RuntimeError(f"unsafe workspace archive member: {member.name}")
        parts = PurePosixPath(normalized_name).parts
        if not parts or parts[0] != "workspace":
            raise RuntimeError(f"workspace archive member outside workspace root: {member.name}")
        if not member.isdir() and not member.isfile():
            raise RuntimeError(f"unsupported workspace archive member type: {member.name}")
        target_path = destination_parent / normalized_name
        if not _real_path_is_within(target_path, destination_root):
            raise RuntimeError(f"workspace archive member escaped destination: {member.name}")
        safe_members.append(member)
    return safe_members


def _read_snapshot_archive_metadata(metadata_path: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _read_workspace_archive_file_paths(archive_path: str) -> tuple[str, ...]:
    paths: list[str] = []
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                normalized_name = normpath(member.name)
                if not normalized_name.startswith("workspace/"):
                    continue
                relpath = normalized_name[len("workspace/"):]
                if relpath:
                    paths.append(relpath)
    except (OSError, tarfile.TarError):
        return ()
    return tuple(sorted(set(paths)))


def _write_workspace_snapshot_archive(
    source_root: str,
    archive_path: str,
    *,
    overlay_files: Sequence[Mapping[str, str]] | None = None,
    path_filter: Sequence[str] = (),
) -> int:
    source = Path(source_root)
    if not source.exists():
        raise RuntimeError(f"workspace snapshot source root is missing: {source_root}")

    archive_target = Path(archive_path)
    archive_target.parent.mkdir(parents=True, exist_ok=True)
    hydrated_files = 0
    normalized_overlays = _normalize_workspace_overlay_files(overlay_files)
    overlay_paths = {overlay["relative_path"] for overlay in normalized_overlays}
    file_entries = sorted(
        (
            (relpath, absolute)
            for relpath, absolute in _workspace_file_entries(
                source_root, path_filter=path_filter
            )
            if relpath not in overlay_paths
        ),
        key=lambda item: item[0],
    )
    # Materialize the set of parent directories we need to represent in the
    # tar so the extracted tree preserves structure. Using a set keeps it
    # O(files) and lets us emit directories in sorted order before their
    # contents (required by tar semantics).
    directories: set[str] = set()
    for relpath, _absolute in file_entries:
        parent = PurePosixPath(relpath).parent
        while parent != PurePosixPath("."):
            directories.add(parent.as_posix())
            parent = parent.parent
    with tarfile.open(archive_target, mode="w:gz") as archive:
        archive.add(source_root, arcname="workspace", recursive=False)
        for relative_dir in sorted(directories):
            absolute_dir = source / relative_dir
            if not absolute_dir.is_dir():
                continue
            archive.add(
                str(absolute_dir),
                arcname=str(PurePosixPath("workspace") / relative_dir),
                recursive=False,
            )
        for relpath, absolute in file_entries:
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
    path_filter = tuple(getattr(snapshot, "path_filter", ()) or ())
    snapshot_ref = str(
        getattr(snapshot, "workspace_snapshot_ref", "")
        or _workspace_snapshot_ref(
            snapshot.source_root,
            overlay_files=getattr(snapshot, "overlay_files", ()),
            path_filter=path_filter,
        )
    ).strip()
    if not snapshot_ref:
        raise RuntimeError("workspace_snapshot_ref must be resolved before hydration")

    cache_root = _ensure_private_directory(Path(_workspace_snapshot_cache_root()))
    cache_dir = Path(_workspace_snapshot_cache_dir(snapshot_ref))
    if not _real_path_is_within(cache_dir, cache_root):
        raise RuntimeError(f"workspace snapshot cache dir escaped cache root: {cache_dir}")
    archive_path = cache_dir / "workspace.tar.gz"
    metadata_path = cache_dir / "metadata.json"

    _ensure_private_directory(cache_dir)
    _validate_workspace_snapshot_archive_path(archive_path)
    metadata = _read_snapshot_archive_metadata(str(metadata_path))
    if archive_path.is_file() and metadata is not None:
        hydrated_paths = tuple(
            sorted(str(path) for path in metadata.get("hydrated_paths") or () if str(path).strip())
        )
        if not hydrated_paths:
            hydrated_paths = _read_workspace_archive_file_paths(str(archive_path))
        return WorkspaceSnapshotArchive(
            workspace_snapshot_ref=snapshot_ref,
            archive_path=str(archive_path),
            hydrated_files=int(metadata.get("hydrated_files") or 0),
            cache_hit=True,
            hydrated_paths=hydrated_paths,
        )

    temp_archive = cache_dir / f".{uuid4().hex}.tar.gz"
    temp_metadata = cache_dir / f".{uuid4().hex}.json"
    hydrated_files = _write_workspace_snapshot_archive(
        snapshot.source_root,
        str(temp_archive),
        overlay_files=getattr(snapshot, "overlay_files", ()),
        path_filter=path_filter,
    )
    hydrated_paths = _workspace_snapshot_hydrated_paths(
        snapshot.source_root,
        overlay_files=getattr(snapshot, "overlay_files", ()),
        path_filter=path_filter,
    )
    temp_metadata.write_text(
        json.dumps(
            {
                "workspace_snapshot_ref": snapshot_ref,
                "hydrated_files": hydrated_files,
                "hydrated_paths": list(hydrated_paths),
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
        hydrated_paths=hydrated_paths,
    )


def _hydrate_copy(source_root: str, destination_root: str) -> int:
    destination = Path(destination_root)
    destination.mkdir(parents=True, exist_ok=True)
    copied = 0
    for relpath, absolute in _workspace_file_entries(source_root):
        target_file = destination / relpath
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(absolute, target_file)
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
        _validate_workspace_snapshot_archive_path(Path(cached_snapshot.archive_path))
        with tarfile.open(cached_snapshot.archive_path, mode="r:gz") as archive:
            archive.extractall(
                workspace_root.parent,
                members=_safe_workspace_archive_members(archive, workspace_root.parent),
            )
        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=cached_snapshot.hydrated_files,
            workspace_materialization=snapshot.materialization,
            workspace_snapshot_ref=cached_snapshot.workspace_snapshot_ref,
            workspace_snapshot_cache_hit=cached_snapshot.cache_hit,
            hydrated_paths=cached_snapshot.hydrated_paths,
        )

    def exec(self, session: SandboxSession, request: SandboxExecRequest) -> SandboxExecutionResult:
        docker_image, image_meta = resolve_docker_image(
            requested_image=request.image,
            image_exists=_docker_image_available,
            agent_slug=getattr(request, "agent_slug", None) or _provider_slug(session.metadata),
        )
        if image_meta.get("rejected") or not docker_image:
            detail = str(image_meta.get("message") or image_meta.get("reason_code") or "").strip()
            raise RuntimeError(
                "docker_local requires provider-family thin sandbox image authority."
                + (f" {detail}" if detail else "")
            )
        if not _docker_image_available(docker_image):
            required_image = str(image_meta.get("required_image") or docker_image).strip()
            build_hint = (
                f" Build thin sandbox image {required_image!r} on the selected "
                "runtime target, or run praxis setup doctor for the exact image contract."
            )
            detail = str(image_meta.get("build_error") or "").strip()
            raise RuntimeError(
                "docker_local requires image "
                f"{docker_image!r}.{build_hint}"
                + (f" {detail}" if detail else "")
            )
        container_name = f"praxis-{uuid4().hex[:12]}"
        auth_mount_policy = validate_auth_mount_policy(
            session.metadata.get("auth_mount_policy") or "provider_scoped"
        )
        session_provider_slug = _provider_slug(session.metadata)
        mounted_provider_slug: str | None = None
        if auth_mount_policy != "none":
            mounted_provider_slug = (
                session_provider_slug
                if auth_mount_policy == "provider_scoped"
                else None
            )
        root_auth_bootstrap = _cli_requires_root_auth_bootstrap(
            provider_slug=session_provider_slug,
            auth_mount_policy=auth_mount_policy,
            requested_user=f"{_CLI_AGENT_UID}:{_CLI_AGENT_GID}",
        )

        # Match the uid used by the cli_llm/docker_runner path. OpenAI starts
        # as root only to copy the root-readable host auth seed, then the
        # command wrapper drops to praxis-agent before invoking the CLI.
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--name", container_name,
            "--user", "0:0" if root_auth_bootstrap else f"{_CLI_AGENT_UID}:{_CLI_AGENT_GID}",
            "--memory",
            _docker_memory(session.metadata),
            "--cpus",
            _docker_cpus(session.metadata),
            "--workdir",
            _CONTAINER_WORKSPACE_ROOT,
            "-v",
            f"{session.workspace_root}:{_CONTAINER_WORKSPACE_ROOT}",
        ]
        if auth_mount_policy != "none":
            # Writable scratch dirs for each CLI's own bookkeeping (projects.json,
            # PATH updates, session state). The :ro file mounts below layer their
            # specific files into these tmpfs dirs. Without this, the container
            # auto-creates parent dirs owned by root:755 when mounting individual
            # files, blocking uid-1100 writes and breaking gemini/codex CLIs.
            docker_cmd.extend(_cli_home_tmpfs_flags())
            docker_cmd.extend(
                _cli_auth_volume_flags(provider_slug=mounted_provider_slug)
            )
        # HOME must match the uid-1100 user so CLIs resolve their config files
        # from the mounted auth targets under the configured container home. Force after
        # request.env so upstream defaults (e.g. HOME=/root inherited from the
        # worker container) do not override it.
        env_items = {**dict(request.env), "HOME": str(_SANDBOX_HOME)}
        for key, value in sorted(env_items.items()):
            docker_cmd.extend(["-e", f"{key}={value}"])
        # Forward host-shell auth env vars (CLAUDE_CODE_OAUTH_TOKEN etc.) that
        # the worker inherited — the ephemeral CLI container needs them for
        # non-file auth paths (Keychain-backed OAuth).
        from adapters.docker_runner import _cli_auth_env_forward
        for key, value in sorted(_cli_auth_env_forward(session_provider_slug).items()):
            if key in env_items:
                continue
            docker_cmd.extend(["-e", f"{key}={value}"])
        if session.network_policy == "disabled":
            docker_cmd.append("--network=none")

        # Live-edit overlay for the `praxis` shell-tool shim. When
        # PRAXIS_HOST_WORKSPACE_ROOT is set, we bind-mount the source file
        # over the baked-in /usr/local/bin/praxis so edits to
        # bin/praxis_sandbox_client.py are picked up by the next sandbox
        # without rebuilding the image. The baked-in binary remains as
        # fallback when the env var is absent (e.g. production hosts that
        # prefer image-pinned tool versions).
        host_workspace_root = str(os.environ.get("PRAXIS_HOST_WORKSPACE_ROOT", "")).strip()
        if host_workspace_root:
            host_binary_path = os.path.join(
                host_workspace_root,
                "Code&DBs",
                "Workflow",
                "bin",
                "praxis_sandbox_client.py",
            )
            docker_cmd.extend([
                "-v",
                f"{host_binary_path}:/usr/local/bin/praxis:ro",
            ])

        docker_command = (
            _cli_auth_bootstrap_command(request.command, provider_slug=session_provider_slug)
            if root_auth_bootstrap
            else request.command
        )
        docker_cmd.extend([docker_image, "bash", "-lc", docker_command])

        # Diagnostic hook: write the exact docker run invocation to stderr when
        # PRAXIS_SANDBOX_DEBUG=1. Captures the per-job command so we can diff
        # against a known-working manual invocation when sandbox_error is
        # reported. Disabled by default to keep worker logs quiet.
        if os.environ.get("PRAXIS_SANDBOX_DEBUG", "").strip() == "1":
            try:
                import shlex as _dbg_shlex
                sys.stderr.write(
                    "PRAXIS_SANDBOX_CMD "
                    + _dbg_shlex.join(str(part) for part in docker_cmd)
                    + "\n"
                )
                sys.stderr.flush()
            except Exception:
                pass

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

        stats_thread = _STATS_THREAD_CLASS(target=_poll_stats, daemon=True, name=f"docker-stats-{container_name}")
        stats_thread.start()

        # DEBUG: log the exact command + env keys + stdin size for root-causing
        # silent hangs. Remove once the sandbox auth path stabilizes.
        try:
            _env_keys = sorted({part.split("=", 1)[0] for flag_idx, part in enumerate(docker_cmd) if flag_idx > 0 and docker_cmd[flag_idx - 1] == "-e"})
            _stdin_size = len(request.stdin_text or "")
            _redacted_cmd = []
            for p in docker_cmd:
                key, sep, _value = p.partition("=")
                if sep and (
                    key.endswith("_API_KEY")
                    or key.endswith("_TOKEN")
                    or key.endswith("_SECRET")
                    or "SECRET" in key
                ):
                    _redacted_cmd.append(f"{key}=<redacted>")
                else:
                    _redacted_cmd.append(p)
            import logging as _lg
            _lg.getLogger("runtime.sandbox_runtime").info(
                "sandbox_docker_spawn container=%s user=%s workdir=%s network=%s stdin_bytes=%d env_keys=%s cmd=%s",
                container_name,
                "1100:1100",
                _CONTAINER_WORKSPACE_ROOT,
                "disabled" if session.network_policy == "disabled" else "default",
                _stdin_size,
                _env_keys,
                " ".join(_redacted_cmd),
            )
        except Exception:
            pass
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
            hydrated_paths=cached_snapshot.hydrated_paths,
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
        workspace_materialization = _validate_workspace_materialization_allowed(
            workspace_materialization,
            provider_metadata,
        )
        write_scope = _execution_write_scope(provider_metadata)
        write_scope_entries = _execution_write_scope_entries(provider_metadata)
        submission_required = _submission_required(provider_metadata)
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
            if _uses_empty_workspace_materialization(workspace_materialization):
                with tempfile.TemporaryDirectory(prefix="praxis-empty-workspace-") as empty_root:
                    workspace_snapshot_ref = _workspace_snapshot_ref(
                        empty_root,
                        overlay_files=workspace_overlays,
                    )
                    hydration_receipt = provider.hydrate_workspace(
                        session,
                        WorkspaceSnapshot(
                            source_root=empty_root,
                            materialization=_EMPTY_WORKSPACE_MATERIALIZATION,
                            workspace_snapshot_ref=workspace_snapshot_ref,
                            overlay_files=workspace_overlays,
                        ),
                    )
            else:
                shard_path_filter = _execution_shard_paths(provider_metadata)
                workspace_snapshot_ref = _workspace_snapshot_ref(
                    workdir,
                    overlay_files=workspace_overlays,
                    path_filter=shard_path_filter,
                )
                hydration_receipt = provider.hydrate_workspace(
                    session,
                    WorkspaceSnapshot(
                        source_root=workdir,
                        materialization=workspace_materialization,
                        workspace_snapshot_ref=workspace_snapshot_ref,
                        overlay_files=workspace_overlays,
                        path_filter=shard_path_filter,
                    ),
                )
            if getattr(provider, "execution_lane", "") == "local":
                # The hydrated workspace is copied into a temp sandbox. Default
                # it to read-only, then open only declared write targets so the
                # local Docker lane denies obvious out-of-scope writes before
                # they can become drift. Jobs without a declared write_scope keep
                # the legacy all-writable behavior.
                if write_scope_entries:
                    _prepare_workspace_write_scope(session.workspace_root, write_scope_entries)
                else:
                    _make_workspace_writable(session.workspace_root)
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
                    agent_slug=str(provider_metadata.get("provider_slug") or "").strip() or None,
                ),
            )
            manifest_audit = _workspace_manifest_audit(
                metadata=provider_metadata,
                hydration_receipt=hydration_receipt,
                result=result,
            )
            artifact_receipt = provider.collect_artifacts(session, before_manifest)
            artifact_refs, artifact_scope_drift = _validated_artifact_refs(
                artifact_receipt.artifact_refs,
                write_scope=write_scope,
                submission_required=submission_required,
            )
            blocked_compat_paths = _authority_binding_blocked_paths(provider_metadata)
            blocked_compat_drift = _validated_blocked_compat_refs(
                artifact_refs,
                blocked_compat=blocked_compat_paths,
                submission_required=submission_required,
            )
            if blocked_compat_drift:
                artifact_scope_drift = tuple(artifact_scope_drift) + tuple(blocked_compat_drift)
            # Dehydrate: copy changed in-scope files from sandbox back to host
            # workdir BEFORE the sandbox is destroyed.
            #
            # Why this fires for submission_required=True (changed 2026-04-27):
            # The submission gate's seal/auto-seal flow runs `_measured_operations`
            # which reads from `baseline.workspace_root` — the host workdir.
            # Without dehydration, sandbox writes only land in `artifact_store`
            # (line below) and the on-disk diff finds 0 changes → every
            # submission_required job fails with `workflow_submission.phantom_ship`
            # or `workflow_submission.required_missing`. The previous comment said
            # "the authoritative deliverable is the workflow_job_submissions row,
            # not on-disk artifacts" — true intent, but it left a gap because
            # the seal flow itself reads on-disk to populate that row.
            #
            # Dehydrating in-scope files keeps three properties:
            #   1. The seal diff has something to compare (auto-seal works).
            #   2. The artifact_store still records hashes via the loop below
            #      (canonical deliverable provenance).
            #   3. The host repo only sees paths inside `write_scope`, so
            #      out-of-scope side-effects (shell history, /tmp scratch)
            #      stay in the sandbox temp dir and get cleaned by destroy_session.
            if (
                artifact_refs
                and getattr(provider, "execution_lane", "") == "local"
                and not _uses_empty_workspace_materialization(workspace_materialization)
            ):
                _dehydrate_copy(session.workspace_root, workdir, artifact_refs)
            # Structured drift emission for ingestion. Downstream consumers
            # (verifier worker, shard-drift tracker) can read stderr for these
            # lines when they need the raw signal; the returned
            # SandboxExecutionResult.artifact_scope_drift is the canonical
            # in-process surface.
            if artifact_scope_drift:
                drift_envelope = {
                    "event": "sandbox.artifact_scope_drift",
                    "sandbox_session_id": sandbox_session_id,
                    "sandbox_group_id": sandbox_group_id,
                    "submission_required": submission_required,
                    "drift_count": len(artifact_scope_drift),
                    "drift": list(artifact_scope_drift),
                }
                sys.stderr.write("PRAXIS_SANDBOX_DRIFT " + json.dumps(drift_envelope, sort_keys=True) + "\n")
                sys.stderr.flush()
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
                artifact_scope_drift=artifact_scope_drift,
                workspace_manifest_audit=manifest_audit,
            )
        except Exception:
            disposition = "failed"
            raise
        finally:
            if getattr(provider, "execution_lane", "") == "local":
                _make_workspace_writable(session.workspace_root)
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
