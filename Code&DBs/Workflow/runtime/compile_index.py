"""Durable compile-index snapshots for fast compiler hydration.

The refresh path is explicit:

* sync the source authority tables
* materialize a content-addressed snapshot row
* record freshness metadata for lookup-time failure checks

The online compiler should only bind against these snapshots. It must not
re-discover the world on the hot path or silently substitute raw prose when
the required authority is missing.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from runtime.capability_catalog import load_capability_catalog, sync_capability_catalog
from runtime.compile_reuse import module_surface_manifest
from runtime.integrations.display_names import (
    base_integration_name,
    display_name_for_integration,
)
from registry.integration_registry_sync import sync_integration_registry
from registry.reference_catalog_sync import sync_reference_catalog

logger = logging.getLogger(__name__)

_DEFAULT_SURFACE_NAME = "compiler"
_DEFAULT_SCHEMA_VERSION = 1
_DEFAULT_STALE_AFTER_SECONDS = 3600
_COMPILE_INDEX_SURFACE_COMPONENTS: tuple[object, ...] = (
    __file__,
    module_surface_manifest,
    "compiler.py",
    "definition_compile_kernel.py",
    load_capability_catalog,
    sync_reference_catalog,
    sync_integration_registry,
)


class CompileIndexAuthorityError(RuntimeError):
    """Raised when compile-index authority is missing, stale, or malformed."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _error(
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> CompileIndexAuthorityError:
    return CompileIndexAuthorityError(reason_code, message, details=details)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _error(
            "compile_index.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _slugify(value: object) -> str:
    text = _as_text(value).lower()
    if not text:
        return ""
    out: list[str] = []
    last_was_dash = False
    for char in text:
        if char.isalnum():
            out.append(char)
            last_was_dash = False
        elif char in {"_", "-", "/", "."}:
            if not last_was_dash and out:
                out.append("-")
                last_was_dash = True
        elif char.isspace() and out and not last_was_dash:
            out.append("-")
            last_was_dash = True
    result = "".join(out).strip("-")
    while "--" in result:
        result = result.replace("--", "-")
    return result


def _json_value(value: object, *, default: object = None) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return value if value is not None else default


def _json_object(value: object) -> dict[str, Any]:
    value = _json_value(value, default={})
    return value if isinstance(value, dict) else {}


def _json_list(value: object) -> list[Any]:
    value = _json_value(value, default=[])
    return value if isinstance(value, list) else []


def _json_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def _stable_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _repo_root_from_module() -> Path:
    return Path(__file__).resolve().parents[3]


def _compile_index_surface_paths() -> tuple[Path, ...]:
    runtime_dir = Path(__file__).resolve().parent
    resolved_paths: list[Path] = []
    seen_paths: set[Path] = set()
    for component in _COMPILE_INDEX_SURFACE_COMPONENTS:
        if isinstance(component, str):
            if component.endswith(".py"):
                path = runtime_dir / component
            else:
                path = Path(component)
        else:
            source_path = inspect.getsourcefile(component)
            if not source_path:
                raise _error(
                    "compile_index.surface_manifest_unavailable",
                    "compile index surface manifest source file could not be resolved",
                    details={"component": repr(component)},
                )
            path = Path(source_path)
        resolved_path = path.resolve()
        if resolved_path in seen_paths:
            continue
        seen_paths.add(resolved_path)
        resolved_paths.append(resolved_path)
    return tuple(resolved_paths)


def _compile_index_ttl_seconds() -> int:
    raw = (
        os.environ.get("WORKFLOW_COMPILE_INDEX_STALE_AFTER_SECONDS", "").strip()
        or os.environ.get("WORKFLOW_COMPILE_INDEX_TTL_SECONDS", "").strip()
    )
    if not raw:
        return _DEFAULT_STALE_AFTER_SECONDS
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_STALE_AFTER_SECONDS
    return max(1, value)


def _git_output(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def current_repo_fingerprint(repo_root: str | Path | None = None) -> dict[str, Any]:
    """Return a stable fingerprint for the current repo state.

    The fingerprint intentionally includes the dirty working tree state so a
    snapshot taken from one code revision does not silently hydrate another.
    """

    resolved_root = Path(repo_root) if repo_root is not None else _repo_root_from_module()
    resolved_root = resolved_root.resolve()
    try:
        git_head = _git_output(resolved_root, "rev-parse", "HEAD")
        git_branch = _git_output(resolved_root, "rev-parse", "--abbrev-ref", "HEAD")
        git_status = _git_output(
            resolved_root,
            "status",
            "--porcelain",
            "--untracked-files=all",
        )
    except Exception as exc:  # pragma: no cover - git is expected in this repo
        raise _error(
            "compile_index.repo_fingerprint_unavailable",
            "repo fingerprint could not be resolved from git",
            details={"repo_root": str(resolved_root)},
        ) from exc

    status_hash = hashlib.sha256(git_status.encode("utf-8")).hexdigest()[:16]
    fingerprint = hashlib.sha256(
        f"{git_head}\n{git_branch}\n{status_hash}\n{resolved_root}".encode("utf-8")
    ).hexdigest()[:16]
    return {
        "repo_root": str(resolved_root),
        "git_head": git_head,
        "git_branch": git_branch,
        "git_dirty": bool(git_status.strip()),
        "git_status_hash": status_hash,
        "repo_fingerprint": fingerprint,
    }


@dataclass(frozen=True, slots=True)
class CompileIndexSnapshot:
    """Immutable compile-index snapshot loaded from Postgres."""

    schema_version: int
    compile_index_ref: str
    compile_surface_revision: str
    compile_surface_name: str
    repo_root: str
    repo_fingerprint: str
    repo_info: Mapping[str, Any]
    surface_manifest: Mapping[str, Any]
    source_fingerprints: Mapping[str, str]
    source_counts: Mapping[str, int]
    decision_ref: str
    refresh_count: int
    refreshed_at: datetime
    stale_after_at: datetime
    freshness_state: str
    freshness_reason: str | None
    reference_catalog: tuple[dict[str, Any], ...]
    integration_registry: tuple[dict[str, Any], ...]
    object_types: tuple[dict[str, Any], ...]
    compiler_route_hints: tuple[tuple[str, str], ...]
    capability_catalog: tuple[dict[str, Any], ...]
    payload: Mapping[str, Any]

    def connected_integrations(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            dict(row)
            for row in self.integration_registry
            if _as_text(row.get("auth_status")) == "connected"
        )

    def route_hint_cache(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (hint, route)
            for hint, route in self.compiler_route_hints
            if hint and route
        )

    def summary(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "compile_index_ref": self.compile_index_ref,
            "compile_surface_revision": self.compile_surface_revision,
            "compile_surface_name": self.compile_surface_name,
            "repo_root": self.repo_root,
            "repo_fingerprint": self.repo_fingerprint,
            "surface_manifest_revision": _as_text(self.surface_manifest.get("surface_revision")),
            "freshness_state": self.freshness_state,
            "freshness_reason": self.freshness_reason,
            "refreshed_at": self.refreshed_at.isoformat(),
            "stale_after_at": self.stale_after_at.isoformat(),
            "refresh_count": self.refresh_count,
            "decision_ref": self.decision_ref,
            "source_counts": dict(self.source_counts),
            "source_fingerprints": dict(self.source_fingerprints),
            "reference_count": len(self.reference_catalog),
            "integration_count": len(self.integration_registry),
            "object_type_count": len(self.object_types),
            "capability_count": len(self.capability_catalog),
            "route_hint_count": len(self.route_hint_cache()),
        }

    def to_compile_context(self) -> dict[str, Any]:
        return {
            "compile_index_ref": self.compile_index_ref,
            "compile_surface_revision": self.compile_surface_revision,
            "compile_surface_name": self.compile_surface_name,
            "repo_root": self.repo_root,
            "repo_fingerprint": self.repo_fingerprint,
            "catalog": [dict(row) for row in self.reference_catalog],
            "integrations": [dict(row) for row in self.connected_integrations()],
            "object_types": [dict(row) for row in self.object_types],
            "capabilities": [dict(row) for row in self.capability_catalog],
            "route_hints": list(self.route_hint_cache()),
        }


def refresh_compile_index(
    conn: Any,
    *,
    repo_root: str | Path | None = None,
    stale_after_seconds: int | None = None,
    decision_ref: str | None = None,
    surface_name: str = _DEFAULT_SURFACE_NAME,
) -> CompileIndexSnapshot:
    """Refresh the durable compile index snapshot explicitly."""
    if conn is None:
        raise _error(
            "compile_index.authority_missing",
            "compile index refresh requires Postgres authority",
        )

    resolved_repo_root = Path(repo_root) if repo_root is not None else _repo_root_from_module()
    resolved_repo_root = resolved_repo_root.resolve()
    ttl_seconds = (
        _compile_index_ttl_seconds()
        if stale_after_seconds is None
        else max(1, int(stale_after_seconds))
    )
    surface_name = _require_text(surface_name, field_name="compile_surface_name")

    sync_integration_registry(conn)
    integrations = _load_integrations(conn)
    object_types = _load_object_types(conn)
    sync_reference_catalog(conn, integrations=integrations, object_types=object_types)
    connected_integrations = [
        row
        for row in integrations
        if _as_text(row.get("auth_status")) == "connected"
    ]
    sync_capability_catalog(conn, integrations=connected_integrations)

    reference_catalog = _load_reference_catalog(conn)
    capability_catalog = load_capability_catalog(conn)
    compiler_route_hints = _load_compiler_route_hints(conn)
    repo_info = current_repo_fingerprint(resolved_repo_root)
    surface_manifest = current_compile_surface_manifest(resolved_repo_root)

    snapshot_payload = {
        "schema_version": _DEFAULT_SCHEMA_VERSION,
        "repo_info": repo_info,
        "surface_manifest": surface_manifest,
        "source_fingerprints": {},
        "source_counts": {},
        "reference_catalog": reference_catalog,
        "integration_registry": integrations,
        "object_types": object_types,
        "compiler_route_hints": [
            {
                "hint_text": hint_text,
                "route_slug": route_slug,
            }
            for hint_text, route_slug in compiler_route_hints
        ],
        "capability_catalog": capability_catalog,
    }
    source_fingerprints = {
        "reference_catalog": _stable_hash(reference_catalog),
        "integration_registry": _stable_hash(integrations),
        "object_types": _stable_hash(object_types),
        "compiler_route_hints": _stable_hash(
            [{"hint_text": hint, "route_slug": route} for hint, route in compiler_route_hints]
        ),
        "capability_catalog": _stable_hash(capability_catalog),
    }
    source_counts = {
        "reference_catalog": len(reference_catalog),
        "integration_registry": len(integrations),
        "object_types": len(object_types),
        "compiler_route_hints": len(compiler_route_hints),
        "capability_catalog": len(capability_catalog),
    }
    snapshot_payload["source_fingerprints"] = source_fingerprints
    snapshot_payload["source_counts"] = source_counts

    payload_digest = _stable_hash(
        {
            key: value
            for key, value in snapshot_payload.items()
            if key != "repo_info"
        }
    )[:16]
    compile_index_ref = f"compile_index.{surface_name}.{payload_digest}"
    compile_surface_revision = f"compile_surface.{surface_name}.{payload_digest}"
    refreshed_at = datetime.now(timezone.utc)
    stale_after_at = refreshed_at + timedelta(seconds=ttl_seconds)
    decision = decision_ref or f"decision.compile.index.refresh.{compile_surface_revision}"

    conn.execute(
        """
        INSERT INTO compile_index_snapshots (
            compile_index_ref,
            compile_surface_revision,
            compile_surface_name,
            schema_version,
            repo_root,
            repo_fingerprint,
            source_fingerprints,
            source_counts,
            payload,
            decision_ref,
            refreshed_at,
            stale_after_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10,
            $11,
            $12
        )
        ON CONFLICT (compile_index_ref) DO UPDATE SET
            compile_surface_revision = EXCLUDED.compile_surface_revision,
            compile_surface_name = EXCLUDED.compile_surface_name,
            schema_version = EXCLUDED.schema_version,
            repo_root = EXCLUDED.repo_root,
            repo_fingerprint = EXCLUDED.repo_fingerprint,
            source_fingerprints = EXCLUDED.source_fingerprints,
            source_counts = EXCLUDED.source_counts,
            payload = EXCLUDED.payload,
            decision_ref = EXCLUDED.decision_ref,
            refreshed_at = EXCLUDED.refreshed_at,
            stale_after_at = EXCLUDED.stale_after_at,
            refresh_count = compile_index_snapshots.refresh_count + 1,
            updated_at = now()
        """,
        compile_index_ref,
        compile_surface_revision,
        surface_name,
        _DEFAULT_SCHEMA_VERSION,
        _as_text(repo_info["repo_root"]),
        repo_info["repo_fingerprint"],
        json.dumps(source_fingerprints, sort_keys=True, default=str),
        json.dumps(source_counts, sort_keys=True, default=str),
        json.dumps(snapshot_payload, sort_keys=True, default=str),
        decision,
        refreshed_at,
        stale_after_at,
    )

    return load_compile_index_snapshot(
        conn,
        snapshot_ref=compile_index_ref,
        surface_revision=compile_surface_revision,
        surface_name=surface_name,
        require_fresh=True,
        repo_root=resolved_repo_root,
    )


def load_compile_index_snapshot(
    conn: Any,
    *,
    snapshot_ref: str | None = None,
    surface_revision: str | None = None,
    surface_name: str = _DEFAULT_SURFACE_NAME,
    require_fresh: bool = True,
    repo_root: str | Path | None = None,
) -> CompileIndexSnapshot:
    """Load the latest or pinned compile-index snapshot from Postgres."""
    if conn is None:
        raise _error(
            "compile_index.authority_missing",
            "compile index lookup requires Postgres authority",
        )

    surface_name = _require_text(surface_name, field_name="compile_surface_name")

    if snapshot_ref is not None:
        snapshot_ref = _require_text(snapshot_ref, field_name="compile_index_ref")
        rows = conn.execute(
            """
            SELECT compile_index_ref,
                   compile_surface_revision,
                   compile_surface_name,
                   schema_version,
                   repo_root,
                   repo_fingerprint,
                   source_fingerprints,
                   source_counts,
                   payload,
                   decision_ref,
                   refreshed_at,
                   stale_after_at,
                   refresh_count
              FROM compile_index_snapshots
             WHERE compile_index_ref = $1
               AND compile_surface_name = $2
             LIMIT 1
            """,
            snapshot_ref,
            surface_name,
        )
    elif surface_revision is not None:
        surface_revision = _require_text(surface_revision, field_name="compile_surface_revision")
        rows = conn.execute(
            """
            SELECT compile_index_ref,
                   compile_surface_revision,
                   compile_surface_name,
                   schema_version,
                   repo_root,
                   repo_fingerprint,
                   source_fingerprints,
                   source_counts,
                   payload,
                   decision_ref,
                   refreshed_at,
                   stale_after_at,
                   refresh_count
              FROM compile_index_snapshots
             WHERE compile_surface_revision = $1
               AND compile_surface_name = $2
             LIMIT 1
            """,
            surface_revision,
            surface_name,
        )
    else:
        rows = conn.execute(
            """
            SELECT compile_index_ref,
                   compile_surface_revision,
                   compile_surface_name,
                   schema_version,
                   repo_root,
                   repo_fingerprint,
                   source_fingerprints,
                   source_counts,
                   payload,
                   decision_ref,
                   refreshed_at,
                   stale_after_at,
                   refresh_count
              FROM compile_index_snapshots
             WHERE compile_surface_name = $1
             ORDER BY refreshed_at DESC, compile_surface_revision DESC
             LIMIT 1
            """,
            surface_name,
        )

    if not rows:
        raise _error(
            "compile_index.snapshot_missing",
            "compile index snapshot is missing",
            details={
                "snapshot_ref": snapshot_ref,
                "compile_surface_revision": surface_revision,
                "compile_surface_name": surface_name,
            },
        )

    snapshot = _snapshot_from_row(dict(rows[0]))
    if snapshot_ref is not None and surface_revision is not None:
        if snapshot.compile_surface_revision != surface_revision:
            raise _error(
                "compile_index.snapshot_surface_mismatch",
                "compile index snapshot revision does not match the pinned revision",
                details={
                    "compile_index_ref": snapshot.compile_index_ref,
                    "compile_surface_revision": snapshot.compile_surface_revision,
                    "requested_surface_revision": surface_revision,
                },
            )
    freshness = _evaluate_freshness(snapshot, repo_root=repo_root)

    if require_fresh and freshness["state"] != "fresh":
        raise _error(
            "compile_index.snapshot_stale",
            "compile index snapshot is stale",
            details={
                "compile_index_ref": snapshot.compile_index_ref,
                "compile_surface_revision": snapshot.compile_surface_revision,
                "reason": freshness["reason"],
                "freshness_state": freshness["state"],
                "refreshed_at": snapshot.refreshed_at.isoformat(),
                "stale_after_at": snapshot.stale_after_at.isoformat(),
                "repo_root": snapshot.repo_root,
            },
        )

    return CompileIndexSnapshot(
        schema_version=snapshot.schema_version,
        compile_index_ref=snapshot.compile_index_ref,
        compile_surface_revision=snapshot.compile_surface_revision,
        compile_surface_name=snapshot.compile_surface_name,
        repo_root=snapshot.repo_root,
        repo_fingerprint=snapshot.repo_fingerprint,
        repo_info=snapshot.repo_info,
        source_fingerprints=snapshot.source_fingerprints,
        source_counts=snapshot.source_counts,
        decision_ref=snapshot.decision_ref,
        refresh_count=snapshot.refresh_count,
        refreshed_at=snapshot.refreshed_at,
        stale_after_at=snapshot.stale_after_at,
        freshness_state=freshness["state"],
        freshness_reason=freshness["reason"],
        surface_manifest=snapshot.surface_manifest,
        reference_catalog=snapshot.reference_catalog,
        integration_registry=snapshot.integration_registry,
        object_types=snapshot.object_types,
        compiler_route_hints=snapshot.compiler_route_hints,
        capability_catalog=snapshot.capability_catalog,
        payload=snapshot.payload,
    )


def _evaluate_freshness(
    snapshot: CompileIndexSnapshot,
    *,
    repo_root: str | Path | None = None,
) -> dict[str, str | None]:
    now = datetime.now(timezone.utc)
    if snapshot.stale_after_at <= now:
        return {"state": "stale", "reason": "expired_after_ttl"}

    if repo_root is not None:
        manifest_revision = _as_text(snapshot.surface_manifest.get("surface_revision"))
        if manifest_revision:
            try:
                current_manifest = current_compile_surface_manifest(repo_root)
            except CompileIndexAuthorityError as exc:
                return {"state": "stale", "reason": exc.reason_code}
            if _as_text(current_manifest.get("surface_revision")) != manifest_revision:
                return {"state": "stale", "reason": "surface_manifest_mismatch"}
        else:
            try:
                current = current_repo_fingerprint(repo_root)
            except CompileIndexAuthorityError as exc:
                return {"state": "stale", "reason": exc.reason_code}
            if _as_text(current.get("repo_fingerprint")) != snapshot.repo_fingerprint:
                return {"state": "stale", "reason": "repo_fingerprint_mismatch"}

    return {"state": "fresh", "reason": None}


def _snapshot_from_row(row: dict[str, Any]) -> CompileIndexSnapshot:
    payload = _json_object(row.get("payload"))
    repo_info = _json_object(payload.get("repo_info"))
    surface_manifest = _json_object(payload.get("surface_manifest"))
    source_fingerprints = _json_object(row.get("source_fingerprints"))
    source_counts = _json_object(row.get("source_counts"))
    reference_catalog = tuple(
        _normalize_reference_row(item)
        for item in _json_list(payload.get("reference_catalog"))
    )
    integration_registry = tuple(
        _normalize_integration_row(item)
        for item in _json_list(payload.get("integration_registry"))
    )
    object_types = tuple(
        _normalize_object_type_row(item)
        for item in _json_list(payload.get("object_types"))
    )
    compiler_route_hints = tuple(
        route_hint
        for route_hint in (
            _normalize_route_hint_row(item)
            for item in _json_list(payload.get("compiler_route_hints"))
        )
        if route_hint[0] and route_hint[1]
    )
    capability_catalog = tuple(
        _normalize_capability_row(item)
        for item in _json_list(payload.get("capability_catalog"))
    )
    compile_index_ref = _require_text(row.get("compile_index_ref"), field_name="compile_index_ref")
    compile_surface_revision = _require_text(
        row.get("compile_surface_revision"),
        field_name="compile_surface_revision",
    )
    compile_surface_name = _require_text(
        row.get("compile_surface_name"),
        field_name="compile_surface_name",
    )
    schema_version_raw = row.get("schema_version")
    if schema_version_raw is None:
        schema_version_raw = payload.get("schema_version", _DEFAULT_SCHEMA_VERSION)
    try:
        schema_version = int(schema_version_raw)
    except (TypeError, ValueError) as exc:
        raise _error(
            "compile_index.invalid_row",
            "compile index schema version is invalid",
            details={"compile_index_ref": compile_index_ref},
        ) from exc
    if schema_version != _DEFAULT_SCHEMA_VERSION:
        raise _error(
            "compile_index.schema_version_unsupported",
            "compile index snapshot schema version is unsupported",
            details={
                "compile_index_ref": compile_index_ref,
                "schema_version": schema_version,
                "expected_schema_version": _DEFAULT_SCHEMA_VERSION,
            },
        )
    repo_root = _require_text(
        row.get("repo_root") or repo_info.get("repo_root"),
        field_name="repo_root",
    )
    repo_fingerprint = _require_text(row.get("repo_fingerprint"), field_name="repo_fingerprint")
    decision_ref = _require_text(row.get("decision_ref"), field_name="decision_ref")
    refreshed_at = row.get("refreshed_at")
    stale_after_at = row.get("stale_after_at")
    if not hasattr(refreshed_at, "isoformat") or not hasattr(stale_after_at, "isoformat"):
        raise _error(
            "compile_index.invalid_row",
            "compile index timestamps are invalid",
            details={"compile_index_ref": compile_index_ref},
        )

    return CompileIndexSnapshot(
        schema_version=schema_version,
        compile_index_ref=compile_index_ref,
        compile_surface_revision=compile_surface_revision,
        compile_surface_name=compile_surface_name,
        repo_root=repo_root,
        repo_fingerprint=repo_fingerprint,
        repo_info=repo_info,
        surface_manifest=surface_manifest,
        source_fingerprints={str(key): _as_text(value) for key, value in source_fingerprints.items()},
        source_counts={
            str(key): int(value)
            for key, value in source_counts.items()
        },
        decision_ref=decision_ref,
        refresh_count=max(1, int(row.get("refresh_count") or 1)),
        refreshed_at=refreshed_at,
        stale_after_at=stale_after_at,
        freshness_state="fresh",
        freshness_reason=None,
        reference_catalog=reference_catalog,
        integration_registry=integration_registry,
        object_types=object_types,
        compiler_route_hints=compiler_route_hints,
        capability_catalog=capability_catalog,
        payload=payload,
    )


def current_compile_surface_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    resolved_root = Path(repo_root) if repo_root is not None else _repo_root_from_module()
    resolved_root = resolved_root.resolve()
    try:
        manifest = module_surface_manifest(*_compile_index_surface_paths())
    except Exception as exc:
        raise _error(
            "compile_index.surface_manifest_unavailable",
            "compile index surface manifest could not be resolved",
            details={"repo_root": str(resolved_root)},
        ) from exc

    raw_fingerprints = manifest.get("file_fingerprints")
    fingerprints = raw_fingerprints if isinstance(raw_fingerprints, dict) else {}
    relative_fingerprints: dict[str, str] = {}
    for path_text, fingerprint in fingerprints.items():
        try:
            relative_path = str(Path(path_text).resolve().relative_to(resolved_root))
        except Exception:
            relative_path = str(path_text)
        relative_fingerprints[relative_path] = _as_text(fingerprint)

    payload = {
        "repo_root": str(resolved_root),
        "surface_name": _DEFAULT_SURFACE_NAME,
        "surface_revision": _stable_hash(relative_fingerprints)[:16],
        "tracked_files": sorted(relative_fingerprints.keys()),
        "file_fingerprints": dict(sorted(relative_fingerprints.items())),
    }
    payload["surface_revision"] = f"surface_{payload['surface_revision']}"
    return payload


def _normalize_reference_row(row: object) -> dict[str, Any]:
    item = _json_object(row)
    ref_type = _slugify(item.get("ref_type"))
    slug = _as_text(item.get("slug"))
    return {
        "slug": _normalize_reference_slug(ref_type, slug),
        "ref_type": ref_type,
        "display_name": _as_text(item.get("display_name")),
        "resolved_id": _as_text(item.get("resolved_id")),
        "resolved_table": _as_text(item.get("resolved_table")),
        "description": _as_text(item.get("description")),
    }


def _normalize_reference_slug(ref_type: str, slug: str) -> str:
    ref_type = _slugify(ref_type)
    slug = _as_text(slug)
    if not ref_type or not slug:
        return ""
    if ref_type == "integration":
        if slug.startswith("@"):
            return slug
        if "/" in slug:
            return f"@{slug.lstrip('@')}"
        return f"@{slug}"
    if ref_type == "object":
        if slug.startswith("#"):
            return slug
        if "/" in slug:
            return f"#{slug.lstrip('#')}"
        return f"#{slug}"
    if ref_type == "agent":
        return _slugify(slug)
    return slug


def _normalize_integration_row(row: object) -> dict[str, Any]:
    item = _json_object(row)
    raw_capabilities = _json_list(item.get("capabilities"))
    capabilities: list[dict[str, Any]] = []
    for capability in raw_capabilities:
        if isinstance(capability, str):
            action = _slugify(capability)
            if action:
                capabilities.append({"action": action})
            continue
        if isinstance(capability, dict):
            action = _slugify(capability.get("action"))
            if not action:
                continue
            capabilities.append(
                {
                    "action": action,
                    "description": _as_text(capability.get("description")),
                    "inputs": capability.get("inputs") if isinstance(capability.get("inputs"), list) else [],
                    "requiredArgs": capability.get("requiredArgs")
                    if isinstance(capability.get("requiredArgs"), list)
                    else [],
                }
            )
    return {
        "id": _slugify(item.get("id")),
        "name": base_integration_name(item),
        "display_name": display_name_for_integration(item),
        "provider": _as_text(item.get("provider")),
        "auth_status": _as_text(item.get("auth_status")),
        "description": _as_text(item.get("description")),
        "icon": _as_text(item.get("icon")),
        "mcp_server_id": _as_text(item.get("mcp_server_id")),
        "capabilities": capabilities,
    }


def _normalize_object_type_row(row: object) -> dict[str, Any]:
    item = _json_object(row)
    raw_fields = _json_list(item.get("property_definitions"))
    fields: list[dict[str, Any]] = []
    for field in raw_fields:
        if not isinstance(field, dict):
            continue
        name = _slugify(field.get("name"))
        if not name:
            continue
        fields.append(
            {
                "name": name,
                "label": _as_text(field.get("label")) or _as_text(field.get("name")),
                "type": _as_text(field.get("type")),
                "description": _as_text(field.get("description")),
                "required": bool(field.get("required")),
            }
        )
    return {
        "type_id": _slugify(item.get("type_id")),
        "name": _as_text(item.get("name")),
        "description": _as_text(item.get("description")),
        "icon": _as_text(item.get("icon")),
        "fields": fields,
    }


def _normalize_route_hint_row(row: object) -> tuple[str, str]:
    item = _json_object(row)
    hint_text = _as_text(item.get("hint_text")).lower()
    route_slug = _as_text(item.get("route_slug"))
    return (hint_text, route_slug)


def _normalize_capability_row(row: object) -> dict[str, Any]:
    item = _json_object(row)
    return {
        "id": _as_text(item.get("id") or item.get("capability_ref")),
        "capability_ref": _as_text(item.get("capability_ref") or item.get("id")),
        "slug": _as_text(item.get("slug") or item.get("capability_slug")),
        "capability_slug": _as_text(item.get("capability_slug") or item.get("slug")),
        "kind": _as_text(item.get("kind") or item.get("capability_kind")),
        "capability_kind": _as_text(item.get("capability_kind") or item.get("kind")),
        "title": _as_text(item.get("title")),
        "summary": _as_text(item.get("summary")),
        "description": _as_text(item.get("description")),
        "route": _as_text(item.get("route")),
        "engines": _json_list(item.get("engines")),
        "signals": _json_list(item.get("signals")),
        "reference_slugs": _json_list(item.get("reference_slugs")),
        "enabled": bool(item.get("enabled", True)),
        "binding_revision": _as_text(item.get("binding_revision")),
        "decision_ref": _as_text(item.get("decision_ref")),
    }


def _load_reference_catalog(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT slug, ref_type, display_name, resolved_id, resolved_table, description
          FROM reference_catalog
         ORDER BY ref_type, slug
        """
    )
    return [_normalize_reference_row(row) for row in (rows or [])]


def _load_integrations(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, provider, capabilities, auth_status, description, icon, mcp_server_id
          FROM integration_registry
         ORDER BY name
        """
    )
    return [_normalize_integration_row(row) for row in (rows or [])]


def _load_object_types(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT type_id, name, description, icon, property_definitions
          FROM object_types
         ORDER BY name
        """
    )
    return [_normalize_object_type_row(row) for row in (rows or [])]


def _load_compiler_route_hints(conn: Any) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT hint_text, route_slug
          FROM compiler_route_hints
         WHERE enabled = TRUE
         ORDER BY priority ASC, hint_text ASC
        """
    )
    hints: list[tuple[str, str]] = []
    for row in rows or []:
        hint_text = _as_text(row.get("hint_text")).lower()
        route_slug = _as_text(row.get("route_slug"))
        if hint_text and route_slug:
            hints.append((hint_text, route_slug))
    return hints


__all__ = [
    "CompileIndexAuthorityError",
    "CompileIndexSnapshot",
    "current_repo_fingerprint",
    "load_compile_index_snapshot",
    "refresh_compile_index",
]
