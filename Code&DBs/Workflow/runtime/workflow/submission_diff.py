"""Diff and comparison helpers for workflow submission capture."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
import difflib
import hashlib
import os

from runtime.sandbox_artifacts import ArtifactStore

_IGNORED_MANIFEST_DIRS = frozenset({".git", "__pycache__", ".pytest_cache", ".mypy_cache"})


def _hash_file(path: Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data).hexdigest()


def _read_artifact_text(path: Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return data.decode("utf-8", errors="replace")


def _artifact_ref(path: str, sha256: str, *, deleted: bool = False) -> str:
    kind = "deleted" if deleted else "current"
    return f"workflow_submission_artifact:{kind}:{sha256}:{path}"


def _diff_artifact_ref(patch_text: str) -> str | None:
    if not patch_text:
        return None
    digest = hashlib.sha256(patch_text.encode("utf-8")).hexdigest()
    return f"workflow_submission_diff:{digest}"


def _workspace_manifest(workspace_root: str) -> dict[str, list[int]]:
    manifest: dict[str, list[int]] = {}
    root_path = Path(workspace_root)
    if not root_path.exists():
        return manifest
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirnames[:] = [name for name in dirnames if name not in _IGNORED_MANIFEST_DIRS]
        current_dir = Path(dirpath)
        for filename in filenames:
            absolute = current_dir / filename
            try:
                stat = absolute.stat()
            except OSError:
                continue
            relpath = absolute.relative_to(root_path).as_posix()
            manifest[relpath] = [int(stat.st_size), int(stat.st_mtime_ns)]
    return manifest


def _scope_allows_path(path: str, write_scope: Sequence[str]) -> bool:
    normalized_path = Path(path).as_posix().lstrip("./")
    for scope_path in write_scope:
        normalized_scope = Path(scope_path).as_posix().lstrip("./")
        if normalized_path == normalized_scope:
            return True
        prefix = normalized_scope.rstrip("/")
        if prefix and normalized_path.startswith(prefix + "/"):
            return True
    return False


def _normalize_path_simple(value: object) -> str:
    text = str(value or "").strip()
    if text.startswith("file:"):
        text = text[5:]
    return Path(text).as_posix().lstrip("./")


def _build_patch_for_operation(
    *,
    action: str,
    path: str,
    previous_content: str,
    current_content: str,
    from_path: str | None = None,
) -> str:
    if action == "rename":
        source = from_path or path
        return f"rename {source} -> {path}\n"
    fromfile = f"a/{from_path or path}"
    tofile = f"b/{path}"
    diff_lines = difflib.unified_diff(
        previous_content.splitlines(keepends=True),
        current_content.splitlines(keepends=True),
        fromfile=fromfile,
        tofile=tofile,
    )
    return "".join(diff_lines)


def _measured_operations(
    *,
    conn,
    workspace_root: str,
    write_scope: Sequence[str],
    baseline: Mapping[str, Any],
) -> tuple[list[str], list[dict[str, str]], list[str], str | None]:
    baseline_manifest_raw = baseline.get("workspace_manifest")
    baseline_manifest = {
        str(path): tuple(value)
        for path, value in dict(baseline_manifest_raw or {}).items()
    }
    current_manifest = _workspace_manifest(workspace_root)
    changed_all_paths = sorted(
        {
            *baseline_manifest.keys(),
            *current_manifest.keys(),
        }
        - {
            path
            for path in set(baseline_manifest.keys()) & set(current_manifest.keys())
            if tuple(baseline_manifest[path]) == tuple(current_manifest[path])
        }
    )
    out_of_scope = [
        path
        for path in changed_all_paths
        if not _scope_allows_path(path, write_scope)
    ]
    measured_changed_paths = [
        path
        for path in changed_all_paths
        if _scope_allows_path(path, write_scope)
    ]
    baseline_artifacts = dict(baseline.get("scoped_artifacts") or {})
    artifact_store = ArtifactStore(conn)

    deleted_paths = [path for path in measured_changed_paths if path in baseline_manifest and path not in current_manifest]
    created_paths = [path for path in measured_changed_paths if path in current_manifest and path not in baseline_manifest]
    updated_paths = [
        path
        for path in measured_changed_paths
        if path in baseline_manifest and path in current_manifest
    ]

    deleted_sha = {
        path: str((baseline_artifacts.get(path) or {}).get("sha256") or "")
        for path in deleted_paths
    }
    created_sha = {
        path: str(_hash_file(Path(workspace_root) / path) or "")
        for path in created_paths
    }

    operations: list[dict[str, str]] = []
    paired_deleted: set[str] = set()
    paired_created: set[str] = set()
    patch_parts: list[str] = []
    artifact_refs: list[str] = []

    for deleted_path in deleted_paths:
        sha = deleted_sha.get(deleted_path) or ""
        match_path = next(
            (
                created_path
                for created_path, created_hash in created_sha.items()
                if created_path not in paired_created and sha and created_hash == sha
            ),
            None,
        )
        if match_path is None:
            continue
        paired_deleted.add(deleted_path)
        paired_created.add(match_path)
        operations.append({"path": match_path, "action": "rename", "from_path": deleted_path})
        patch_parts.append(
            _build_patch_for_operation(
                action="rename",
                path=match_path,
                previous_content="",
                current_content="",
                from_path=deleted_path,
            )
        )
        artifact_refs.append(_artifact_ref(match_path, sha))

    for path in sorted(updated_paths):
        current_text = _read_artifact_text(Path(workspace_root) / path) or ""
        baseline_artifact_id = str((baseline_artifacts.get(path) or {}).get("artifact_id") or "")
        previous_text = artifact_store.get_content(baseline_artifact_id) or ""
        operations.append({"path": path, "action": "update"})
        patch_parts.append(
            _build_patch_for_operation(
                action="update",
                path=path,
                previous_content=previous_text,
                current_content=current_text,
            )
        )
        sha = _hash_file(Path(workspace_root) / path)
        if sha:
            artifact_refs.append(_artifact_ref(path, sha))

    for path in sorted(created_paths):
        if path in paired_created:
            continue
        current_text = _read_artifact_text(Path(workspace_root) / path) or ""
        operations.append({"path": path, "action": "create"})
        patch_parts.append(
            _build_patch_for_operation(
                action="create",
                path=path,
                previous_content="",
                current_content=current_text,
            )
        )
        sha = created_sha.get(path) or _hash_file(Path(workspace_root) / path)
        if sha:
            artifact_refs.append(_artifact_ref(path, sha))

    for path in sorted(deleted_paths):
        if path in paired_deleted:
            continue
        baseline_artifact_id = str((baseline_artifacts.get(path) or {}).get("artifact_id") or "")
        previous_text = artifact_store.get_content(baseline_artifact_id) or ""
        sha = deleted_sha.get(path) or hashlib.sha256(previous_text.encode("utf-8")).hexdigest()
        operations.append({"path": path, "action": "delete"})
        patch_parts.append(
            _build_patch_for_operation(
                action="delete",
                path=path,
                previous_content=previous_text,
                current_content="",
            )
        )
        artifact_refs.append(_artifact_ref(path, sha, deleted=True))

    operations = sorted(
        operations,
        key=lambda item: (item["path"], item["action"], item.get("from_path", "")),
    )
    artifact_refs = sorted(dict.fromkeys(artifact_refs))
    patch_text = "".join(patch_parts)
    return measured_changed_paths, operations, out_of_scope, _diff_artifact_ref(patch_text)


def _comparison_result(
    *,
    declared_operations: Sequence[Mapping[str, Any]],
    measured_operations: Sequence[Mapping[str, Any]],
) -> tuple[str, dict[str, Any]]:
    if not declared_operations:
        return (
            "not_provided",
            {
                "matched": None,
                "declared_count": 0,
                "measured_count": len(measured_operations),
            },
        )

    def _normalize_item(item: Mapping[str, Any]) -> tuple[str, str, str]:
        return (
            _normalize_path_simple(item.get("path")),
            str(item.get("action") or "").strip().lower(),
            _normalize_path_simple(item.get("from_path"))
            if item.get("from_path") is not None
            else "",
        )

    declared_set = sorted({_normalize_item(item) for item in declared_operations})
    measured_set = sorted({_normalize_item(item) for item in measured_operations})
    missing = [item for item in declared_set if item not in measured_set]
    extra = [item for item in measured_set if item not in declared_set]
    if not missing and not extra:
        return (
            "matched",
            {
                "matched": True,
                "declared_count": len(declared_set),
                "measured_count": len(measured_set),
                "missing": [],
                "extra": [],
            },
        )
    return (
        "mismatched",
        {
            "matched": False,
            "declared_count": len(declared_set),
            "measured_count": len(measured_set),
            "missing": [
                {"path": path, "action": action, **({"from_path": from_path} if from_path else {})}
                for path, action, from_path in missing
            ],
            "extra": [
                {"path": path, "action": action, **({"from_path": from_path} if from_path else {})}
                for path, action, from_path in extra
            ],
        },
    )
