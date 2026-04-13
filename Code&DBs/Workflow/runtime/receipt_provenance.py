"""Shared helpers for receipt mutation and repo provenance."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _normalize_write_paths(raw_paths: object) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    if isinstance(raw_paths, str):
        raw_paths = [raw_paths]
    if not isinstance(raw_paths, (list, tuple, set)):
        return []
    for raw_path in raw_paths:
        text = str(raw_path or "").strip()
        if not text:
            continue
        if text.startswith("file:"):
            text = text[5:]
        if text not in seen:
            seen.add(text)
            paths.append(text)
    return paths


def extract_write_paths(*candidates: object) -> list[str]:
    """Return a stable de-duplicated list of candidate write paths."""

    paths: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if isinstance(candidate, list):
            if candidate and all(isinstance(item, dict) for item in candidate):
                for entry in candidate:
                    key = str(entry.get("key") or "").strip()
                    mode = str(entry.get("mode") or "write").strip().lower()
                    if not key.startswith("file:") or mode == "read":
                        continue
                    path = key[5:]
                    if path and path not in seen:
                        seen.add(path)
                        paths.append(path)
                continue
        for path in _normalize_write_paths(candidate):
            if path not in seen:
                seen.add(path)
                paths.append(path)
    return paths


def build_workspace_provenance(
    *,
    workspace_root: str | Path | None,
    workspace_ref: str | None = None,
    runtime_profile_ref: str | None = None,
    packet_provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_root = Path(workspace_root).resolve() if workspace_root else None
    payload: dict[str, Any] = {
        "workspace_root": str(resolved_root) if resolved_root is not None else "",
        "workspace_ref": str(workspace_ref or ""),
        "runtime_profile_ref": str(runtime_profile_ref or ""),
        "captured_at": _utc_now().isoformat(),
    }
    if isinstance(packet_provenance, dict) and packet_provenance:
        payload["packet_provenance"] = _json_safe(packet_provenance)
    return payload


def build_git_provenance(
    *,
    workspace_root: str | Path | None,
    workspace_ref: str | None = None,
    runtime_profile_ref: str | None = None,
    packet_provenance: dict[str, Any] | None = None,
    conn=None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "captured_at": _utc_now().isoformat(),
    }
    try:
        if conn is not None:
            from runtime.repo_snapshot_store import record_repo_snapshot

            payload.update(
                record_repo_snapshot(
                    conn=conn,
                    workspace_root=workspace_root,
                    workspace_ref=workspace_ref,
                    runtime_profile_ref=runtime_profile_ref,
                    packet_provenance=packet_provenance,
                )
            )
        else:
            from runtime.compile_index import current_repo_fingerprint

            payload.update(_json_safe(current_repo_fingerprint(workspace_root)))
        payload["available"] = True
        return payload
    except Exception as exc:
        payload["available"] = False
        payload["reason_code"] = "git_provenance_unavailable"
        payload["error"] = str(exc)[:200]
        return payload


def build_write_manifest(
    *,
    workspace_root: str | Path | None,
    write_paths: list[str] | tuple[str, ...],
    source: str,
) -> dict[str, Any]:
    resolved_root = Path(workspace_root).resolve() if workspace_root else None
    results: list[dict[str, Any]] = []
    for write_path in extract_write_paths(write_paths):
        absolute_path = (
            (resolved_root / write_path).resolve()
            if resolved_root is not None and not Path(write_path).is_absolute()
            else Path(write_path).resolve()
        )
        within_workspace = (
            resolved_root is not None
            and (
                absolute_path == resolved_root
                or resolved_root in absolute_path.parents
            )
        )
        entry: dict[str, Any] = {
            "file_path": write_path,
            "absolute_path": str(absolute_path),
            "source": source,
            "within_workspace": within_workspace,
            "exists": absolute_path.exists(),
            "is_file": absolute_path.is_file(),
        }
        if absolute_path.is_file():
            content = absolute_path.read_bytes()
            entry["bytes"] = len(content)
            entry["content_sha256"] = hashlib.sha256(content).hexdigest()
        results.append(entry)
    return {
        "workspace_root": str(resolved_root) if resolved_root is not None else "",
        "source": source,
        "captured_at": _utc_now().isoformat(),
        "total_files": len(results),
        "existing_files": sum(1 for row in results if row.get("exists")),
        "results": results,
    }


def build_mutation_provenance(
    *,
    workspace_root: str | Path | None,
    write_paths: list[str] | tuple[str, ...],
    touch_entries: list[dict[str, Any]] | None = None,
    source: str,
) -> dict[str, Any]:
    normalized_paths = extract_write_paths(write_paths)
    return {
        "workspace_root": str(Path(workspace_root).resolve()) if workspace_root else "",
        "source": source,
        "is_mutating": bool(normalized_paths),
        "write_paths": normalized_paths,
        "write_count": len(normalized_paths),
        "touch_entries": _json_safe(touch_entries or []),
        "captured_at": _utc_now().isoformat(),
    }
