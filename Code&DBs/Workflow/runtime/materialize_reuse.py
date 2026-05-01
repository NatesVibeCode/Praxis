"""Stable helpers for content-addressed compile artifact reuse."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class MaterializeReuseError(RuntimeError):
    """Raised when compile reuse fingerprints cannot be resolved safely."""


def json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def module_surface_revision(*paths: str | Path) -> str:
    return str(module_surface_manifest(*paths)["surface_revision"])


def module_surface_manifest(*paths: str | Path) -> dict[str, Any]:
    resolved_paths = [Path(path).resolve() for path in paths]
    if not resolved_paths:
        raise MaterializeReuseError("module surface revision requires at least one path")

    payload: list[dict[str, str]] = []
    for path in resolved_paths:
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise MaterializeReuseError(f"failed to read module surface {path}: {exc}") from exc
        payload.append(
            {
                "path": str(path),
                "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            }
        )
    payload.sort(key=lambda item: item["path"])

    return {
        "surface_revision": f"surface_{stable_hash(payload)[:16]}",
        "file_fingerprints": {item["path"]: item["content_hash"] for item in payload},
        "files": [item["path"] for item in payload],
    }


__all__ = [
    "MaterializeReuseError",
    "json_clone",
    "module_surface_manifest",
    "module_surface_revision",
    "stable_hash",
    "stable_json",
]
