"""Canonical workspace path helpers.

Every path-resolution seam in the runtime routes through this module. The
on-disk tree names live in ``config/workspace_layout.json`` at repo root so
that no other module hardcodes ``Code&DBs`` / ``Workflow`` / ``Databases``.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Mapping

_LAYOUT_FILENAME = "workspace_layout.json"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def _layout() -> Mapping[str, object]:
    layout_path = _repo_root() / "config" / _LAYOUT_FILENAME
    with layout_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def code_tree_dirname() -> str:
    """Canonical on-disk dirname for the code/db tree (e.g. ``Code&DBs``)."""
    code_tree = _layout()["code_tree"]
    assert isinstance(code_tree, Mapping)
    return str(code_tree["canonical"])


def tree_aliases() -> tuple[str, ...]:
    """Alternate dirnames that may appear in stored or symlinked paths."""
    code_tree = _layout()["code_tree"]
    assert isinstance(code_tree, Mapping)
    aliases = code_tree.get("aliases", ())
    return tuple(str(alias) for alias in aliases)


def _subdir(name: str) -> str:
    subdirs = _layout()["subdirs"]
    assert isinstance(subdirs, Mapping)
    return str(subdirs[name])


def code_tree_root(repo_root: Path | None = None) -> Path:
    root = (repo_root or _repo_root()).resolve()
    canonical = code_tree_dirname()
    for alias in tree_aliases():
        candidate = root / alias
        if candidate.exists():
            return candidate
    return root / canonical


def workflow_root(repo_root: Path | None = None) -> Path:
    return code_tree_root(repo_root) / _subdir("workflow")


def databases_root(repo_root: Path | None = None) -> Path:
    return code_tree_root(repo_root) / _subdir("databases")


def workflow_migrations_root(repo_root: Path | None = None) -> Path:
    return (repo_root or _repo_root()).resolve() / code_tree_dirname() / _subdir(
        "workflow_migrations"
    )


def log_path(name: str, *, repo_root: Path | None = None) -> Path:
    log_paths = _layout()["log_paths"]
    assert isinstance(log_paths, Mapping)
    relative = str(log_paths[name])
    return code_tree_root(repo_root) / relative


def to_repo_ref(path: Path | str, *, repo_root: Path | None = None) -> str:
    """Return the canonical repo-relative form for ``path``.

    Always starts with the canonical tree dirname (never an alias, never an
    absolute path). Use this at every emit site that writes a path into a DB
    row, JSON receipt, log line, or manifest.
    """
    root = (repo_root or _repo_root()).resolve()
    canonical = code_tree_dirname()
    raw = str(path).replace("\\", "/")
    candidate = Path(raw)
    if candidate.is_absolute():
        try:
            relative = candidate.resolve().relative_to(root)
        except ValueError:
            relative = Path(raw)
        rel_str = str(relative)
    else:
        rel_str = raw.lstrip("./")

    for alias in tree_aliases():
        prefix = f"{alias}/"
        if rel_str.startswith(prefix):
            rel_str = f"{canonical}/{rel_str[len(prefix):]}"
            break
        if rel_str == alias:
            rel_str = canonical
            break
    return rel_str


def strip_workflow_prefix(repo_ref: str) -> str:
    """Humanize a canonical repo-ref by dropping the ``Code&DBs/Workflow/``
    prefix. Display-only — never store the result."""
    canonical = code_tree_dirname()
    workflow_subdir = _subdir("workflow")
    prefix = f"{canonical}/{workflow_subdir}/"
    if repo_ref.startswith(prefix):
        return repo_ref[len(prefix):]
    return repo_ref
