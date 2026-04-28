"""Canonical workspace path helpers.

Every path-resolution seam in the runtime routes through this module. The
on-disk tree names live in ``config/workspace_layout.json`` at repo root so
that no other module hardcodes ``Code&DBs`` / ``Workflow`` / ``Databases``.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Mapping

_LAYOUT_FILENAME = "workspace_layout.json"
_HOST_WORKSPACE_ROOT_ENV = "PRAXIS_HOST_WORKSPACE_ROOT"
_WORKSPACE_BASE_PATH_ENV = "PRAXIS_WORKSPACE_BASE_PATH"
_CONTAINER_WORKSPACE_ROOT_ENV = "PRAXIS_CONTAINER_WORKSPACE_ROOT"
_CONTAINER_HOME_ENV = "PRAXIS_CONTAINER_HOME"
_CONTAINER_AUTH_SEED_DIR_ENV = "PRAXIS_CONTAINER_AUTH_SEED_DIR"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def repo_root() -> Path:
    """Repo root for the checked-out Praxis workspace."""
    return _repo_root()


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
    candidates = [root / canonical, *(root / alias for alias in tree_aliases())]
    workflow_subdir = _subdir("workflow")
    for candidate in candidates:
        if (candidate / workflow_subdir / "runtime").is_dir():
            return candidate
    if (root / canonical).exists():
        return root / canonical
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


def scratch_path(name: str, *, repo_root: Path | None = None) -> Path:
    scratch_paths = _layout()["scratch_paths"]
    assert isinstance(scratch_paths, Mapping)
    relative = str(scratch_paths[name])
    return code_tree_root(repo_root) / relative


def module_index_subdirs(*, repo_root: Path | None = None) -> tuple[str, ...]:
    """Canonical repo refs the semantic module indexer should scan by default."""
    configured = _layout()["module_index_subdirs"]
    assert isinstance(configured, list)
    root = (repo_root or _repo_root()).resolve()
    workflow_dir = workflow_root(root)
    return tuple(
        to_repo_ref(workflow_dir / str(subdir), repo_root=root)
        for subdir in configured
    )


def container_workspace_root(*, env: Mapping[str, str] | None = None) -> Path:
    """Canonical workspace root inside sandbox/container execution."""
    source = env if env is not None else os.environ
    configured = str(source.get(_CONTAINER_WORKSPACE_ROOT_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    execution_mounts = _layout()["execution_mounts"]
    assert isinstance(execution_mounts, Mapping)
    return Path(str(execution_mounts["container_workspace_root"])).expanduser()


def container_home(*, env: Mapping[str, str] | None = None) -> Path:
    """Canonical home directory inside sandbox/container execution."""
    source = env if env is not None else os.environ
    configured = str(source.get(_CONTAINER_HOME_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    execution_mounts = _layout()["execution_mounts"]
    assert isinstance(execution_mounts, Mapping)
    return Path(str(execution_mounts["container_home"])).expanduser()


def container_auth_seed_dir(*, env: Mapping[str, str] | None = None) -> Path:
    """Root for root-readable auth seed files inside sandbox containers."""
    source = env if env is not None else os.environ
    configured = str(source.get(_CONTAINER_AUTH_SEED_DIR_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    execution_mounts = _layout()["execution_mounts"]
    assert isinstance(execution_mounts, Mapping)
    return Path(str(execution_mounts["container_auth_seed_dir"])).expanduser()


def _repo_env_value(name: str) -> str | None:
    env_path = _repo_root() / ".env"
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        if key.strip() != name:
            continue
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        return value.strip() or None
    return None


def authority_workspace_roots(*, env: Mapping[str, str] | None = None) -> tuple[Path, ...]:
    """Workspace roots asserted by runtime authority, ordered by precedence."""
    source = env if env is not None else os.environ
    roots: list[Path] = []

    def _append(value: object) -> None:
        if not isinstance(value, (str, Path)):
            return
        raw = str(value).strip()
        if not raw:
            return
        candidate = Path(raw).expanduser()
        try:
            candidate = candidate.resolve()
        except OSError:
            candidate = candidate.absolute()
        if candidate not in roots:
            roots.append(candidate)

    _append(source.get(_HOST_WORKSPACE_ROOT_ENV))
    _append(source.get(_WORKSPACE_BASE_PATH_ENV))
    _append(_repo_env_value(_WORKSPACE_BASE_PATH_ENV))
    try:
        from runtime.instance import native_instance_contract
        from runtime.instance import NativeInstanceResolutionError
    except ImportError:
        native_instance_contract = None
        NativeInstanceResolutionError = None
    if native_instance_contract is not None:
        try:
            contract = native_instance_contract(env=source)
        except Exception as exc:
            if NativeInstanceResolutionError is None or not isinstance(exc, NativeInstanceResolutionError):
                raise
        else:
            _append(contract.get("repo_root"))
            _append(contract.get("workdir"))
    _append(_repo_root())
    return tuple(roots)


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
