"""Contract tests for workspace_layout.json + workspace_paths helpers.

Asserts that the JSON authority and the on-disk tree agree, and that all
emit-side helpers return canonical, alias-free, repo-relative refs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime.workspace_paths import (
    _layout,
    _repo_root,
    code_tree_dirname,
    code_tree_root,
    databases_root,
    log_path,
    strip_workflow_prefix,
    to_repo_ref,
    tree_aliases,
    workflow_migrations_root,
    workflow_root,
)


def test_layout_file_exists_and_parses() -> None:
    layout = _layout()
    assert "code_tree" in layout
    assert "subdirs" in layout
    assert "log_paths" in layout


def test_canonical_tree_dirname_matches_disk() -> None:
    root = _repo_root()
    canonical = code_tree_dirname()
    assert (root / canonical).is_dir() or any(
        (root / alias).is_dir() for alias in tree_aliases()
    ), f"neither canonical {canonical!r} nor aliases {tree_aliases()!r} exist on disk"


def test_workflow_root_exists() -> None:
    assert workflow_root().is_dir()


def test_databases_root_exists() -> None:
    assert databases_root().is_dir()


def test_workflow_migrations_root_exists() -> None:
    assert workflow_migrations_root().is_dir()


def test_log_path_resolves() -> None:
    pg = log_path("postgres")
    assert pg.name == "postgres.log"
    assert "postgres-dev" in str(pg)


def test_to_repo_ref_canonicalizes_absolute() -> None:
    canonical = code_tree_dirname()
    sample = workflow_root() / "runtime" / "workspace_paths.py"
    ref = to_repo_ref(sample)
    assert ref.startswith(f"{canonical}/")
    assert ref.endswith("runtime/workspace_paths.py")


def test_to_repo_ref_rewrites_alias_prefix() -> None:
    canonical = code_tree_dirname()
    for alias in tree_aliases():
        rewritten = to_repo_ref(f"{alias}/Workflow/runtime/foo.py")
        assert rewritten == f"{canonical}/Workflow/runtime/foo.py"


def test_to_repo_ref_passes_through_canonical() -> None:
    canonical = code_tree_dirname()
    ref = to_repo_ref(f"{canonical}/Workflow/runtime/foo.py")
    assert ref == f"{canonical}/Workflow/runtime/foo.py"


def test_strip_workflow_prefix_humanizes() -> None:
    canonical = code_tree_dirname()
    assert (
        strip_workflow_prefix(f"{canonical}/Workflow/runtime/intake.py")
        == "runtime/intake.py"
    )


def test_strip_workflow_prefix_leaves_others_alone() -> None:
    assert strip_workflow_prefix("config/workspace_layout.json") == (
        "config/workspace_layout.json"
    )
