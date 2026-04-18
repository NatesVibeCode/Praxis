"""Round-trip tests for the canonical repo-ref helper.

Every emit site (receipts, manifests, indexer rows, log lines) must produce a
canonical form that starts with the canonical tree dirname. Aliases and
absolute paths must be normalised at the boundary.
"""

from __future__ import annotations

from pathlib import Path

from runtime.workspace_paths import (
    _repo_root,
    code_tree_dirname,
    code_tree_root,
    to_repo_ref,
    tree_aliases,
)


def test_round_trip_absolute_to_canonical_starts_with_tree() -> None:
    canonical = code_tree_dirname()
    sample = code_tree_root() / "Workflow" / "runtime" / "workspace_paths.py"
    ref = to_repo_ref(sample)
    assert ref.startswith(f"{canonical}/")
    assert "/" in ref
    assert ref.endswith("workspace_paths.py")


def test_alias_input_rewrites_to_canonical() -> None:
    canonical = code_tree_dirname()
    for alias in tree_aliases() or ():
        rewritten = to_repo_ref(f"{alias}/Workflow/runtime/foo.py")
        assert rewritten.startswith(f"{canonical}/")
        assert not rewritten.startswith(alias + "/")


def test_canonical_input_passes_through() -> None:
    canonical = code_tree_dirname()
    ref = f"{canonical}/Workflow/runtime/intake.py"
    assert to_repo_ref(ref) == ref


def test_path_outside_tree_round_trips() -> None:
    repo_root = _repo_root()
    sample = repo_root / "config" / "workspace_layout.json"
    ref = to_repo_ref(sample)
    assert ref == "config/workspace_layout.json"
