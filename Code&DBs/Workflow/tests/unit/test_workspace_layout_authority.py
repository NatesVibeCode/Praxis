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
    authority_workspace_roots,
    code_tree_dirname,
    code_tree_root,
    container_auth_seed_dir,
    container_home,
    container_workspace_root,
    databases_root,
    log_path,
    scratch_path,
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
    assert "scratch_paths" in layout
    assert "execution_mounts" in layout


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
    supervisor = log_path("supervisor_stdout")
    assert supervisor.name == "praxis-engine.log"
    assert "Workflow/artifacts/logs" in str(supervisor)


def test_scratch_path_resolves() -> None:
    atlas = scratch_path("atlas_heuristic_map")
    assert atlas.name == "atlas_heuristic_map.json"
    assert "Workflow/artifacts/workflow" in str(atlas)
    sandbox = scratch_path("workflow_sandbox_root")
    assert sandbox.name == "workflow-sandbox"
    assert "Workflow/artifacts" in str(sandbox)


def test_container_workspace_root_comes_from_layout_authority() -> None:
    assert container_workspace_root() == Path(_layout()["execution_mounts"]["container_workspace_root"])


def test_container_workspace_root_allows_explicit_authority_override() -> None:
    assert container_workspace_root(env={"PRAXIS_CONTAINER_WORKSPACE_ROOT": "/sandbox"}) == Path("/sandbox")


def test_container_home_comes_from_layout_authority() -> None:
    assert container_home() == Path(_layout()["execution_mounts"]["container_home"])


def test_container_auth_seed_dir_comes_from_layout_authority() -> None:
    assert container_auth_seed_dir() == Path(_layout()["execution_mounts"]["container_auth_seed_dir"])


def test_authority_workspace_roots_includes_explicit_host_override() -> None:
    roots = authority_workspace_roots(env={"PRAXIS_HOST_WORKSPACE_ROOT": "/host/workspace"})
    assert roots[0] == Path("/host/workspace")


def test_authority_workspace_roots_includes_launcher_resolved_repo_root() -> None:
    roots = authority_workspace_roots(
        env={"PRAXIS_LAUNCHER_RESOLVED_REPO_ROOT": "/authority/resolved/praxis"}
    )

    assert roots[0] == Path("/authority/resolved/praxis")


def test_authority_workspace_roots_propagates_instance_authority_errors(monkeypatch) -> None:
    import runtime.instance as instance_module

    def _boom(*, env=None):
        raise RuntimeError("native authority exploded")

    monkeypatch.setattr(instance_module, "native_instance_contract", _boom)

    with pytest.raises(RuntimeError, match="native authority exploded"):
        authority_workspace_roots(env={})


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
