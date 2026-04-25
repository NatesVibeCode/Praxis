from __future__ import annotations

from runtime.module_indexer import default_index_subdirs, walk_codebase


def test_walk_codebase_skips_artifact_trees(tmp_path) -> None:
    source_dir = tmp_path / "Code&DBs" / "Workflow" / "runtime"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "real_module.py"
    source_file.write_text(
        "def live_authority():\n    return 'source'\n",
        encoding="utf-8",
    )

    artifact_dir = tmp_path / "artifacts" / "sandbox_cleanup_20260414" / "variant"
    artifact_dir.mkdir(parents=True)
    artifact_file = artifact_dir / "stale_module.py"
    artifact_file.write_text(
        "def stale_authority():\n    return 'artifact'\n",
        encoding="utf-8",
    )

    units = walk_codebase(str(tmp_path))
    module_paths = {unit.module_path for unit in units}

    assert "Code&DBs/Workflow/runtime/real_module.py" in module_paths
    assert not any(path.startswith("artifacts/") for path in module_paths)


def test_default_index_subdirs_follow_workspace_layout(tmp_path) -> None:
    refs = default_index_subdirs(tmp_path)

    assert refs == [
        "Code&DBs/Workflow/runtime",
        "Code&DBs/Workflow/memory",
        "Code&DBs/Workflow/storage",
        "Code&DBs/Workflow/surfaces",
        "Code&DBs/Workflow/adapters",
        "Code&DBs/Workflow/registry",
        "Code&DBs/Workflow/observability",
    ]
