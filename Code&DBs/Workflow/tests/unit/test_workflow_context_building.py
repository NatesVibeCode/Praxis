from __future__ import annotations

from runtime.workflow import _context_building as context_building


class _Conn:
    def execute(self, *_args, **_kwargs):
        return []


def test_context_shard_resolves_scopes_against_container_workspace_when_host_root_is_absent(
    monkeypatch,
    tmp_path,
) -> None:
    container_root = tmp_path / "workspace"
    target = container_root / "Code&DBs" / "Workflow" / "runtime" / "spec_compiler.py"
    target.parent.mkdir(parents=True)
    target.write_text("VALUE = 1\n", encoding="utf-8")

    monkeypatch.setattr(
        context_building,
        "container_workspace_root",
        lambda: container_root,
    )
    resolution_root = context_building._scope_resolution_root(
        repo_root=str(tmp_path / "missing_host"),
        scope_paths=["Code&DBs/Workflow/runtime/spec_compiler.py"],
    )

    shard = context_building._job_execution_context_shard(
        conn=_Conn(),
        job={
            "label": "proof",
            "write_scope": ["Code&DBs/Workflow/runtime/spec_compiler.py"],
        },
        spec_verify_refs=[],
        repo_root=str(tmp_path / "missing_host"),
        proof_snapshot={},
    )

    assert resolution_root == str(container_root)
    assert "scope_resolution_error" not in shard
    assert shard["write_scope"] == ["Code&DBs/Workflow/runtime/spec_compiler.py"]
    assert shard["metrics"]["write_scope_count"] == 1


def test_context_shard_skips_workspace_root_scope_resolution(tmp_path) -> None:
    shard = context_building._job_execution_context_shard(
        conn=_Conn(),
        job={
            "label": "broad",
            "write_scope": ["."],
        },
        spec_verify_refs=[],
        repo_root=str(tmp_path),
        proof_snapshot={},
    )

    assert shard["write_scope"] == ["."]
    assert shard["scope_resolution_skipped"]["reason_code"] == "scope.workspace_root_too_broad"
    assert shard["metrics"]["scope_resolution_skipped"] == 1
    assert "resolved_read_scope" not in shard
    rendered = context_building._render_execution_context_shard(shard)
    assert "scope_resolution_skipped" in rendered
