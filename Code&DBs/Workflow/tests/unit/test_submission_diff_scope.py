from __future__ import annotations

from unittest.mock import MagicMock

from runtime.workflow.submission_diff import _measured_operations


def test_measured_operations_ignores_out_of_scope_file_changes(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.workflow.submission_diff._workspace_manifest",
        lambda _workspace_root: {
            "src/main.py": [100, 200],
            "src/new_file.py": [300, 400],
            "tmp/scratch.log": [500, 600],
        },
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_diff._read_artifact_text",
        lambda _path: "payload",
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_diff._hash_file",
        lambda _path: "sha256:new",
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_diff.ArtifactStore",
        lambda _conn: MagicMock(get_content=lambda _artifact_id: "old"),
    )

    changed_paths, operation_set, out_of_scope, _diff_ref = _measured_operations(
        conn=MagicMock(),
        workspace_root="/workspace",
        write_scope=("src/",),
        baseline={
            "workspace_manifest": {
                "src/main.py": [100, 200],
            },
            "scoped_artifacts": {
                "src/main.py": {"artifact_id": "artifact-old", "sha256": "sha-old"},
            },
        },
    )

    assert changed_paths == ["src/new_file.py"]
    assert operation_set == [{"path": "src/new_file.py", "action": "create"}]
    assert out_of_scope == []
