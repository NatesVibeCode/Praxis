from __future__ import annotations

from unittest.mock import MagicMock

from runtime.workflow.submission_capture import capture_submission_baseline_for_job


def test_capture_submission_baseline_scopes_workspace_manifest_to_write_scope(monkeypatch) -> None:
    conn = object()
    execution_bundle = {"completion_contract": {"submission_required": True}}

    monkeypatch.setattr(
        "runtime.workflow.submission_capture._load_runtime_context_state",
        lambda *_args, **_kwargs: ({}, {}, None),
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_capture._submission_protocol_state",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_capture._set_submission_protocol_state",
        lambda _shard, protocol: protocol,
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_capture._persist_runtime_context_state",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_capture._workspace_manifest",
        lambda _root: {
            "allowed/keep.txt": [12, 34],
            "outside/skipped.txt": [56, 78],
        },
    )
    monkeypatch.setattr(
        "runtime.workflow.submission_capture._read_artifact_text",
        lambda _path: "x",
    )

    fake_store = MagicMock()
    fake_store.capture.return_value = MagicMock(artifact_id="artifact", sha256="hash")
    monkeypatch.setattr(
        "runtime.workflow.submission_capture.ArtifactStore",
        lambda _conn: fake_store,
    )

    baseline = capture_submission_baseline_for_job(
        conn,
        run_id="run_1",
        workflow_id="wf_1",
        job_label="job_1",
        workspace_root="/workspace",
        write_scope=["allowed/"],
        execution_bundle=execution_bundle,
    )

    assert baseline["workspace_manifest"] == {"allowed/keep.txt": [12, 34]}
    assert "outside/skipped.txt" not in baseline["workspace_manifest"]
    assert fake_store.capture.call_count == 1
    assert fake_store.capture.call_args.args[0] == "allowed/keep.txt"
