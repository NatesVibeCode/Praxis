from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from runtime.workflow.submission_capture import _submit_submission, WorkflowSubmissionServiceError

def test_submit_submission_rejects_phantom_ship(monkeypatch) -> None:
    # Setup mocks
    mock_conn = MagicMock()
    mock_repo = MagicMock()
    monkeypatch.setattr("runtime.workflow.submission_capture._repo", lambda conn: (mock_conn, mock_repo))
    monkeypatch.setattr("runtime.workflow.submission_capture._current_job_row", lambda *args, **kwargs: {"attempt": 1})
    
    execution_context_shard = {"write_scope": ["src"]}
    execution_bundle = {}
    monkeypatch.setattr("runtime.workflow.submission_capture._load_runtime_context_state", 
                        lambda *args, **kwargs: (execution_context_shard, execution_bundle, None))
    monkeypatch.setattr("runtime.workflow.submission_capture._submission_protocol_state",
                        lambda *args: {"baseline": {"workspace_root": "/tmp/workspace", "write_scope": ["src"]}})
    
    # Mock _measured_operations to return NO changed paths (this is the phantom ship condition)
    # Returns: changed_paths, operation_set, out_of_scope, diff_artifact_ref
    monkeypatch.setattr("runtime.workflow.submission_capture._measured_operations",
                        lambda *args, **kwargs: ([], [], [], None))
    
    # Expect a WorkflowSubmissionServiceError for "phantom_ship"
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _submit_submission(
            run_id="run1",
            workflow_id="wf1",
            job_label="job1",
            summary="Did some work",
            primary_paths=["src/main.py"],
            result_kind="code_change",
            conn=mock_conn,
        )
        
    assert exc_info.value.reason_code == "workflow_submission.phantom_ship"
    assert "no files were changed on disk" in str(exc_info.value)
