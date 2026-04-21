from __future__ import annotations

from typing import Any

import pytest

from runtime.workflow import submission_capture
from runtime.workflow.submission_gate import resolve_submission_for_job


class _Conn:
    pass


def _bundle(*, result_kind: str = "research_result") -> dict[str, Any]:
    return {
        "completion_contract": {
            "submission_required": True,
            "result_kind": result_kind,
        }
    }


def test_submission_gate_fails_closed_when_auto_seal_service_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        submission_capture,
        "attach_verification_artifact_refs_for_job",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        submission_capture,
        "get_submission_for_job_attempt",
        lambda *_args, **_kwargs: None,
    )

    def _raise_seal_failure(**_kwargs):
        raise submission_capture.WorkflowSubmissionServiceError(
            "workflow_submission.repository_unavailable",
            "repository is unavailable",
        )

    monkeypatch.setattr(
        submission_capture,
        "submit_research_result",
        _raise_seal_failure,
    )
    result = resolve_submission_for_job(
        _Conn(),
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job.alpha",
        attempt_no=1,
        execution_bundle=_bundle(),
        result={"stdout": "finished work", "stderr": ""},
        final_status="succeeded",
        final_error_code="",
        verification_artifact_refs=[],
    )

    assert result.submission_state is None
    assert result.final_status == "failed"
    assert result.final_error_code == "workflow_submission.repository_unavailable"
    assert "submission auto-seal failed: repository is unavailable" in result.result["stderr"]


def test_submission_gate_reports_final_lookup_outage_instead_of_required_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def _lookup(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        raise RuntimeError("submission repository offline")

    monkeypatch.setattr(
        submission_capture,
        "attach_verification_artifact_refs_for_job",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        submission_capture,
        "get_submission_for_job_attempt",
        _lookup,
    )
    result = resolve_submission_for_job(
        _Conn(),
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job.alpha",
        attempt_no=1,
        execution_bundle=_bundle(),
        result={"stdout": "", "stderr": ""},
        final_status="succeeded",
        final_error_code="",
        verification_artifact_refs=[],
    )

    assert calls == 2
    assert result.submission_state is None
    assert result.final_status == "failed"
    assert result.final_error_code == "workflow_submission.lookup_failed"
    assert "submission lookup failed before final enforcement" in result.result["stderr"]
    assert "submission repository offline" in result.result["stderr"]
