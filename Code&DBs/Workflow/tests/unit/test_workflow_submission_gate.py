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


def test_submission_gate_fails_closed_when_required_submission_is_missing(
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
    assert result.final_error_code == "workflow_submission.required_missing"
    assert "submission_required=true but no sealed submission exists" in result.result["stderr"]


def test_submission_gate_requires_verification_after_submission_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sealed_submission = {
        "submission_id": "submission.alpha",
        "acceptance_status": "passed",
        "acceptance_report": {},
    }
    monkeypatch.setattr(
        submission_capture,
        "attach_verification_artifact_refs_for_job",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        submission_capture,
        "get_submission_for_job_attempt",
        lambda *_args, **_kwargs: sealed_submission,
    )

    bundle = _bundle()
    bundle["completion_contract"]["verification_required"] = True
    result = resolve_submission_for_job(
        _Conn(),
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job.alpha",
        attempt_no=1,
        execution_bundle=bundle,
        result={"stdout": "finished work", "stderr": ""},
        final_status="succeeded",
        final_error_code="",
        verification_artifact_refs=[],
    )

    assert result.submission_state == sealed_submission
    assert result.final_status == "failed"
    assert result.final_error_code == "verification.required_not_run"
    assert "verification_required=true but no verify_refs were executed" in result.result["stderr"]


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


def test_submission_gate_can_precheck_submission_before_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sealed_submission = {
        "submission_id": "submission.alpha",
        "acceptance_status": "passed",
        "acceptance_report": {},
    }
    monkeypatch.setattr(
        submission_capture,
        "attach_verification_artifact_refs_for_job",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        submission_capture,
        "get_submission_for_job_attempt",
        lambda *_args, **_kwargs: sealed_submission,
    )

    bundle = _bundle(result_kind="code_change")
    bundle["completion_contract"]["verification_required"] = True
    result = resolve_submission_for_job(
        _Conn(),
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job.alpha",
        attempt_no=1,
        execution_bundle=bundle,
        result={"stdout": "finished work", "stderr": ""},
        final_status="succeeded",
        final_error_code="",
        verification_artifact_refs=[],
        enforce_verification_contract=False,
        enforce_acceptance_contract=False,
    )

    assert result.submission_state == sealed_submission
    assert result.final_status == "succeeded"
    assert result.final_error_code == ""
