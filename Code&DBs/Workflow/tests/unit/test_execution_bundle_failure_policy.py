from __future__ import annotations

from datetime import datetime, timezone

import pytest

from adapters.deterministic import DeterministicTaskRequest
from adapters.task_profiles import TaskProfile
from runtime._helpers import _fail
from runtime.workflow.execution_bundle import _completion_contract, build_execution_bundle


@pytest.fixture(autouse=True)
def _task_profile_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "runtime.workflow.execution_bundle.resolve_profile",
        lambda task_type: TaskProfile(
            task_type=task_type,
            allowed_tools=("Read", "Write"),
            default_tier="mid",
            file_attach=False,
            system_prompt_hint=f"{task_type} profile",
        ),
    )


def test_architecture_jobs_do_not_auto_require_submission() -> None:
    contract = _completion_contract(
        task_type="architecture",
        bucket="architecture",
        submission_required=None,
        downstream_labels=(),
        verify_refs=(),
    )

    assert contract["submission_required"] is False
    assert contract["result_kind"] == "research_result"
    assert contract["submit_tool_names"] == []


def test_mutating_jobs_require_sealed_code_submission_by_default() -> None:
    contract = _completion_contract(
        task_type="build",
        bucket="build",
        submission_required=None,
        downstream_labels=(),
        verify_refs=("verify.job.local",),
    )

    assert contract["submission_required"] is True
    assert contract["verification_required"] is True
    assert contract["result_kind"] == "code_change"
    assert contract["submit_tool_names"] == [
        "praxis_submit_code_change",
        "praxis_get_submission",
    ]


def test_write_scope_requires_sealed_code_submission_even_without_mutating_task_type() -> None:
    bundle = build_execution_bundle(
        job_label="job.alpha",
        prompt="Audit and patch the declared file.",
        task_type="architecture",
        write_scope=("runtime/example.py",),
        verify_refs=("verify.job.alpha",),
        submission_required=False,
        verification_required=False,
    )

    contract = bundle["completion_contract"]
    assert contract["submission_required"] is True
    assert contract["verification_required"] is True
    assert contract["result_kind"] == "code_change"
    assert contract["submit_tool_names"] == [
        "praxis_submit_code_change",
        "praxis_get_submission",
    ]


def test_mutating_jobs_require_verification_even_when_verify_refs_missing() -> None:
    contract = _completion_contract(
        task_type="build",
        bucket="build",
        submission_required=None,
        downstream_labels=(),
        verify_refs=(),
    )

    assert contract["submission_required"] is True
    assert contract["verification_required"] is True


def test_explicit_submission_requirement_is_still_honored() -> None:
    contract = _completion_contract(
        task_type="architecture",
        bucket="architecture",
        submission_required=True,
        downstream_labels=(),
        verify_refs=(),
    )

    assert contract["submission_required"] is True
    assert contract["submit_tool_names"] == [
        "praxis_submit_research_result",
        "praxis_get_submission",
    ]


def test_fail_normalizes_blank_failure_code_for_deterministic_results() -> None:
    request = DeterministicTaskRequest(
        node_id="node.alpha",
        task_name="phase_alpha",
        input_payload={},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="boundary.alpha",
    )

    result = _fail(
        "workflow_submission.required_missing",
        "submission missing",
        request=request,
        failure_code="",
        started_at=datetime.now(timezone.utc),
        inputs={"job": "phase_alpha"},
        outputs={},
        executor_type="workflow.test",
    )

    assert result.status == "failed"
    assert result.reason_code == "workflow_submission.required_missing"
    assert result.failure_code == "workflow_submission.required_missing"
