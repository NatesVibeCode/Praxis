from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import surfaces.api.workflow_submission as workflow_submission
from surfaces.mcp.runtime_context import WorkflowMcpRequestContext


@dataclass
class _FakeService:
    calls: list[tuple[str, dict[str, Any]]]

    def submit_code_change(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("submit_code_change", dict(kwargs)))
        return {"submission_id": "sub-1", "kind": kwargs["result_kind"]}

    def submit_research_result(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("submit_research_result", dict(kwargs)))
        return {"submission_id": "sub-2", "kind": kwargs["result_kind"]}

    def submit_artifact_bundle(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("submit_artifact_bundle", dict(kwargs)))
        return {"submission_id": "sub-3", "kind": kwargs["result_kind"]}

    def get_submission(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_submission", dict(kwargs)))
        return {"submission_id": kwargs.get("submission_id") or "sub-target"}

    def review_submission(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("review_submission", dict(kwargs)))
        return {"review_id": "rev-1", "decision": kwargs["decision"]}


def _context(*, allowed_tools: tuple[str, ...] = ()) -> WorkflowMcpRequestContext:
    return WorkflowMcpRequestContext(
        run_id="run-1",
        workflow_id="workflow-1",
        job_label="job-reviewer",
        allowed_tools=allowed_tools,
        expires_at=1_800_000_000,
    )


def test_submit_code_change_binds_context_and_forwards_service_args(monkeypatch) -> None:
    service = _FakeService(calls=[])
    monkeypatch.setattr(workflow_submission, "_load_submission_service", lambda: service)

    payload = workflow_submission.submit_code_change(
        summary="Updated the parser",
        primary_paths=["runtime/workflow/submission_capture.py"],
        result_kind="code_change",
        tests_ran=["pytest tests/unit/test_workflow_submission_frontdoor.py"],
        notes="ready for review",
        declared_operations=[{"path": "runtime/workflow/submission_capture.py", "action": "update"}],
        context=_context(allowed_tools=("praxis_submit_code_change",)),
    )

    assert payload["ok"] is True
    assert payload["tool"] == "praxis_submit_code_change"
    assert payload["submission"]["submission_id"] == "sub-1"
    assert service.calls == [
        (
            "submit_code_change",
            {
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "job_label": "job-reviewer",
                "result_kind": "code_change",
                "summary": "Updated the parser",
                "primary_paths": ["runtime/workflow/submission_capture.py"],
                "tests_ran": ["pytest tests/unit/test_workflow_submission_frontdoor.py"],
                "notes": "ready for review",
                "declared_operations": [
                    {"path": "runtime/workflow/submission_capture.py", "action": "update"},
                ],
            },
        )
    ]


def test_submit_frontdoor_fails_closed_when_tool_not_admitted(monkeypatch) -> None:
    service = _FakeService(calls=[])
    monkeypatch.setattr(workflow_submission, "_load_submission_service", lambda: service)

    payload = workflow_submission.submit_research_result(
        summary="Search complete",
        primary_paths=["docs/research.md"],
        result_kind="research_result",
        context=_context(allowed_tools=("praxis_submit_code_change",)),
    )

    assert payload["ok"] is False
    assert payload["error"]["reason_code"] == "workflow_submission.tool_not_allowed"
    assert service.calls == []


def test_review_submission_rejects_ambiguous_target(monkeypatch) -> None:
    service = _FakeService(calls=[])
    monkeypatch.setattr(workflow_submission, "_load_submission_service", lambda: service)

    payload = workflow_submission.review_submission(
        submission_id="sub-1",
        job_label="job-1",
        decision="approve",
        summary="Looks good",
        context=_context(allowed_tools=("praxis_review_submission", "praxis_get_submission")),
    )

    assert payload["ok"] is False
    assert payload["error"]["reason_code"] == "workflow_submission.invalid_input"
    assert "exactly one" in payload["error"]["message"]
    assert service.calls == []


def test_get_submission_uses_current_run_authority(monkeypatch) -> None:
    service = _FakeService(calls=[])
    monkeypatch.setattr(workflow_submission, "_load_submission_service", lambda: service)

    payload = workflow_submission.get_submission(
        job_label="job-target",
        context=_context(allowed_tools=("praxis_get_submission",)),
    )

    assert payload["ok"] is True
    assert payload["submission"]["submission_id"] == "sub-target"
    assert service.calls == [
        (
            "get_submission",
            {
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "job_label": "job-target",
            },
        )
    ]


def test_review_submission_forwards_reviewer_identity(monkeypatch) -> None:
    service = _FakeService(calls=[])
    monkeypatch.setattr(workflow_submission, "_load_submission_service", lambda: service)

    payload = workflow_submission.review_submission(
        submission_id="sub-1",
        decision="approve",
        summary="Looks good",
        notes="no issues",
        context=_context(allowed_tools=("praxis_review_submission",)),
    )

    assert payload["ok"] is True
    assert payload["submission"]["review_id"] == "rev-1"
    assert service.calls == [
        (
            "review_submission",
            {
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "submission_id": "sub-1",
                "reviewer_job_label": "job-reviewer",
                "decision": "approve",
                "summary": "Looks good",
                "notes": "no issues",
            },
        )
    ]


def test_review_submission_forwards_optional_publish_policy_fields(monkeypatch) -> None:
    service = _FakeService(calls=[])
    monkeypatch.setattr(workflow_submission, "_load_submission_service", lambda: service)

    payload = workflow_submission.review_submission(
        submission_id="sub-1",
        decision="approve",
        summary="publish approved",
        policy_snapshot_ref="policy_snapshot:custom",
        target_ref="repo:canonical",
        current_head_ref="head:abc123",
        promotion_intent_at="2026-04-09T12:00:00+00:00",
        finalized_at="2026-04-09T12:01:00+00:00",
        canonical_commit_ref="commit:abc123",
        context=_context(allowed_tools=("praxis_review_submission",)),
    )

    assert payload["ok"] is True
    assert service.calls == [
        (
            "review_submission",
            {
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "submission_id": "sub-1",
                "reviewer_job_label": "job-reviewer",
                "decision": "approve",
                "summary": "publish approved",
                "notes": None,
                "policy_snapshot_ref": "policy_snapshot:custom",
                "target_ref": "repo:canonical",
                "current_head_ref": "head:abc123",
                "promotion_intent_at": "2026-04-09T12:00:00+00:00",
                "finalized_at": "2026-04-09T12:01:00+00:00",
                "canonical_commit_ref": "commit:abc123",
            },
        )
    ]


def test_frontdoor_preserves_service_reason_codes(monkeypatch) -> None:
    class _ServiceError(RuntimeError):
        def __init__(self) -> None:
            super().__init__("out of scope change detected")
            self.reason_code = "workflow_submission.out_of_scope"
            self.details = {"paths": ["outside.py"]}

    def _raising_service():
        class _RaisingService:
            def submit_code_change(self, **kwargs: Any) -> dict[str, Any]:
                raise _ServiceError()

        return _RaisingService()

    monkeypatch.setattr(workflow_submission, "_load_submission_service", _raising_service)

    payload = workflow_submission.submit_code_change(
        summary="Updated the parser",
        primary_paths=["runtime/workflow/submission_capture.py"],
        result_kind="code_change",
        context=_context(allowed_tools=("praxis_submit_code_change",)),
    )

    assert payload["ok"] is False
    assert payload["error"]["reason_code"] == "workflow_submission.out_of_scope"
