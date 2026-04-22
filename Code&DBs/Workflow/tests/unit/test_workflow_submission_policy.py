from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.workflow import submission_capture


@dataclass
class _FakeRepository:
    submission: dict[str, Any]
    reviews: list[dict[str, Any]] = field(default_factory=list)

    def fetch_submission_by_id(self, *, submission_id: str) -> dict[str, Any] | None:
        if submission_id == self.submission["submission_id"]:
            return dict(self.submission)
        return None

    def fetch_latest_submission_summary_by_run_job(
        self,
        *,
        run_id: str,
        job_label: str,
    ) -> dict[str, Any] | None:
        if run_id == self.submission["run_id"] and job_label == self.submission["job_label"]:
            return dict(self.submission)
        return None

    def record_review(self, **kwargs: Any) -> dict[str, Any]:
        review = {
            "review_id": "review-1",
            "submission_id": self.submission["submission_id"],
            "run_id": self.submission["run_id"],
            "workflow_id": self.submission["workflow_id"],
            "reviewer_job_label": kwargs["reviewer_job_label"],
            "reviewer_role": kwargs["reviewer_role"],
            "decision": kwargs["decision"],
            "summary": kwargs["summary"],
            "notes": kwargs.get("notes"),
            "evidence_refs": [],
            "reviewed_at": "2026-04-09T12:02:00+00:00",
        }
        self.reviews.append(review)
        return dict(review)

    def list_reviews_for_submission(self, *, submission_id: str) -> tuple[dict[str, Any], ...]:
        if submission_id != self.submission["submission_id"]:
            return ()
        return tuple(dict(review) for review in self.reviews)

    def update_submission_acceptance(
        self,
        *,
        submission_id: str,
        acceptance_status: str,
        acceptance_report: object | None,
    ) -> dict[str, Any]:
        assert submission_id == self.submission["submission_id"]
        self.submission["acceptance_status"] = acceptance_status
        self.submission["acceptance_report"] = acceptance_report or {}
        return dict(self.submission)


@dataclass
class _FakeConn:
    executed: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.executed.append((" ".join(query.split()), args))
        return []


def test_publish_policy_review_projects_gate_and_promotion_rows(monkeypatch) -> None:
    submission = {
        "submission_id": "sub-1",
        "run_id": "run-1",
        "workflow_id": "workflow-1",
        "job_label": "build.codegen",
        "attempt_no": 1,
        "result_kind": "code_change",
        "summary": "sealed worker output",
        "primary_paths": ["runtime/workflow/submission_capture.py"],
        "tests_ran": ["pytest tests/unit/test_workflow_submission_policy.py"],
        "notes": None,
        "declared_operations": [{"path": "runtime/workflow/submission_capture.py", "action": "update"}],
        "changed_paths": ["runtime/workflow/submission_capture.py"],
        "operation_set": [{"path": "runtime/workflow/submission_capture.py", "action": "update"}],
        "comparison_status": "matched",
        "comparison_report": {"matched": True},
        "acceptance_status": "pending_review",
        "acceptance_report": {"contract_requested": True},
        "diff_artifact_ref": "workflow_submission_diff:abc123",
        "artifact_refs": ["workflow_submission_artifact:current:abc123:runtime/workflow/submission_capture.py"],
        "verification_artifact_refs": ["receipt:verify:sub-1"],
        "sealed_at": "2026-04-09T12:00:00+00:00",
    }
    repository = _FakeRepository(submission=submission)
    conn = _FakeConn()

    monkeypatch.setattr(submission_capture, "_repo", lambda active_conn=None: (conn, repository))
    monkeypatch.setattr(
        submission_capture,
        "_current_job_row",
        lambda *_args, **_kwargs: {
            "id": 7,
            "run_id": "run-1",
            "label": "publish.finalize",
            "attempt": 1,
            "route_task_type": "publish_policy",
            "status": "running",
        },
    )
    monkeypatch.setattr(
        submission_capture,
        "_emit_workflow_event",
        lambda *_args, **_kwargs: "workflow_event:test",
    )
    monkeypatch.setattr(
        submission_capture,
        "_load_runtime_context_state",
        lambda *_args, **_kwargs: ({}, {}, None),
    )

    payload = submission_capture.review_submission(
        run_id="run-1",
        workflow_id="workflow-1",
        reviewer_job_label="publish.finalize",
        submission_id="sub-1",
        decision="approve",
        summary="publish approved",
        promotion_intent_at="2026-04-09T12:03:00+00:00",
        finalized_at="2026-04-09T12:04:00+00:00",
        canonical_commit_ref="commit:abc123",
        conn=conn,
    )

    assert payload["decision"] == "approve"
    assert payload["policy"]["gate_evaluation"]["proposal_id"] == "proposal:sub-1"
    assert payload["policy"]["promotion_decision"]["decision"] == "accept"
    executed_queries = [query for query, _ in conn.executed]
    assert any("INSERT INTO gate_evaluations" in query for query in executed_queries)
    assert any("grant_ref, plan_envelope_hash" in query for query in executed_queries)
    assert any("INSERT INTO promotion_decisions" in query for query in executed_queries)
