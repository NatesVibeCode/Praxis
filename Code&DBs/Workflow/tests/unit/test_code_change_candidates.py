from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from runtime.operations.commands import candidate_review
from runtime.workflow import candidate_materialization
from runtime.workflow.candidate_authoring import (
    CandidateAuthoringError,
    derive_candidate_patch_from_sources,
)


def test_candidate_authoring_derives_diff_from_exact_block_replace() -> None:
    projection = derive_candidate_patch_from_sources(
        proposal_payload={
            "intended_files": ["Code&DBs/Workflow/runtime/example.py"],
            "rationale": "fix return value",
            "edits": [
                {
                    "file": "Code&DBs/Workflow/runtime/example.py",
                    "action": "exact_block_replace",
                    "old_block": "return False\n",
                    "new_block": "return True\n",
                }
            ],
            "verifier_ref": "verifier.job.python.pytest_file",
            "verifier_inputs": {"path": "Code&DBs/Workflow/tests/unit/test_example.py"},
        },
        source_context_refs={
            "Code&DBs/Workflow/runtime/example.py": "def works():\n    return False\n",
        },
    )

    assert projection.changed_paths == ("Code&DBs/Workflow/runtime/example.py",)
    assert projection.operation_set == (
        {"path": "Code&DBs/Workflow/runtime/example.py", "action": "update"},
    )
    assert "-    return False" in projection.unified_diff
    assert "+    return True" in projection.unified_diff
    assert projection.patch_sha256.startswith("sha256:")


def test_candidate_authoring_rejects_ambiguous_old_block() -> None:
    with pytest.raises(CandidateAuthoringError) as exc_info:
        derive_candidate_patch_from_sources(
            proposal_payload={
                "intended_files": ["Code&DBs/Workflow/runtime/example.py"],
                "edits": [
                    {
                        "file": "Code&DBs/Workflow/runtime/example.py",
                        "action": "exact_block_replace",
                        "old_block": "same\n",
                        "new_block": "different\n",
                    }
                ],
            },
            source_context_refs={
                "Code&DBs/Workflow/runtime/example.py": "same\nsame\n",
            },
        )

    assert exc_info.value.reason_code == "code_change_candidate.old_block_match_failed"
    assert exc_info.value.details["matches"] == 2


def test_candidate_authoring_rejects_tracking_doc_only_candidate() -> None:
    with pytest.raises(CandidateAuthoringError) as exc_info:
        derive_candidate_patch_from_sources(
            proposal_payload={
                "intended_files": ["artifacts/workflow/run/packets/PLAN.md"],
                "edits": [
                    {
                        "file": "artifacts/workflow/run/packets/PLAN.md",
                        "action": "full_file_replace",
                        "new_content": "done\n",
                    }
                ],
            },
            source_context_refs={
                "artifacts/workflow/run/packets/PLAN.md": "todo\n",
            },
        )

    assert exc_info.value.reason_code == "code_change_candidate.tracking_doc_only"


def test_human_review_candidate_can_preflight_before_approval() -> None:
    approved = candidate_materialization._assert_review_preconditions(
        {
            "candidate_id": "11111111-1111-1111-1111-111111111111",
            "submission_id": "workflow_job_submission:test",
            "review_routing": "human_review",
            "acceptance_status": "pending_review",
        },
        None,
    )

    assert approved is False


def test_auto_apply_candidate_requires_routing_record() -> None:
    with pytest.raises(candidate_materialization.CandidateMaterializationError) as exc_info:
        candidate_materialization._assert_review_preconditions(
            {
                "candidate_id": "11111111-1111-1111-1111-111111111111",
                "submission_id": "workflow_job_submission:test",
                "review_routing": "auto_apply",
                "acceptance_status": "pending",
                "routing_decision_record": {},
            },
            None,
        )

    assert exc_info.value.reason_code == "code_change_candidate.routing_record_missing"


@dataclass
class _FakeRepository:
    review: dict[str, Any] = field(default_factory=dict)

    def record_review(self, **kwargs: Any) -> dict[str, Any]:
        self.review = {
            "review_id": "workflow_job_submission_review:test",
            "submission_id": kwargs["submission_id"],
            "run_id": kwargs["run_id"],
            "workflow_id": kwargs["workflow_id"],
            "reviewer_job_label": kwargs["reviewer_job_label"],
            "reviewer_role": kwargs["reviewer_role"],
            "decision": kwargs["decision"],
            "summary": kwargs["summary"],
            "notes": kwargs["notes"],
            "evidence_refs": kwargs["evidence_refs"],
            "reviewed_at": "2026-04-29T00:00:00+00:00",
        }
        return dict(self.review)


@dataclass
class _FakeConn:
    updates: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "FROM code_change_candidate_payloads" in query
        return {
            "candidate_id": str(args[0]),
            "submission_id": "workflow_job_submission:test",
            "bug_id": "BUG-12345678",
            "review_routing": "human_review",
            "materialization_status": "pending",
            "run_id": "run:test",
            "workflow_id": "workflow:test",
            "acceptance_status": "pending_review",
        }

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.updates.append((" ".join(query.split()), args))
        return []


def test_candidate_review_records_review_and_updates_projections(monkeypatch) -> None:
    conn = _FakeConn()
    repository = _FakeRepository()
    monkeypatch.setattr(
        candidate_review,
        "PostgresWorkflowSubmissionRepository",
        lambda _conn: repository,
    )

    class _Subsystems:
        def get_pg_conn(self) -> _FakeConn:
            return conn

    result = candidate_review.handle_review_candidate(
        candidate_review.ReviewCodeChangeCandidate(
            candidate_id="11111111-1111-1111-1111-111111111111",
            reviewer_ref="human:nate",
            decision="approve",
            reasons=["looks good"],
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["review"]["decision"] == "approve"
    assert result["event_payload"]["decision"] == "approve"
    assert repository.review["reviewer_role"] == "human"
    assert any("UPDATE workflow_job_submissions" in query for query, _ in conn.updates)
    assert any("UPDATE code_change_candidate_payloads" in query for query, _ in conn.updates)
