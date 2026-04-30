from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import subprocess
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


def _git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        text=True,
        capture_output=True,
        check=True,
    )
    return completed.stdout.strip()


def _init_git_repo(repo_root: Path) -> None:
    _git(repo_root, "init")
    _git(repo_root, "config", "user.name", "Test User")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / "app.py").write_text("value = 1\n", encoding="utf-8")
    _git(repo_root, "add", "app.py")
    _git(repo_root, "commit", "-m", "initial")


def test_commit_live_apply_returns_real_commit_ref(tmp_path: Path) -> None:
    _init_git_repo(tmp_path)
    base_head = _git(tmp_path, "rev-parse", "HEAD")
    (tmp_path / "app.py").write_text("value = 2\n", encoding="utf-8")

    canonical_commit_ref = candidate_materialization._commit_live_apply(
        tmp_path,
        candidate_id="11111111-1111-1111-1111-111111111111",
        intended_files=["app.py"],
        materialized_by="human:nate",
    )

    assert canonical_commit_ref != base_head
    assert canonical_commit_ref == _git(tmp_path, "rev-parse", "HEAD")
    assert _git(tmp_path, "log", "-1", "--format=%s").startswith(
        "Materialize code-change candidate",
    )
    assert "candidate_patch:" not in canonical_commit_ref


def test_commit_live_apply_rejects_ambient_staged_changes(tmp_path: Path) -> None:
    _init_git_repo(tmp_path)
    (tmp_path / "ambient.txt").write_text("do not include\n", encoding="utf-8")
    _git(tmp_path, "add", "ambient.txt")
    (tmp_path / "app.py").write_text("value = 2\n", encoding="utf-8")

    with pytest.raises(candidate_materialization.CandidateMaterializationError) as exc_info:
        candidate_materialization._commit_live_apply(
            tmp_path,
            candidate_id="11111111-1111-1111-1111-111111111111",
            intended_files=["app.py"],
            materialized_by="human:nate",
        )

    assert exc_info.value.reason_code == "code_change_candidate.index_dirty"
    assert exc_info.value.details["staged_paths"] == ["ambient.txt"]


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
    preflight_row: dict[str, Any] | None = None

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "FROM candidate_latest_preflight" in query:
            return self.preflight_row
        assert "FROM code_change_candidate_payloads" in query
        return {
            "candidate_id": str(args[0]),
            "submission_id": "workflow_job_submission:test",
            "bug_id": "BUG-12345678",
            "base_head_ref": "abcdef1234567890",
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
    conn = _FakeConn(
        preflight_row={
            "preflight_id": "preflight:test",
            "preflight_status": "passed",
            "base_head_ref_at_preflight": "abcdef1234567890",
            "runtime_derived_patch_sha256": "sha:runtime",
            "agent_declared_patch_sha256": "sha:runtime",
            "temp_verifier_passed": True,
            "impact_contract_complete": True,
            "contested_impact_count": 0,
            "runtime_addition_impact_count": 0,
            "created_at": "2026-04-29T00:00:00+00:00",
            "completed_at": "2026-04-29T00:00:01+00:00",
        }
    )
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


def test_candidate_review_approve_refused_without_preflight(monkeypatch) -> None:
    conn = _FakeConn(preflight_row=None)
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

    assert result["ok"] is False
    assert result["reason_code"] == "code_change_candidate.preflight_required"
    assert repository.review == {}
    assert conn.updates == []


def test_candidate_review_approve_refused_when_preflight_stale(monkeypatch) -> None:
    conn = _FakeConn(
        preflight_row={
            "preflight_id": "preflight:stale",
            "preflight_status": "passed",
            "base_head_ref_at_preflight": "deadbeefdeadbeef",
            "runtime_derived_patch_sha256": None,
            "agent_declared_patch_sha256": None,
            "temp_verifier_passed": True,
            "impact_contract_complete": True,
            "contested_impact_count": 0,
            "runtime_addition_impact_count": 0,
            "created_at": "2026-04-29T00:00:00+00:00",
            "completed_at": "2026-04-29T00:00:01+00:00",
        }
    )
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
        ),
        _Subsystems(),
    )

    assert result["ok"] is False
    assert result["reason_code"] == "code_change_candidate.preflight_stale"
    assert repository.review == {}


def test_candidate_review_approve_refused_when_preflight_not_passed(monkeypatch) -> None:
    conn = _FakeConn(
        preflight_row={
            "preflight_id": "preflight:contested",
            "preflight_status": "failed_impact_contract",
            "base_head_ref_at_preflight": "abcdef1234567890",
            "runtime_derived_patch_sha256": "sha:runtime",
            "agent_declared_patch_sha256": "sha:runtime",
            "temp_verifier_passed": True,
            "impact_contract_complete": False,
            "contested_impact_count": 2,
            "runtime_addition_impact_count": 1,
            "created_at": "2026-04-29T00:00:00+00:00",
            "completed_at": "2026-04-29T00:00:01+00:00",
        }
    )
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
        ),
        _Subsystems(),
    )

    assert result["ok"] is False
    assert result["reason_code"] == "code_change_candidate.preflight_not_passed"
    assert result["details"]["preflight_status"] == "failed_impact_contract"
    assert result["details"]["contested_impact_count"] == 2


def test_candidate_review_reject_skips_preflight_gate(monkeypatch) -> None:
    conn = _FakeConn(preflight_row=None)
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
            decision="reject",
            reasons=["wrong shape"],
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert repository.review["decision"] == "reject"
