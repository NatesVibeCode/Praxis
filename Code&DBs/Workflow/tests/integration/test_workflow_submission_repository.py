from __future__ import annotations

from datetime import datetime, timezone
import json
import uuid

import pytest

from storage import migrations as workflow_migrations
from storage.migrations import workflow_migration_expected_objects
from storage.postgres import PostgresConfigurationError, ensure_postgres_available
from storage.postgres.workflow_submission_repository import (
    PostgresWorkflowSubmissionRepository,
    WorkflowSubmissionRepositoryError,
)


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _seed_workflow_run(conn, *, suffix: str) -> dict[str, str]:
    workflow_id = f"workflow.{suffix}"
    workflow_definition_id = f"workflow_definition.{suffix}"
    admission_decision_id = f"admission_decision.{suffix}"
    run_id = f"run.{suffix}"
    request_id = f"request.{suffix}"
    requested_at = datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)
    admitted_at = datetime(2026, 4, 6, 12, 0, 5, tzinfo=timezone.utc)

    conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES ($1, $2, 1, 1, $3, 'admitted', $4::jsonb, $5::jsonb, $6, NULL)
        ON CONFLICT (workflow_definition_id) DO NOTHING
        """,
        workflow_definition_id,
        workflow_id,
        f"sha256:{suffix}",
        json.dumps({"workflow_id": workflow_id, "suffix": suffix}),
        json.dumps({"workflow_id": workflow_id, "suffix": suffix}),
        requested_at,
    )
    conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES ($1, $2, $3, 'admit', $4, $5, $6, $7, $8, $9)
        ON CONFLICT (admission_decision_id) DO NOTHING
        """,
        admission_decision_id,
        workflow_id,
        request_id,
        "policy.admit",
        admitted_at,
        "policy.engine",
        f"policy_snapshot.{suffix}",
        f"validation_result.{suffix}",
        f"authority_context.{suffix}",
    )
    conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 1,
            $9::jsonb, $10, $11, 'claim_accepted', NULL, $12, $13, NULL, NULL, NULL
        )
        ON CONFLICT (run_id) DO NOTHING
        """,
        run_id,
        workflow_id,
        request_id,
        f"digest.{suffix}",
        f"authority_digest.{suffix}",
        workflow_definition_id,
        f"sha256:{suffix}",
        request_id,
        json.dumps({"workflow_id": workflow_id, "run_id": run_id}),
        f"context_bundle.{suffix}",
        admission_decision_id,
        requested_at,
        admitted_at,
    )
    return {
        "workflow_id": workflow_id,
        "run_id": run_id,
    }


def test_workflow_submission_migration_is_registered() -> None:
    filenames = [entry.filename for entry in workflow_migrations.workflow_migration_manifest()]
    assert "080_workflow_job_submissions.sql" in filenames

    objects = workflow_migration_expected_objects("080_workflow_job_submissions.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "workflow_job_submissions",
        "workflow_job_submission_reviews",
        "workflow_job_submissions_run_job_attempt_key",
        "workflow_job_submission_reviews_submission_reviewed_idx",
    }


def test_workflow_submission_repository_is_idempotent_and_reviews_are_append_only() -> None:
    try:
        conn = ensure_postgres_available()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for workflow submission repository integration test: "
            f"{exc.reason_code}"
        )

    suffix = _unique_suffix()
    authority = _seed_workflow_run(conn, suffix=suffix)
    repository = PostgresWorkflowSubmissionRepository(conn)

    first = repository.record_submission(
        submission_id=f"workflow_job_submission.{suffix}.1",
        run_id=authority["run_id"],
        workflow_id=authority["workflow_id"],
        job_label="build.codegen",
        attempt_no=1,
        result_kind="code_change_candidate",
        summary="sealed the first worker result",
        primary_paths=["runtime/workflow/submission.py"],
        tests_ran=["pytest tests/integration/test_workflow_submission_repository.py"],
        notes="initial seal",
        declared_operations=[
            {"path": "runtime/workflow/submission.py", "action": "update"},
        ],
        changed_paths=["runtime/workflow/submission.py"],
        operation_set=[
            {"path": "runtime/workflow/submission.py", "action": "update"},
        ],
        comparison_status="matched",
        comparison_report={"matched": True},
        diff_artifact_ref=f"artifact.diff.{suffix}",
        artifact_refs=[f"artifact.bundle.{suffix}"],
        verification_artifact_refs=[f"artifact.verify.{suffix}"],
        sealed_at=datetime(2026, 4, 6, 12, 0, 30, tzinfo=timezone.utc),
    )

    duplicate = repository.record_submission(
        submission_id=f"workflow_job_submission.{suffix}.duplicate",
        run_id=authority["run_id"],
        workflow_id=authority["workflow_id"],
        job_label="build.codegen",
        attempt_no=1,
        result_kind="code_change_candidate",
        summary="sealed the first worker result",
        primary_paths=["runtime/workflow/submission.py"],
        tests_ran=["pytest tests/integration/test_workflow_submission_repository.py"],
        notes="initial seal",
        declared_operations=[
            {"path": "runtime/workflow/submission.py", "action": "update"},
        ],
        changed_paths=["runtime/workflow/submission.py"],
        operation_set=[
            {"path": "runtime/workflow/submission.py", "action": "update"},
        ],
        comparison_status="matched",
        comparison_report={"matched": True},
        diff_artifact_ref=f"artifact.diff.{suffix}",
        artifact_refs=[f"artifact.bundle.{suffix}"],
        verification_artifact_refs=[f"artifact.verify.{suffix}"],
        sealed_at=datetime(2026, 4, 6, 12, 0, 30, tzinfo=timezone.utc),
    )
    assert duplicate["submission_id"] == first["submission_id"]

    with pytest.raises(WorkflowSubmissionRepositoryError) as exc_info:
        repository.record_submission(
            submission_id=f"workflow_job_submission.{suffix}.conflict",
            run_id=authority["run_id"],
            workflow_id=authority["workflow_id"],
            job_label="build.codegen",
            attempt_no=1,
            result_kind="code_change_candidate",
            summary="changed sealed result",
            primary_paths=["runtime/workflow/submission.py"],
            tests_ran=["pytest tests/integration/test_workflow_submission_repository.py"],
            notes="initial seal",
            declared_operations=[
                {"path": "runtime/workflow/submission.py", "action": "update"},
            ],
            changed_paths=["runtime/workflow/submission.py"],
            operation_set=[
                {"path": "runtime/workflow/submission.py", "action": "update"},
            ],
            comparison_status="matched",
            comparison_report={"matched": True},
            diff_artifact_ref=f"artifact.diff.{suffix}",
            artifact_refs=[f"artifact.bundle.{suffix}"],
            verification_artifact_refs=[f"artifact.verify.{suffix}"],
            sealed_at=datetime(2026, 4, 6, 12, 0, 30, tzinfo=timezone.utc),
        )
    assert exc_info.value.reason_code == "workflow_submission.conflict"

    second = repository.record_submission(
        submission_id=f"workflow_job_submission.{suffix}.2",
        run_id=authority["run_id"],
        workflow_id=authority["workflow_id"],
        job_label="build.codegen",
        attempt_no=2,
        result_kind="code_change_candidate",
        summary="sealed the second worker result",
        primary_paths=["runtime/workflow/submission.py"],
        tests_ran=["pytest tests/integration/test_workflow_submission_repository.py"],
        notes=None,
        declared_operations=[
            {"path": "runtime/workflow/submission.py", "action": "update"},
        ],
        changed_paths=["runtime/workflow/submission.py"],
        operation_set=[
            {"path": "runtime/workflow/submission.py", "action": "update"},
        ],
        comparison_status="matched",
        comparison_report={"matched": True},
        diff_artifact_ref=f"artifact.diff.{suffix}.2",
        artifact_refs=[f"artifact.bundle.{suffix}.2"],
        verification_artifact_refs=[f"artifact.verify.{suffix}.2"],
        sealed_at=datetime(2026, 4, 6, 12, 1, 30, tzinfo=timezone.utc),
    )

    latest = repository.fetch_latest_submission_summary_by_run_job(
        run_id=authority["run_id"],
        job_label="build.codegen",
    )
    assert latest is not None
    assert latest["submission_id"] == second["submission_id"]
    assert latest["attempt_no"] == 2

    attempt_one = repository.fetch_submission_by_run_job_attempt(
        run_id=authority["run_id"],
        job_label="build.codegen",
        attempt_no=1,
    )
    assert attempt_one is not None
    assert attempt_one["submission_id"] == first["submission_id"]

    review_one = repository.record_review(
        submission_id=second["submission_id"],
        run_id=authority["run_id"],
        workflow_id=authority["workflow_id"],
        reviewer_job_label="review.codegen",
        reviewer_role="reviewer",
        decision="request_changes",
        summary="needs a follow-up pass",
        notes="adjust the sealed diff",
        evidence_refs=[f"artifact.review.{suffix}.1"],
        review_id=f"workflow_job_submission_review.{suffix}.1",
        reviewed_at=datetime(2026, 4, 6, 12, 2, 0, tzinfo=timezone.utc),
    )
    review_two = repository.record_review(
        submission_id=second["submission_id"],
        run_id=authority["run_id"],
        workflow_id=authority["workflow_id"],
        reviewer_job_label="review.codegen",
        reviewer_role="reviewer",
        decision="approve",
        summary="sealed result looks good",
        notes=None,
        evidence_refs=[f"artifact.review.{suffix}.2"],
        review_id=f"workflow_job_submission_review.{suffix}.2",
        reviewed_at=datetime(2026, 4, 6, 12, 3, 0, tzinfo=timezone.utc),
    )

    timeline = repository.list_reviews_for_submission(
        submission_id=second["submission_id"],
    )
    assert [row["review_id"] for row in timeline] == [
        review_one["review_id"],
        review_two["review_id"],
    ]

    latest_review = repository.fetch_latest_review_summary_by_submission_id(
        submission_id=second["submission_id"],
    )
    assert latest_review is not None
    assert latest_review["review_id"] == review_two["review_id"]
    assert latest_review["decision"] == "approve"

    latest_review_by_run_job = repository.fetch_latest_review_summary_by_run_job(
        run_id=authority["run_id"],
        job_label="build.codegen",
    )
    assert latest_review_by_run_job is not None
    assert latest_review_by_run_job["review_id"] == review_two["review_id"]
