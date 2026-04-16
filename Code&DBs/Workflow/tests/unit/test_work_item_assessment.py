from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from runtime.work_item_assessment import assess_work_items


def test_assess_work_items_flags_architecture_changes_and_candidate_resolution(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    target = repo_root / "registry" / "operator.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('{"version": 2}\n', encoding="utf-8")

    updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    assessment_time = datetime.now(timezone.utc)

    assessments = assess_work_items(
        bugs=[
            {
                "bug_id": "bug.alpha",
                "updated_at": updated_at,
                "resolved_at": None,
            }
        ],
        roadmap_items=[
            {
                "roadmap_item_id": "roadmap_item.alpha",
                "source_bug_id": "bug.alpha",
                "registry_paths": ["registry/operator.json"],
                "updated_at": updated_at,
                "completed_at": None,
                "target_end_at": None,
            }
        ],
        bug_evidence_links={
            "bug.alpha": (
                {
                    "bug_id": "bug.alpha",
                    "evidence_kind": "workflow_receipt",
                    "evidence_ref": "receipt.alpha",
                    "evidence_role": "validates_fix",
                },
            )
        },
        as_of=assessment_time,
        repo_root=repo_root,
    )

    by_key = {
        (record.item_kind, record.item_id): record
        for record in assessments
    }
    bug_assessment = by_key[("bug", "bug.alpha")]
    roadmap_assessment = by_key[("roadmap_item", "roadmap_item.alpha")]

    assert bug_assessment.freshness_state == "needs_review"
    assert bug_assessment.resolution_state == "candidate_resolved"
    assert bug_assessment.closeout_state == "review_before_closeout"
    assert bug_assessment.closeout_action == "preview_work_item_closeout"
    assert bug_assessment.closeout_bug_ids == ("bug.alpha",)
    assert bug_assessment.closeout_roadmap_item_ids == ("roadmap_item.alpha",)
    assert "architecture_changed" in bug_assessment.reason_codes
    assert "validating_fix_evidence_present" in bug_assessment.reason_codes
    assert bug_assessment.associated_paths == ("registry/operator.json",)

    assert roadmap_assessment.freshness_state == "needs_review"
    assert roadmap_assessment.resolution_state == "candidate_completed"
    assert roadmap_assessment.closeout_state == "review_before_closeout"
    assert roadmap_assessment.closeout_action == "preview_work_item_closeout"
    assert roadmap_assessment.closeout_bug_ids == ("bug.alpha",)
    assert roadmap_assessment.closeout_roadmap_item_ids == ("roadmap_item.alpha",)
    assert "architecture_changed" in roadmap_assessment.reason_codes
    assert "source_bug_fix_proof_present" in roadmap_assessment.reason_codes
    assert roadmap_assessment.associated_paths == ("registry/operator.json",)


def test_assess_work_items_marks_old_open_items_stale() -> None:
    assessment_time = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)
    updated_at = assessment_time - timedelta(days=45)

    assessments = assess_work_items(
        bugs=[
            {
                "bug_id": "bug.stale",
                "updated_at": updated_at,
                "resolved_at": None,
            }
        ],
        roadmap_items=[
            {
                "roadmap_item_id": "roadmap_item.stale",
                "source_bug_id": None,
                "registry_paths": [],
                "updated_at": updated_at,
                "completed_at": None,
                "target_end_at": None,
            }
        ],
        bug_evidence_links={},
        as_of=assessment_time,
        repo_root=Path("/tmp"),
    )

    by_key = {
        (record.item_kind, record.item_id): record
        for record in assessments
    }
    assert by_key[("bug", "bug.stale")].freshness_state == "stale"
    assert by_key[("roadmap_item", "roadmap_item.stale")].freshness_state == "stale"
    assert by_key[("bug", "bug.stale")].closeout_state == "none"
    assert by_key[("roadmap_item", "roadmap_item.stale")].closeout_state == "none"


def test_assess_work_items_uses_bindings_and_idle_timeout_for_pipeline_state() -> None:
    assessment_time = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    active_touch = assessment_time - timedelta(hours=6)
    stale_touch = assessment_time - timedelta(hours=72)

    assessments = assess_work_items(
        bugs=[
            {
                "bug_id": "bug.active",
                "updated_at": assessment_time - timedelta(days=3),
                "resolved_at": None,
            },
            {
                "bug_id": "bug.stale-pipeline",
                "updated_at": assessment_time - timedelta(days=3),
                "resolved_at": None,
            },
        ],
        roadmap_items=[
            {
                "roadmap_item_id": "roadmap_item.active",
                "source_bug_id": "bug.active",
                "registry_paths": [],
                "updated_at": assessment_time - timedelta(days=3),
                "completed_at": None,
                "target_end_at": None,
            }
        ],
        bug_evidence_links={},
        work_item_workflow_bindings=[
            {
                "work_item_workflow_binding_id": "binding.active",
                "binding_status": "active",
                "bug_id": "bug.active",
                "workflow_run_id": "run.active",
                "updated_at": active_touch,
                "created_at": active_touch,
            },
            {
                "work_item_workflow_binding_id": "binding.stale",
                "binding_status": "active",
                "bug_id": "bug.stale-pipeline",
                "workflow_run_id": "run.stale",
                "updated_at": stale_touch,
                "created_at": stale_touch,
            },
        ],
        workflow_run_activity={
            "run.active": {
                "workflow_run_id": "run.active",
                "current_state": "lease_active",
                "last_touched_at": active_touch,
                "started_at": active_touch,
                "finished_at": None,
            },
            "run.stale": {
                "workflow_run_id": "run.stale",
                "current_state": "lease_active",
                "last_touched_at": stale_touch,
                "started_at": stale_touch,
                "finished_at": None,
            },
        },
        as_of=assessment_time,
        repo_root=Path("/tmp"),
    )

    by_key = {
        (record.item_kind, record.item_id): record
        for record in assessments
    }
    active_bug = by_key[("bug", "bug.active")]
    stale_bug = by_key[("bug", "bug.stale-pipeline")]
    active_roadmap = by_key[("roadmap_item", "roadmap_item.active")]

    assert active_bug.activity_state == "in_progress"
    assert active_bug.pipeline_state == "in_progress"
    assert active_bug.promotion_state == "promoted"
    assert active_bug.workflow_run_ids == ("run.active",)
    assert active_bug.binding_ids == ("binding.active",)
    assert active_bug.stale_after_at == active_touch + timedelta(hours=48)

    assert stale_bug.activity_state == "stale"
    assert stale_bug.pipeline_state == "stale_in_progress"
    assert stale_bug.promotion_state == "auto_promote_to_roadmap"
    assert "workflow_idle_timeout" in stale_bug.reason_codes

    assert active_roadmap.activity_state == "planned"
    assert active_roadmap.pipeline_state == "planned"


def test_assess_work_items_projects_issue_lineage_and_inherited_activity() -> None:
    assessment_time = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    active_touch = assessment_time - timedelta(hours=2)

    assessments = assess_work_items(
        issues=[
            {
                "issue_id": "issue.dispatch-gap",
                "updated_at": assessment_time - timedelta(days=5),
                "resolved_at": None,
            },
            {
                "issue_id": "issue.waiting-room",
                "updated_at": assessment_time - timedelta(days=1),
                "resolved_at": None,
            },
        ],
        bugs=[
            {
                "bug_id": "bug.auto_issue.dispatch-gap",
                "updated_at": assessment_time - timedelta(days=4),
                "resolved_at": None,
                "source_issue_id": "issue.dispatch-gap",
            },
        ],
        roadmap_items=[
            {
                "roadmap_item_id": "roadmap_item.auto_bug.dispatch-gap",
                "source_bug_id": "bug.auto_issue.dispatch-gap",
                "registry_paths": [],
                "updated_at": assessment_time - timedelta(days=4),
                "completed_at": None,
                "target_end_at": None,
            }
        ],
        bug_evidence_links={},
        work_item_workflow_bindings=[
            {
                "work_item_workflow_binding_id": "binding.bug.active",
                "binding_status": "active",
                "bug_id": "bug.auto_issue.dispatch-gap",
                "workflow_run_id": "run.issue.promoted",
                "updated_at": active_touch,
                "created_at": active_touch,
            },
            {
                "work_item_workflow_binding_id": "binding.issue.direct",
                "binding_status": "active",
                "issue_id": "issue.waiting-room",
                "workflow_run_id": "run.issue.direct",
                "updated_at": active_touch,
                "created_at": active_touch,
            },
        ],
        workflow_run_activity={
            "run.issue.promoted": {
                "workflow_run_id": "run.issue.promoted",
                "current_state": "lease_active",
                "last_touched_at": active_touch,
                "started_at": active_touch,
                "finished_at": None,
            },
            "run.issue.direct": {
                "workflow_run_id": "run.issue.direct",
                "current_state": "lease_active",
                "last_touched_at": active_touch,
                "started_at": active_touch,
                "finished_at": None,
            },
        },
        as_of=assessment_time,
        repo_root=Path("/tmp"),
    )

    by_key = {
        (record.item_kind, record.item_id): record
        for record in assessments
    }
    promoted_issue = by_key[("issue", "issue.dispatch-gap")]
    waiting_issue = by_key[("issue", "issue.waiting-room")]

    assert promoted_issue.activity_state == "in_progress"
    assert promoted_issue.pipeline_state == "in_progress"
    assert promoted_issue.promotion_state == "promoted"
    assert promoted_issue.binding_ids == ("binding.bug.active",)
    assert promoted_issue.workflow_run_ids == ("run.issue.promoted",)
    assert "linked_bug_activity" in promoted_issue.reason_codes
    assert {"kind": "bug", "id": "bug.auto_issue.dispatch-gap"} in promoted_issue.linked_items
    assert {
        "kind": "roadmap_item",
        "id": "roadmap_item.auto_bug.dispatch-gap",
    } in promoted_issue.linked_items

    assert waiting_issue.activity_state == "in_progress"
    assert waiting_issue.pipeline_state == "in_progress"
    assert waiting_issue.promotion_state == "auto_promote_to_bug"
    assert waiting_issue.binding_ids == ("binding.issue.direct",)
    assert waiting_issue.workflow_run_ids == ("run.issue.direct",)
