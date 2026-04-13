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
