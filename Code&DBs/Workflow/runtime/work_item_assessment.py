"""Derived work-item assessment helpers.

These helpers infer review/staleness signals from canonical bug and roadmap
rows plus explicit evidence. They never mutate canonical truth directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from runtime.bug_evidence import EVIDENCE_ROLE_VALIDATES_FIX
from runtime.bug_tracker import BugStatus


_VALIDATING_EVIDENCE_ROLES = frozenset({EVIDENCE_ROLE_VALIDATES_FIX})
_FRESHNESS_FRESH = "fresh"
_FRESHNESS_NEEDS_REVIEW = "needs_review"
_FRESHNESS_STALE = "stale"
_DEFAULT_IDLE_TIMEOUT = timedelta(hours=48)
_INACTIVE_BINDING_STATUSES = frozenset({"inactive", "closed", "completed", "superseded", "cancelled"})
_RUN_SUCCESS_STATES = frozenset({"promoted"})
_RUN_FAILURE_STATES = frozenset(
    {
        "claim_blocked",
        "claim_rejected",
        "proposal_invalid",
        "gate_blocked",
        "promotion_rejected",
        "promotion_failed",
        "cancelled",
        "lease_expired",
    }
)
_ROADMAP_LIFECYCLES = frozenset({"idea", "planned", "claimed", "completed"})


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _optional_datetime(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _require_datetime(value, field_name=field_name)


def _normalize_string_list(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field_name} must be a sequence of strings when provided")
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(_require_text(item, field_name=f"{field_name}[{index}]"))
    return tuple(dict.fromkeys(result))


def _evidence_ref(
    *,
    kind: str,
    ref: str,
    role: str | None = None,
) -> dict[str, str]:
    payload = {"kind": kind, "ref": ref}
    if role is not None:
        payload["role"] = role
    return payload


@dataclass(frozen=True, slots=True)
class WorkItemAssessmentRecord:
    """Derived assessment for one bug or roadmap item."""

    item_kind: str
    item_id: str
    freshness_state: str
    resolution_state: str
    confidence: float
    suggested_action: str
    closeout_state: str
    closeout_action: str
    closeout_bug_ids: tuple[str, ...]
    closeout_roadmap_item_ids: tuple[str, ...]
    activity_state: str
    pipeline_state: str
    promotion_state: str
    last_touched_at: datetime | None
    stale_after_at: datetime | None
    binding_ids: tuple[str, ...]
    workflow_run_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    evidence_refs: tuple[Mapping[str, str], ...]
    associated_paths: tuple[str, ...]
    linked_items: tuple[Mapping[str, str], ...]
    assessed_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "item_kind": self.item_kind,
            "item_id": self.item_id,
            "freshness_state": self.freshness_state,
            "resolution_state": self.resolution_state,
            "confidence": round(self.confidence, 4),
            "suggested_action": self.suggested_action,
            "closeout": {
                "state": self.closeout_state,
                "action": self.closeout_action,
                "bug_ids": list(self.closeout_bug_ids),
                "roadmap_item_ids": list(self.closeout_roadmap_item_ids),
            },
            "activity_state": self.activity_state,
            "pipeline_state": self.pipeline_state,
            "promotion_state": self.promotion_state,
            "last_touched_at": (
                None if self.last_touched_at is None else self.last_touched_at.isoformat()
            ),
            "stale_after_at": (
                None if self.stale_after_at is None else self.stale_after_at.isoformat()
            ),
            "binding_ids": list(self.binding_ids),
            "workflow_run_ids": list(self.workflow_run_ids),
            "reason_codes": list(self.reason_codes),
            "evidence_refs": [dict(ref) for ref in self.evidence_refs],
            "associated_paths": list(self.associated_paths),
            "linked_items": [dict(ref) for ref in self.linked_items],
            "assessed_at": self.assessed_at.isoformat(),
        }


def _binding_source(
    binding: Mapping[str, Any],
) -> tuple[str, str] | None:
    issue_id = _optional_text(binding.get("issue_id"), field_name="issue_id")
    if issue_id is not None:
        return ("issue", issue_id)
    bug_id = _optional_text(binding.get("bug_id"), field_name="bug_id")
    if bug_id is not None:
        return ("bug", bug_id)
    roadmap_item_id = _optional_text(
        binding.get("roadmap_item_id"),
        field_name="roadmap_item_id",
    )
    if roadmap_item_id is not None:
        return ("roadmap_item", roadmap_item_id)
    source = binding.get("source")
    if isinstance(source, Mapping):
        source_kind = _optional_text(source.get("kind"), field_name="source.kind")
        source_id = _optional_text(source.get("id"), field_name="source.id")
        if source_kind is not None and source_id is not None:
            return (source_kind, source_id)
    return None


def _last_activity_datetime(
    binding: Mapping[str, Any],
) -> datetime | None:
    for field_name in ("updated_at", "created_at"):
        value = binding.get(field_name)
        if value is None:
            continue
        if isinstance(value, datetime):
            return _require_datetime(value, field_name=field_name)
        if isinstance(value, str) and value.strip():
            return _require_datetime(
                datetime.fromisoformat(value.strip().replace("Z", "+00:00")),
                field_name=field_name,
            )
    return None


def _run_activity_datetime(
    activity: Mapping[str, Any],
) -> datetime | None:
    for field_name in ("last_touched_at", "finished_at", "started_at"):
        value = activity.get(field_name)
        if value is None:
            continue
        if isinstance(value, datetime):
            return _require_datetime(value, field_name=field_name)
        if isinstance(value, str) and value.strip():
            return _require_datetime(
                datetime.fromisoformat(value.strip().replace("Z", "+00:00")),
                field_name=field_name,
            )
    return None


def _normalize_binding_status(value: object) -> str:
    if value is None:
        return "active"
    return _require_text(value, field_name="binding_status").lower()


def _roadmap_lifecycle(roadmap_item: Mapping[str, Any]) -> str:
    value = roadmap_item.get("lifecycle")
    if value is None:
        if _optional_datetime(roadmap_item.get("completed_at"), field_name="completed_at") is not None:
            return "completed"
        normalized_status = str(roadmap_item.get("status") or "").strip().lower()
        if normalized_status in {"completed", "done"}:
            return "completed"
        return "planned"
    normalized = _require_text(value, field_name="lifecycle").lower()
    if normalized not in _ROADMAP_LIFECYCLES:
        raise ValueError(
            "lifecycle must be one of " + ", ".join(sorted(_ROADMAP_LIFECYCLES))
        )
    return normalized


def _activity_snapshot(
    *,
    item_kind: str,
    item_id: str,
    bindings: Sequence[Mapping[str, Any]],
    workflow_run_activity: Mapping[str, Mapping[str, Any]],
    assessed_at: datetime,
    idle_timeout: timedelta,
    declared_claimed: bool = False,
    declared_backlog: bool = False,
) -> dict[str, Any]:
    relevant_bindings = [
        binding
        for binding in bindings
        if _binding_source(binding) == (item_kind, item_id)
    ]
    binding_ids = tuple(
        dict.fromkeys(
            _require_text(
                binding.get("work_item_workflow_binding_id"),
                field_name="work_item_workflow_binding_id",
            )
            for binding in relevant_bindings
            if binding.get("work_item_workflow_binding_id") is not None
        )
    )
    workflow_run_ids = tuple(
        dict.fromkeys(
            _require_text(
                binding.get("workflow_run_id"),
                field_name="workflow_run_id",
            )
            for binding in relevant_bindings
            if binding.get("workflow_run_id") is not None
        )
    )
    last_touched_candidates: list[datetime] = []
    active_binding_present = False
    active_run_present = False
    successful_run_present = False
    failed_run_present = False
    reason_codes: list[str] = []

    for binding in relevant_bindings:
        binding_status = _normalize_binding_status(binding.get("binding_status"))
        if binding_status not in _INACTIVE_BINDING_STATUSES:
            active_binding_present = True
        touched_at = _last_activity_datetime(binding)
        if touched_at is not None:
            last_touched_candidates.append(touched_at)

    for run_id in workflow_run_ids:
        activity = workflow_run_activity.get(run_id)
        if not isinstance(activity, Mapping):
            continue
        touched_at = _run_activity_datetime(activity)
        if touched_at is not None:
            last_touched_candidates.append(touched_at)
        current_state = _optional_text(
            activity.get("current_state"),
            field_name="current_state",
        )
        if current_state is None:
            continue
        normalized_state = current_state.lower()
        if normalized_state in _RUN_SUCCESS_STATES:
            successful_run_present = True
            reason_codes.append("workflow_run_promoted")
            continue
        if normalized_state in _RUN_FAILURE_STATES:
            failed_run_present = True
            reason_codes.append("workflow_run_blocked")
            continue
        active_run_present = True
        reason_codes.append("workflow_run_active")

    if active_binding_present:
        reason_codes.append("workflow_binding_present")

    last_touched_at = max(last_touched_candidates) if last_touched_candidates else None
    stale_after_at = (
        None if last_touched_at is None else last_touched_at + idle_timeout
    )
    if active_run_present or active_binding_present:
        if stale_after_at is not None and assessed_at >= stale_after_at:
            activity_state = "stale"
            reason_codes.append("workflow_idle_timeout")
        else:
            activity_state = "in_progress"
    elif successful_run_present:
        activity_state = "built"
    elif failed_run_present:
        activity_state = "blocked"
    elif declared_claimed:
        activity_state = "in_progress"
    else:
        activity_state = "backlog" if declared_backlog or item_kind in {"bug", "issue"} else "planned"

    return {
        "activity_state": activity_state,
        "last_touched_at": last_touched_at,
        "stale_after_at": stale_after_at,
        "binding_ids": binding_ids,
        "workflow_run_ids": workflow_run_ids,
        "reason_codes": tuple(dict.fromkeys(reason_codes)),
    }


def _changed_paths_since(
    *,
    repo_root: Path,
    associated_paths: Sequence[str],
    baseline: datetime,
) -> tuple[tuple[str, ...], tuple[Mapping[str, str], ...]]:
    changed_paths: list[str] = []
    evidence_refs: list[Mapping[str, str]] = []
    for rel_path in associated_paths:
        candidate = Path(rel_path)
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        try:
            stat = candidate.stat()
        except OSError:
            continue
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        if modified_at > baseline:
            changed_paths.append(rel_path)
            evidence_refs.append(
                _evidence_ref(
                    kind="repo_path",
                    ref=rel_path,
                    role="architecture_changed",
                )
            )
    return tuple(changed_paths), tuple(evidence_refs)


def _bug_related_paths(
    *,
    bug_id: str,
    roadmap_items: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    paths: list[str] = []
    for item in roadmap_items:
        if item.get("source_bug_id") != bug_id:
            continue
        for path in _normalize_string_list(item.get("registry_paths"), field_name="registry_paths"):
            paths.append(path)
    return tuple(dict.fromkeys(paths))


def _issue_related_paths(
    *,
    issue_id: str,
    bugs: Sequence[Mapping[str, Any]],
    roadmap_items: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    paths: list[str] = []
    related_bug_ids = {
        _require_text(bug.get("bug_id"), field_name="bug_id")
        for bug in bugs
        if _optional_text(bug.get("source_issue_id"), field_name="source_issue_id") == issue_id
    }
    for item in roadmap_items:
        if item.get("source_bug_id") not in related_bug_ids:
            continue
        for path in _normalize_string_list(item.get("registry_paths"), field_name="registry_paths"):
            paths.append(path)
    return tuple(dict.fromkeys(paths))


def _roadmap_linked_items(
    *,
    roadmap_item: Mapping[str, Any],
) -> tuple[Mapping[str, str], ...]:
    linked: list[Mapping[str, str]] = []
    source_bug_id = _optional_text(roadmap_item.get("source_bug_id"), field_name="source_bug_id")
    if source_bug_id is not None:
        linked.append({"kind": "bug", "id": source_bug_id})
    return tuple(linked)


def assess_work_items(
    *,
    issues: Sequence[Mapping[str, Any]] | None = None,
    bugs: Sequence[Mapping[str, Any]],
    roadmap_items: Sequence[Mapping[str, Any]],
    bug_evidence_links: Mapping[str, Sequence[Mapping[str, Any]]],
    as_of: datetime,
    repo_root: Path,
    work_item_workflow_bindings: Sequence[Mapping[str, Any]] | None = None,
    workflow_run_activity: Mapping[str, Mapping[str, Any]] | None = None,
    idle_timeout: timedelta = _DEFAULT_IDLE_TIMEOUT,
) -> tuple[WorkItemAssessmentRecord, ...]:
    """Return derived work-item assessments for the supplied bug and roadmap rows."""

    assessed_at = _require_datetime(as_of, field_name="as_of")
    normalized_issues = tuple(issues or ())
    normalized_roadmap_items = tuple(roadmap_items)
    normalized_bindings = tuple(work_item_workflow_bindings or ())
    normalized_run_activity = dict(workflow_run_activity or {})
    bug_by_id = {
        _require_text(bug.get("bug_id"), field_name="bug_id"): bug
        for bug in bugs
    }
    assessments: list[WorkItemAssessmentRecord] = []

    for bug in bugs:
        bug_id = _require_text(bug.get("bug_id"), field_name="bug_id")
        updated_at = _require_datetime(bug.get("updated_at"), field_name="updated_at")
        resolved_at = _optional_datetime(bug.get("resolved_at"), field_name="resolved_at")
        reason_codes: list[str] = []
        linked_open_roadmap_item_ids = tuple(
            _require_text(item.get("roadmap_item_id"), field_name="roadmap_item_id")
            for item in normalized_roadmap_items
            if item.get("source_bug_id") == bug_id
            and _optional_datetime(item.get("completed_at"), field_name="completed_at") is None
        )
        source_issue_id = _optional_text(
            bug.get("source_issue_id"),
            field_name="source_issue_id",
        )
        linked_items = tuple(
            {"kind": "roadmap_item", "id": _require_text(item.get("roadmap_item_id"), field_name="roadmap_item_id")}
            for item in normalized_roadmap_items
            if item.get("source_bug_id") == bug_id
        ) + (
            ({"kind": "issue", "id": source_issue_id},)
            if source_issue_id is not None
            else ()
        )
        evidence_refs: list[Mapping[str, str]] = []
        validating_evidence = []
        for evidence in bug_evidence_links.get(bug_id, ()):
            evidence_kind = _require_text(evidence.get("evidence_kind"), field_name="evidence_kind")
            evidence_ref = _require_text(evidence.get("evidence_ref"), field_name="evidence_ref")
            evidence_role = _require_text(evidence.get("evidence_role"), field_name="evidence_role")
            evidence_refs.append(
                _evidence_ref(kind=evidence_kind, ref=evidence_ref, role=evidence_role)
            )
            if evidence_role in _VALIDATING_EVIDENCE_ROLES:
                validating_evidence.append(evidence_ref)

        associated_paths = _bug_related_paths(
            bug_id=bug_id,
            roadmap_items=normalized_roadmap_items,
        )
        changed_paths, changed_path_refs = _changed_paths_since(
            repo_root=repo_root,
            associated_paths=associated_paths,
            baseline=updated_at,
        )
        if changed_paths:
            reason_codes.append("architecture_changed")
            evidence_refs.extend(changed_path_refs)

        activity = _activity_snapshot(
            item_kind="bug",
            item_id=bug_id,
            bindings=normalized_bindings,
            workflow_run_activity=normalized_run_activity,
            assessed_at=assessed_at,
            idle_timeout=idle_timeout,
            declared_claimed=str(bug.get("status") or "").strip().upper() == BugStatus.IN_PROGRESS.value,
        )
        reason_codes.extend(activity["reason_codes"])
        promotion_state = (
            "promoted"
            if linked_items
            else (
                "auto_promote_to_roadmap"
                if activity["binding_ids"]
                else "none"
            )
        )

        if resolved_at is None and bool(validating_evidence):
            reason_codes.append("validating_fix_evidence_present")

        if resolved_at is None and any(
            _optional_datetime(item.get("completed_at"), field_name="completed_at") is not None
            for item in normalized_roadmap_items
            if item.get("source_bug_id") == bug_id
        ):
            reason_codes.append("linked_roadmap_completed")

        if resolved_at is None and updated_at <= assessed_at - timedelta(days=30):
            reason_codes.append("stale_open_bug")

        if resolved_at is not None:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "resolved"
            confidence = 1.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "completed"
        elif "stale_open_bug" in reason_codes:
            freshness_state = _FRESHNESS_STALE
            resolution_state = "open"
            confidence = 0.81 if reason_codes else 0.0
            suggested_action = "review_bug_staleness"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "stale_backlog"
        elif "validating_fix_evidence_present" in reason_codes or "linked_roadmap_completed" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "candidate_resolved"
            confidence = 0.92 if "validating_fix_evidence_present" in reason_codes else 0.78
            suggested_action = "review_bug_resolution"
            if "validating_fix_evidence_present" in reason_codes:
                closeout_state = (
                    "review_before_closeout"
                    if "architecture_changed" in reason_codes
                    else "safe_to_autoclose"
                )
                closeout_action = (
                    "preview_work_item_closeout"
                    if closeout_state == "review_before_closeout"
                    else "commit_work_item_closeout"
                )
            else:
                closeout_state = "none"
                closeout_action = "none"
            pipeline_state = "built_candidate"
        elif "architecture_changed" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "open"
            confidence = 0.76
            suggested_action = "review_bug_scope"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "scope_changed"
        else:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "open"
            confidence = 0.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "backlog"

        if resolved_at is None:
            if activity["activity_state"] == "in_progress":
                pipeline_state = "in_progress"
            elif activity["activity_state"] == "stale":
                pipeline_state = "stale_in_progress"
            elif activity["activity_state"] == "built":
                pipeline_state = "built_candidate"
            elif activity["activity_state"] == "blocked":
                pipeline_state = "blocked"
            elif promotion_state == "auto_promote_to_roadmap":
                pipeline_state = "promotion_pending"

        assessments.append(
            WorkItemAssessmentRecord(
                item_kind="bug",
                item_id=bug_id,
                freshness_state=freshness_state,
                resolution_state=resolution_state,
                confidence=confidence,
                suggested_action=suggested_action,
                closeout_state=closeout_state,
                closeout_action=closeout_action,
                closeout_bug_ids=(bug_id,) if closeout_state != "none" else (),
                closeout_roadmap_item_ids=(
                    linked_open_roadmap_item_ids
                    if closeout_state != "none"
                    else ()
                ),
                activity_state=activity["activity_state"],
                pipeline_state=pipeline_state,
                promotion_state=promotion_state,
                last_touched_at=activity["last_touched_at"],
                stale_after_at=activity["stale_after_at"],
                binding_ids=activity["binding_ids"],
                workflow_run_ids=activity["workflow_run_ids"],
                reason_codes=tuple(dict.fromkeys(reason_codes)),
                evidence_refs=tuple(evidence_refs),
                associated_paths=associated_paths,
                linked_items=linked_items,
                assessed_at=assessed_at,
            )
        )

    for roadmap_item in normalized_roadmap_items:
        roadmap_item_id = _require_text(
            roadmap_item.get("roadmap_item_id"),
            field_name="roadmap_item_id",
        )
        updated_at = _require_datetime(roadmap_item.get("updated_at"), field_name="updated_at")
        completed_at = _optional_datetime(
            roadmap_item.get("completed_at"),
            field_name="completed_at",
        )
        target_end_at = _optional_datetime(
            roadmap_item.get("target_end_at"),
            field_name="target_end_at",
        )
        associated_paths = _normalize_string_list(
            roadmap_item.get("registry_paths"),
            field_name="registry_paths",
        )
        changed_paths, changed_path_refs = _changed_paths_since(
            repo_root=repo_root,
            associated_paths=associated_paths,
            baseline=updated_at,
        )
        reason_codes: list[str] = []
        evidence_refs: list[Mapping[str, str]] = list(changed_path_refs)
        linked_items = list(_roadmap_linked_items(roadmap_item=roadmap_item))

        if changed_paths:
            reason_codes.append("architecture_changed")

        source_bug_id = _optional_text(roadmap_item.get("source_bug_id"), field_name="source_bug_id")
        source_idea_id = _optional_text(roadmap_item.get("source_idea_id"), field_name="source_idea_id")
        if source_idea_id is not None:
            linked_items.append({"kind": "operator_idea", "id": source_idea_id})
        related_bug = bug_by_id.get(source_bug_id) if source_bug_id is not None else None
        lifecycle = _roadmap_lifecycle(roadmap_item)
        activity = _activity_snapshot(
            item_kind="roadmap_item",
            item_id=roadmap_item_id,
            bindings=normalized_bindings,
            workflow_run_activity=normalized_run_activity,
            assessed_at=assessed_at,
            idle_timeout=idle_timeout,
            declared_claimed=lifecycle == "claimed",
            declared_backlog=lifecycle == "idea",
        )
        reason_codes.extend(activity["reason_codes"])
        if related_bug is not None and _optional_datetime(
            related_bug.get("resolved_at"),
            field_name="resolved_at",
        ) is not None and completed_at is None:
            reason_codes.append("source_bug_resolved")
            linked_items.append({"kind": "bug", "id": source_bug_id})
        source_bug_has_fix_proof = bool(
            source_bug_id is not None
            and any(
                _require_text(evidence.get("evidence_role"), field_name="evidence_role")
                in _VALIDATING_EVIDENCE_ROLES
                for evidence in bug_evidence_links.get(source_bug_id, ())
            )
        )
        if completed_at is None and source_bug_has_fix_proof:
            reason_codes.append("source_bug_fix_proof_present")

        if completed_at is None and target_end_at is not None and target_end_at < assessed_at:
            reason_codes.append("target_date_elapsed")

        if completed_at is None and updated_at <= assessed_at - timedelta(days=30):
            reason_codes.append("stale_open_roadmap_item")

        if completed_at is not None:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "completed"
            confidence = 1.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "completed"
        elif "stale_open_roadmap_item" in reason_codes:
            freshness_state = _FRESHNESS_STALE
            resolution_state = "open"
            confidence = 0.79
            suggested_action = "review_roadmap_staleness"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "stale_backlog"
        elif "source_bug_fix_proof_present" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "candidate_completed"
            confidence = 0.95
            suggested_action = "review_roadmap_completion"
            closeout_state = (
                "review_before_closeout"
                if "architecture_changed" in reason_codes
                else "safe_to_autoclose"
            )
            closeout_action = (
                "preview_work_item_closeout"
                if closeout_state == "review_before_closeout"
                else "commit_work_item_closeout"
            )
            pipeline_state = "built_candidate"
        elif "source_bug_resolved" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "candidate_completed"
            confidence = 0.73
            suggested_action = "review_roadmap_completion"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "candidate_completed"
        elif "architecture_changed" in reason_codes or "target_date_elapsed" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "open"
            confidence = 0.8 if "architecture_changed" in reason_codes else 0.67
            suggested_action = "review_roadmap_scope"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "scope_changed"
        else:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "open"
            confidence = 0.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "planned"

        if completed_at is None:
            if activity["activity_state"] == "in_progress":
                pipeline_state = "in_progress"
            elif activity["activity_state"] == "stale":
                pipeline_state = "stale_in_progress"
            elif activity["activity_state"] == "built":
                pipeline_state = "built_candidate"
            elif activity["activity_state"] == "blocked":
                pipeline_state = "blocked"
            elif activity["activity_state"] == "backlog":
                pipeline_state = "backlog"

        assessments.append(
            WorkItemAssessmentRecord(
                item_kind="roadmap_item",
                item_id=roadmap_item_id,
                freshness_state=freshness_state,
                resolution_state=resolution_state,
                confidence=confidence,
                suggested_action=suggested_action,
                closeout_state=closeout_state,
                closeout_action=closeout_action,
                closeout_bug_ids=(
                    (source_bug_id,) if closeout_state != "none" and source_bug_id is not None else ()
                ),
                closeout_roadmap_item_ids=(
                    (roadmap_item_id,) if closeout_state != "none" else ()
                ),
                activity_state=activity["activity_state"],
                pipeline_state=pipeline_state,
                promotion_state=(
                    "promoted_from_bug" if source_bug_id is not None else "none"
                ),
                last_touched_at=activity["last_touched_at"],
                stale_after_at=activity["stale_after_at"],
                binding_ids=activity["binding_ids"],
                workflow_run_ids=activity["workflow_run_ids"],
                reason_codes=tuple(dict.fromkeys(reason_codes)),
                evidence_refs=tuple(evidence_refs),
                associated_paths=associated_paths,
                linked_items=tuple(linked_items),
                assessed_at=assessed_at,
            )
        )

    assessment_by_key = {
        (record.item_kind, record.item_id): record
        for record in assessments
    }

    for issue in normalized_issues:
        issue_id = _require_text(issue.get("issue_id"), field_name="issue_id")
        updated_at = _require_datetime(issue.get("updated_at"), field_name="updated_at")
        resolved_at = _optional_datetime(issue.get("resolved_at"), field_name="resolved_at")
        linked_bug_ids = tuple(
            _require_text(bug.get("bug_id"), field_name="bug_id")
            for bug in bugs
            if _optional_text(bug.get("source_issue_id"), field_name="source_issue_id") == issue_id
        )
        linked_bug_assessments = tuple(
            assessment_by_key[("bug", bug_id)]
            for bug_id in linked_bug_ids
            if ("bug", bug_id) in assessment_by_key
        )
        linked_items = tuple(
            {"kind": "bug", "id": bug_id}
            for bug_id in linked_bug_ids
        ) + tuple(
            {"kind": "roadmap_item", "id": _require_text(item.get("roadmap_item_id"), field_name="roadmap_item_id")}
            for item in normalized_roadmap_items
            if item.get("source_bug_id") in linked_bug_ids
        )
        evidence_refs: list[Mapping[str, str]] = []
        reason_codes: list[str] = []
        associated_paths = _issue_related_paths(
            issue_id=issue_id,
            bugs=bugs,
            roadmap_items=normalized_roadmap_items,
        )
        changed_paths, changed_path_refs = _changed_paths_since(
            repo_root=repo_root,
            associated_paths=associated_paths,
            baseline=updated_at,
        )
        if changed_paths:
            reason_codes.append("architecture_changed")
            evidence_refs.extend(changed_path_refs)

        activity = _activity_snapshot(
            item_kind="issue",
            item_id=issue_id,
            bindings=normalized_bindings,
            workflow_run_activity=normalized_run_activity,
            assessed_at=assessed_at,
            idle_timeout=idle_timeout,
        )
        reason_codes.extend(activity["reason_codes"])
        primary_bug_assessment = linked_bug_assessments[0] if linked_bug_assessments else None
        activity_state = activity["activity_state"]
        last_touched_at = activity["last_touched_at"]
        stale_after_at = activity["stale_after_at"]
        binding_ids = activity["binding_ids"]
        workflow_run_ids = activity["workflow_run_ids"]
        if (
            primary_bug_assessment is not None
            and activity_state in {"backlog", "planned"}
            and primary_bug_assessment.activity_state in {"in_progress", "stale", "built", "blocked"}
        ):
            activity_state = primary_bug_assessment.activity_state
            last_touched_at = primary_bug_assessment.last_touched_at
            stale_after_at = primary_bug_assessment.stale_after_at
            binding_ids = primary_bug_assessment.binding_ids
            workflow_run_ids = primary_bug_assessment.workflow_run_ids
            reason_codes.append("linked_bug_activity")
        promotion_state = (
            "promoted"
            if linked_bug_ids
            else (
                "auto_promote_to_bug"
                if binding_ids
                else "none"
            )
        )
        if resolved_at is None and updated_at <= assessed_at - timedelta(days=30):
            reason_codes.append("stale_open_issue")
        if resolved_at is None and primary_bug_assessment is not None and primary_bug_assessment.resolution_state in {
            "resolved",
            "candidate_resolved",
        }:
            reason_codes.append("linked_bug_resolved")

        if resolved_at is not None:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "resolved"
            confidence = 1.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "completed"
        elif "linked_bug_resolved" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "candidate_resolved"
            confidence = 0.9
            suggested_action = "review_issue_resolution"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = (
                "candidate_resolved"
                if primary_bug_assessment is None
                else primary_bug_assessment.pipeline_state
            )
        elif "stale_open_issue" in reason_codes and not linked_bug_ids:
            freshness_state = _FRESHNESS_STALE
            resolution_state = "open"
            confidence = 0.78
            suggested_action = "review_issue_staleness"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "stale_backlog"
        elif "architecture_changed" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "open"
            confidence = 0.74
            suggested_action = "review_issue_scope"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "scope_changed"
        else:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "open"
            confidence = 0.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"
            pipeline_state = "backlog"

        if resolved_at is None:
            if activity_state == "in_progress":
                pipeline_state = "in_progress"
            elif activity_state == "stale":
                pipeline_state = "stale_in_progress"
            elif activity_state == "built":
                pipeline_state = "built_candidate"
            elif activity_state == "blocked":
                pipeline_state = "blocked"
            elif primary_bug_assessment is not None:
                pipeline_state = primary_bug_assessment.pipeline_state
            elif promotion_state == "auto_promote_to_bug":
                pipeline_state = "promotion_pending"

        assessments.append(
            WorkItemAssessmentRecord(
                item_kind="issue",
                item_id=issue_id,
                freshness_state=freshness_state,
                resolution_state=resolution_state,
                confidence=confidence,
                suggested_action=suggested_action,
                closeout_state=closeout_state,
                closeout_action=closeout_action,
                closeout_bug_ids=(),
                closeout_roadmap_item_ids=(),
                activity_state=activity_state,
                pipeline_state=pipeline_state,
                promotion_state=promotion_state,
                last_touched_at=last_touched_at,
                stale_after_at=stale_after_at,
                binding_ids=binding_ids,
                workflow_run_ids=workflow_run_ids,
                reason_codes=tuple(dict.fromkeys(reason_codes)),
                evidence_refs=tuple(evidence_refs),
                associated_paths=associated_paths,
                linked_items=linked_items,
                assessed_at=assessed_at,
            )
        )

    return tuple(
        sorted(
            assessments,
            key=lambda record: (record.item_kind, record.item_id),
        )
    )


__all__ = [
    "WorkItemAssessmentRecord",
    "assess_work_items",
]
