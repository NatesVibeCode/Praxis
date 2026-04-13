"""Derived work-item assessment helpers.

These helpers infer review/staleness signals from canonical bug and roadmap
rows plus explicit evidence. They never mutate canonical truth directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


_VALIDATING_EVIDENCE_ROLES = frozenset({"validates_fix"})
_FRESHNESS_FRESH = "fresh"
_FRESHNESS_NEEDS_REVIEW = "needs_review"
_FRESHNESS_STALE = "stale"


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
            "reason_codes": list(self.reason_codes),
            "evidence_refs": [dict(ref) for ref in self.evidence_refs],
            "associated_paths": list(self.associated_paths),
            "linked_items": [dict(ref) for ref in self.linked_items],
            "assessed_at": self.assessed_at.isoformat(),
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
    bugs: Sequence[Mapping[str, Any]],
    roadmap_items: Sequence[Mapping[str, Any]],
    bug_evidence_links: Mapping[str, Sequence[Mapping[str, Any]]],
    as_of: datetime,
    repo_root: Path,
) -> tuple[WorkItemAssessmentRecord, ...]:
    """Return derived work-item assessments for the supplied bug and roadmap rows."""

    assessed_at = _require_datetime(as_of, field_name="as_of")
    normalized_roadmap_items = tuple(roadmap_items)
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
        linked_items = tuple(
            {"kind": "roadmap_item", "id": _require_text(item.get("roadmap_item_id"), field_name="roadmap_item_id")}
            for item in normalized_roadmap_items
            if item.get("source_bug_id") == bug_id
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
        elif "stale_open_bug" in reason_codes:
            freshness_state = _FRESHNESS_STALE
            resolution_state = "open"
            confidence = 0.81 if reason_codes else 0.0
            suggested_action = "review_bug_staleness"
            closeout_state = "none"
            closeout_action = "none"
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
        elif "architecture_changed" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "open"
            confidence = 0.76
            suggested_action = "review_bug_scope"
            closeout_state = "none"
            closeout_action = "none"
        else:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "open"
            confidence = 0.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"

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
        related_bug = bug_by_id.get(source_bug_id) if source_bug_id is not None else None
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
        elif "stale_open_roadmap_item" in reason_codes:
            freshness_state = _FRESHNESS_STALE
            resolution_state = "open"
            confidence = 0.79
            suggested_action = "review_roadmap_staleness"
            closeout_state = "none"
            closeout_action = "none"
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
        elif "source_bug_resolved" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "candidate_completed"
            confidence = 0.73
            suggested_action = "review_roadmap_completion"
            closeout_state = "none"
            closeout_action = "none"
        elif "architecture_changed" in reason_codes or "target_date_elapsed" in reason_codes:
            freshness_state = _FRESHNESS_NEEDS_REVIEW
            resolution_state = "open"
            confidence = 0.8 if "architecture_changed" in reason_codes else 0.67
            suggested_action = "review_roadmap_scope"
            closeout_state = "none"
            closeout_action = "none"
        else:
            freshness_state = _FRESHNESS_FRESH
            resolution_state = "open"
            confidence = 0.0
            suggested_action = "none"
            closeout_state = "none"
            closeout_action = "none"

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
                reason_codes=tuple(dict.fromkeys(reason_codes)),
                evidence_refs=tuple(evidence_refs),
                associated_paths=associated_paths,
                linked_items=tuple(linked_items),
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
