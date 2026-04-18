"""Frozen contracts for the dataset refinery.

Mirrors the column shape of the tables in
``Code&DBs/Databases/migrations/workflow/155_dataset_refinery_authority.sql``
and ``156_dataset_refinery_projections.sql``. These types are the single
typed surface used by the candidate subscriber, scorer, projection
subscriber, exporter, and read API.

Kept in a dedicated module (not ``contracts/domain.py``) so the workflow
request contract surface stays focused on intake validation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


CANDIDATE_KINDS: frozenset[str] = frozenset(
    {"review", "triage", "operator_explain", "route_choice", "repair"}
)
REDACTION_STATUSES: frozenset[str] = frozenset(
    {"clean", "unverified", "redaction_required", "sensitive_blocked"}
)
STALENESS_STATUSES: frozenset[str] = frozenset(
    {"fresh", "definition_stale", "evidence_stale"}
)
ELIGIBILITY_VALUES: frozenset[str] = frozenset(
    {
        "rejected",
        "manual_review",
        "sft_eligible",
        "preference_eligible",
        "eval_eligible",
        "routing_eligible",
    }
)
DATASET_FAMILIES: frozenset[str] = frozenset({"sft", "preference", "eval", "routing"})
SPLIT_TAGS: frozenset[str] = frozenset({"train", "eval", "holdout"})
PROMOTION_KINDS: frozenset[str] = frozenset({"manual", "auto"})
EVIDENCE_KINDS: frozenset[str] = frozenset(
    {
        "receipt",
        "verification_run",
        "semantic_assertion",
        "operator_decision",
        "bug",
        "route_eligibility",
        "dispatch_run",
        "workflow_run",
    }
)
EVIDENCE_ROLES: frozenset[str] = frozenset(
    {
        "source_input",
        "verifier_signal",
        "reviewer_signal",
        "operator_signal",
        "failure_signature",
        "downstream_outcome",
    }
)


class DatasetContractError(ValueError):
    """Raised when a dataset contract is constructed with invalid values."""


def _require_in(value: str, allowed: frozenset[str], field_name: str) -> str:
    if value not in allowed:
        raise DatasetContractError(
            f"{field_name} must be one of {sorted(allowed)}; got {value!r}"
        )
    return value


def _require_nonblank(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise DatasetContractError(f"{field_name} must be non-blank")
    return value


@dataclass(frozen=True, slots=True)
class CandidateEvidenceLink:
    """One lineage edge from a candidate to an authoritative evidence row."""

    candidate_id: str
    evidence_kind: str
    evidence_ref: str
    evidence_role: str
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_nonblank(self.candidate_id, "candidate_id")
        _require_nonblank(self.evidence_ref, "evidence_ref")
        _require_in(self.evidence_kind, EVIDENCE_KINDS, "evidence_kind")
        _require_in(self.evidence_role, EVIDENCE_ROLES, "evidence_role")


@dataclass(frozen=True, slots=True)
class RawDatasetCandidate:
    """One evidence-linked training-data candidate."""

    candidate_id: str
    candidate_kind: str
    source_receipt_id: str
    source_run_id: str
    source_node_id: str
    raw_input_ref: Mapping[str, Any]
    raw_output_ref: Mapping[str, Any]
    dedupe_signature: str
    source_workflow_id: str | None = None
    task_type: str | None = None
    route_slug: str | None = None
    persona: str | None = None
    provider_ref: str | None = None
    model_ref: str | None = None
    workflow_definition_id: str | None = None
    admitted_definition_hash: str | None = None
    repo_snapshot_ref: str | None = None
    parsed_output_ref: Mapping[str, Any] | None = None
    verifier_summary: Mapping[str, Any] | None = None
    review_summary: Mapping[str, Any] | None = None
    operator_decision_summary: Mapping[str, Any] | None = None
    downstream_summary: Mapping[str, Any] | None = None
    linked_bug_ids: tuple[str, ...] = ()
    linked_roadmap_ids: tuple[str, ...] = ()
    redaction_status: str = "unverified"
    staleness_status: str = "fresh"
    ingested_at: datetime | None = None
    evidence_links: tuple[CandidateEvidenceLink, ...] = ()

    def __post_init__(self) -> None:
        _require_nonblank(self.candidate_id, "candidate_id")
        _require_nonblank(self.source_receipt_id, "source_receipt_id")
        _require_nonblank(self.source_run_id, "source_run_id")
        _require_nonblank(self.source_node_id, "source_node_id")
        _require_nonblank(self.dedupe_signature, "dedupe_signature")
        _require_in(self.candidate_kind, CANDIDATE_KINDS, "candidate_kind")
        _require_in(self.redaction_status, REDACTION_STATUSES, "redaction_status")
        _require_in(self.staleness_status, STALENESS_STATUSES, "staleness_status")


@dataclass(frozen=True, slots=True)
class DatasetScoringPolicy:
    """Authority record for a per-specialist scoring rubric."""

    policy_id: str
    policy_slug: str
    specialist_target: str
    rubric: Mapping[str, Any]
    decided_by: str
    rationale: str
    auto_promote: bool = False
    created_at: datetime | None = None
    superseded_by: str | None = None

    def __post_init__(self) -> None:
        _require_nonblank(self.policy_id, "policy_id")
        _require_nonblank(self.policy_slug, "policy_slug")
        _require_nonblank(self.specialist_target, "specialist_target")
        _require_nonblank(self.decided_by, "decided_by")
        _require_nonblank(self.rationale, "rationale")
        if "factors" not in self.rubric or "thresholds" not in self.rubric:
            raise DatasetContractError(
                "rubric must contain 'factors' and 'thresholds' keys"
            )


@dataclass(frozen=True, slots=True)
class CandidateScore:
    """Per-(candidate, policy) score and eligibility classification."""

    candidate_id: str
    policy_id: str
    eligibility: str
    confidence: float
    factors: Mapping[str, Any]
    rationale: str
    scored_at: datetime | None = None
    scored_against_definition_hash: str | None = None

    def __post_init__(self) -> None:
        _require_nonblank(self.candidate_id, "candidate_id")
        _require_nonblank(self.policy_id, "policy_id")
        _require_nonblank(self.rationale, "rationale")
        _require_in(self.eligibility, ELIGIBILITY_VALUES, "eligibility")
        if not 0.0 <= float(self.confidence) <= 1.0:
            raise DatasetContractError(
                f"confidence must be in [0,1]; got {self.confidence}"
            )


@dataclass(frozen=True, slots=True)
class DatasetPromotion:
    """Append-only authority record for a single dataset promotion."""

    promotion_id: str
    candidate_ids: tuple[str, ...]
    dataset_family: str
    specialist_target: str
    policy_id: str
    payload: Mapping[str, Any]
    promoted_by: str
    promotion_kind: str
    rationale: str
    split_tag: str | None = None
    decision_ref: str | None = None
    superseded_by: str | None = None
    superseded_reason: str | None = None
    promoted_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_nonblank(self.promotion_id, "promotion_id")
        _require_nonblank(self.specialist_target, "specialist_target")
        _require_nonblank(self.policy_id, "policy_id")
        _require_nonblank(self.promoted_by, "promoted_by")
        _require_nonblank(self.rationale, "rationale")
        _require_in(self.dataset_family, DATASET_FAMILIES, "dataset_family")
        _require_in(self.promotion_kind, PROMOTION_KINDS, "promotion_kind")
        if self.split_tag is not None:
            _require_in(self.split_tag, SPLIT_TAGS, "split_tag")
        if not self.candidate_ids:
            raise DatasetContractError("candidate_ids must be non-empty")
        if self.dataset_family == "preference" and len(self.candidate_ids) != 2:
            raise DatasetContractError(
                "preference family requires exactly 2 candidate_ids (chosen, rejected)"
            )
        if self.promotion_kind == "manual" and not self.decision_ref:
            raise DatasetContractError(
                "manual promotion requires decision_ref (operator_decision_id)"
            )
        if (self.superseded_by is None) != (self.superseded_reason is None):
            raise DatasetContractError(
                "superseded_by and superseded_reason must be set together"
            )


@dataclass(frozen=True, slots=True)
class CuratedExample:
    """SFT projection row."""

    promotion_id: str
    specialist_target: str
    candidate_id: str
    prompt: Mapping[str, Any]
    target_output: Mapping[str, Any]
    split_tag: str | None = None
    is_active: bool = True
    refreshed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CuratedPreferencePair:
    """Preference-pair projection row."""

    promotion_id: str
    specialist_target: str
    chosen_candidate_id: str
    rejected_candidate_id: str
    prompt: Mapping[str, Any]
    chosen_output: Mapping[str, Any]
    rejected_output: Mapping[str, Any]
    pair_evidence: Mapping[str, Any]
    split_tag: str | None = None
    is_active: bool = True
    refreshed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CuratedEvalCase:
    """Eval-case projection row."""

    promotion_id: str
    specialist_target: str
    case_input: Mapping[str, Any]
    revision_scope: Mapping[str, Any]
    expected_output: Mapping[str, Any] | None = None
    rubric: Mapping[str, Any] | None = None
    difficulty_tags: tuple[str, ...] = ()
    domain_tags: tuple[str, ...] = ()
    excluded_from_training: bool = True
    is_active: bool = True
    refreshed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DatasetExportManifest:
    """Durable manifest row produced by every export."""

    manifest_id: str
    dataset_family: str
    specialist_target: str
    split_tag: str
    promotion_ids: tuple[str, ...]
    output_path: str
    output_sha256: str
    row_count: int
    exported_by: str
    exported_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_in(self.dataset_family, DATASET_FAMILIES, "dataset_family")
        _require_in(self.split_tag, SPLIT_TAGS, "split_tag")
        if self.row_count < 0:
            raise DatasetContractError("row_count must be >= 0")


__all__ = [
    "CANDIDATE_KINDS",
    "DATASET_FAMILIES",
    "ELIGIBILITY_VALUES",
    "EVIDENCE_KINDS",
    "EVIDENCE_ROLES",
    "PROMOTION_KINDS",
    "REDACTION_STATUSES",
    "SPLIT_TAGS",
    "STALENESS_STATUSES",
    "CandidateEvidenceLink",
    "CandidateScore",
    "CuratedEvalCase",
    "CuratedExample",
    "CuratedPreferencePair",
    "DatasetContractError",
    "DatasetExportManifest",
    "DatasetPromotion",
    "DatasetScoringPolicy",
    "RawDatasetCandidate",
]
