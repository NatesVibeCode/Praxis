"""Pure-function rule-based scorer for dataset candidates.

A rubric is a JSON object stored on a ``dataset_scoring_policies`` row::

    {
      "factors": {
        "verifier_passed": {"weight": 0.30, "required": true},
        "reviewer_agreed": {"weight": 0.25},
        ...
      },
      "thresholds": {
        "manual_review": 0.50,
        "sft_eligible": 0.80,
        "preference_eligible": 0.85,
        "eval_eligible": 0.90,
        "routing_eligible": 0.85
      }
    }

Factor values are observed booleans (or ``None`` if undetermined) on the
candidate. The scorer is the *only* place those booleans turn into a
score. There is no learned model here; the rubric is the contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from contracts.dataset import (
    CandidateScore,
    DatasetScoringPolicy,
    RawDatasetCandidate,
)


# Eligibility tier order, lowest threshold first. Eligibility is the
# *highest* tier whose threshold the candidate clears.
_ELIGIBILITY_ORDER: tuple[str, ...] = (
    "manual_review",
    "sft_eligible",
    "preference_eligible",
    "routing_eligible",
    "eval_eligible",
)


@dataclass(frozen=True, slots=True)
class FactorObservations:
    """Observed factor values for one candidate.

    Each field is ``True``, ``False``, or ``None`` (undetermined). The
    scorer treats ``None`` as failing for required factors and as a
    zero-weight contribution otherwise.
    """

    verifier_passed: bool | None = None
    reviewer_agreed: bool | None = None
    operator_approved: bool | None = None
    downstream_success: bool | None = None
    no_linked_open_bug: bool | None = None
    redaction_clean: bool | None = None
    not_stale: bool | None = None
    not_duplicate: bool | None = None
    bug_linked: bool | None = None
    bug_status_resolved: bool | None = None
    route_eligibility_was_eligible: bool | None = None

    def as_dict(self) -> dict[str, bool | None]:
        return {
            "verifier_passed": self.verifier_passed,
            "reviewer_agreed": self.reviewer_agreed,
            "operator_approved": self.operator_approved,
            "downstream_success": self.downstream_success,
            "no_linked_open_bug": self.no_linked_open_bug,
            "redaction_clean": self.redaction_clean,
            "not_stale": self.not_stale,
            "not_duplicate": self.not_duplicate,
            "bug_linked": self.bug_linked,
            "bug_status_resolved": self.bug_status_resolved,
            "route_eligibility_was_eligible": self.route_eligibility_was_eligible,
        }


def derive_factor_observations(candidate: RawDatasetCandidate) -> FactorObservations:
    """Read factor booleans straight off the candidate's summaries.

    The candidate subscriber populates the summary JSONB blobs at
    ingest; this function never touches the database. Unknown factors
    stay ``None`` so the scorer can distinguish "missing evidence" from
    "evidence says no".
    """

    verifier = candidate.verifier_summary or {}
    review = candidate.review_summary or {}
    operator = candidate.operator_decision_summary or {}
    downstream = candidate.downstream_summary or {}

    verifier_passed: bool | None = None
    if verifier:
        status = str(verifier.get("status", "")).lower()
        if status in {"passed", "failed", "error"}:
            verifier_passed = status == "passed"

    reviewer_agreed: bool | None = None
    if review:
        status = str(review.get("status", "")).lower()
        predicate = str(review.get("predicate", "")).lower()
        if status:
            reviewer_agreed = status == "active" and "validates" in predicate

    operator_approved: bool | None = None
    if operator:
        status = str(operator.get("status", "")).lower()
        if status:
            operator_approved = status in {"decided", "approved"}

    downstream_success: bool | None = None
    if downstream:
        jobs = downstream.get("dependent_jobs") or []
        if jobs:
            downstream_success = all(
                str(job.get("status", "")).lower() == "succeeded" for job in jobs
            )

    no_linked_open_bug: bool | None = None
    bug_linked: bool | None = None
    bug_status_resolved: bool | None = None
    bugs = downstream.get("linked_bug_states") if downstream else None
    if isinstance(bugs, list):
        bug_linked = bool(bugs) or bool(candidate.linked_bug_ids)
        statuses = [str(b.get("status", "")).lower() for b in bugs]
        if statuses:
            no_linked_open_bug = not any(
                s in {"open", "in_progress"} for s in statuses
            )
            bug_status_resolved = all(
                s in {"fixed", "resolved", "wont_fix"} for s in statuses
            )
        elif not candidate.linked_bug_ids:
            # Explicit empty linked_bug_states with no candidate-level
            # links means we checked and found no open bugs.
            no_linked_open_bug = True
    elif candidate.linked_bug_ids:
        bug_linked = True
        # Without status info we can't say whether the bug is open;
        # leave the *open*-related factors undetermined.
    else:
        # No downstream bug info and no candidate-level bug links:
        # interpret absence as "no open bug touches this receipt".
        no_linked_open_bug = True
        bug_linked = False

    redaction_clean = candidate.redaction_status == "clean"
    not_stale = candidate.staleness_status == "fresh"
    # Duplicate detection is a global property and is set on the
    # candidate by the subscriber before scoring; if the subscriber did
    # not annotate it, default to True (innocent until proven).
    not_duplicate = True

    route_eligibility_was_eligible: bool | None = None
    if candidate.route_slug:
        # The candidate subscriber may have stuffed an eligibility flag
        # into the operator_decision_summary or downstream_summary; if
        # we don't see one, leave undetermined.
        flag = (downstream or {}).get("route_eligibility_was_eligible")
        if isinstance(flag, bool):
            route_eligibility_was_eligible = flag

    return FactorObservations(
        verifier_passed=verifier_passed,
        reviewer_agreed=reviewer_agreed,
        operator_approved=operator_approved,
        downstream_success=downstream_success,
        no_linked_open_bug=no_linked_open_bug,
        redaction_clean=redaction_clean,
        not_stale=not_stale,
        not_duplicate=not_duplicate,
        bug_linked=bug_linked,
        bug_status_resolved=bug_status_resolved,
        route_eligibility_was_eligible=route_eligibility_was_eligible,
    )


def _eligibility_for(score: float, thresholds: Mapping[str, Any]) -> str:
    manual = float(thresholds.get("manual_review", 0.50))
    if score < manual:
        return "rejected"
    best = "manual_review"
    best_threshold = manual
    for tier in _ELIGIBILITY_ORDER[1:]:
        if tier not in thresholds:
            continue
        t = float(thresholds[tier])
        if score >= t and t >= best_threshold:
            best = tier
            best_threshold = t
    return best


def score_candidate(
    candidate: RawDatasetCandidate,
    policy: DatasetScoringPolicy,
    *,
    observations: FactorObservations | None = None,
) -> CandidateScore:
    """Apply ``policy``'s rubric to ``candidate`` and return a score.

    Hard rules:

    * Any *required* factor that is ``False`` or ``None`` → ``rejected``
      with confidence 0.
    * Otherwise the score is the weighted sum of factor weights for
      factors that observed ``True``, divided by the sum of all factor
      weights (so the score is always in [0, 1]).
    * The eligibility tier is the highest threshold the score clears.
    """

    obs = observations or derive_factor_observations(candidate)
    factors_def: Mapping[str, Mapping[str, Any]] = policy.rubric.get("factors", {})
    thresholds: Mapping[str, Any] = policy.rubric.get("thresholds", {})
    obs_dict = obs.as_dict()

    failed_required: list[str] = []
    contributions: dict[str, dict[str, Any]] = {}
    weight_total = 0.0
    weight_earned = 0.0

    for factor_name, factor_cfg in factors_def.items():
        weight = float(factor_cfg.get("weight", 0.0))
        required = bool(factor_cfg.get("required", False))
        value = obs_dict.get(factor_name)
        contributions[factor_name] = {
            "value": value,
            "weight": weight,
            "required": required,
        }
        if required and value is not True:
            failed_required.append(factor_name)
            continue
        weight_total += weight
        if value is True:
            weight_earned += weight

    if failed_required:
        rationale = "rejected: required factor(s) not satisfied: " + ", ".join(
            sorted(failed_required)
        )
        return CandidateScore(
            candidate_id=candidate.candidate_id,
            policy_id=policy.policy_id,
            eligibility="rejected",
            confidence=0.0,
            factors={
                "observations": obs_dict,
                "contributions": contributions,
                "failed_required": failed_required,
                "weight_earned": 0.0,
                "weight_total": weight_total,
            },
            rationale=rationale,
            scored_against_definition_hash=candidate.admitted_definition_hash,
        )

    confidence = (weight_earned / weight_total) if weight_total > 0 else 0.0
    confidence = max(0.0, min(1.0, round(confidence, 3)))
    eligibility = _eligibility_for(confidence, thresholds)
    rationale = (
        f"{eligibility} @ confidence={confidence:.3f} "
        f"(earned={weight_earned:.3f}/{weight_total:.3f})"
    )

    return CandidateScore(
        candidate_id=candidate.candidate_id,
        policy_id=policy.policy_id,
        eligibility=eligibility,
        confidence=confidence,
        factors={
            "observations": obs_dict,
            "contributions": contributions,
            "weight_earned": weight_earned,
            "weight_total": weight_total,
        },
        rationale=rationale,
        scored_against_definition_hash=candidate.admitted_definition_hash,
    )


__all__ = [
    "FactorObservations",
    "derive_factor_observations",
    "score_candidate",
]
