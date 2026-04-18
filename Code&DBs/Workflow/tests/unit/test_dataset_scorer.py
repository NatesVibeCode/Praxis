"""Unit tests for runtime/dataset_scorer.py."""

from __future__ import annotations

from contracts.dataset import DatasetScoringPolicy, RawDatasetCandidate
from runtime.dataset_scorer import (
    FactorObservations,
    derive_factor_observations,
    score_candidate,
)


_REVIEW_RUBRIC = {
    "factors": {
        "verifier_passed": {"weight": 0.30, "required": True},
        "reviewer_agreed": {"weight": 0.25},
        "operator_approved": {"weight": 0.20},
        "downstream_success": {"weight": 0.15},
        "no_linked_open_bug": {"weight": 0.10, "required": True},
        "redaction_clean": {"weight": 0.0, "required": True},
        "not_stale": {"weight": 0.0, "required": True},
        "not_duplicate": {"weight": 0.0, "required": True},
    },
    "thresholds": {
        "manual_review": 0.50,
        "sft_eligible": 0.80,
        "preference_eligible": 0.85,
        "eval_eligible": 0.90,
    },
}


def _policy() -> DatasetScoringPolicy:
    return DatasetScoringPolicy(
        policy_id="pol_review_v1",
        policy_slug="review.v1",
        specialist_target="slm/review",
        rubric=_REVIEW_RUBRIC,
        decided_by="nathan",
        rationale="initial",
    )


def _candidate(**overrides: object) -> RawDatasetCandidate:
    base = dict(
        candidate_id="cand_1",
        candidate_kind="review",
        source_receipt_id="rcpt_1",
        source_run_id="run_1",
        source_node_id="review_step",
        raw_input_ref={"path": "$.inputs"},
        raw_output_ref={"path": "$.outputs"},
        dedupe_signature="sig_1",
        admitted_definition_hash="sha256:abc",
        redaction_status="clean",
        staleness_status="fresh",
    )
    base.update(overrides)
    return RawDatasetCandidate(**base)  # type: ignore[arg-type]


def test_required_factor_failure_rejects() -> None:
    cand = _candidate()
    obs = FactorObservations(
        verifier_passed=False,
        reviewer_agreed=True,
        no_linked_open_bug=True,
        redaction_clean=True,
        not_stale=True,
        not_duplicate=True,
    )
    score = score_candidate(cand, _policy(), observations=obs)
    assert score.eligibility == "rejected"
    assert score.confidence == 0.0
    assert "verifier_passed" in score.factors["failed_required"]


def test_undetermined_required_factor_rejects() -> None:
    cand = _candidate()
    obs = FactorObservations(
        verifier_passed=None,
        no_linked_open_bug=True,
        redaction_clean=True,
        not_stale=True,
        not_duplicate=True,
    )
    score = score_candidate(cand, _policy(), observations=obs)
    assert score.eligibility == "rejected"


def test_full_signal_clears_eval_threshold() -> None:
    cand = _candidate()
    obs = FactorObservations(
        verifier_passed=True,
        reviewer_agreed=True,
        operator_approved=True,
        downstream_success=True,
        no_linked_open_bug=True,
        redaction_clean=True,
        not_stale=True,
        not_duplicate=True,
    )
    score = score_candidate(cand, _policy(), observations=obs)
    assert score.eligibility == "eval_eligible"
    assert score.confidence == 1.0


def test_partial_signal_lands_in_manual_review() -> None:
    cand = _candidate()
    # verifier (.30) + no_bug (.10) + reviewer (.25) = .65 / 1.0 = .65
    obs = FactorObservations(
        verifier_passed=True,
        reviewer_agreed=True,
        operator_approved=False,
        downstream_success=False,
        no_linked_open_bug=True,
        redaction_clean=True,
        not_stale=True,
        not_duplicate=True,
    )
    score = score_candidate(cand, _policy(), observations=obs)
    assert score.eligibility == "manual_review"
    assert 0.60 <= score.confidence < 0.80


def test_below_manual_review_is_rejected() -> None:
    cand = _candidate()
    # Only verifier (.30) earns; total weight is 1.0 → 0.30
    obs = FactorObservations(
        verifier_passed=True,
        reviewer_agreed=False,
        operator_approved=False,
        downstream_success=False,
        no_linked_open_bug=True,
        redaction_clean=True,
        not_stale=True,
        not_duplicate=True,
    )
    score = score_candidate(cand, _policy(), observations=obs)
    assert score.eligibility == "rejected"
    assert score.confidence < 0.50


def test_derive_observations_reads_summaries() -> None:
    cand = _candidate(
        verifier_summary={"status": "passed"},
        review_summary={"status": "active", "predicate": "validates_review"},
        operator_decision_summary={"status": "decided"},
        downstream_summary={
            "dependent_jobs": [{"status": "succeeded"}],
            "linked_bug_states": [{"status": "fixed"}],
        },
    )
    obs = derive_factor_observations(cand)
    assert obs.verifier_passed is True
    assert obs.reviewer_agreed is True
    assert obs.operator_approved is True
    assert obs.downstream_success is True
    assert obs.no_linked_open_bug is True
    assert obs.bug_status_resolved is True
    assert obs.redaction_clean is True
    assert obs.not_stale is True


def test_stale_candidate_fails_required_not_stale() -> None:
    cand = _candidate(staleness_status="definition_stale")
    obs = derive_factor_observations(cand)
    assert obs.not_stale is False
    score = score_candidate(cand, _policy(), observations=obs)
    assert score.eligibility == "rejected"
    assert "not_stale" in score.factors["failed_required"]


def test_eligibility_picks_highest_threshold() -> None:
    # verifier .30 + reviewer .25 + operator .20 + downstream .15 + bug .10 = 1.00
    cand = _candidate()
    obs = FactorObservations(
        verifier_passed=True,
        reviewer_agreed=True,
        operator_approved=True,
        downstream_success=True,
        no_linked_open_bug=True,
        redaction_clean=True,
        not_stale=True,
        not_duplicate=True,
    )
    score = score_candidate(cand, _policy(), observations=obs)
    # Highest tier the score clears.
    assert score.eligibility == "eval_eligible"
