"""Unit tests for runtime/dataset_candidate_subscriber.py.

Focuses on the pure helpers: ``classify_candidate_kinds``,
``compute_dedupe_signature``, and ``build_candidate_from_bundle``.
The DB-driven outer loop is exercised through integration tests once
migrations land; here we only test the deterministic logic that decides
*whether* and *how* a candidate is shaped.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from runtime.dataset_candidate_subscriber import (
    _ReceiptEvidenceBundle,
    build_candidate_from_bundle,
    classify_candidate_kinds,
    compute_dedupe_signature,
)


def test_classify_review_by_output_key() -> None:
    receipt = {"node_id": "step_42", "outputs": {"review_verdict": {"verdict": "approve"}}}
    assert classify_candidate_kinds(receipt) == ("review",)


def test_classify_review_by_node_id_hint() -> None:
    receipt = {"node_id": "review_step", "outputs": {}}
    assert classify_candidate_kinds(receipt) == ("review",)


def test_classify_emits_nothing_for_unknown_receipt() -> None:
    receipt = {"node_id": "compile_step", "outputs": {"artifact_id": "a1"}}
    assert classify_candidate_kinds(receipt) == ()


def test_classify_triage_by_output_key() -> None:
    receipt = {
        "node_id": "classify_failure",
        "outputs": {"triage_verdict": {"severity": "P2"}},
    }
    assert classify_candidate_kinds(receipt) == ("triage",)


def test_classify_triage_by_task_type() -> None:
    receipt = {
        "node_id": "step_17",
        "inputs": {"task_type": "triage"},
        "outputs": {"notes": "x"},
    }
    assert classify_candidate_kinds(receipt) == ("triage",)


def test_classify_operator_explain_by_output_key() -> None:
    receipt = {
        "node_id": "step_9",
        "outputs": {"operator_explanation": "because the circuit opened"},
    }
    assert classify_candidate_kinds(receipt) == ("operator_explain",)


def test_classify_operator_explain_by_task_type() -> None:
    receipt = {
        "node_id": "step_9",
        "inputs": {"task_type": "operator_explain"},
        "outputs": {},
    }
    assert classify_candidate_kinds(receipt) == ("operator_explain",)


def test_classify_review_wins_over_explain_when_both_signals_present() -> None:
    receipt = {
        "node_id": "review_and_explain",
        "outputs": {
            "review_verdict": {"verdict": "approve"},
            "operator_explanation": "ok",
        },
    }
    assert classify_candidate_kinds(receipt) == ("review",)


def test_classify_triage_precedes_review_when_both_signals_present() -> None:
    receipt = {
        "node_id": "triage_and_review",
        "outputs": {
            "triage_verdict": {"severity": "P1"},
            "review_verdict": {"verdict": "approve"},
        },
    }
    assert classify_candidate_kinds(receipt) == ("triage",)


def test_dedupe_signature_is_stable_and_kind_sensitive() -> None:
    inp = {"diff": "--- a\n+++ b", "task_type": "code_review"}
    sig_review = compute_dedupe_signature(
        candidate_kind="review", route_slug="slm/review", raw_input=inp
    )
    sig_review_again = compute_dedupe_signature(
        candidate_kind="review", route_slug="slm/review", raw_input=inp
    )
    sig_triage = compute_dedupe_signature(
        candidate_kind="triage", route_slug="slm/review", raw_input=inp
    )
    assert sig_review == sig_review_again
    assert sig_review != sig_triage
    assert sig_review.startswith("sha256:")


def test_dedupe_signature_strips_volatile_tokens() -> None:
    a = {
        "diff": "review at 2026-04-18T13:02:11Z by 01HZABCDEFGHJKMNPQRSTVWXYZ",
        "path": str(Path(tempfile.gettempdir()) / "praxis" / "file.py"),
    }
    b = {
        "diff": "review at 2024-01-01T00:00:00Z by 01HXYZAAAAAAAAAAAAAAAAAAAA",
        "path": "/tmp/someone/file.py",
    }
    sig_a = compute_dedupe_signature(candidate_kind="review", route_slug=None, raw_input=a)
    sig_b = compute_dedupe_signature(candidate_kind="review", route_slug=None, raw_input=b)
    assert sig_a == sig_b


def _bundle(**overrides) -> _ReceiptEvidenceBundle:
    receipt = overrides.pop(
        "receipt",
        {
            "receipt_id": "rcpt_1",
            "run_id": "run_1",
            "node_id": "review_step",
            "workflow_id": "wf_1",
            "inputs": {"task_type": "code_review", "diff": "...", "persona": "strict"},
            "outputs": {
                "review_verdict": {"verdict": "approve"},
                "parsed": {"verdict": "approve"},
                "git_provenance": {"commit": "deadbeef"},
            },
            "decision_refs": [],
        },
    )
    workflow_run = overrides.pop(
        "workflow_run",
        {
            "run_id": "run_1",
            "workflow_definition_id": "wfdef_1",
            "admitted_definition_hash": "sha256:def",
            "current_state": "succeeded",
        },
    )
    return _ReceiptEvidenceBundle(
        receipt=receipt,
        verifications=overrides.pop("verifications", ()),
        assertions=overrides.pop("assertions", ()),
        operator_decisions=overrides.pop("operator_decisions", ()),
        bug_links=overrides.pop("bug_links", ()),
        dispatch_run=overrides.pop("dispatch_run", None),
        workflow_run=workflow_run,
    )


def test_build_candidate_basic_shape() -> None:
    cand = build_candidate_from_bundle(_bundle(), candidate_kind="review")
    assert cand.candidate_kind == "review"
    assert cand.source_receipt_id == "rcpt_1"
    assert cand.source_run_id == "run_1"
    assert cand.source_node_id == "review_step"
    assert cand.workflow_definition_id == "wfdef_1"
    assert cand.admitted_definition_hash == "sha256:def"
    assert cand.repo_snapshot_ref == "deadbeef"
    assert cand.persona == "strict"
    assert cand.task_type == "code_review"
    assert cand.redaction_status == "clean"
    assert cand.staleness_status == "fresh"
    assert cand.dedupe_signature.startswith("sha256:")
    # Always at least one evidence link (the receipt itself).
    kinds = {l.evidence_kind for l in cand.evidence_links}
    assert "receipt" in kinds


def test_build_candidate_includes_all_evidence_link_kinds() -> None:
    bundle = _bundle(
        verifications=({"verification_run_id": "vr_1", "status": "passed"},),
        assertions=(
            {
                "semantic_assertion_id": "sa_1",
                "predicate_slug": "validates_review",
                "assertion_status": "active",
            },
        ),
        operator_decisions=(
            {"operator_decision_id": "od_1", "decision_status": "decided"},
        ),
        bug_links=({"bug_id": "bug_1"},),
        dispatch_run={"run_id": "run_1", "status": "succeeded"},
    )
    cand = build_candidate_from_bundle(bundle, candidate_kind="review")
    kinds = {l.evidence_kind for l in cand.evidence_links}
    assert kinds >= {
        "receipt",
        "verification_run",
        "semantic_assertion",
        "operator_decision",
        "bug",
        "dispatch_run",
        "workflow_run",
    }
    assert cand.linked_bug_ids == ("bug_1",)


def test_build_candidate_dedupe_signature_matches_compute() -> None:
    bundle = _bundle()
    cand = build_candidate_from_bundle(bundle, candidate_kind="review")
    expected = compute_dedupe_signature(
        candidate_kind="review",
        route_slug=cand.route_slug,
        raw_input=bundle.receipt["inputs"],
    )
    assert cand.dedupe_signature == expected


def test_build_candidate_redaction_blocks_on_secret() -> None:
    receipt = {
        "receipt_id": "rcpt_2",
        "run_id": "run_2",
        "node_id": "review_step",
        "workflow_id": "wf_1",
        "inputs": {"prompt": "review this", "auth": "Authorization: Bearer abc.def.ghi"},
        "outputs": {"review_verdict": {"verdict": "approve"}},
    }
    cand = build_candidate_from_bundle(
        _bundle(receipt=receipt), candidate_kind="review"
    )
    assert cand.redaction_status == "sensitive_blocked"
