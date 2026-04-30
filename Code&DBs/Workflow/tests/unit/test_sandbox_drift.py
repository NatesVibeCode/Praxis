from __future__ import annotations

import pytest

from runtime.virtual_lab.sandbox_drift import (
    ClassificationCauseAssessment,
    DriftClassification,
    HandoffReference,
    PredictedActualCheck,
    PromotionCandidate,
    PromotionManifest,
    SandboxDriftError,
    SandboxEvidencePackage,
    SandboxExecutionRecord,
    SandboxReadbackEvidence,
    build_drift_ledger,
    build_stop_continue_summary,
    compare_predicted_actual,
)


def _candidate() -> PromotionCandidate:
    return PromotionCandidate(
        candidate_id="candidate.phase8.account_sync",
        owner="operator:nate",
        build_ref="build.account_sync.20260430",
        sandbox_target="sandbox.crm.dev",
        scope_ref="scope.client_operating_model.phase_08",
        scenario_refs=("scenario.qualify_account",),
        prediction_refs=("prediction.qualify_account.owner_assignment",),
        contract_refs=("contract.crm.account.status.v1",),
    )


def _manifest(candidate: PromotionCandidate | None = None) -> PromotionManifest:
    return PromotionManifest(
        manifest_id="manifest.phase8",
        created_at="2026-04-30T18:00:00Z",
        created_by="agent.phase_08",
        candidates=(candidate or _candidate(),),
        validation_window_ref="validation.phase8.window1",
    )


def _execution(status: str = "completed") -> SandboxExecutionRecord:
    return SandboxExecutionRecord(
        execution_id="execution.qualify_account.1",
        candidate_id="candidate.phase8.account_sync",
        scenario_ref="scenario.qualify_account",
        sandbox_target="sandbox.crm.dev",
        environment_ref="sandbox.crm.dev/env/phase8",
        config_ref="config.crm.phase8.locked",
        seed_data_ref="seed.crm.phase8.accounts",
        status=status,
        started_at="2026-04-30T18:05:00Z",
        ended_at="2026-04-30T18:06:00Z",
    )


def _evidence(*, available: bool = True, trusted: bool = True) -> SandboxReadbackEvidence:
    return SandboxReadbackEvidence(
        evidence_id="evidence.account_status",
        candidate_id="candidate.phase8.account_sync",
        scenario_ref="scenario.qualify_account",
        observable_ref="crm.account.account:001.status",
        evidence_kind="api_response",
        captured_at="2026-04-30T18:06:01Z",
        available=available,
        trusted=trusted,
        immutable_ref="receipt.sandbox.readback.1" if available else None,
        payload={"status": "qualified"},
        failure_reason=None if available and trusted else "readback unavailable",
    )


def _package(evidence: SandboxReadbackEvidence | None = None) -> SandboxEvidencePackage:
    return SandboxEvidencePackage(
        package_id="evidence_package.qualify_account",
        execution_id="execution.qualify_account.1",
        candidate_id="candidate.phase8.account_sync",
        scenario_ref="scenario.qualify_account",
        evidence=(evidence or _evidence(),),
    )


def _report_for(check: PredictedActualCheck, evidence: SandboxReadbackEvidence | None = None):
    return compare_predicted_actual(
        candidate=_candidate(),
        execution=_execution(),
        evidence_package=_package(evidence),
        checks=(check,),
        report_id="comparison.qualify_account",
    )


def test_predicted_actual_match_allows_continue_summary() -> None:
    report = _report_for(
        PredictedActualCheck(
            check_id="check.status",
            dimension="output",
            prediction={"status": "qualified"},
            actual={"status": "qualified"},
            evidence_refs=("evidence.account_status",),
        )
    )

    summary = build_stop_continue_summary(
        manifest=_manifest(),
        reports=(report,),
        summary_id="summary.phase8.match",
    )

    assert report.status == "match"
    assert report.rows[0].status == "match"
    assert summary.recommendation == "continue"
    assert summary.candidate_decisions[0].decision == "validated"


def test_partial_match_requires_classification_and_continues_with_constraints() -> None:
    report = _report_for(
        PredictedActualCheck(
            check_id="check.owner_assignment",
            dimension="state_transition",
            prediction={"owner": "sales", "status": "qualified"},
            actual={"owner": "sales", "status": "qualified", "audit_label": "sandbox-only"},
            evidence_refs=("evidence.account_status",),
            accepted_status="partial_match",
            delta="sandbox emitted an extra audit label outside the core flow",
            impact="core transition valid; contract needs tolerated-variance note",
            disposition="document",
        )
    )
    classification = DriftClassification(
        classification_id="classification.contract_variance",
        comparison_report_id=report.report_id,
        row_id=report.rows[0].row_id,
        reason_codes=("CONTRACT_UNDERSPECIFIED",),
        severity="low",
        layer="contract",
        disposition="document",
        owner="operator:nate",
        cause_assessment=ClassificationCauseAssessment(environment_excluded=True, harness_excluded=True),
        handoff_refs=(
            HandoffReference(
                handoff_kind="contract_note",
                target_ref="contract.crm.account.status.v1",
                evidence_refs=("evidence.account_status",),
            ),
        ),
    )
    ledger = build_drift_ledger(report=report, classifications=(classification,))
    summary = build_stop_continue_summary(manifest=_manifest(), reports=(report,), ledgers=(ledger,))

    assert report.status == "partial_match"
    assert ledger.classifications[0].reason_codes == ("CONTRACT_UNDERSPECIFIED",)
    assert summary.recommendation == "continue_with_constraints"
    assert summary.candidate_decisions[0].decision == "drifted"


def test_drift_records_reason_codes_and_gap_handoff() -> None:
    report = _report_for(
        PredictedActualCheck(
            check_id="check.status",
            dimension="output",
            prediction={"status": "qualified"},
            actual={"status": "prospect"},
            evidence_refs=("evidence.account_status",),
            impact="candidate would leave the account in the wrong lifecycle stage",
        )
    )
    classification = DriftClassification(
        classification_id="classification.prediction_error",
        comparison_report_id=report.report_id,
        row_id=report.rows[0].row_id,
        reason_codes=("PREDICTION_ERROR",),
        severity="medium",
        layer="workflow",
        disposition="fix_now",
        owner="operator:nate",
        cause_assessment=ClassificationCauseAssessment(environment_excluded=True, contract_excluded=True),
        handoff_refs=(
            HandoffReference(
                handoff_kind="gap",
                target_ref="gap.phase8.account_status_prediction",
                evidence_refs=("evidence.account_status",),
            ),
        ),
    )
    ledger = build_drift_ledger(report=report, classifications=(classification,))

    assert report.status == "drift"
    assert report.rows[0].delta == "actual differs from prediction"
    assert ledger.classifications[0].handoff_refs[0].handoff_kind == "gap"


def test_blocked_evidence_classifies_as_observability_gap_and_rerun() -> None:
    blocked_evidence = _evidence(available=False, trusted=False)
    report = _report_for(
        PredictedActualCheck(
            check_id="check.status",
            dimension="output",
            prediction={"status": "qualified"},
            actual=None,
            evidence_refs=("evidence.account_status",),
        ),
        blocked_evidence,
    )
    classification = DriftClassification(
        classification_id="classification.observability_gap",
        comparison_report_id=report.report_id,
        row_id=report.rows[0].row_id,
        reason_codes=("OBSERVABILITY_GAP",),
        severity="high",
        layer="observability",
        disposition="rerun_required",
        owner="operator:nate",
        cause_assessment=ClassificationCauseAssessment(),
        handoff_refs=(
            HandoffReference(
                handoff_kind="gap",
                target_ref="gap.phase8.sandbox_readback",
                evidence_refs=("evidence.account_status",),
            ),
        ),
    )
    ledger = build_drift_ledger(report=report, classifications=(classification,))
    summary = build_stop_continue_summary(manifest=_manifest(), reports=(report,), ledgers=(ledger,))

    assert report.status == "blocked"
    assert report.rows[0].blocker_reason == "required evidence is missing or untrusted"
    assert ledger.classifications[0].reason_codes == ("OBSERVABILITY_GAP",)
    assert summary.recommendation == "rerun_phase"
    assert summary.candidate_decisions[0].decision == "blocked"


def test_environment_origin_drift_stays_out_of_bug_defect_lane() -> None:
    report = _report_for(
        PredictedActualCheck(
            check_id="check.seed_owner",
            dimension="data_shape",
            prediction={"seed_owner": "marketing"},
            actual={"seed_owner": "legacy-import"},
            evidence_refs=("evidence.account_status",),
            impact="seed data invalidates the planned comparison",
        )
    )
    classification = DriftClassification(
        classification_id="classification.seed_variance",
        comparison_report_id=report.report_id,
        row_id=report.rows[0].row_id,
        reason_codes=("SEED_DATA_VARIANCE", "ENV_MISCONFIG"),
        severity="high",
        layer="environment",
        disposition="rerun_required",
        owner="operator:nate",
        cause_assessment=ClassificationCauseAssessment(
            contract_excluded=True,
            harness_excluded=True,
            notes="sandbox seed did not match the manifest",
        ),
        handoff_refs=(
            HandoffReference(
                handoff_kind="gap",
                target_ref="gap.phase8.seed_restaging",
                evidence_refs=("evidence.account_status",),
            ),
        ),
    )
    ledger = build_drift_ledger(report=report, classifications=(classification,))

    assert "IMPLEMENTATION_DEFECT" not in ledger.classifications[0].reason_codes
    assert ledger.classifications[0].layer == "environment"
    assert ledger.classifications[0].handoff_refs[0].handoff_kind == "gap"


def test_implementation_defect_classification_requires_excluding_environment_contract_and_harness() -> None:
    report = _report_for(
        PredictedActualCheck(
            check_id="check.status",
            dimension="contract",
            prediction={"status": "qualified"},
            actual={"status": "prospect"},
            evidence_refs=("evidence.account_status",),
        )
    )

    with pytest.raises(SandboxDriftError) as excinfo:
        DriftClassification(
            classification_id="classification.bad_defect_claim",
            comparison_report_id=report.report_id,
            row_id=report.rows[0].row_id,
            reason_codes=("IMPLEMENTATION_DEFECT",),
            severity="high",
            layer="workflow",
            disposition="fix_now",
            owner="operator:nate",
            cause_assessment=ClassificationCauseAssessment(environment_excluded=True, contract_excluded=True),
        )

    assert excinfo.value.reason_code == "sandbox_drift.implementation_defect_guardrail"

    classification = DriftClassification(
        classification_id="classification.supported_defect_claim",
        comparison_report_id=report.report_id,
        row_id=report.rows[0].row_id,
        reason_codes=("IMPLEMENTATION_DEFECT",),
        severity="high",
        layer="workflow",
        disposition="fix_now",
        owner="operator:nate",
        cause_assessment=ClassificationCauseAssessment(
            environment_excluded=True,
            contract_excluded=True,
            harness_excluded=True,
        ),
        handoff_refs=(
            HandoffReference(
                handoff_kind="bug",
                target_ref="BUG-PROPOSED-account-status",
                evidence_refs=("evidence.account_status",),
            ),
        ),
    )

    assert classification.cause_assessment.implementation_defect_supported is True
    assert classification.handoff_refs[0].handoff_kind == "bug"
