"""CQRS command for Virtual Lab sandbox promotion authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

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
from runtime.virtual_lab.state import virtual_lab_digest
from storage.postgres.virtual_lab_sandbox_promotion_repository import (
    persist_virtual_lab_sandbox_promotion_record,
)
from storage.postgres.virtual_lab_simulation_repository import load_virtual_lab_simulation_run


class SandboxPromotionCandidateRecordInput(BaseModel):
    """Evidence needed to promote and read back one manifest candidate."""

    candidate_id: str
    simulation_run_id: str
    execution: dict[str, Any]
    evidence_package: dict[str, Any]
    checks: list[dict[str, Any]] = Field(min_length=1)
    classifications: list[dict[str, Any]] = Field(default_factory=list)
    report_id: str | None = None
    ledger_id: str | None = None

    @field_validator("candidate_id", "simulation_run_id", "report_id", "ledger_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object, info) -> str | None:
        if value is None or value == "":
            if info.field_name in {"report_id", "ledger_id"}:
                return None
            raise ValueError(f"{info.field_name} is required")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{info.field_name} must be a non-empty string")
        return value.strip()


class RecordVirtualLabSandboxPromotionCommand(BaseModel):
    """Persist sandbox promotion, readback, drift, and stop/continue proof."""

    manifest: dict[str, Any]
    candidate_records: list[SandboxPromotionCandidateRecordInput] = Field(min_length=1)
    promotion_record_id: str | None = None
    summary_id: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None
    require_simulation_verifier_proof: bool = True

    @field_validator("promotion_record_id", "summary_id", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @model_validator(mode="after")
    def _candidate_ids_are_unique(self) -> "RecordVirtualLabSandboxPromotionCommand":
        candidate_ids = [record.candidate_id for record in self.candidate_records]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("candidate_records cannot contain duplicate candidate_id values")
        return self


def handle_virtual_lab_sandbox_promotion_record(
    command: RecordVirtualLabSandboxPromotionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    manifest = _manifest_from_dict(command.manifest)
    candidates = {candidate.candidate_id: candidate for candidate in manifest.candidates}
    record_ids = {record.candidate_id for record in command.candidate_records}
    manifest_ids = set(candidates)
    if record_ids != manifest_ids:
        raise SandboxDriftError(
            "sandbox_drift.promotion_candidate_coverage_mismatch",
            "sandbox promotion command must provide one candidate record for every manifest candidate",
            details={
                "missing_candidate_ids": sorted(manifest_ids - record_ids),
                "unknown_candidate_ids": sorted(record_ids - manifest_ids),
            },
        )

    persisted_candidate_records: list[dict[str, Any]] = []
    reports = []
    ledgers = []
    for record in command.candidate_records:
        simulation_proof = (
            _require_verified_simulation_run(conn, record.simulation_run_id)
            if command.require_simulation_verifier_proof
            else {}
        )
        candidate = candidates[record.candidate_id]
        execution = _execution_from_dict(record.execution)
        if execution.candidate_id != candidate.candidate_id:
            raise SandboxDriftError(
                "sandbox_drift.execution_candidate_mismatch",
                "sandbox execution record must match the manifest candidate",
                details={
                    "candidate_id": candidate.candidate_id,
                    "execution_candidate_id": execution.candidate_id,
                },
            )
        evidence_package = _evidence_package_from_dict(record.evidence_package)
        checks = tuple(_check_from_dict(item) for item in record.checks)
        report = compare_predicted_actual(
            candidate=candidate,
            execution=execution,
            evidence_package=evidence_package,
            checks=checks,
            report_id=record.report_id,
        )
        classifications = tuple(_classification_from_dict(item) for item in record.classifications)
        ledger = build_drift_ledger(
            report=report,
            classifications=classifications,
            ledger_id=record.ledger_id,
        )
        reports.append(report)
        ledgers.append(ledger)
        persisted_candidate_records.append(
            {
                "candidate": candidate.to_json(),
                "simulation_run_id": record.simulation_run_id,
                "simulation_proof": _simulation_proof_summary(simulation_proof),
                "execution": execution.to_json(),
                "evidence_package": evidence_package.to_json(),
                "checks": [check.to_json() for check in checks],
                "report": report.to_json(),
                "ledger": ledger.to_json(),
            }
        )

    summary = build_stop_continue_summary(
        manifest=manifest,
        reports=tuple(reports),
        ledgers=tuple(ledgers),
        summary_id=command.summary_id,
    )
    promotion_record_id = command.promotion_record_id or _stable_ref(
        "sandbox_promotion_record",
        {
            "manifest_digest": manifest.manifest_digest,
            "summary_digest": summary.summary_digest,
            "reports": [report.report_digest for report in reports],
        },
    )
    persisted = persist_virtual_lab_sandbox_promotion_record(
        conn,
        promotion_record_id=promotion_record_id,
        manifest=manifest.to_json(),
        candidate_records=persisted_candidate_records,
        summary=summary.to_json(),
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    drift_count = sum(len(ledger.classifications) for ledger in ledgers)
    handoff_count = sum(
        len(classification.handoff_refs)
        for ledger in ledgers
        for classification in ledger.classifications
    )
    event_payload = {
        "promotion_record_id": promotion_record_id,
        "manifest_id": manifest.manifest_id,
        "manifest_digest": manifest.manifest_digest,
        "recommendation": summary.recommendation,
        "candidate_ids": list(manifest.candidate_ids()),
        "simulation_run_ids": [record["simulation_run_id"] for record in persisted_candidate_records],
        "report_ids": [report.report_id for report in reports],
        "report_statuses": [report.status for report in reports],
        "drift_classification_count": drift_count,
        "handoff_count": handoff_count,
        "summary_digest": summary.summary_digest,
    }
    return {
        "ok": True,
        "operation": "virtual_lab_sandbox_promotion_record",
        "promotion_record_id": promotion_record_id,
        "manifest_id": manifest.manifest_id,
        "recommendation": summary.recommendation,
        "candidate_count": len(persisted_candidate_records),
        "drift_classification_count": drift_count,
        "handoff_count": handoff_count,
        "summary": summary.to_json(),
        "persisted": persisted,
        "event_payload": event_payload,
    }


def _require_verified_simulation_run(conn: Any, run_id: str) -> dict[str, Any]:
    run = load_virtual_lab_simulation_run(
        conn,
        run_id=run_id,
        include_events=False,
        include_state_events=False,
        include_transitions=False,
        include_actions=False,
        include_automation=False,
        include_assertions=False,
        include_verifiers=True,
        include_gaps=False,
        include_blockers=True,
    )
    if run is None:
        raise SandboxDriftError(
            "sandbox_drift.simulation_run_not_found",
            "sandbox promotion requires a persisted simulation run",
            details={"simulation_run_id": run_id},
        )
    verifier_results = list(run.get("verifier_results") or [])
    blockers = list(run.get("promotion_blockers") or [])
    failed_verifiers = [
        item
        for item in verifier_results
        if str(item.get("status") or "").strip() != "passed"
    ]
    if run.get("status") != "passed" or not verifier_results or failed_verifiers or blockers:
        raise SandboxDriftError(
            "sandbox_drift.simulation_verifier_proof_required",
            "sandbox promotion requires a passed simulation run with passing verifier proof and no blockers",
            details={
                "simulation_run_id": run_id,
                "status": run.get("status"),
                "verifier_count": len(verifier_results),
                "failed_verifier_count": len(failed_verifiers),
                "blocker_count": len(blockers),
            },
        )
    return dict(run)


def _simulation_proof_summary(run: dict[str, Any]) -> dict[str, Any]:
    if not run:
        return {}
    return {
        "run_id": run.get("run_id"),
        "scenario_id": run.get("scenario_id"),
        "status": run.get("status"),
        "result_digest": run.get("result_digest"),
        "trace_digest": run.get("trace_digest"),
        "verifier_count": len(run.get("verifier_results") or []),
        "blocker_count": len(run.get("promotion_blockers") or []),
    }


def _manifest_from_dict(payload: dict[str, Any]) -> PromotionManifest:
    candidates = tuple(_candidate_from_dict(item) for item in payload.get("candidates", []))
    return PromotionManifest(
        manifest_id=payload.get("manifest_id"),
        created_at=payload.get("created_at"),
        created_by=payload.get("created_by"),
        validation_window_ref=payload.get("validation_window_ref"),
        candidates=candidates,
        notes=payload.get("notes", ""),
    )


def _candidate_from_dict(payload: dict[str, Any]) -> PromotionCandidate:
    return PromotionCandidate(
        candidate_id=payload.get("candidate_id"),
        owner=payload.get("owner"),
        build_ref=payload.get("build_ref"),
        sandbox_target=payload.get("sandbox_target"),
        scope_ref=payload.get("scope_ref"),
        scenario_refs=tuple(payload.get("scenario_refs") or ()),
        prediction_refs=tuple(payload.get("prediction_refs") or ()),
        contract_refs=tuple(payload.get("contract_refs") or ()),
        assumption_refs=tuple(payload.get("assumption_refs") or ()),
        metadata=dict(payload.get("metadata") or {}),
    )


def _execution_from_dict(payload: dict[str, Any]) -> SandboxExecutionRecord:
    return SandboxExecutionRecord(
        execution_id=payload.get("execution_id"),
        candidate_id=payload.get("candidate_id"),
        scenario_ref=payload.get("scenario_ref"),
        sandbox_target=payload.get("sandbox_target"),
        environment_ref=payload.get("environment_ref"),
        config_ref=payload.get("config_ref"),
        seed_data_ref=payload.get("seed_data_ref"),
        status=payload.get("status"),
        started_at=payload.get("started_at"),
        ended_at=payload.get("ended_at"),
        operator_intervention=bool(payload.get("operator_intervention", False)),
        deviations=tuple(payload.get("deviations") or ()),
        raw_evidence_refs=tuple(payload.get("raw_evidence_refs") or ()),
        metadata=dict(payload.get("metadata") or {}),
    )


def _evidence_package_from_dict(payload: dict[str, Any]) -> SandboxEvidencePackage:
    return SandboxEvidencePackage(
        package_id=payload.get("package_id"),
        execution_id=payload.get("execution_id"),
        candidate_id=payload.get("candidate_id"),
        scenario_ref=payload.get("scenario_ref"),
        evidence=tuple(_readback_evidence_from_dict(item) for item in payload.get("evidence", [])),
    )


def _readback_evidence_from_dict(payload: dict[str, Any]) -> SandboxReadbackEvidence:
    return SandboxReadbackEvidence(
        evidence_id=payload.get("evidence_id"),
        candidate_id=payload.get("candidate_id"),
        scenario_ref=payload.get("scenario_ref"),
        observable_ref=payload.get("observable_ref"),
        evidence_kind=payload.get("evidence_kind"),
        captured_at=payload.get("captured_at"),
        available=bool(payload.get("available")),
        trusted=bool(payload.get("trusted")),
        immutable_ref=payload.get("immutable_ref"),
        payload=payload.get("payload"),
        failure_reason=payload.get("failure_reason"),
    )


def _check_from_dict(payload: dict[str, Any]) -> PredictedActualCheck:
    return PredictedActualCheck(
        check_id=payload.get("check_id"),
        dimension=payload.get("dimension"),
        prediction=payload.get("prediction"),
        actual=payload.get("actual"),
        evidence_refs=tuple(payload.get("evidence_refs") or ()),
        required_evidence=bool(payload.get("required_evidence", True)),
        accepted_status=payload.get("accepted_status"),
        delta=payload.get("delta", ""),
        impact=payload.get("impact", ""),
        disposition=payload.get("disposition"),
    )


def _classification_from_dict(payload: dict[str, Any]) -> DriftClassification:
    cause = dict(payload.get("cause_assessment") or {})
    return DriftClassification(
        classification_id=payload.get("classification_id"),
        comparison_report_id=payload.get("comparison_report_id"),
        row_id=payload.get("row_id"),
        reason_codes=tuple(payload.get("reason_codes") or ()),
        severity=payload.get("severity"),
        layer=payload.get("layer"),
        disposition=payload.get("disposition"),
        owner=payload.get("owner"),
        cause_assessment=ClassificationCauseAssessment(
            environment_excluded=bool(cause.get("environment_excluded", False)),
            contract_excluded=bool(cause.get("contract_excluded", False)),
            harness_excluded=bool(cause.get("harness_excluded", False)),
            dependency_excluded=bool(cause.get("dependency_excluded", False)),
            prediction_excluded=bool(cause.get("prediction_excluded", False)),
            notes=str(cause.get("notes") or ""),
        ),
        handoff_refs=tuple(_handoff_from_dict(item) for item in payload.get("handoff_refs", [])),
        notes=str(payload.get("notes") or ""),
    )


def _handoff_from_dict(payload: dict[str, Any]) -> HandoffReference:
    return HandoffReference(
        handoff_kind=payload.get("handoff_kind"),
        target_ref=payload.get("target_ref"),
        status=payload.get("status", "proposed"),
        evidence_refs=tuple(payload.get("evidence_refs") or ()),
        notes=str(payload.get("notes") or ""),
    )


def _stable_ref(prefix: str, payload: dict[str, Any]) -> str:
    digest = virtual_lab_digest(payload, purpose=f"virtual_lab.{prefix}.id.v1").split(":")[-1]
    return f"{prefix}.{digest[:20]}"


__all__ = [
    "RecordVirtualLabSandboxPromotionCommand",
    "SandboxPromotionCandidateRecordInput",
    "handle_virtual_lab_sandbox_promotion_record",
]
