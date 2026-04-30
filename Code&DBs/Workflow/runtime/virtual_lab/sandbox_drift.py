"""Virtual Lab sandbox promotion and drift feedback primitives.

This module is pure domain code. It records the contracts needed to promote
bounded candidates into a sandbox, attach readback evidence, compare predicted
and actual behavior, classify drift, and summarize stop/continue posture.

It does not deploy candidates, mutate a sandbox, call live integrations,
persist records, file bugs, or open gaps. Bug and gap outputs are explicit
handoff references for the caller to execute through the authoritative
surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from core.object_truth_ops import canonical_value

from .state import virtual_lab_digest


SANDBOX_DRIFT_SCHEMA_VERSION = 1
SANDBOX_DRIFT_RUNTIME_VERSION = "virtual_lab.sandbox_drift.v1"

ComparisonStatus = Literal["match", "partial_match", "drift", "blocked"]
ExecutionStatus = Literal["completed", "failed", "blocked", "aborted"]
DriftReasonCode = Literal[
    "ENV_MISCONFIG",
    "SEED_DATA_VARIANCE",
    "CONTRACT_UNDERSPECIFIED",
    "CONTRACT_INCORRECT",
    "IMPLEMENTATION_DEFECT",
    "PREDICTION_ERROR",
    "DEPENDENCY_CHANGE",
    "OBSERVABILITY_GAP",
    "TEST_HARNESS_FAULT",
    "NONDETERMINISM",
    "UNKNOWN",
]
DriftSeverity = Literal["critical", "high", "medium", "low"]
DriftLayer = Literal["contract", "workflow", "integration", "data", "environment", "observability"]
DriftDisposition = Literal["fix_now", "document", "defer", "rerun_required", "stop_phase"]
HandoffKind = Literal["bug", "gap", "contract_note", "evidence", "receipt"]
HandoffStatus = Literal["proposed", "open", "linked", "closed"]
CandidateDecision = Literal["validated", "drifted", "blocked", "stopped"]
PhaseRecommendation = Literal["continue", "continue_with_constraints", "rerun_phase", "stop"]
ComparisonDimension = Literal[
    "contract",
    "output",
    "state_transition",
    "error_path",
    "sequencing",
    "data_shape",
    "operational",
]

COMPARISON_STATUSES = {"match", "partial_match", "drift", "blocked"}
EXECUTION_STATUSES = {"completed", "failed", "blocked", "aborted"}
DRIFT_REASON_CODES = {
    "ENV_MISCONFIG",
    "SEED_DATA_VARIANCE",
    "CONTRACT_UNDERSPECIFIED",
    "CONTRACT_INCORRECT",
    "IMPLEMENTATION_DEFECT",
    "PREDICTION_ERROR",
    "DEPENDENCY_CHANGE",
    "OBSERVABILITY_GAP",
    "TEST_HARNESS_FAULT",
    "NONDETERMINISM",
    "UNKNOWN",
}
DRIFT_SEVERITIES = {"critical", "high", "medium", "low"}
DRIFT_LAYERS = {"contract", "workflow", "integration", "data", "environment", "observability"}
DRIFT_DISPOSITIONS = {"fix_now", "document", "defer", "rerun_required", "stop_phase"}
HANDOFF_KINDS = {"bug", "gap", "contract_note", "evidence", "receipt"}
HANDOFF_STATUSES = {"proposed", "open", "linked", "closed"}
COMPARISON_DIMENSIONS = {
    "contract",
    "output",
    "state_transition",
    "error_path",
    "sequencing",
    "data_shape",
    "operational",
}


class SandboxDriftError(RuntimeError):
    """Raised when sandbox drift evidence cannot be represented safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


@dataclass(frozen=True, slots=True)
class HandoffReference:
    handoff_kind: HandoffKind
    target_ref: str
    status: HandoffStatus = "proposed"
    evidence_refs: tuple[str, ...] = ()
    notes: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "handoff_kind", _member(self.handoff_kind, HANDOFF_KINDS, "handoff_kind"))
        object.__setattr__(self, "target_ref", _required_text(self.target_ref, "target_ref"))
        object.__setattr__(self, "status", _member(self.status, HANDOFF_STATUSES, "status"))
        object.__setattr__(self, "evidence_refs", _clean_text_tuple(self.evidence_refs))
        object.__setattr__(self, "notes", _optional_text(self.notes) or "")

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.sandbox_handoff_ref.v1",
            "handoff_kind": self.handoff_kind,
            "target_ref": self.target_ref,
            "status": self.status,
            "evidence_refs": list(self.evidence_refs),
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class PromotionCandidate:
    candidate_id: str
    owner: str
    build_ref: str
    sandbox_target: str
    scope_ref: str
    scenario_refs: tuple[str, ...]
    prediction_refs: tuple[str, ...]
    contract_refs: tuple[str, ...] = ()
    assumption_refs: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", _required_text(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "owner", _required_text(self.owner, "owner"))
        object.__setattr__(self, "build_ref", _required_text(self.build_ref, "build_ref"))
        object.__setattr__(self, "sandbox_target", _required_text(self.sandbox_target, "sandbox_target"))
        object.__setattr__(self, "scope_ref", _required_text(self.scope_ref, "scope_ref"))
        object.__setattr__(self, "scenario_refs", _required_text_tuple(self.scenario_refs, "scenario_refs"))
        object.__setattr__(self, "prediction_refs", _required_text_tuple(self.prediction_refs, "prediction_refs"))
        object.__setattr__(self, "contract_refs", _clean_text_tuple(self.contract_refs))
        object.__setattr__(self, "assumption_refs", _clean_text_tuple(self.assumption_refs))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.promotion_candidate.v1",
            "candidate_id": self.candidate_id,
            "owner": self.owner,
            "build_ref": self.build_ref,
            "sandbox_target": self.sandbox_target,
            "scope_ref": self.scope_ref,
            "scenario_refs": list(self.scenario_refs),
            "prediction_refs": list(self.prediction_refs),
            "contract_refs": list(self.contract_refs),
            "assumption_refs": list(self.assumption_refs),
            "metadata": canonical_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class PromotionManifest:
    manifest_id: str
    created_at: str
    created_by: str
    candidates: tuple[PromotionCandidate, ...]
    validation_window_ref: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        candidates = tuple(self.candidates or ())
        if not candidates:
            raise SandboxDriftError(
                "sandbox_drift.manifest_empty",
                "promotion manifest requires at least one candidate",
            )
        candidate_ids = [candidate.candidate_id for candidate in candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise SandboxDriftError(
                "sandbox_drift.duplicate_candidate",
                "promotion manifest cannot contain duplicate candidate ids",
                details={"candidate_ids": candidate_ids},
            )
        object.__setattr__(self, "manifest_id", _required_text(self.manifest_id, "manifest_id"))
        object.__setattr__(self, "created_at", _normalize_datetime(self.created_at, "created_at"))
        object.__setattr__(self, "created_by", _required_text(self.created_by, "created_by"))
        object.__setattr__(self, "candidates", tuple(sorted(candidates, key=lambda item: item.candidate_id)))
        object.__setattr__(self, "validation_window_ref", _optional_text(self.validation_window_ref))
        object.__setattr__(self, "notes", _optional_text(self.notes) or "")

    @property
    def manifest_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.sandbox_promotion_manifest.v1")

    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(candidate.candidate_id for candidate in self.candidates)

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.sandbox_promotion_manifest.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "manifest_id": self.manifest_id,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "validation_window_ref": self.validation_window_ref,
            "candidates": [candidate.to_json() for candidate in self.candidates],
            "notes": self.notes,
        }
        if include_digest:
            payload["manifest_digest"] = self.manifest_digest
        return payload


@dataclass(frozen=True, slots=True)
class SandboxExecutionRecord:
    execution_id: str
    candidate_id: str
    scenario_ref: str
    sandbox_target: str
    environment_ref: str
    config_ref: str
    seed_data_ref: str
    status: ExecutionStatus
    started_at: str
    ended_at: str | None = None
    operator_intervention: bool = False
    deviations: tuple[str, ...] = ()
    raw_evidence_refs: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "execution_id", _required_text(self.execution_id, "execution_id"))
        object.__setattr__(self, "candidate_id", _required_text(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "scenario_ref", _required_text(self.scenario_ref, "scenario_ref"))
        object.__setattr__(self, "sandbox_target", _required_text(self.sandbox_target, "sandbox_target"))
        object.__setattr__(self, "environment_ref", _required_text(self.environment_ref, "environment_ref"))
        object.__setattr__(self, "config_ref", _required_text(self.config_ref, "config_ref"))
        object.__setattr__(self, "seed_data_ref", _required_text(self.seed_data_ref, "seed_data_ref"))
        object.__setattr__(self, "status", _member(self.status, EXECUTION_STATUSES, "status"))
        object.__setattr__(self, "started_at", _normalize_datetime(self.started_at, "started_at"))
        object.__setattr__(self, "ended_at", _normalize_optional_datetime(self.ended_at, "ended_at"))
        object.__setattr__(self, "operator_intervention", bool(self.operator_intervention))
        object.__setattr__(self, "deviations", _clean_text_tuple(self.deviations))
        object.__setattr__(self, "raw_evidence_refs", _clean_text_tuple(self.raw_evidence_refs))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.sandbox_execution_record.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "execution_id": self.execution_id,
            "candidate_id": self.candidate_id,
            "scenario_ref": self.scenario_ref,
            "sandbox_target": self.sandbox_target,
            "environment_ref": self.environment_ref,
            "config_ref": self.config_ref,
            "seed_data_ref": self.seed_data_ref,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "operator_intervention": self.operator_intervention,
            "deviations": list(self.deviations),
            "raw_evidence_refs": list(self.raw_evidence_refs),
            "metadata": canonical_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class SandboxReadbackEvidence:
    evidence_id: str
    candidate_id: str
    scenario_ref: str
    observable_ref: str
    evidence_kind: str
    captured_at: str
    available: bool
    trusted: bool
    immutable_ref: str | None = None
    payload: Any = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_id", _required_text(self.evidence_id, "evidence_id"))
        object.__setattr__(self, "candidate_id", _required_text(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "scenario_ref", _required_text(self.scenario_ref, "scenario_ref"))
        object.__setattr__(self, "observable_ref", _required_text(self.observable_ref, "observable_ref"))
        object.__setattr__(self, "evidence_kind", _required_text(self.evidence_kind, "evidence_kind"))
        object.__setattr__(self, "captured_at", _normalize_datetime(self.captured_at, "captured_at"))
        object.__setattr__(self, "available", bool(self.available))
        object.__setattr__(self, "trusted", bool(self.trusted))
        object.__setattr__(self, "immutable_ref", _optional_text(self.immutable_ref))
        object.__setattr__(self, "failure_reason", _optional_text(self.failure_reason))

    @property
    def usable(self) -> bool:
        return self.available and self.trusted

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.sandbox_readback_evidence.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "evidence_id": self.evidence_id,
            "candidate_id": self.candidate_id,
            "scenario_ref": self.scenario_ref,
            "observable_ref": self.observable_ref,
            "evidence_kind": self.evidence_kind,
            "captured_at": self.captured_at,
            "available": self.available,
            "trusted": self.trusted,
            "immutable_ref": self.immutable_ref,
            "payload": canonical_value(self.payload),
            "failure_reason": self.failure_reason,
        }


@dataclass(frozen=True, slots=True)
class SandboxEvidencePackage:
    package_id: str
    execution_id: str
    candidate_id: str
    scenario_ref: str
    evidence: tuple[SandboxReadbackEvidence, ...]

    def __post_init__(self) -> None:
        evidence = tuple(self.evidence or ())
        evidence_ids = [item.evidence_id for item in evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise SandboxDriftError(
                "sandbox_drift.duplicate_evidence",
                "evidence package cannot contain duplicate evidence ids",
                details={"evidence_ids": evidence_ids},
            )
        object.__setattr__(self, "package_id", _required_text(self.package_id, "package_id"))
        object.__setattr__(self, "execution_id", _required_text(self.execution_id, "execution_id"))
        object.__setattr__(self, "candidate_id", _required_text(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "scenario_ref", _required_text(self.scenario_ref, "scenario_ref"))
        for item in evidence:
            if item.candidate_id != self.candidate_id or item.scenario_ref != self.scenario_ref:
                raise SandboxDriftError(
                    "sandbox_drift.evidence_scope_mismatch",
                    "readback evidence must belong to the package candidate and scenario",
                    details={"evidence_id": item.evidence_id},
                )
        object.__setattr__(self, "evidence", tuple(sorted(evidence, key=lambda item: item.evidence_id)))

    def evidence_by_id(self) -> dict[str, SandboxReadbackEvidence]:
        return {item.evidence_id: item for item in self.evidence}

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.sandbox_evidence_package.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "package_id": self.package_id,
            "execution_id": self.execution_id,
            "candidate_id": self.candidate_id,
            "scenario_ref": self.scenario_ref,
            "evidence": [item.to_json() for item in self.evidence],
        }


@dataclass(frozen=True, slots=True)
class PredictedActualCheck:
    check_id: str
    dimension: ComparisonDimension
    prediction: Any
    actual: Any
    evidence_refs: tuple[str, ...]
    required_evidence: bool = True
    accepted_status: ComparisonStatus | None = None
    delta: str = ""
    impact: str = ""
    disposition: DriftDisposition | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "check_id", _required_text(self.check_id, "check_id"))
        object.__setattr__(self, "dimension", _member(self.dimension, COMPARISON_DIMENSIONS, "dimension"))
        object.__setattr__(self, "evidence_refs", _clean_text_tuple(self.evidence_refs))
        object.__setattr__(self, "required_evidence", bool(self.required_evidence))
        if self.accepted_status is not None:
            object.__setattr__(self, "accepted_status", _member(self.accepted_status, COMPARISON_STATUSES, "accepted_status"))
        object.__setattr__(self, "delta", _optional_text(self.delta) or "")
        object.__setattr__(self, "impact", _optional_text(self.impact) or "")
        if self.disposition is not None:
            object.__setattr__(self, "disposition", _member(self.disposition, DRIFT_DISPOSITIONS, "disposition"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.predicted_actual_check.v1",
            "check_id": self.check_id,
            "dimension": self.dimension,
            "prediction": canonical_value(self.prediction),
            "actual": canonical_value(self.actual),
            "evidence_refs": list(self.evidence_refs),
            "required_evidence": self.required_evidence,
            "accepted_status": self.accepted_status,
            "delta": self.delta,
            "impact": self.impact,
            "disposition": self.disposition,
        }


@dataclass(frozen=True, slots=True)
class ComparisonRow:
    row_id: str
    check_id: str
    dimension: ComparisonDimension
    status: ComparisonStatus
    prediction: Any
    actual: Any
    delta: str
    impact: str
    evidence_refs: tuple[str, ...]
    disposition: DriftDisposition | None = None
    blocker_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "row_id", _required_text(self.row_id, "row_id"))
        object.__setattr__(self, "check_id", _required_text(self.check_id, "check_id"))
        object.__setattr__(self, "dimension", _member(self.dimension, COMPARISON_DIMENSIONS, "dimension"))
        object.__setattr__(self, "status", _member(self.status, COMPARISON_STATUSES, "status"))
        object.__setattr__(self, "delta", _optional_text(self.delta) or "")
        object.__setattr__(self, "impact", _optional_text(self.impact) or "")
        object.__setattr__(self, "evidence_refs", _clean_text_tuple(self.evidence_refs))
        if self.disposition is not None:
            object.__setattr__(self, "disposition", _member(self.disposition, DRIFT_DISPOSITIONS, "disposition"))
        object.__setattr__(self, "blocker_reason", _optional_text(self.blocker_reason))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.comparison_row.v1",
            "row_id": self.row_id,
            "check_id": self.check_id,
            "dimension": self.dimension,
            "status": self.status,
            "prediction": canonical_value(self.prediction),
            "actual": canonical_value(self.actual),
            "delta": self.delta,
            "impact": self.impact,
            "evidence_refs": list(self.evidence_refs),
            "disposition": self.disposition,
            "blocker_reason": self.blocker_reason,
        }


@dataclass(frozen=True, slots=True)
class SandboxComparisonReport:
    report_id: str
    candidate_id: str
    scenario_ref: str
    execution_id: str
    evidence_package_id: str
    status: ComparisonStatus
    rows: tuple[ComparisonRow, ...]

    def __post_init__(self) -> None:
        rows = tuple(self.rows or ())
        if not rows:
            raise SandboxDriftError(
                "sandbox_drift.comparison_rows_required",
                "comparison report requires at least one row",
            )
        object.__setattr__(self, "report_id", _required_text(self.report_id, "report_id"))
        object.__setattr__(self, "candidate_id", _required_text(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "scenario_ref", _required_text(self.scenario_ref, "scenario_ref"))
        object.__setattr__(self, "execution_id", _required_text(self.execution_id, "execution_id"))
        object.__setattr__(self, "evidence_package_id", _required_text(self.evidence_package_id, "evidence_package_id"))
        object.__setattr__(self, "status", _member(self.status, COMPARISON_STATUSES, "status"))
        object.__setattr__(self, "rows", tuple(sorted(rows, key=lambda item: item.row_id)))

    @property
    def report_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.sandbox_comparison_report.v1")

    def non_match_rows(self) -> tuple[ComparisonRow, ...]:
        return tuple(row for row in self.rows if row.status != "match")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.sandbox_comparison_report.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "runtime_version": SANDBOX_DRIFT_RUNTIME_VERSION,
            "report_id": self.report_id,
            "candidate_id": self.candidate_id,
            "scenario_ref": self.scenario_ref,
            "execution_id": self.execution_id,
            "evidence_package_id": self.evidence_package_id,
            "status": self.status,
            "rows": [row.to_json() for row in self.rows],
        }
        if include_digest:
            payload["report_digest"] = self.report_digest
        return payload


@dataclass(frozen=True, slots=True)
class ClassificationCauseAssessment:
    environment_excluded: bool = False
    contract_excluded: bool = False
    harness_excluded: bool = False
    dependency_excluded: bool = False
    prediction_excluded: bool = False
    notes: str = ""

    @property
    def implementation_defect_supported(self) -> bool:
        return self.environment_excluded and self.contract_excluded and self.harness_excluded

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.classification_cause_assessment.v1",
            "environment_excluded": self.environment_excluded,
            "contract_excluded": self.contract_excluded,
            "harness_excluded": self.harness_excluded,
            "dependency_excluded": self.dependency_excluded,
            "prediction_excluded": self.prediction_excluded,
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class DriftClassification:
    classification_id: str
    comparison_report_id: str
    row_id: str
    reason_codes: tuple[DriftReasonCode, ...]
    severity: DriftSeverity
    layer: DriftLayer
    disposition: DriftDisposition
    owner: str
    cause_assessment: ClassificationCauseAssessment
    handoff_refs: tuple[HandoffReference, ...] = ()
    notes: str = ""

    def __post_init__(self) -> None:
        reason_codes = _required_text_tuple(self.reason_codes, "reason_codes")
        invalid = sorted(code for code in reason_codes if code not in DRIFT_REASON_CODES)
        if invalid:
            raise SandboxDriftError(
                "sandbox_drift.invalid_reason_code",
                "drift classification contains unsupported reason codes",
                details={"reason_codes": invalid},
            )
        if "IMPLEMENTATION_DEFECT" in reason_codes and not self.cause_assessment.implementation_defect_supported:
            raise SandboxDriftError(
                "sandbox_drift.implementation_defect_guardrail",
                "IMPLEMENTATION_DEFECT requires evidence excluding environment, contract, and harness causes",
                details={
                    "environment_excluded": self.cause_assessment.environment_excluded,
                    "contract_excluded": self.cause_assessment.contract_excluded,
                    "harness_excluded": self.cause_assessment.harness_excluded,
                },
            )
        object.__setattr__(self, "classification_id", _required_text(self.classification_id, "classification_id"))
        object.__setattr__(self, "comparison_report_id", _required_text(self.comparison_report_id, "comparison_report_id"))
        object.__setattr__(self, "row_id", _required_text(self.row_id, "row_id"))
        object.__setattr__(self, "reason_codes", tuple(reason_codes))
        object.__setattr__(self, "severity", _member(self.severity, DRIFT_SEVERITIES, "severity"))
        object.__setattr__(self, "layer", _member(self.layer, DRIFT_LAYERS, "layer"))
        object.__setattr__(self, "disposition", _member(self.disposition, DRIFT_DISPOSITIONS, "disposition"))
        object.__setattr__(self, "owner", _required_text(self.owner, "owner"))
        object.__setattr__(self, "handoff_refs", tuple(self.handoff_refs or ()))
        object.__setattr__(self, "notes", _optional_text(self.notes) or "")

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.drift_classification.v1",
            "classification_id": self.classification_id,
            "comparison_report_id": self.comparison_report_id,
            "row_id": self.row_id,
            "reason_codes": list(self.reason_codes),
            "severity": self.severity,
            "layer": self.layer,
            "disposition": self.disposition,
            "owner": self.owner,
            "cause_assessment": self.cause_assessment.to_json(),
            "handoff_refs": [ref.to_json() for ref in self.handoff_refs],
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class DriftLedger:
    ledger_id: str
    comparison_report_id: str
    classifications: tuple[DriftClassification, ...]

    def __post_init__(self) -> None:
        classifications = tuple(self.classifications or ())
        ids = [item.classification_id for item in classifications]
        if len(ids) != len(set(ids)):
            raise SandboxDriftError(
                "sandbox_drift.duplicate_classification",
                "drift ledger cannot contain duplicate classification ids",
                details={"classification_ids": ids},
            )
        object.__setattr__(self, "ledger_id", _required_text(self.ledger_id, "ledger_id"))
        object.__setattr__(self, "comparison_report_id", _required_text(self.comparison_report_id, "comparison_report_id"))
        object.__setattr__(self, "classifications", tuple(sorted(classifications, key=lambda item: item.classification_id)))

    @property
    def ledger_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.sandbox_drift_ledger.v1")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.sandbox_drift_ledger.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "ledger_id": self.ledger_id,
            "comparison_report_id": self.comparison_report_id,
            "classifications": [item.to_json() for item in self.classifications],
        }
        if include_digest:
            payload["ledger_digest"] = self.ledger_digest
        return payload


@dataclass(frozen=True, slots=True)
class CandidateExitDecision:
    candidate_id: str
    decision: CandidateDecision
    comparison_status: ComparisonStatus
    reason_codes: tuple[str, ...] = ()
    handoff_refs: tuple[HandoffReference, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", _required_text(self.candidate_id, "candidate_id"))
        if self.decision not in {"validated", "drifted", "blocked", "stopped"}:
            raise SandboxDriftError(
                "sandbox_drift.invalid_candidate_decision",
                "candidate exit decision is unsupported",
                details={"decision": self.decision},
            )
        object.__setattr__(self, "comparison_status", _member(self.comparison_status, COMPARISON_STATUSES, "comparison_status"))
        object.__setattr__(self, "reason_codes", _clean_text_tuple(self.reason_codes))
        object.__setattr__(self, "handoff_refs", tuple(self.handoff_refs or ()))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.candidate_exit_decision.v1",
            "candidate_id": self.candidate_id,
            "decision": self.decision,
            "comparison_status": self.comparison_status,
            "reason_codes": list(self.reason_codes),
            "handoff_refs": [ref.to_json() for ref in self.handoff_refs],
        }


@dataclass(frozen=True, slots=True)
class StopContinueSummary:
    summary_id: str
    manifest_id: str
    recommendation: PhaseRecommendation
    candidate_decisions: tuple[CandidateExitDecision, ...]
    stop_reasons: tuple[str, ...] = ()
    continue_constraints: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "summary_id", _required_text(self.summary_id, "summary_id"))
        object.__setattr__(self, "manifest_id", _required_text(self.manifest_id, "manifest_id"))
        if self.recommendation not in {"continue", "continue_with_constraints", "rerun_phase", "stop"}:
            raise SandboxDriftError(
                "sandbox_drift.invalid_phase_recommendation",
                "phase recommendation is unsupported",
                details={"recommendation": self.recommendation},
            )
        decisions = tuple(self.candidate_decisions or ())
        if not decisions:
            raise SandboxDriftError(
                "sandbox_drift.summary_decisions_required",
                "stop/continue summary requires at least one candidate decision",
            )
        object.__setattr__(self, "candidate_decisions", tuple(sorted(decisions, key=lambda item: item.candidate_id)))
        object.__setattr__(self, "stop_reasons", _clean_text_tuple(self.stop_reasons))
        object.__setattr__(self, "continue_constraints", _clean_text_tuple(self.continue_constraints))

    @property
    def summary_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.sandbox_stop_continue_summary.v1")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.sandbox_stop_continue_summary.v1",
            "schema_version": SANDBOX_DRIFT_SCHEMA_VERSION,
            "summary_id": self.summary_id,
            "manifest_id": self.manifest_id,
            "recommendation": self.recommendation,
            "candidate_decisions": [item.to_json() for item in self.candidate_decisions],
            "stop_reasons": list(self.stop_reasons),
            "continue_constraints": list(self.continue_constraints),
        }
        if include_digest:
            payload["summary_digest"] = self.summary_digest
        return payload


def compare_predicted_actual(
    *,
    candidate: PromotionCandidate,
    execution: SandboxExecutionRecord,
    evidence_package: SandboxEvidencePackage,
    checks: tuple[PredictedActualCheck, ...] | list[PredictedActualCheck],
    report_id: str | None = None,
) -> SandboxComparisonReport:
    """Compare explicit predictions with sandbox readback evidence."""

    normalized_checks = tuple(checks or ())
    if not normalized_checks:
        raise SandboxDriftError(
            "sandbox_drift.comparison_checks_required",
            "predicted-vs-actual comparison requires at least one check",
        )
    if execution.candidate_id != candidate.candidate_id:
        raise SandboxDriftError(
            "sandbox_drift.execution_candidate_mismatch",
            "execution record must belong to the candidate",
            details={"candidate_id": candidate.candidate_id, "execution_candidate_id": execution.candidate_id},
        )
    if evidence_package.candidate_id != candidate.candidate_id or evidence_package.execution_id != execution.execution_id:
        raise SandboxDriftError(
            "sandbox_drift.evidence_package_scope_mismatch",
            "evidence package must belong to the candidate execution",
            details={
                "candidate_id": candidate.candidate_id,
                "package_candidate_id": evidence_package.candidate_id,
                "execution_id": execution.execution_id,
                "package_execution_id": evidence_package.execution_id,
            },
        )
    evidence_by_id = evidence_package.evidence_by_id()
    rows = tuple(
        _comparison_row(
            candidate=candidate,
            execution=execution,
            evidence_by_id=evidence_by_id,
            check=check,
        )
        for check in normalized_checks
    )
    status = _rollup_status(row.status for row in rows)
    resolved_report_id = _optional_text(report_id) or _stable_ref(
        "sandbox_comparison",
        {
            "candidate_id": candidate.candidate_id,
            "execution_id": execution.execution_id,
            "evidence_package_id": evidence_package.package_id,
            "rows": [row.to_json() for row in rows],
        },
    )
    return SandboxComparisonReport(
        report_id=resolved_report_id,
        candidate_id=candidate.candidate_id,
        scenario_ref=execution.scenario_ref,
        execution_id=execution.execution_id,
        evidence_package_id=evidence_package.package_id,
        status=status,
        rows=rows,
    )


def build_drift_ledger(
    *,
    report: SandboxComparisonReport,
    classifications: tuple[DriftClassification, ...] | list[DriftClassification],
    ledger_id: str | None = None,
) -> DriftLedger:
    """Build a drift ledger and require classification coverage for non-matches."""

    normalized = tuple(classifications or ())
    non_match_row_ids = {row.row_id for row in report.non_match_rows()}
    classified_row_ids = {item.row_id for item in normalized}
    missing = sorted(non_match_row_ids - classified_row_ids)
    if missing:
        raise SandboxDriftError(
            "sandbox_drift.unclassified_non_match",
            "every non-match comparison row requires a drift classification",
            details={"row_ids": missing},
        )
    unknown = sorted(classified_row_ids - {row.row_id for row in report.rows})
    if unknown:
        raise SandboxDriftError(
            "sandbox_drift.classification_unknown_row",
            "drift classification references a comparison row that does not exist",
            details={"row_ids": unknown},
        )
    for item in normalized:
        if item.comparison_report_id != report.report_id:
            raise SandboxDriftError(
                "sandbox_drift.classification_report_mismatch",
                "drift classification must reference the comparison report",
                details={
                    "classification_id": item.classification_id,
                    "classification_report_id": item.comparison_report_id,
                    "report_id": report.report_id,
                },
            )
    resolved_ledger_id = _optional_text(ledger_id) or _stable_ref(
        "sandbox_drift_ledger",
        {
            "report_id": report.report_id,
            "classifications": [item.to_json() for item in normalized],
        },
    )
    return DriftLedger(
        ledger_id=resolved_ledger_id,
        comparison_report_id=report.report_id,
        classifications=normalized,
    )


def build_stop_continue_summary(
    *,
    manifest: PromotionManifest,
    reports: tuple[SandboxComparisonReport, ...] | list[SandboxComparisonReport],
    ledgers: tuple[DriftLedger, ...] | list[DriftLedger] = (),
    summary_id: str | None = None,
) -> StopContinueSummary:
    """Return a bounded phase recommendation from comparison and drift records."""

    normalized_reports = tuple(reports or ())
    if not normalized_reports:
        raise SandboxDriftError(
            "sandbox_drift.summary_reports_required",
            "stop/continue summary requires at least one comparison report",
        )
    reports_by_candidate = {report.candidate_id: report for report in normalized_reports}
    missing_candidates = sorted(set(manifest.candidate_ids()) - set(reports_by_candidate))
    if missing_candidates:
        raise SandboxDriftError(
            "sandbox_drift.summary_missing_candidate_report",
            "every manifest candidate requires a comparison report",
            details={"candidate_ids": missing_candidates},
        )
    classifications = [item for ledger in tuple(ledgers or ()) for item in ledger.classifications]
    classifications_by_report: dict[str, list[DriftClassification]] = {}
    for item in classifications:
        classifications_by_report.setdefault(item.comparison_report_id, []).append(item)

    candidate_decisions: list[CandidateExitDecision] = []
    stop_reasons: list[str] = []
    continue_constraints: list[str] = []
    for candidate in manifest.candidates:
        report = reports_by_candidate[candidate.candidate_id]
        report_classifications = classifications_by_report.get(report.report_id, [])
        reason_codes = tuple(
            sorted({code for item in report_classifications for code in item.reason_codes})
        )
        handoff_refs = tuple(ref for item in report_classifications for ref in item.handoff_refs)
        decision = _candidate_decision(report, report_classifications)
        candidate_decisions.append(
            CandidateExitDecision(
                candidate_id=candidate.candidate_id,
                decision=decision,
                comparison_status=report.status,
                reason_codes=reason_codes,
                handoff_refs=handoff_refs,
            )
        )
        for item in report_classifications:
            if item.disposition == "stop_phase" or item.severity == "critical":
                stop_reasons.append(f"{candidate.candidate_id}:{','.join(item.reason_codes)}")
            elif item.disposition in {"fix_now", "document", "defer", "rerun_required"}:
                continue_constraints.append(f"{candidate.candidate_id}:{item.disposition}:{','.join(item.reason_codes)}")
        if report.status == "blocked":
            continue_constraints.append(f"{candidate.candidate_id}:blocked")

    recommendation = _phase_recommendation(candidate_decisions, classifications)
    resolved_summary_id = _optional_text(summary_id) or _stable_ref(
        "sandbox_stop_continue",
        {
            "manifest_id": manifest.manifest_id,
            "candidate_decisions": [item.to_json() for item in candidate_decisions],
            "stop_reasons": stop_reasons,
            "continue_constraints": continue_constraints,
        },
    )
    return StopContinueSummary(
        summary_id=resolved_summary_id,
        manifest_id=manifest.manifest_id,
        recommendation=recommendation,
        candidate_decisions=tuple(candidate_decisions),
        stop_reasons=tuple(sorted(set(stop_reasons))),
        continue_constraints=tuple(sorted(set(continue_constraints))),
    )


def _comparison_row(
    *,
    candidate: PromotionCandidate,
    execution: SandboxExecutionRecord,
    evidence_by_id: dict[str, SandboxReadbackEvidence],
    check: PredictedActualCheck,
) -> ComparisonRow:
    missing_refs = [ref for ref in check.evidence_refs if ref not in evidence_by_id]
    blocked_refs = [
        ref
        for ref in check.evidence_refs
        if ref in evidence_by_id and not evidence_by_id[ref].usable
    ]
    if check.required_evidence and (not check.evidence_refs or missing_refs or blocked_refs):
        status: ComparisonStatus = "blocked"
        blocker_reason = "required evidence is missing or untrusted"
        delta = check.delta or blocker_reason
    elif check.accepted_status is not None:
        status = check.accepted_status
        blocker_reason = None
        delta = check.delta or _default_delta(check.prediction, check.actual, status)
    elif canonical_value(check.prediction) == canonical_value(check.actual):
        status = "match"
        blocker_reason = None
        delta = check.delta or "actual matches prediction"
    else:
        status = "drift"
        blocker_reason = None
        delta = check.delta or _default_delta(check.prediction, check.actual, "drift")
    return ComparisonRow(
        row_id=_stable_ref(
            "sandbox_comparison_row",
            {
                "candidate_id": candidate.candidate_id,
                "execution_id": execution.execution_id,
                "check": check.to_json(),
                "status": status,
            },
        ),
        check_id=check.check_id,
        dimension=check.dimension,
        status=status,
        prediction=check.prediction,
        actual=check.actual,
        delta=delta,
        impact=check.impact,
        evidence_refs=check.evidence_refs,
        disposition=check.disposition,
        blocker_reason=blocker_reason,
    )


def _rollup_status(statuses: Any) -> ComparisonStatus:
    values = tuple(statuses)
    if "blocked" in values:
        return "blocked"
    if "drift" in values:
        return "drift"
    if "partial_match" in values:
        return "partial_match"
    return "match"


def _candidate_decision(
    report: SandboxComparisonReport,
    classifications: list[DriftClassification],
) -> CandidateDecision:
    if any(item.disposition == "stop_phase" or item.severity == "critical" for item in classifications):
        return "stopped"
    if report.status == "blocked":
        return "blocked"
    if report.status == "match":
        return "validated"
    return "drifted"


def _phase_recommendation(
    decisions: list[CandidateExitDecision],
    classifications: list[DriftClassification],
) -> PhaseRecommendation:
    if any(item.decision == "stopped" for item in decisions):
        return "stop"
    if any(item.disposition == "rerun_required" for item in classifications) or any(item.decision == "blocked" for item in decisions):
        return "rerun_phase"
    if any(item.decision == "drifted" for item in decisions):
        return "continue_with_constraints"
    return "continue"


def _default_delta(prediction: Any, actual: Any, status: ComparisonStatus) -> str:
    if status == "partial_match":
        return "actual contains bounded deviation from prediction"
    if status == "drift":
        return "actual differs from prediction"
    return "actual matches prediction"


def _stable_ref(prefix: str, payload: dict[str, Any]) -> str:
    digest = virtual_lab_digest(payload, purpose=f"virtual_lab.{prefix}.id.v1").split(":")[-1]
    return f"{prefix}.{digest[:20]}"


def _required_text(value: Any, field_name: str) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise SandboxDriftError(
            f"sandbox_drift.{field_name}_required",
            f"{field_name} is required",
        )
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _clean_text_tuple(values: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    return tuple(value for value in (_optional_text(item) for item in values) if value is not None)


def _required_text_tuple(values: tuple[str, ...] | list[str] | None, field_name: str) -> tuple[str, ...]:
    normalized = _clean_text_tuple(values)
    if not normalized:
        raise SandboxDriftError(
            f"sandbox_drift.{field_name}_required",
            f"{field_name} requires at least one value",
        )
    return normalized


def _member(value: Any, allowed: set[str], field_name: str) -> Any:
    normalized = _required_text(value, field_name)
    if normalized not in allowed:
        raise SandboxDriftError(
            f"sandbox_drift.invalid_{field_name}",
            f"{field_name} is unsupported",
            details={"value": normalized, "allowed": sorted(allowed)},
        )
    return normalized


def _mapping(value: dict[str, Any] | None, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise SandboxDriftError(
            f"sandbox_drift.invalid_{field_name}",
            f"{field_name} must be a mapping",
            details={"value_type": type(value).__name__},
        )
    return dict(value)


def _normalize_datetime(value: Any, field_name: str) -> str:
    normalized = _required_text(value, field_name)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SandboxDriftError(
            f"sandbox_drift.invalid_{field_name}",
            f"{field_name} must be an ISO-8601 timestamp",
            details={field_name: normalized},
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_optional_datetime(value: Any, field_name: str) -> str | None:
    if _optional_text(value) is None:
        return None
    return _normalize_datetime(value, field_name)
