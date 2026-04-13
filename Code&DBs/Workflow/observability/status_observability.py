"""Derived observability summaries for the native frontdoor status surface.

This module combines the already-loaded run row, evidence inspection, job
summary, and packet inspection into one compact truth block so callers do not
have to re-stitch the same state from multiple payloads.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _append_indexed_lines, _dedupe, _format_bool

from .read_models import InspectionReadModel, ProjectionCompleteness, ProjectionWatermark

__all__ = [
    "FrontdoorContractDriftIssue",
    "FrontdoorContractDriftReadModel",
    "FrontdoorFailureTaxonomyReadModel",
    "FrontdoorObservabilityAnomalyDigest",
    "FrontdoorObservabilityReadModel",
    "FrontdoorProvenanceCoverageReadModel",
    "FrontdoorProvenanceCoverageSection",
    "FrontdoorRunIdentityReadModel",
    "build_frontdoor_observability",
    "render_frontdoor_observability",
]


_FAILURE_TOKENS = ("failed", "dead_letter", "cancelled", "blocked", "expired", "rejected")
_SUCCESS_TOKENS = ("succeeded", "success", "promoted", "complete", "completed")
_IN_PROGRESS_TOKENS = (
    "running",
    "claim_",
    "lease_",
    "proposal_",
    "gate_",
    "queued",
    "ready",
    "pending",
    "accepted",
    "requested",
    "submitted",
)
_SCHEMA_DRIFT_TOKENS = (
    "schema",
    "migration",
    "column",
    "undefinedcolumn",
    "packet_inspection_column_missing",
)
_PROVIDER_TIMEOUT_TOKENS = ("timeout", "timed_out", "deadline", "latency")
_SANDBOX_TOKENS = ("sandbox", "seatbelt", "permission denied", "denied")
_IDEMPOTENCY_TOKENS = ("idempotency", "duplicate", "dedupe", "conflict")
_DB_TOKENS = ("database", "postgres", "sqlstate", "unreachable", "connection", "connect")
_POLICY_TOKENS = ("admission", "gate", "blocked", "rejected", "invalid_smoke_contract")
_PACKET_TOKENS = ("packet", "drift", "compile_index")
_SYNTHETIC_TOKENS = ("smoke", "synthetic", "probe", "canary")
_EXPLICIT_CONTRACT_DRIFT_ISSUES: dict[str, tuple[str, str, str]] = {
    "workflow_runs.packet_inspection_column_missing": (
        "high",
        "workflow_runs",
        "workflow_runs.packet_inspection was missing, so the status read fell back to the legacy query shape",
    ),
    "workflow_runs.request_envelope_invalid": (
        "medium",
        "workflow_runs.request_envelope",
        "workflow_runs.request_envelope was not a valid object-shaped payload",
    ),
}


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _mapping_text(value: object, key: str) -> str | None:
    if not isinstance(value, Mapping):
        return None
    field_value = value.get(key)
    if isinstance(field_value, str) and field_value.strip():
        return field_value.strip()
    return None


def _text(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _datetime_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _job_status(job: Mapping[str, Any]) -> str | None:
    return _text(job.get("status"))


def _job_label(job: Mapping[str, Any]) -> str | None:
    return _text(job.get("label"))


def _job_last_error_code(job: Mapping[str, Any]) -> str | None:
    return _text(job.get("last_error_code"))


def _job_status_counts(jobs: Sequence[Mapping[str, Any]]) -> tuple[tuple[str, int], ...]:
    counts = Counter(status for status in (_job_status(job) for job in jobs) if status is not None)
    return tuple(sorted(counts.items()))


def _terminal_status(status: str | None) -> bool:
    if not isinstance(status, str):
        return False
    normalized = status.strip().lower()
    if normalized in _SUCCESS_TOKENS:
        return True
    return any(token in normalized for token in _FAILURE_TOKENS)


def _latest_job_status(jobs: Sequence[Mapping[str, Any]]) -> str | None:
    for job in reversed(tuple(jobs)):
        status = _job_status(job)
        if status is not None:
            return status
    return None


def _failed_job_labels(jobs: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    labels: list[str] = []
    for job in jobs:
        category = _classify_failure_category(_job_last_error_code(job), _job_status(job))
        if category in {"success", "in_progress", "unknown"}:
            continue
        label = _job_label(job)
        if label is not None:
            labels.append(label)
    return _dedupe(labels)


def _packet_drift_status(packet_inspection: object | None) -> str | None:
    if not isinstance(packet_inspection, Mapping):
        return None
    drift = packet_inspection.get("drift")
    if not isinstance(drift, Mapping):
        return None
    return _mapping_text(drift, "status")


def _contains_any(text: str, tokens: Sequence[str]) -> bool:
    return any(token in text for token in tokens)


def _classify_failure_category(*values: object) -> str:
    parts = [str(value).strip().lower() for value in values if str(value or "").strip()]
    if not parts:
        return "unknown"
    normalized = " ".join(parts)
    if _contains_any(normalized, _SUCCESS_TOKENS):
        return "success"
    if _contains_any(normalized, _SCHEMA_DRIFT_TOKENS):
        return "schema_drift"
    if _contains_any(normalized, _PROVIDER_TIMEOUT_TOKENS):
        return "provider_timeout"
    if _contains_any(normalized, _SANDBOX_TOKENS):
        return "sandbox_denied"
    if _contains_any(normalized, _IDEMPOTENCY_TOKENS):
        return "idempotency_conflict"
    if _contains_any(normalized, _DB_TOKENS):
        return "db_unreachable"
    if _contains_any(normalized, _POLICY_TOKENS):
        return "policy_blocked"
    if _contains_any(normalized, _PACKET_TOKENS):
        return "packet_drift"
    if "cancel" in normalized:
        return "cancelled"
    if _contains_any(normalized, _IN_PROGRESS_TOKENS):
        return "in_progress"
    if _contains_any(normalized, _FAILURE_TOKENS) or "error" in normalized or "invalid" in normalized:
        return "execution_failed"
    return "unknown"


def _is_synthetic_run(*values: object) -> bool:
    normalized = " ".join(str(value).strip().lower() for value in values if str(value or "").strip())
    return bool(normalized) and _contains_any(normalized, _SYNTHETIC_TOKENS)


def _shared_isolation_suffix(values: Sequence[str | None]) -> str | None:
    suffixes: list[str] = []
    for value in values:
        if value is None:
            continue
        if "." not in value:
            return None
        suffix = value.rsplit(".", 1)[-1].strip()
        if len(suffix) < 6:
            return None
        if not suffix.replace("-", "").replace("_", "").isalnum():
            return None
        suffixes.append(suffix)
    if len(suffixes) < 2:
        return None
    first = suffixes[0]
    return first if all(item == first for item in suffixes[1:]) else None


def _job_failure_entries(jobs: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    entries: list[dict[str, Any]] = []
    for job in jobs:
        category = _classify_failure_category(_job_last_error_code(job), _job_status(job))
        if category in {"success", "in_progress", "unknown"}:
            continue
        entries.append(
            {
                "label": _job_label(job),
                "status": _job_status(job),
                "last_error_code": _job_last_error_code(job),
                "category": category,
            }
        )
    return tuple(entries)


@dataclass(frozen=True, slots=True)
class FrontdoorContractDriftIssue:
    """One observed contract-drift issue in the status path."""

    issue_code: str
    severity: str
    authority: str
    reason: str

    def to_json(self) -> dict[str, Any]:
        return {
            "issue_code": self.issue_code,
            "severity": self.severity,
            "authority": self.authority,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class FrontdoorContractDriftReadModel:
    """Contract-drift digest for a single status read."""

    status: str
    issues: tuple[FrontdoorContractDriftIssue, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "frontdoor_contract_drift",
            "status": self.status,
            "issue_count": len(self.issues),
            "issues": [issue.to_json() for issue in self.issues],
        }


@dataclass(frozen=True, slots=True)
class FrontdoorFailureTaxonomyReadModel:
    """Failure taxonomy rollup for one run."""

    current_category: str
    terminal_category: str
    dominant_category: str
    current_failure_signature: str | None
    category_counts: tuple[tuple[str, int], ...]
    failing_jobs: tuple[Mapping[str, Any], ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "frontdoor_failure_taxonomy",
            "current_category": self.current_category,
            "terminal_category": self.terminal_category,
            "dominant_category": self.dominant_category,
            "current_failure_signature": self.current_failure_signature,
            "failing_job_count": len(self.failing_jobs),
            "category_counts": {name: count for name, count in self.category_counts},
            "failing_jobs": [dict(item) for item in self.failing_jobs],
        }


@dataclass(frozen=True, slots=True)
class FrontdoorProvenanceCoverageSection:
    """One provenance section coverage verdict."""

    section_name: str
    source: str
    authority: str
    observed: bool

    def to_json(self) -> dict[str, Any]:
        return {
            "section_name": self.section_name,
            "source": self.source,
            "authority": self.authority,
            "observed": self.observed,
        }


@dataclass(frozen=True, slots=True)
class FrontdoorProvenanceCoverageReadModel:
    """Coverage summary for the stitched status payload."""

    sections: tuple[FrontdoorProvenanceCoverageSection, ...]
    authoritative_count: int
    derived_count: int
    defaulted_count: int
    missing_count: int
    coverage_rate: float

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "frontdoor_provenance_coverage",
            "authoritative_count": self.authoritative_count,
            "derived_count": self.derived_count,
            "defaulted_count": self.defaulted_count,
            "missing_count": self.missing_count,
            "coverage_rate": self.coverage_rate,
            "sections": [section.to_json() for section in self.sections],
        }


@dataclass(frozen=True, slots=True)
class FrontdoorRunIdentityReadModel:
    """Run identity and dedupe telemetry exposed on the status path."""

    workflow_id: str | None
    request_id: str | None
    request_digest: str | None
    workflow_definition_id: str | None
    admitted_definition_hash: str | None
    definition_hash: str | None
    run_idempotency_key: str | None
    request_name: str | None
    packet_revision: str | None
    packet_hash: str | None
    synthetic_run: bool
    isolation_suffix: str | None
    dedupe_decision: str
    idempotency_scope: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "frontdoor_run_identity",
            "workflow_id": self.workflow_id,
            "request_id": self.request_id,
            "request_digest": self.request_digest,
            "workflow_definition_id": self.workflow_definition_id,
            "admitted_definition_hash": self.admitted_definition_hash,
            "definition_hash": self.definition_hash,
            "run_idempotency_key": self.run_idempotency_key,
            "request_name": self.request_name,
            "packet_revision": self.packet_revision,
            "packet_hash": self.packet_hash,
            "synthetic_run": self.synthetic_run,
            "isolation_suffix": self.isolation_suffix,
            "dedupe_decision": self.dedupe_decision,
            "idempotency_scope": self.idempotency_scope,
        }


def _contract_drift(
    *,
    run_row: Mapping[str, Any],
    contract_drift_refs: Sequence[str],
) -> FrontdoorContractDriftReadModel:
    issues: list[FrontdoorContractDriftIssue] = []
    seen_codes: set[str] = set()
    for issue_code in contract_drift_refs:
        spec = _EXPLICIT_CONTRACT_DRIFT_ISSUES.get(issue_code)
        if spec is None or issue_code in seen_codes:
            continue
        severity, authority, reason = spec
        issues.append(
            FrontdoorContractDriftIssue(
                issue_code=issue_code,
                severity=severity,
                authority=authority,
                reason=reason,
            )
        )
        seen_codes.add(issue_code)

    raw_request_envelope = run_row.get("request_envelope")
    if raw_request_envelope is not None and raw_request_envelope != "":
        normalized_request_envelope = _json_mapping(raw_request_envelope)
        if not normalized_request_envelope:
            issue_code = "workflow_runs.request_envelope_invalid"
            if issue_code not in seen_codes:
                severity, authority, reason = _EXPLICIT_CONTRACT_DRIFT_ISSUES[issue_code]
                issues.append(
                    FrontdoorContractDriftIssue(
                        issue_code=issue_code,
                        severity=severity,
                        authority=authority,
                        reason=reason,
                    )
                )

    return FrontdoorContractDriftReadModel(
        status="drifted" if issues else "aligned",
        issues=tuple(issues),
    )


def _failure_taxonomy(
    *,
    current_state: str | None,
    terminal_reason_code: str | None,
    jobs: Sequence[Mapping[str, Any]],
    packet_drift_status: str | None,
) -> FrontdoorFailureTaxonomyReadModel:
    current_category = _classify_failure_category(current_state)
    terminal_category = _classify_failure_category(terminal_reason_code)
    failing_jobs = _job_failure_entries(jobs)
    counts = Counter(
        category
        for category in (
            current_category,
            terminal_category,
            *(
                _classify_failure_category(_job_last_error_code(job), _job_status(job))
                for job in jobs
            ),
            "packet_drift" if packet_drift_status not in {None, "aligned"} else "unknown",
        )
        if category not in {"unknown", ""}
    )
    ranked_failures = [
        item
        for item in counts.most_common()
        if item[0] not in {"success", "in_progress"}
    ]
    if ranked_failures:
        dominant_category = ranked_failures[0][0]
    elif counts:
        dominant_category = counts.most_common(1)[0][0]
    else:
        dominant_category = "unknown"
    failure_signature = terminal_reason_code
    if failure_signature is None and failing_jobs:
        first_failure = failing_jobs[0]
        failure_signature = _text(first_failure.get("last_error_code")) or _text(first_failure.get("status"))
    if failure_signature is None:
        failure_signature = current_state
    return FrontdoorFailureTaxonomyReadModel(
        current_category=current_category,
        terminal_category=terminal_category,
        dominant_category=dominant_category,
        current_failure_signature=failure_signature,
        category_counts=tuple(sorted(counts.items())),
        failing_jobs=failing_jobs,
    )


def _coverage_bucket(source: str) -> str:
    if source in {"authoritative", "materialized"}:
        return "authoritative"
    if source == "derived":
        return "derived"
    if source == "defaulted":
        return "defaulted"
    return "missing"


def _provenance_coverage(
    *,
    run_row: Mapping[str, Any],
    inspection_present: bool,
    packet_inspection_source: str,
) -> FrontdoorProvenanceCoverageReadModel:
    request_envelope_source = "missing"
    raw_request_envelope = run_row.get("request_envelope")
    if isinstance(raw_request_envelope, Mapping):
        request_envelope_source = "authoritative"
    elif isinstance(raw_request_envelope, str) and raw_request_envelope.strip():
        request_envelope_source = "derived" if _json_mapping(raw_request_envelope) else "missing"

    sections = (
        FrontdoorProvenanceCoverageSection(
            section_name="inspection",
            source="authoritative" if inspection_present else "missing",
            authority="runtime.execution.RuntimeOrchestrator.inspect_run",
            observed=inspection_present,
        ),
        FrontdoorProvenanceCoverageSection(
            section_name="packet_inspection",
            source=packet_inspection_source,
            authority="workflow_runs.packet_inspection",
            observed=packet_inspection_source != "missing",
        ),
        FrontdoorProvenanceCoverageSection(
            section_name="request_envelope",
            source=request_envelope_source,
            authority="workflow_runs.request_envelope",
            observed=request_envelope_source != "missing",
        ),
        FrontdoorProvenanceCoverageSection(
            section_name="run_identity",
            source=(
                "authoritative"
                if _text(run_row.get("request_digest")) and _text(run_row.get("workflow_definition_id"))
                else "missing"
            ),
            authority="workflow_runs",
            observed=bool(
                _text(run_row.get("request_digest")) and _text(run_row.get("workflow_definition_id"))
            ),
        ),
    )
    buckets = Counter(_coverage_bucket(section.source) for section in sections)
    total_sections = len(sections)
    coverage_rate = 0.0
    if total_sections:
        coverage_rate = round(
            (buckets.get("authoritative", 0) + buckets.get("derived", 0)) / total_sections,
            4,
        )
    return FrontdoorProvenanceCoverageReadModel(
        sections=sections,
        authoritative_count=buckets.get("authoritative", 0),
        derived_count=buckets.get("derived", 0),
        defaulted_count=buckets.get("defaulted", 0),
        missing_count=buckets.get("missing", 0),
        coverage_rate=coverage_rate,
    )


def _run_identity(
    *,
    run_row: Mapping[str, Any],
    packet_inspection: object | None,
) -> FrontdoorRunIdentityReadModel:
    request_envelope = _json_mapping(run_row.get("request_envelope"))
    current_packet = _json_mapping(
        _json_mapping(packet_inspection).get("current_packet")
        if isinstance(packet_inspection, Mapping)
        else None
    )
    workflow_id = _text(run_row.get("workflow_id"))
    request_id = _text(run_row.get("request_id"))
    workflow_definition_id = _text(run_row.get("workflow_definition_id"))
    run_idempotency_key = _text(run_row.get("run_idempotency_key"))
    request_name = _text(request_envelope.get("name")) or _text(request_envelope.get("spec_name"))
    isolation_suffix = _shared_isolation_suffix(
        (
            workflow_id,
            request_id,
            workflow_definition_id,
            _text(request_envelope.get("definition_hash")),
        )
    )
    if isolation_suffix is not None:
        dedupe_decision = "isolated_request"
    elif request_id is not None and run_idempotency_key == request_id:
        dedupe_decision = "stable_idempotency_key"
    elif run_idempotency_key is not None:
        dedupe_decision = "custom_idempotency_key"
    else:
        dedupe_decision = "missing"
    synthetic_run = _is_synthetic_run(
        workflow_id,
        request_id,
        request_name,
        request_envelope.get("workflow_id"),
    )
    return FrontdoorRunIdentityReadModel(
        workflow_id=workflow_id,
        request_id=request_id,
        request_digest=_text(run_row.get("request_digest")),
        workflow_definition_id=workflow_definition_id,
        admitted_definition_hash=_text(run_row.get("admitted_definition_hash")),
        definition_hash=_text(request_envelope.get("definition_hash")),
        run_idempotency_key=run_idempotency_key,
        request_name=request_name,
        packet_revision=_text(current_packet.get("packet_revision")),
        packet_hash=_text(current_packet.get("packet_hash")),
        synthetic_run=synthetic_run,
        isolation_suffix=isolation_suffix,
        dedupe_decision=dedupe_decision,
        idempotency_scope=(
            f"{workflow_id}:{run_idempotency_key}"
            if workflow_id is not None and run_idempotency_key is not None
            else None
        ),
    )


def _health_state(
    *,
    current_state: str | None,
    inspection_present: bool,
    inspection_completeness: ProjectionCompleteness,
    failed_job_count: int,
    packet_drift_status: str | None,
    job_count: int,
    contract_drift: FrontdoorContractDriftReadModel,
) -> tuple[str, str]:
    if current_state is not None and any(token in current_state.lower() for token in _FAILURE_TOKENS):
        return "failed", f"run current_state is {current_state}"
    if failed_job_count > 0:
        return "failed", f"{failed_job_count} job(s) reported terminal failure"
    if contract_drift.issues:
        return "degraded", f"{len(contract_drift.issues)} contract drift issue(s) detected on the status path"
    if not inspection_present:
        if job_count == 0:
            return "missing", "no inspection snapshot or jobs were available"
        return "degraded", "inspection snapshot missing"
    if not inspection_completeness.is_complete:
        return "degraded", f"inspection missing {len(inspection_completeness.missing_evidence_refs)} evidence refs"
    if packet_drift_status is not None and packet_drift_status != "aligned":
        return "degraded", f"packet inspection drift status is {packet_drift_status}"
    return "healthy", "evidence, jobs, packet inspection, and status stitching are aligned"


def _anomaly_headline(
    *,
    health_state: str,
    inspection_present: bool,
    inspection_completeness: ProjectionCompleteness,
    failed_job_count: int,
    packet_drift_status: str | None,
    contract_drift: FrontdoorContractDriftReadModel,
) -> str:
    if health_state == "failed":
        if failed_job_count > 0:
            return f"{failed_job_count} job(s) failed and need review"
        return "Run state is terminally failed"
    if contract_drift.issues:
        return f"Status path detected {len(contract_drift.issues)} contract drift issue(s)"
    if not inspection_present:
        return "No inspection snapshot was produced for this run"
    if not inspection_completeness.is_complete:
        return (
            "Inspection is incomplete "
            f"({len(inspection_completeness.missing_evidence_refs)} missing evidence refs)"
        )
    if packet_drift_status is not None and packet_drift_status != "aligned":
        return f"Packet inspection drift is {packet_drift_status}"
    if health_state == "degraded":
        return "Observed run state is partially stitched"
    return "Run observations are internally consistent"


@dataclass(frozen=True, slots=True)
class FrontdoorObservabilityAnomalyDigest:
    """Short anomaly digest for a single native frontdoor status payload."""

    headline: str
    focus_refs: tuple[str, ...] = ()
    failed_job_labels: tuple[str, ...] = ()
    packet_drift_status: str | None = None
    latest_job_status: str | None = None
    terminal_reason_code: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "frontdoor_observability_anomaly_digest",
            "headline": self.headline,
            "focus_refs": list(self.focus_refs),
            "failed_job_labels": list(self.failed_job_labels),
            "packet_drift_status": self.packet_drift_status,
            "latest_job_status": self.latest_job_status,
            "terminal_reason_code": self.terminal_reason_code,
        }


@dataclass(frozen=True, slots=True)
class FrontdoorObservabilityReadModel:
    """Combined observability snapshot for one frontdoor status response."""

    run_id: str
    current_state: str | None
    terminal_reason_code: str | None
    inspection_present: bool
    inspection_completeness: ProjectionCompleteness
    inspection_watermark: ProjectionWatermark
    inspection_node_timeline: tuple[str, ...]
    job_count: int
    terminal_job_count: int
    failed_job_count: int
    running_job_count: int
    job_completion_rate: float
    failure_rate: float
    latest_job_status: str | None
    job_status_counts: tuple[tuple[str, int], ...]
    packet_inspection_present: bool
    packet_inspection_source: str
    packet_drift_status: str | None
    health_state: str
    health_reason: str
    contract_drift: FrontdoorContractDriftReadModel
    failure_taxonomy: FrontdoorFailureTaxonomyReadModel
    provenance_coverage: FrontdoorProvenanceCoverageReadModel
    run_identity: FrontdoorRunIdentityReadModel
    anomaly_digest: FrontdoorObservabilityAnomalyDigest

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "frontdoor_observability",
            "run_id": self.run_id,
            "current_state": self.current_state,
            "terminal_reason_code": self.terminal_reason_code,
            "inspection_present": self.inspection_present,
            "inspection_completeness": {
                "is_complete": self.inspection_completeness.is_complete,
                "missing_evidence_refs": list(self.inspection_completeness.missing_evidence_refs),
            },
            "inspection_watermark": {
                "evidence_seq": self.inspection_watermark.evidence_seq,
                "source": self.inspection_watermark.source,
            },
            "inspection_node_timeline": list(self.inspection_node_timeline),
            "job_count": self.job_count,
            "terminal_job_count": self.terminal_job_count,
            "failed_job_count": self.failed_job_count,
            "running_job_count": self.running_job_count,
            "job_completion_rate": self.job_completion_rate,
            "failure_rate": self.failure_rate,
            "latest_job_status": self.latest_job_status,
            "job_status_counts": {name: count for name, count in self.job_status_counts},
            "packet_inspection_present": self.packet_inspection_present,
            "packet_inspection_source": self.packet_inspection_source,
            "packet_drift_status": self.packet_drift_status,
            "health_state": self.health_state,
            "health_reason": self.health_reason,
            "contract_drift": self.contract_drift.to_json(),
            "failure_taxonomy": self.failure_taxonomy.to_json(),
            "provenance_coverage": self.provenance_coverage.to_json(),
            "run_identity": self.run_identity.to_json(),
            "anomaly_digest": self.anomaly_digest.to_json(),
        }


def build_frontdoor_observability(
    *,
    run_id: str,
    run_row: Mapping[str, Any],
    inspection: InspectionReadModel | None,
    jobs: Sequence[Mapping[str, Any]],
    packet_inspection: object | None,
    packet_inspection_source: str = "missing",
    contract_drift_refs: Sequence[str] = (),
) -> FrontdoorObservabilityReadModel:
    """Combine frontdoor status inputs into one observability block."""

    current_state = _mapping_text(run_row, "current_state")
    terminal_reason_code = _mapping_text(run_row, "terminal_reason_code")
    inspection_present = inspection is not None
    if inspection_present:
        inspection_completeness = inspection.completeness
        inspection_watermark = inspection.watermark
        inspection_node_timeline = inspection.node_timeline
    else:
        inspection_completeness = ProjectionCompleteness(
            is_complete=False,
            missing_evidence_refs=("inspection:missing",),
        )
        inspection_watermark = ProjectionWatermark(
            evidence_seq=None,
            source="inspection_missing",
        )
        inspection_node_timeline = ()

    normalized_jobs = tuple(dict(job) for job in jobs if isinstance(job, Mapping))
    job_count = len(normalized_jobs)
    job_status_counts = _job_status_counts(normalized_jobs)
    latest_job_status = _latest_job_status(normalized_jobs)
    running_job_count = sum(
        1
        for job in normalized_jobs
        if _classify_failure_category(_job_last_error_code(job), _job_status(job)) == "in_progress"
    )
    failed_job_count = sum(
        1
        for job in normalized_jobs
        if _classify_failure_category(_job_last_error_code(job), _job_status(job))
        not in {"success", "in_progress", "unknown"}
    )
    terminal_job_count = sum(1 for job in normalized_jobs if _terminal_status(_job_status(job)))
    job_completion_rate = 0.0 if job_count == 0 else terminal_job_count / job_count
    failure_rate = 0.0 if job_count == 0 else failed_job_count / job_count
    packet_drift_status = _packet_drift_status(packet_inspection)
    packet_inspection_present = packet_inspection is not None
    contract_drift = _contract_drift(run_row=run_row, contract_drift_refs=contract_drift_refs)
    failure_taxonomy = _failure_taxonomy(
        current_state=current_state,
        terminal_reason_code=terminal_reason_code,
        jobs=normalized_jobs,
        packet_drift_status=packet_drift_status,
    )
    provenance_coverage = _provenance_coverage(
        run_row=run_row,
        inspection_present=inspection_present,
        packet_inspection_source=packet_inspection_source,
    )
    run_identity = _run_identity(run_row=run_row, packet_inspection=packet_inspection)
    health_state, health_reason = _health_state(
        current_state=current_state,
        inspection_present=inspection_present,
        inspection_completeness=inspection_completeness,
        failed_job_count=failed_job_count,
        packet_drift_status=packet_drift_status,
        job_count=job_count,
        contract_drift=contract_drift,
    )

    focus_refs = list(inspection_completeness.missing_evidence_refs[:8])
    if contract_drift.issues:
        focus_refs.extend(issue.issue_code for issue in contract_drift.issues[:8])
    if not focus_refs and failed_job_count > 0:
        focus_refs.extend(label for label in _failed_job_labels(normalized_jobs)[:8])
    if packet_drift_status is not None and packet_drift_status != "aligned":
        focus_refs.append("packet_inspection:drift")
    anomaly_digest = FrontdoorObservabilityAnomalyDigest(
        headline=_anomaly_headline(
            health_state=health_state,
            inspection_present=inspection_present,
            inspection_completeness=inspection_completeness,
            failed_job_count=failed_job_count,
            packet_drift_status=packet_drift_status,
            contract_drift=contract_drift,
        ),
        focus_refs=_dedupe(focus_refs),
        failed_job_labels=_failed_job_labels(normalized_jobs),
        packet_drift_status=packet_drift_status,
        latest_job_status=latest_job_status,
        terminal_reason_code=terminal_reason_code,
    )

    return FrontdoorObservabilityReadModel(
        run_id=run_id,
        current_state=current_state,
        terminal_reason_code=terminal_reason_code,
        inspection_present=inspection_present,
        inspection_completeness=inspection_completeness,
        inspection_watermark=inspection_watermark,
        inspection_node_timeline=inspection_node_timeline,
        job_count=job_count,
        terminal_job_count=terminal_job_count,
        failed_job_count=failed_job_count,
        running_job_count=running_job_count,
        job_completion_rate=job_completion_rate,
        failure_rate=failure_rate,
        latest_job_status=latest_job_status,
        job_status_counts=job_status_counts,
        packet_inspection_present=packet_inspection_present,
        packet_inspection_source=packet_inspection_source,
        packet_drift_status=packet_drift_status,
        health_state=health_state,
        health_reason=health_reason,
        contract_drift=contract_drift,
        failure_taxonomy=failure_taxonomy,
        provenance_coverage=provenance_coverage,
        run_identity=run_identity,
        anomaly_digest=anomaly_digest,
    )


def render_frontdoor_observability(view: FrontdoorObservabilityReadModel) -> str:
    """Render the frontdoor observability snapshot for CLI/debug output."""

    lines: list[str] = [
        "observability.kind: frontdoor_observability",
        f"observability.run_id: {view.run_id}",
        f"observability.current_state: {view.current_state or '-'}",
        f"observability.terminal_reason_code: {view.terminal_reason_code or '-'}",
        f"observability.health_state: {view.health_state}",
        f"observability.health_reason: {view.health_reason}",
        f"observability.inspection.present: {_format_bool(view.inspection_present)}",
        f"observability.inspection.completeness.is_complete: {_format_bool(view.inspection_completeness.is_complete)}",
        f"observability.inspection.watermark.evidence_seq: {view.inspection_watermark.evidence_seq if view.inspection_watermark.evidence_seq is not None else '-'}",
        f"observability.job_count: {view.job_count}",
        f"observability.terminal_job_count: {view.terminal_job_count}",
        f"observability.failed_job_count: {view.failed_job_count}",
        f"observability.running_job_count: {view.running_job_count}",
        f"observability.job_completion_rate: {view.job_completion_rate:.3f}",
        f"observability.failure_rate: {view.failure_rate:.3f}",
        f"observability.latest_job_status: {view.latest_job_status or '-'}",
        f"observability.packet_inspection.present: {_format_bool(view.packet_inspection_present)}",
        f"observability.packet_inspection.source: {view.packet_inspection_source}",
        f"observability.packet_drift_status: {view.packet_drift_status or '-'}",
        f"observability.contract_drift.status: {view.contract_drift.status}",
        f"observability.failure_taxonomy.dominant_category: {view.failure_taxonomy.dominant_category}",
        f"observability.provenance_coverage.coverage_rate: {view.provenance_coverage.coverage_rate:.3f}",
        f"observability.run_identity.synthetic_run: {_format_bool(view.run_identity.synthetic_run)}",
        f"observability.run_identity.isolation_suffix: {view.run_identity.isolation_suffix or '-'}",
        f"observability.anomaly_digest.headline: {view.anomaly_digest.headline}",
    ]
    _append_indexed_lines(
        lines,
        "observability.inspection.missing_evidence_refs",
        view.inspection_completeness.missing_evidence_refs,
    )
    _append_indexed_lines(
        lines,
        "observability.contract_drift.issues",
        tuple(issue.issue_code for issue in view.contract_drift.issues),
    )
    _append_indexed_lines(
        lines,
        "observability.anomaly_digest.focus_refs",
        view.anomaly_digest.focus_refs,
    )
    _append_indexed_lines(
        lines,
        "observability.anomaly_digest.failed_job_labels",
        view.anomaly_digest.failed_job_labels,
    )
    return "\n".join(lines)
