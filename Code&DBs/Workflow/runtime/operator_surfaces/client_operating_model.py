"""Phase 11 Client Operating Model operator read-model builders.

This module is pure domain code. It normalizes already-provided evidence into
operator inspection payloads and never persists, mutates, calls live systems,
registers CQRS operations, or creates UI-only authority.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any


READ_MODEL_SCHEMA_VERSION = 1
READ_MODEL_KIND_PREFIX = "client_operating_model.operator_surface"

SURFACE_STATES = {
    "unknown",
    "missing",
    "not_authorized",
    "stale",
    "blocked",
    "conflict",
    "healthy",
    "empty",
    "partial",
}

BLOCKING_VERIFIER_SEVERITIES = {"error", "blocker"}
BLOCKING_DRIFT_SEVERITIES = {"critical", "high"}
BLOCKING_DRIFT_DISPOSITIONS = {"fix_now", "rerun_required", "stop_phase"}


class OperatorSurfaceValidationError(ValueError):
    """Raised when a Phase 11 read model cannot be represented safely."""

    def __init__(self, reason_code: str, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def build_system_census_view(
    *,
    system_records: Sequence[Any] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
    subsystem_states: Mapping[str, str] | None = None,
    simulation_results: Sequence[Any] = (),
    verifier_results: Sequence[Any] = (),
    drift_views: Sequence[Any] = (),
    cartridge_views: Sequence[Any] = (),
) -> dict[str, Any]:
    """Build the top-level operator census from scoped evidence rows."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    records = [_object_payload(record) for record in system_records]
    if permission["visibility"] == "not_authorized":
        records = []

    systems: list[dict[str, Any]] = []
    counts = {
        "systems": len(records),
        "connectors": 0,
        "integrations": 0,
        "objects": {},
        "lifecycle_states": {},
        "authority_states": {},
        "verification_states": {},
        "active_simulations": 0,
        "failed_simulations": 0,
        "pending_verifications": 0,
        "drift_alerts": 0,
        "cartridges": {"healthy": 0, "degraded": 0, "blocked": 0, "missing": 0, "stale": 0},
    }
    for record in records:
        metadata = _mapping(record.get("metadata"))
        state = _record_state(record.get("discovery_status") or record.get("status"))
        authority_state = str(metadata.get("authority_state") or record.get("authority_state") or "unknown")
        verification_state = str(metadata.get("verification_state") or record.get("verification_state") or "unknown")
        object_type = str(metadata.get("object_type") or record.get("category") or "unknown")
        counts["connectors"] += _int(record.get("connector_count"), len(_list(record.get("connectors"))))
        counts["integrations"] += _int(record.get("integration_count"), len(_list(record.get("integrations"))))
        _increment(counts["objects"], object_type)
        _increment(counts["lifecycle_states"], state)
        _increment(counts["authority_states"], authority_state)
        _increment(counts["verification_states"], verification_state)
        systems.append(
            {
                "system_ref": record.get("census_id") or record.get("system_slug"),
                "system_slug": record.get("system_slug"),
                "system_name": record.get("system_name"),
                "environment": record.get("environment") or "unknown",
                "discovery_status": record.get("discovery_status") or record.get("status") or "unknown",
                "authority_state": authority_state,
                "verification_state": verification_state,
                "connector_count": _int(record.get("connector_count"), len(_list(record.get("connectors")))),
                "integration_count": _int(record.get("integration_count"), len(_list(record.get("integrations")))),
                "evidence_ref": record.get("census_id") or record.get("evidence_ref"),
            }
        )

    for result in simulation_results:
        payload = _object_payload(result)
        status = str(payload.get("status") or "unknown")
        if status in {"running", "queued", "started", "active"}:
            counts["active_simulations"] += 1
        if status in {"failed", "blocked"}:
            counts["failed_simulations"] += 1
    for result in verifier_results:
        payload = _object_payload(result)
        status = str(payload.get("status") or payload.get("verdict") or "unknown")
        if status in {"pending", "queued", "running"}:
            counts["pending_verifications"] += 1
    for item in drift_views:
        payload = _object_payload(item)
        if _drift_is_alert(payload):
            counts["drift_alerts"] += 1
    for item in cartridge_views:
        payload = _object_payload(item)
        status = str(payload.get("status") or payload.get("state") or "unknown")
        if status in counts["cartridges"]:
            counts["cartridges"][status] += 1
        else:
            _increment(counts["cartridges"], status)

    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        empty=not systems,
        partial=any(str(value) in {"partial", "unknown", "unavailable"} for value in (subsystem_states or {}).values()),
        blocked=any(str(value) == "blocked" for value in (subsystem_states or {}).values()),
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "counts": counts,
        "systems": systems,
        "subsystem_states": dict(subsystem_states or {}),
        "scope_note": "counts are permission-scoped" if permission["visibility"] == "limited" else None,
    }
    return _envelope(
        "system_census",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=_merge_refs(evidence_refs, (system.get("evidence_ref") for system in systems)),
        state=state,
        payload=payload,
        stable_basis={"systems": [system.get("system_ref") for system in systems], "scope": permission["scope_ref"]},
    )


def build_object_truth_view(
    *,
    object_ref: str,
    canonical_summary: Mapping[str, Any] | None = None,
    fields: Sequence[Mapping[str, Any]] = (),
    conflicts: Sequence[Mapping[str, Any]] = (),
    gaps: Sequence[Mapping[str, Any]] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    """Build object truth inspection with field provenance and scoped values."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    redacted = set(permission.get("redacted_fields") or ())
    not_authorized = permission["visibility"] == "not_authorized"

    normalized_fields: list[dict[str, Any]] = []
    for field in fields:
        item = dict(field)
        field_name = str(item.get("field_name") or item.get("name") or "")
        field_visibility = str(item.get("visibility") or item.get("permission") or "visible")
        hidden = not_authorized or field_name in redacted or field_visibility == "not_authorized"
        status = str(item.get("state") or item.get("status") or "healthy")
        if hidden:
            status = "not_authorized"
        elif item.get("value") is None and item.get("observed_value") is None:
            status = "missing"
        normalized_fields.append(
            {
                "field_name": field_name,
                "state": status,
                "observed_value": None if hidden else item.get("observed_value", item.get("value")),
                "canonical_value": None if hidden else item.get("canonical_value", item.get("value")),
                "value_class": item.get("value_class") or item.get("source_kind") or "unknown",
                "authority": item.get("authority") or item.get("authority_tag") or "unknown",
                "confidence": item.get("confidence"),
                "provenance": [] if hidden else _list(item.get("provenance")),
                "evidence_refs": _clean_refs(item.get("evidence_refs") or item.get("evidence_ref")),
            }
        )

    normalized_conflicts = [dict(item) for item in conflicts]
    normalized_gaps = [dict(item) for item in gaps]
    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        missing=canonical_summary is None and not normalized_fields,
        conflict=bool(normalized_conflicts),
        blocked=any(bool(item.get("is_blocker") or item.get("blocking")) for item in normalized_gaps),
        partial=permission["visibility"] == "limited" or any(field["state"] == "not_authorized" for field in normalized_fields),
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "object_ref": object_ref,
        "canonical_object_summary": (
            {"object_ref": object_ref, "state": "not_authorized"}
            if not_authorized
            else dict(canonical_summary or {"object_ref": object_ref, "state": "missing"})
        ),
        "fields": normalized_fields,
        "conflicts": [] if not_authorized else normalized_conflicts,
        "gaps": [] if not_authorized else normalized_gaps,
        "last_mutation": None if not_authorized else _first_present(canonical_summary, "last_mutation", "last_mutated_at"),
        "last_verification": None if not_authorized else _first_present(canonical_summary, "last_verification", "last_verified_at"),
        "last_simulation_touch": None if not_authorized else _first_present(canonical_summary, "last_simulation_touch"),
    }
    return _envelope(
        "object_truth_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=_merge_refs(
            evidence_refs,
            (ref for field in normalized_fields for ref in field["evidence_refs"]),
        ),
        state=state,
        payload=payload,
        stable_basis={"object_ref": object_ref, "scope": permission["scope_ref"]},
    )


def build_identity_authority_view(
    *,
    object_ref: str,
    identity: Mapping[str, Any] | None = None,
    source_authority: Sequence[Mapping[str, Any]] = (),
    conflicts: Sequence[Mapping[str, Any]] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    """Build identity/source-authority inspection for one object."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    authority_rows: list[dict[str, Any]] = []
    missing_authority = False
    for row in source_authority:
        item = dict(row)
        winner = item.get("winner") or item.get("winning_source") or item.get("source_ref")
        if not winner:
            missing_authority = True
        authority_rows.append(
            {
                "field_group": item.get("field_group") or item.get("field_name") or "unknown",
                "winner": winner,
                "ranking": _list(item.get("ranking") or item.get("source_rankings")),
                "freshness": item.get("freshness") or item.get("freshness_state") or "unknown",
                "explanation": item.get("explanation") or item.get("selection_reason") or "unknown",
                "evidence_refs": _clean_refs(item.get("evidence_refs") or item.get("evidence_ref")),
                "state": "missing" if not winner else item.get("state") or "healthy",
            }
        )

    identity_payload = dict(identity or {})
    identity_conflicts = [dict(item) for item in conflicts]
    if identity_payload.get("cluster_state") in {"review-required", "split-required"}:
        identity_conflicts.append(
            {
                "reason_code": f"identity.{identity_payload['cluster_state']}",
                "detail": "identity cluster requires operator review",
            }
        )
    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        missing=not identity_payload or missing_authority,
        conflict=bool(identity_conflicts),
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "object_ref": object_ref,
        "identity_graph": {
            "canonical_id": identity_payload.get("canonical_id") or identity_payload.get("cluster_id"),
            "aliases": _list(identity_payload.get("aliases")),
            "external_ids": _list(identity_payload.get("external_ids")),
            "cartridge_local_ids": _list(identity_payload.get("cartridge_local_ids")),
            "cluster_state": identity_payload.get("cluster_state") or identity_payload.get("review_status") or "unknown",
        },
        "source_authority": authority_rows,
        "conflicts": identity_conflicts,
        "missing_authority": missing_authority,
    }
    return _envelope(
        "identity_authority_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=_merge_refs(evidence_refs, (ref for row in authority_rows for ref in row["evidence_refs"])),
        state=state,
        payload=payload,
        stable_basis={"object_ref": object_ref, "identity": payload["identity_graph"], "scope": permission["scope_ref"]},
    )


def build_simulation_timeline_view(
    *,
    subject_ref: str,
    events: Sequence[Any] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
    filters: Mapping[str, Any] | None = None,
    reverse_chronological: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    """Build a bounded reverse-chronological simulation/operator timeline."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    event_filters = dict(filters or {})
    normalized = [_timeline_event(event) for event in events]
    filtered = [event for event in normalized if _timeline_matches(event, event_filters)]
    filtered.sort(key=lambda item: (item["occurred_at"] or "", item["event_id"]), reverse=reverse_chronological)
    if limit is not None:
        filtered = filtered[: max(0, int(limit))]
    state, reasons = _surface_state(permission=permission, freshness=freshness, empty=not filtered)
    payload = {
        "state": state,
        "state_reasons": reasons,
        "subject_ref": subject_ref,
        "filters": event_filters,
        "reverse_chronological": reverse_chronological,
        "events": [] if permission["visibility"] == "not_authorized" else filtered,
    }
    return _envelope(
        "simulation_timeline_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=_merge_refs(correlation_ids, (event.get("correlation_id") for event in filtered)),
        evidence_refs=_merge_refs(evidence_refs, (event.get("event_id") for event in filtered)),
        state=state,
        payload=payload,
        stable_basis={"subject_ref": subject_ref, "filters": event_filters, "scope": permission["scope_ref"]},
    )


def build_verifier_results_view(
    *,
    subject_ref: str,
    verifier_results: Sequence[Any] = (),
    snapshot_ref: str | None = None,
    input_snapshot_at: Any | None = None,
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    """Build legible verifier output with blocking/advisory separation."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    checks: list[dict[str, Any]] = []
    for result in verifier_results:
        payload = _object_payload(result)
        status = str(payload.get("status") or payload.get("verdict") or "unknown")
        severity = str(payload.get("severity") or "warning")
        blocking = status not in {"passed", "success"} and severity in BLOCKING_VERIFIER_SEVERITIES
        checks.append(
            {
                "verifier_id": payload.get("verifier_id") or payload.get("check_id"),
                "verifier_kind": payload.get("verifier_kind") or payload.get("rule") or "unknown",
                "status": status,
                "severity": severity,
                "blocking": blocking,
                "findings": _list(payload.get("findings")),
                "summary": payload.get("summary") or payload.get("message") or "",
                "version_ref": payload.get("version_ref") or payload.get("verifier_version"),
                "evidence_refs": _clean_refs(payload.get("evidence_refs") or payload.get("evidence_ref")),
            }
        )
    blocking_checks = [check for check in checks if check["blocking"]]
    advisory_checks = [check for check in checks if not check["blocking"] and check["status"] not in {"passed", "success"}]
    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        empty=not checks,
        blocked=bool(blocking_checks),
        partial=any(check["status"] in {"unknown", "skipped"} for check in checks),
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "subject_ref": subject_ref,
        "snapshot_ref": snapshot_ref,
        "input_snapshot_at": _optional_timestamp(input_snapshot_at),
        "latest_verdict": "blocked" if blocking_checks else ("healthy" if checks else "missing"),
        "blocking_findings": blocking_checks,
        "advisory_findings": advisory_checks,
        "check_matrix": checks,
    }
    return _envelope(
        "verifier_results_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=_merge_refs(evidence_refs, (ref for check in checks for ref in check["evidence_refs"])),
        state=state,
        payload=payload,
        stable_basis={"subject_ref": subject_ref, "snapshot_ref": snapshot_ref, "scope": permission["scope_ref"]},
    )


def build_sandbox_drift_view(
    *,
    sandbox_ref: str,
    comparison_report: Any | None = None,
    drift_ledger: Any | None = None,
    expected_snapshot_ref: str | None = None,
    observed_snapshot_ref: str | None = None,
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    """Build sandbox drift inspection with severity and action derivation."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    report = _object_payload(comparison_report) if comparison_report is not None else {}
    ledger = _object_payload(drift_ledger) if drift_ledger is not None else {}
    rows = [dict(row) for row in _list(report.get("rows"))]
    classifications = [dict(item) for item in _list(ledger.get("classifications"))]
    by_row: dict[str, list[dict[str, Any]]] = {}
    for classification in classifications:
        by_row.setdefault(str(classification.get("row_id") or ""), []).append(classification)

    category_summary: dict[str, dict[str, Any]] = {}
    blockers: list[dict[str, Any]] = []
    suggested_actions: list[dict[str, Any]] = []
    for row in rows:
        category = str(row.get("dimension") or "unknown")
        bucket = category_summary.setdefault(
            category,
            {"category": category, "match": 0, "partial_match": 0, "drift": 0, "blocked": 0, "severity": "informational"},
        )
        row_status = str(row.get("status") or "unknown")
        bucket[row_status] = bucket.get(row_status, 0) + 1
        row_classes = by_row.get(str(row.get("row_id") or ""), [])
        severity = _drift_severity(row_status, row_classes)
        bucket["severity"] = _max_drift_severity(bucket["severity"], severity)
        if severity == "blocking":
            blockers.append(
                {
                    "row_id": row.get("row_id"),
                    "reason_code": "sandbox_drift.blocking",
                    "dimension": category,
                    "classifications": row_classes,
                }
            )
        action = _drift_action(row, row_classes, severity)
        if action:
            suggested_actions.append(action)

    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        missing=not report,
        blocked=bool(blockers),
        conflict=any(str(row.get("status")) in {"drift", "partial_match"} for row in rows),
        partial=any(str(row.get("status")) == "blocked" for row in rows) and not blockers,
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "sandbox_ref": sandbox_ref,
        "expected_snapshot_ref": expected_snapshot_ref or report.get("prediction_ref"),
        "observed_snapshot_ref": observed_snapshot_ref or report.get("evidence_package_id"),
        "comparison_status": report.get("status") or "missing",
        "categories": sorted(category_summary.values(), key=lambda item: item["category"]),
        "blockers": blockers,
        "suggested_actions": suggested_actions,
        "first_seen": _first_timestamp(rows, "first_seen", "occurred_at", "captured_at"),
        "last_seen": _last_timestamp(rows, "last_seen", "occurred_at", "captured_at"),
    }
    return _envelope(
        "sandbox_drift_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=_merge_refs(evidence_refs, (report.get("report_id"), ledger.get("ledger_id"))),
        state=state,
        payload=payload,
        stable_basis={"sandbox_ref": sandbox_ref, "report": report.get("report_id"), "scope": permission["scope_ref"]},
    )


def build_cartridge_status_view(
    *,
    cartridge_ref: str,
    validation_report: Any | None = None,
    manifest: Any | None = None,
    deployment_findings: Sequence[Any] = (),
    runtime_findings: Sequence[Any] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    """Build cartridge availability, compatibility, and readiness status."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    report = _object_payload(validation_report) if validation_report is not None else {}
    manifest_payload = _object_payload(manifest) if manifest is not None else _object_payload(report.get("manifest"))
    findings = [_finding_payload(item) for item in _list(report.get("findings"))]
    findings.extend(_finding_payload(item) for item in deployment_findings)
    findings.extend(_finding_payload(item) for item in runtime_findings)
    errors = [item for item in findings if item.get("severity") == "error"]
    warnings = [item for item in findings if item.get("severity") == "warning"]
    status = "missing"
    if manifest_payload or report:
        status = "blocked" if errors else ("degraded" if warnings else "healthy")
    if freshness["status"] == "stale" and status == "healthy":
        status = "stale"
    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        missing=not manifest_payload and not report,
        blocked=bool(errors),
        partial=bool(warnings),
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "cartridge_ref": cartridge_ref,
        "status": status,
        "installed_version": manifest_payload.get("cartridge_version"),
        "expected_version": manifest_payload.get("expected_version") or report.get("expected_version"),
        "build_id": manifest_payload.get("build_id"),
        "capabilities": _list(_nested(manifest_payload, "compatibility", "capabilities")),
        "health": {"state": status, "reason_codes": [item.get("reason_code") for item in findings]},
        "compatibility": manifest_payload.get("compatibility") or {},
        "blockers": errors,
        "warnings": warnings,
        "last_successful_run": report.get("last_successful_run"),
        "last_failure": report.get("last_failure"),
    }
    return _envelope(
        "cartridge_status_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=_merge_refs(evidence_refs, (report.get("canonical_digest"), manifest_payload.get("canonical_digest"))),
        state=state,
        payload=payload,
        stable_basis={"cartridge_ref": cartridge_ref, "scope": permission["scope_ref"]},
    )


def build_managed_runtime_accounting_summary(
    *,
    subject_ref: str,
    receipts: Sequence[Any] = (),
    usage_summaries: Sequence[Any] = (),
    pool_health: Any | None = None,
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    """Build managed-runtime run, cost, and health inspection summary."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    receipt_rows = [_object_payload(receipt) for receipt in receipts]
    usage_rows = [_object_payload(summary) for summary in usage_summaries]
    health = _object_payload(pool_health) if pool_health is not None else None
    cost_total = Decimal("0")
    for row in receipt_rows + usage_rows:
        amount = _nested(row, "cost_summary", "amount")
        if amount is not None:
            cost_total += Decimal(str(amount))
    blocked = bool(health and health.get("dispatch_allowed") is False and health.get("state") in {"stale", "unavailable"})
    partial = bool(health and health.get("state") == "degraded")
    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        empty=not receipt_rows and not usage_rows and not health,
        blocked=blocked,
        partial=partial,
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "subject_ref": subject_ref,
        "run_count": len(receipt_rows) or len(usage_rows),
        "terminal_statuses": _counts(row.get("terminal_status") or row.get("status") for row in receipt_rows),
        "execution_modes": _counts(row.get("execution_mode") for row in receipt_rows + usage_rows),
        "cost": {"amount": _decimal_string(cost_total), "basis": "provided_receipts_and_usage_summaries"},
        "pool_health": health,
        "receipts": receipt_rows,
        "usage_summaries": usage_rows,
    }
    return _envelope(
        "managed_runtime_accounting_summary",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=evidence_refs,
        state=state,
        payload=payload,
        stable_basis={"subject_ref": subject_ref, "scope": permission["scope_ref"]},
    )


def build_next_safe_actions_view(
    *,
    subject_ref: str,
    snapshot_ref: str,
    action_candidates: Sequence[Mapping[str, Any]] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
    require_fresh_snapshot: bool = True,
) -> dict[str, Any]:
    """Build bounded next-safe-action recommendations from scoped evidence."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    freshness = _freshness(generated, source_updated_at=source_updated_at, max_age_seconds=max_age_seconds)
    allowed: list[dict[str, Any]] = []
    blocked_actions: list[dict[str, Any]] = []
    snapshot_stale = require_fresh_snapshot and freshness["status"] == "stale"
    for candidate in action_candidates:
        action = _normalize_action(candidate, snapshot_ref=snapshot_ref)
        blockers = list(action["blockers"])
        if snapshot_stale:
            blockers.append(
                {
                    "reason_code": "safe_action.snapshot_stale",
                    "detail": "action was derived from a stale snapshot",
                    "snapshot_ref": snapshot_ref,
                }
            )
        if action.pop("_unsafe", False):
            blockers.append(
                {
                    "reason_code": "safe_action.unsafe_or_ambiguous",
                    "detail": "candidate action is unsafe or insufficiently specified",
                }
            )
        if blockers:
            action["blockers"] = blockers
            blocked_actions.append(action)
        else:
            allowed.append(action)

    state, reasons = _surface_state(
        permission=permission,
        freshness=freshness,
        empty=not allowed and not blocked_actions,
        blocked=bool(blocked_actions) and not allowed,
        partial=bool(blocked_actions) and bool(allowed),
    )
    payload = {
        "state": state,
        "state_reasons": reasons,
        "subject_ref": subject_ref,
        "snapshot_ref": snapshot_ref,
        "actions": allowed,
        "blocked_actions": blocked_actions,
    }
    return _envelope(
        "next_safe_actions_view",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=evidence_refs,
        state=state,
        payload=payload,
        stable_basis={"subject_ref": subject_ref, "snapshot_ref": snapshot_ref, "scope": permission["scope_ref"]},
    )


def validate_workflow_builder_graph(
    *,
    graph: Mapping[str, Any],
    approved_blocks: Mapping[str, Mapping[str, Any]],
    allowed_edges: Sequence[Mapping[str, str]] = (),
    generated_at: Any | None = None,
    permission_scope: Mapping[str, Any] | None = None,
    correlation_ids: Sequence[str] = (),
    evidence_refs: Sequence[str] = (),
) -> dict[str, Any]:
    """Validate a builder composition without side effects."""

    generated = _timestamp(generated_at)
    permission = _permission_scope(permission_scope)
    nodes = [dict(node) for node in _list(graph.get("nodes"))]
    edges = [dict(edge) for edge in _list(graph.get("edges"))]
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    node_ids = [str(node.get("node_id") or "") for node in nodes]
    duplicates = sorted({node_id for node_id in node_ids if node_id and node_ids.count(node_id) > 1})
    for node_id in duplicates:
        errors.append(_validation_reason("builder.duplicate_node", "node id is duplicated", node_id=node_id))

    node_by_id = {str(node.get("node_id") or ""): node for node in nodes if str(node.get("node_id") or "")}
    for node in nodes:
        block_ref = str(node.get("block_ref") or "")
        node_id = str(node.get("node_id") or "")
        if not node_id:
            errors.append(_validation_reason("builder.node_id_required", "node_id is required"))
        if block_ref not in approved_blocks:
            errors.append(_validation_reason("builder.block_not_approved", "block is not in the approved palette", node_id=node_id, block_ref=block_ref))

    allowed_pairs = {
        (str(edge.get("from_block") or edge.get("from") or ""), str(edge.get("to_block") or edge.get("to") or ""))
        for edge in allowed_edges
    }
    for edge in edges:
        from_id = str(edge.get("from") or edge.get("from_node_id") or "")
        to_id = str(edge.get("to") or edge.get("to_node_id") or "")
        if from_id not in node_by_id or to_id not in node_by_id:
            errors.append(_validation_reason("builder.edge_unknown_node", "edge references a missing node", from_node_id=from_id, to_node_id=to_id))
            continue
        from_block = str(node_by_id[from_id].get("block_ref") or "")
        to_block = str(node_by_id[to_id].get("block_ref") or "")
        if allowed_pairs and (from_block, to_block) not in allowed_pairs and (from_id, to_id) not in allowed_pairs:
            errors.append(
                _validation_reason(
                    "builder.edge_not_allowed",
                    "edge is not allowed by the approved composition contract",
                    from_node_id=from_id,
                    to_node_id=to_id,
                    from_block=from_block,
                    to_block=to_block,
                )
            )

    cycle = _cycle_path(nodes, edges)
    if cycle:
        errors.append(_validation_reason("builder.cycle_detected", "builder graph must be acyclic", cycle=cycle))

    produced = set()
    for node in nodes:
        block = approved_blocks.get(str(node.get("block_ref") or ""), {})
        required = set(_clean_refs(block.get("requires") or node.get("requires")))
        missing = sorted(required - produced)
        if missing:
            errors.append(
                _validation_reason(
                    "builder.prerequisite_missing",
                    "node requires upstream data that has not been produced",
                    node_id=node.get("node_id"),
                    missing=missing,
                )
            )
        produced.update(_clean_refs(block.get("provides") or node.get("provides")))

    ok = not errors and permission["visibility"] != "not_authorized"
    freshness = _freshness(generated)
    state, reasons = _surface_state(permission=permission, freshness=freshness, blocked=not ok, partial=bool(warnings))
    payload = {
        "state": state,
        "state_reasons": reasons,
        "validation": {
            "ok": ok,
            "errors": errors,
            "warnings": warnings,
            "approved_block_count": len(approved_blocks),
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
        "safe_action_summary": [] if errors else [{"action_ref": "workflow_builder.save_candidate", "preconditions": ["validation.ok"]}],
    }
    return _envelope(
        "workflow_builder_validation",
        generated_at=generated,
        permission_scope=permission,
        freshness=freshness,
        correlation_ids=correlation_ids,
        evidence_refs=evidence_refs,
        state=state,
        payload=payload,
        stable_basis={"graph": graph, "scope": permission["scope_ref"]},
    )


def _envelope(
    view_name: str,
    *,
    generated_at: str,
    permission_scope: Mapping[str, Any],
    freshness: Mapping[str, Any],
    correlation_ids: Sequence[str],
    evidence_refs: Sequence[str],
    state: str,
    payload: Mapping[str, Any],
    stable_basis: Mapping[str, Any],
) -> dict[str, Any]:
    stable_id = f"{view_name}.{_digest({'view': view_name, **stable_basis})[:20]}"
    return {
        "kind": f"{READ_MODEL_KIND_PREFIX}.{view_name}.v1",
        "schema_version": READ_MODEL_SCHEMA_VERSION,
        "stable_id": stable_id,
        "view_id": stable_id,
        "generated_at": generated_at,
        "freshness": dict(freshness),
        "permission_scope": dict(permission_scope),
        "correlation_ids": _clean_refs(correlation_ids),
        "evidence_refs": _clean_refs(evidence_refs),
        "state": _surface_state_member(state),
        "payload": _json_safe(payload),
    }


def _surface_state(
    *,
    permission: Mapping[str, Any],
    freshness: Mapping[str, Any],
    empty: bool = False,
    partial: bool = False,
    stale: bool = False,
    missing: bool = False,
    blocked: bool = False,
    conflict: bool = False,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if permission.get("visibility") == "not_authorized":
        return "not_authorized", ["permission.not_authorized"]
    if stale or freshness.get("status") == "stale":
        reasons.append("freshness.stale")
    if blocked:
        reasons.append("state.blocked")
    if conflict:
        reasons.append("state.conflict")
    if missing:
        reasons.append("state.missing")
    if empty:
        reasons.append("state.empty")
    if partial:
        reasons.append("state.partial")
    if blocked:
        return "blocked", reasons
    if conflict:
        return "conflict", reasons
    if missing:
        return "missing", reasons
    if freshness.get("status") == "stale" or stale:
        return "stale", reasons
    if empty:
        return "empty", reasons
    if partial:
        return "partial", reasons
    return "healthy", ["state.healthy"]


def _surface_state_member(value: Any) -> str:
    text = str(value or "unknown")
    return text if text in SURFACE_STATES else "unknown"


def _permission_scope(value: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(value or {})
    visibility = str(raw.get("visibility") or raw.get("mode") or "full")
    if visibility not in {"full", "limited", "not_authorized"}:
        visibility = "limited"
    return {
        "scope_ref": str(raw.get("scope_ref") or raw.get("tenant_ref") or "operator_scope.unspecified"),
        "visibility": visibility,
        "redacted_fields": _clean_refs(raw.get("redacted_fields")),
        "filters": dict(raw.get("filters") or {}),
    }


def _freshness(
    generated_at: str,
    *,
    source_updated_at: Any | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    source = _optional_timestamp(source_updated_at)
    if source is None:
        return {
            "status": "unknown",
            "is_stale": False,
            "generated_at": generated_at,
            "source_updated_at": None,
            "max_age_seconds": max_age_seconds,
            "age_seconds": None,
        }
    age = max(0, int((_parse_datetime(generated_at) - _parse_datetime(source)).total_seconds()))
    stale = max_age_seconds is not None and age > int(max_age_seconds)
    return {
        "status": "stale" if stale else "fresh",
        "is_stale": stale,
        "generated_at": generated_at,
        "source_updated_at": source,
        "max_age_seconds": max_age_seconds,
        "age_seconds": age,
    }


def _timestamp(value: Any | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return _optional_timestamp(value) or str(value)


def _optional_timestamp(value: Any | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _object_payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    for method_name in ("to_json", "to_dict", "as_dict", "to_contract"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                payload = method()
            except TypeError:
                continue
            if isinstance(payload, Mapping):
                return dict(payload)
    if is_dataclass(value):
        return dict(asdict(value))
    return dict(getattr(value, "__dict__", {}) or {})


def _finding_payload(value: Any) -> dict[str, Any]:
    payload = _object_payload(value)
    if "severity" not in payload:
        payload["severity"] = "error" if payload.get("blocking") else "warning"
    return payload


def _timeline_event(value: Any) -> dict[str, Any]:
    payload = _object_payload(value)
    occurred = _optional_timestamp(
        payload.get("occurred_at") or payload.get("timestamp") or payload.get("created_at") or payload.get("captured_at")
    )
    event_id = str(payload.get("event_id") or payload.get("id") or _stable_ref("timeline_event", payload))
    return {
        "event_id": event_id,
        "event_type": payload.get("event_type") or payload.get("type") or "unknown",
        "occurred_at": occurred,
        "severity": payload.get("severity") or _nested(payload, "payload", "severity") or "info",
        "actor_ref": payload.get("actor_ref") or _nested(payload, "payload", "actor_ref"),
        "cartridge_ref": payload.get("cartridge_ref") or _nested(payload, "payload", "cartridge_ref"),
        "correlation_id": payload.get("correlation_id"),
        "related_refs": _clean_refs(payload.get("related_refs") or payload.get("evidence_refs")),
        "payload": payload.get("payload") or {},
    }


def _timeline_matches(event: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    if not filters:
        return True
    checks = {
        "event_type": event.get("event_type"),
        "severity": event.get("severity"),
        "actor": event.get("actor_ref"),
        "actor_ref": event.get("actor_ref"),
        "cartridge": event.get("cartridge_ref"),
        "cartridge_ref": event.get("cartridge_ref"),
        "correlation_id": event.get("correlation_id"),
    }
    for key, expected in filters.items():
        if expected in (None, "", (), []):
            continue
        actual = checks.get(key)
        allowed = set(_clean_refs(expected))
        if allowed and str(actual) not in allowed:
            return False
    return True


def _normalize_action(candidate: Mapping[str, Any], *, snapshot_ref: str) -> dict[str, Any]:
    action_ref = str(candidate.get("action_ref") or candidate.get("ref") or "")
    if not action_ref:
        action_ref = _stable_ref("safe_action", candidate)
    confidence = candidate.get("confidence")
    unsafe = bool(candidate.get("unsafe") or candidate.get("ambiguous"))
    if confidence is not None:
        try:
            unsafe = unsafe or float(confidence) < 0.5
        except (TypeError, ValueError):
            unsafe = True
    return {
        "action_ref": action_ref,
        "snapshot_ref": str(candidate.get("snapshot_ref") or snapshot_ref),
        "preconditions": _list(candidate.get("preconditions")),
        "expected_effects": _list(candidate.get("expected_effects") or candidate.get("effects")),
        "reversibility_class": candidate.get("reversibility_class") or "unknown",
        "blockers": [dict(item) for item in _list(candidate.get("blockers")) if isinstance(item, Mapping)],
        "evidence_refs": _clean_refs(candidate.get("evidence_refs") or candidate.get("evidence_ref")),
        "_unsafe": unsafe,
    }


def _drift_is_alert(payload: Mapping[str, Any]) -> bool:
    return str(payload.get("state") or payload.get("status") or payload.get("comparison_status")) in {
        "blocked",
        "conflict",
        "drift",
        "partial_match",
    }


def _drift_severity(row_status: str, classifications: Sequence[Mapping[str, Any]]) -> str:
    if row_status == "blocked":
        return "blocking"
    for item in classifications:
        if str(item.get("severity")) in BLOCKING_DRIFT_SEVERITIES:
            return "blocking"
        if str(item.get("disposition")) in BLOCKING_DRIFT_DISPOSITIONS:
            return "blocking"
    if row_status in {"drift", "partial_match"} or classifications:
        return "caution"
    return "informational"


def _max_drift_severity(left: str, right: str) -> str:
    order = {"informational": 0, "caution": 1, "blocking": 2}
    return right if order.get(right, 0) > order.get(left, 0) else left


def _drift_action(row: Mapping[str, Any], classifications: Sequence[Mapping[str, Any]], severity: str) -> dict[str, Any] | None:
    if severity == "informational":
        return None
    reason_codes = [code for item in classifications for code in _clean_refs(item.get("reason_codes"))]
    if "OBSERVABILITY_GAP" in reason_codes:
        action_ref = "sandbox_drift.capture_missing_evidence"
    elif severity == "blocking":
        action_ref = "sandbox_drift.resolve_blocking_drift"
    else:
        action_ref = "sandbox_drift.review_caution"
    return {
        "action_ref": action_ref,
        "row_id": row.get("row_id"),
        "preconditions": ["operator_review", "evidence_available"],
        "expected_effects": ["drift disposition becomes explicit"],
        "reversibility_class": "read_only",
        "blockers": [] if severity != "blocking" else [{"reason_code": "sandbox_drift.blocks_safe_actions"}],
    }


def _cycle_path(nodes: Sequence[Mapping[str, Any]], edges: Sequence[Mapping[str, Any]]) -> list[str]:
    graph: dict[str, list[str]] = {str(node.get("node_id") or ""): [] for node in nodes}
    for edge in edges:
        source = str(edge.get("from") or edge.get("from_node_id") or "")
        target = str(edge.get("to") or edge.get("to_node_id") or "")
        if source in graph and target in graph:
            graph[source].append(target)
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(node: str) -> bool:
        visiting.add(node)
        path.append(node)
        for target in graph.get(node, []):
            if target in visiting:
                path.append(target)
                return True
            if target not in visited and visit(target):
                return True
        visiting.remove(node)
        visited.add(node)
        path.pop()
        return False

    for node in sorted(graph):
        if node not in visited and visit(node):
            return path
    return []


def _validation_reason(reason_code: str, detail: str, **context: Any) -> dict[str, Any]:
    payload = {"reason_code": reason_code, "detail": detail}
    payload.update({key: _json_safe(value) for key, value in context.items() if value not in (None, "", [], {})})
    return payload


def _record_state(value: Any) -> str:
    text = str(value or "unknown")
    if text in {"verified", "captured", "valid", "completed", "succeeded"}:
        return "healthy"
    if text in {"blocked", "failed", "revoked", "expired"}:
        return "blocked"
    if text in {"conflict", "conflicting"}:
        return "conflict"
    if text in {"missing", "not_found"}:
        return "missing"
    return text if text in SURFACE_STATES else "unknown"


def _first_present(source: Mapping[str, Any] | None, *keys: str) -> Any:
    if not source:
        return None
    for key in keys:
        if key in source:
            return source[key]
    return None


def _nested(source: Mapping[str, Any], *keys: str) -> Any:
    value: Any = source
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return list(value)
    return [value]


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _increment(counts: dict[str, int], key: Any) -> None:
    text = str(key or "unknown")
    counts[text] = counts.get(text, 0) + 1


def _counts(values: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value is not None:
            _increment(counts, value)
    return counts


def _clean_refs(value: Any) -> list[str]:
    return [str(item).strip() for item in _list(value) if str(item).strip()]


def _merge_refs(*values: Iterable[Any]) -> list[str]:
    refs: list[str] = []
    for value in values:
        refs.extend(_clean_refs(value))
    return sorted(dict.fromkeys(refs))


def _first_timestamp(rows: Sequence[Mapping[str, Any]], *keys: str) -> str | None:
    timestamps = [_optional_timestamp(row.get(key)) for row in rows for key in keys if row.get(key)]
    timestamps = [item for item in timestamps if item]
    return min(timestamps) if timestamps else None


def _last_timestamp(rows: Sequence[Mapping[str, Any]], *keys: str) -> str | None:
    timestamps = [_optional_timestamp(row.get(key)) for row in rows for key in keys if row.get(key)]
    timestamps = [item for item in timestamps if item]
    return max(timestamps) if timestamps else None


def _decimal_string(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000001")), "f")


def _stable_ref(prefix: str, value: Any) -> str:
    return f"{prefix}.{_digest(value)[:20]}"


def _digest(value: Any) -> str:
    payload = json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _optional_timestamp(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if hasattr(value, "value"):
        return value.value
    return value


__all__ = [
    "OperatorSurfaceValidationError",
    "READ_MODEL_SCHEMA_VERSION",
    "build_cartridge_status_view",
    "build_identity_authority_view",
    "build_managed_runtime_accounting_summary",
    "build_next_safe_actions_view",
    "build_object_truth_view",
    "build_sandbox_drift_view",
    "build_simulation_timeline_view",
    "build_system_census_view",
    "build_verifier_results_view",
    "validate_workflow_builder_graph",
]
