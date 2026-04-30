"""Deterministic Object Truth MDM/source-authority primitives.

This module is pure domain code. It builds JSON-ready evidence records for
identity clusters, normalization, reversible lineage, freshness, source
authority, field comparison, hierarchy signals, and typed gaps. It performs no
IO and does not register CQRS operations.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any, Literal

from core.object_truth_ops import (
    ObjectTruthOperationError,
    canonical_digest,
    canonical_value,
)


OBJECT_TRUTH_MDM_SCHEMA_VERSION = 1

EntityType = Literal["person", "organization", "account", "location", "asset"]
ClusterState = Literal["proposed", "auto-accepted", "review-required", "rejected", "split-required"]
MatchSignalClass = Literal[
    "exact_identifier",
    "strong_quasi_identifier",
    "weak_descriptive_similarity",
    "relational_context",
    "temporal_consistency",
    "source_provenance_confidence",
]
AntiMatchSignalClass = Literal[
    "mutually_exclusive_official_identifier",
    "impossible_temporal_overlap",
    "contradictory_legal_entity_type",
    "known_deprecation_or_reassignment",
    "hard_authoritative_parent_conflict",
]
FieldVolatility = Literal["stable", "moderate-change", "high-change"]
StalenessBand = Literal["current", "aging", "stale", "unknown"]
EvidenceType = Literal[
    "policy_decision",
    "contractual_source_of_record",
    "system_stewardship_assignment",
    "regulatory_registry_designation",
    "operational_reliability_evidence",
    "historical_accuracy_evidence",
]
GapType = Literal[
    "missing-required",
    "missing-authoritative",
    "conflicting-values",
    "unverifiable-identity",
    "stale-value",
    "ambiguous-hierarchy",
    "non-normalizable",
    "broken-lineage",
    "policy-missing",
    "manual-review-pending",
]
GapSeverity = Literal["critical", "high", "medium", "low"]
HierarchySignalType = Literal[
    "parent-child",
    "ultimate-parent",
    "rollup-eligibility",
    "alias-dba",
    "branch-headquarters",
    "site-mailing-address",
    "account-stewardship",
    "asset-containment",
    "flattened-parent",
]

ENTITY_TYPES = {"person", "organization", "account", "location", "asset"}
CLUSTER_STATES = {"proposed", "auto-accepted", "review-required", "rejected", "split-required"}
MATCH_SIGNAL_CLASSES = {
    "exact_identifier",
    "strong_quasi_identifier",
    "weak_descriptive_similarity",
    "relational_context",
    "temporal_consistency",
    "source_provenance_confidence",
}
ANTI_MATCH_SIGNAL_CLASSES = {
    "mutually_exclusive_official_identifier",
    "impossible_temporal_overlap",
    "contradictory_legal_entity_type",
    "known_deprecation_or_reassignment",
    "hard_authoritative_parent_conflict",
}
FIELD_VOLATILITY_CLASSES = {"stable", "moderate-change", "high-change"}
EVIDENCE_TYPES = {
    "policy_decision",
    "contractual_source_of_record",
    "system_stewardship_assignment",
    "regulatory_registry_designation",
    "operational_reliability_evidence",
    "historical_accuracy_evidence",
}
GAP_TYPES = {
    "missing-required",
    "missing-authoritative",
    "conflicting-values",
    "unverifiable-identity",
    "stale-value",
    "ambiguous-hierarchy",
    "non-normalizable",
    "broken-lineage",
    "policy-missing",
    "manual-review-pending",
}
GAP_SEVERITIES = {"critical", "high", "medium", "low"}
HIERARCHY_SIGNAL_TYPES = {
    "parent-child",
    "ultimate-parent",
    "rollup-eligibility",
    "alias-dba",
    "branch-headquarters",
    "site-mailing-address",
    "account-stewardship",
    "asset-containment",
    "flattened-parent",
}

DEFAULT_CLUSTER_THRESHOLDS = {
    "auto_accept": 0.82,
    "manual_review": 0.50,
    "auto_reject": 0.20,
}
MATCH_SIGNAL_WEIGHTS = {
    "exact_identifier": 0.55,
    "strong_quasi_identifier": 0.25,
    "weak_descriptive_similarity": 0.05,
    "relational_context": 0.12,
    "temporal_consistency": 0.08,
    "source_provenance_confidence": 0.08,
}
ANTI_MATCH_PENALTIES = {
    "mutually_exclusive_official_identifier": 1.0,
    "impossible_temporal_overlap": 1.0,
    "contradictory_legal_entity_type": 1.0,
    "known_deprecation_or_reassignment": 1.0,
    "hard_authoritative_parent_conflict": 1.0,
}
VOLATILITY_BASELINE_HOURS = {
    "stable": 24.0 * 365.0,
    "moderate-change": 24.0 * 90.0,
    "high-change": 24.0 * 14.0,
}
LOSSY_NORMALIZATION_STEPS = {
    "casefold",
    "lowercase",
    "uppercase",
    "remove_punctuation",
    "remove_legal_suffix",
    "digits_only",
    "status_synonym",
}
LEGAL_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "limited",
    "llc",
    "ltd",
}


def build_cluster_member(
    *,
    entity_type: EntityType,
    source_system: str,
    source_record_id: str,
    source_object_ref: str | None = None,
    object_version_digest: str | None = None,
    source_record: dict[str, Any] | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one deterministic member reference for an identity cluster."""

    system = _required_text(source_system, "source_system")
    record_id = _required_text(source_record_id, "source_record_id")
    payload = {
        "kind": "object_truth.mdm.cluster_member.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "entity_type": _validate_member(entity_type, ENTITY_TYPES, "entity_type"),
        "source_system": system,
        "source_record_id": record_id,
        "source_record_ref": f"{system}:{record_id}",
        "source_object_ref": _optional_text(source_object_ref),
        "object_version_digest": _optional_text(object_version_digest),
        "source_record_digest": (
            canonical_digest(source_record, purpose="object_truth.mdm.source_record.v1")
            if source_record is not None
            else None
        ),
        "source_metadata": canonical_value(source_metadata or {}),
    }
    payload["member_digest"] = canonical_digest(payload, purpose="object_truth.mdm.cluster_member.v1")
    return payload


def build_match_signal(
    *,
    signal_class: MatchSignalClass,
    left_member_ref: str,
    right_member_ref: str,
    field_name: str,
    evidence_value: Any,
    confidence: float = 1.0,
    reason: str | None = None,
    signal_weight: float | None = None,
) -> dict[str, Any]:
    """Build a positive identity signal between two source records."""

    signal = _validate_member(signal_class, MATCH_SIGNAL_CLASSES, "signal_class")
    resolved_confidence = _bounded_float(confidence, "confidence")
    weight = (
        _bounded_float(signal_weight, "signal_weight")
        if signal_weight is not None
        else MATCH_SIGNAL_WEIGHTS[signal]
    )
    payload = {
        "kind": "object_truth.mdm.match_signal.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "signal_class": signal,
        "left_member_ref": _required_text(left_member_ref, "left_member_ref"),
        "right_member_ref": _required_text(right_member_ref, "right_member_ref"),
        "field_name": _required_text(field_name, "field_name"),
        "evidence_value": canonical_value(evidence_value),
        "confidence": resolved_confidence,
        "signal_weight": weight,
        "signal_score": round(resolved_confidence * weight, 6),
        "reason": _optional_text(reason),
    }
    payload["match_signal_digest"] = canonical_digest(payload, purpose="object_truth.mdm.match_signal.v1")
    return payload


def build_anti_match_signal(
    *,
    signal_class: AntiMatchSignalClass,
    left_member_ref: str,
    right_member_ref: str,
    field_name: str,
    evidence_value: Any,
    reason: str,
    blocking: bool | None = None,
    penalty: float | None = None,
) -> dict[str, Any]:
    """Build a negative identity signal that can block or penalize a cluster."""

    signal = _validate_member(signal_class, ANTI_MATCH_SIGNAL_CLASSES, "signal_class")
    resolved_penalty = (
        _bounded_float(penalty, "penalty")
        if penalty is not None
        else ANTI_MATCH_PENALTIES[signal]
    )
    payload = {
        "kind": "object_truth.mdm.anti_match_signal.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "signal_class": signal,
        "left_member_ref": _required_text(left_member_ref, "left_member_ref"),
        "right_member_ref": _required_text(right_member_ref, "right_member_ref"),
        "field_name": _required_text(field_name, "field_name"),
        "evidence_value": canonical_value(evidence_value),
        "reason": _required_text(reason, "reason"),
        "blocking": bool(True if blocking is None else blocking),
        "penalty": resolved_penalty,
    }
    payload["anti_match_signal_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.anti_match_signal.v1",
    )
    return payload


def build_identity_cluster(
    *,
    entity_type: EntityType,
    member_records: list[dict[str, Any]],
    match_signals: list[dict[str, Any]] | None = None,
    anti_match_signals: list[dict[str, Any]] | None = None,
    canonical_candidate: dict[str, Any] | None = None,
    created_at: Any,
    updated_at: Any,
    thresholds: dict[str, float] | None = None,
    cluster_id: str | None = None,
) -> dict[str, Any]:
    """Build an explainable identity cluster without performing a merge."""

    entity = _validate_member(entity_type, ENTITY_TYPES, "entity_type")
    members = sorted(
        (_require_mapping(item, "member_record") for item in member_records),
        key=lambda item: str(item.get("source_record_ref") or item.get("member_digest") or ""),
    )
    if not members:
        raise ObjectTruthOperationError(
            "object_truth.mdm.cluster_members_missing",
            "identity clusters require at least one member record",
        )
    matches = sorted(
        (_require_mapping(item, "match_signal") for item in (match_signals or [])),
        key=lambda item: str(item.get("match_signal_digest") or canonical_digest(item)),
    )
    anti_matches = sorted(
        (_require_mapping(item, "anti_match_signal") for item in (anti_match_signals or [])),
        key=lambda item: str(item.get("anti_match_signal_digest") or canonical_digest(item)),
    )
    resolved_thresholds = _cluster_thresholds(thresholds)
    confidence = _cluster_confidence(matches, anti_matches)
    state = _cluster_state(confidence, anti_matches, resolved_thresholds, member_count=len(members))
    basis = {
        "entity_type": entity,
        "member_refs": [str(item.get("source_record_ref") or item.get("member_digest")) for item in members],
    }
    resolved_cluster_id = (
        _optional_text(cluster_id)
        or f"object_truth_cluster.{entity}.{canonical_digest(basis, purpose='object_truth.mdm.cluster_id.v1')[:16]}"
    )
    payload = {
        "kind": "object_truth.mdm.identity_cluster.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "cluster_id": resolved_cluster_id,
        "entity_type": entity,
        "member_records": [canonical_value(item) for item in members],
        "match_signals": [canonical_value(item) for item in matches],
        "anti_match_signals": [canonical_value(item) for item in anti_matches],
        "cluster_confidence": confidence,
        "review_status": state,
        "cluster_state": state,
        "canonical_candidate": canonical_value(
            canonical_candidate
            if canonical_candidate is not None
            else {"selection_state": "not_selected", "reason": "field_level_selection_required"}
        ),
        "thresholds": resolved_thresholds,
        "created_at": _normalize_required_datetime(created_at, "created_at"),
        "updated_at": _normalize_required_datetime(updated_at, "updated_at"),
    }
    payload["identity_cluster_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.identity_cluster.v1",
    )
    return payload


def build_normalization_rule_record(
    *,
    entity_type: EntityType,
    field_name: str,
    input_pattern: str,
    normalization_steps: list[str],
    output_type: str,
    reversible: bool,
    loss_risk: str,
    exception_policy: str,
    test_examples: list[dict[str, Any]],
    locale_assumptions: list[str] | None = None,
    rule_ref: str | None = None,
) -> dict[str, Any]:
    """Build a catalogable deterministic normalization rule record."""

    steps = _normalized_text_list(normalization_steps, "normalization_steps")
    payload = {
        "kind": "object_truth.mdm.normalization_rule.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "rule_ref": _optional_text(rule_ref),
        "entity_type": _validate_member(entity_type, ENTITY_TYPES, "entity_type"),
        "field_name": _required_text(field_name, "field_name"),
        "input_pattern": _required_text(input_pattern, "input_pattern"),
        "normalization_steps": steps,
        "output_type": _required_text(output_type, "output_type"),
        "reversible": bool(reversible),
        "loss_risk": _required_text(loss_risk, "loss_risk"),
        "exception_policy": _required_text(exception_policy, "exception_policy"),
        "locale_assumptions": _normalized_text_list(locale_assumptions or [], "locale_assumptions"),
        "test_examples": [
            canonical_value(_require_mapping(item, "test_example"))
            for item in test_examples
        ],
    }
    digest = canonical_digest(payload, purpose="object_truth.mdm.normalization_rule.v1")
    payload["rule_ref"] = payload["rule_ref"] or f"object_truth_normalization_rule.{digest[:16]}"
    payload["normalization_rule_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.normalization_rule.v1",
    )
    return payload


def normalize_field_value(
    *,
    entity_type: EntityType,
    field_name: str,
    source_value_raw: Any,
    rule: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize a source value while preserving the raw value and transform chain."""

    entity = _validate_member(entity_type, ENTITY_TYPES, "entity_type")
    field = _required_text(field_name, "field_name")
    raw_value = canonical_value(source_value_raw)
    if _is_empty(raw_value):
        payload = {
            "kind": "object_truth.mdm.normalized_value.v1",
            "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
            "entity_type": entity,
            "field_name": field,
            "source_value_raw": raw_value,
            "source_value_normalized": None,
            "normalization_status": "empty",
            "normalization_rule_ref": _optional_text((rule or {}).get("rule_ref") if isinstance(rule, dict) else None),
            "transform_chain": [],
            "reversible": True,
            "loss_risk": "none",
            "gap_type": None,
        }
        payload["normalized_value_digest"] = canonical_digest(
            None,
            purpose="object_truth.mdm.normalized_value.v1",
        )
        payload["normalization_digest"] = canonical_digest(
            payload,
            purpose="object_truth.mdm.normalization.v1",
        )
        return payload

    resolved_rule = _require_mapping(rule, "rule") if rule is not None else None
    steps = (
        _normalized_text_list(resolved_rule.get("normalization_steps") or [], "normalization_steps")
        if resolved_rule is not None
        else _default_normalization_steps(field)
    )
    current = raw_value
    transform_chain: list[dict[str, Any]] = []
    status = "normalized"
    gap_type = None
    failure_reason = None
    for step in steps:
        before = current
        try:
            current = _apply_normalization_step(current, step)
        except ObjectTruthOperationError as exc:
            current = raw_value
            status = "non_normalizable"
            gap_type = "non-normalizable"
            failure_reason = exc.reason_code
            transform_chain.append(
                _transform_record(
                    step=step,
                    before=before,
                    after=current,
                    lossy=step in LOSSY_NORMALIZATION_STEPS,
                    status="failed",
                    reason_code=exc.reason_code,
                )
            )
            break
        transform_chain.append(
            _transform_record(
                step=step,
                before=before,
                after=current,
                lossy=step in LOSSY_NORMALIZATION_STEPS,
                status="applied",
                reason_code=None,
            )
        )
    normalized_value = canonical_value(current)
    payload = {
        "kind": "object_truth.mdm.normalized_value.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "entity_type": entity,
        "field_name": field,
        "source_value_raw": raw_value,
        "source_value_normalized": normalized_value,
        "normalization_status": status,
        "normalization_rule_ref": _optional_text(resolved_rule.get("rule_ref") if resolved_rule else None),
        "transform_chain": transform_chain,
        "reversible": True,
        "loss_risk": _normalization_loss_risk(transform_chain),
        "gap_type": gap_type,
        "failure_reason": failure_reason,
    }
    payload["normalized_value_digest"] = canonical_digest(
        normalized_value,
        purpose="object_truth.mdm.normalized_value.v1",
    )
    payload["normalization_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.normalization.v1",
    )
    return payload


def build_reversible_source_link(
    *,
    canonical_record_id: str,
    canonical_field: str,
    source_system: str,
    source_record_id: str,
    source_field: str,
    source_value_raw: Any,
    source_value_normalized: Any,
    transform_chain: list[dict[str, Any]],
    selection_reason: str,
    authority_basis: dict[str, Any],
    observed_at: Any,
    loaded_at: Any,
) -> dict[str, Any]:
    """Build a canonical-field-to-source-field link with raw value recovery."""

    payload = {
        "kind": "object_truth.mdm.reversible_source_link.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "canonical_record_id": _required_text(canonical_record_id, "canonical_record_id"),
        "canonical_field": _required_text(canonical_field, "canonical_field"),
        "source_system": _required_text(source_system, "source_system"),
        "source_record_id": _required_text(source_record_id, "source_record_id"),
        "source_field": _required_text(source_field, "source_field"),
        "source_value_raw": canonical_value(source_value_raw),
        "source_value_normalized": canonical_value(source_value_normalized),
        "transform_chain": [
            canonical_value(_require_mapping(item, "transform_chain_item"))
            for item in transform_chain
        ],
        "selection_reason": _required_text(selection_reason, "selection_reason"),
        "authority_basis": canonical_value(_require_mapping(authority_basis, "authority_basis")),
        "observed_at": _normalize_required_datetime(observed_at, "observed_at"),
        "loaded_at": _normalize_required_datetime(loaded_at, "loaded_at"),
    }
    payload["source_value_digest"] = canonical_digest(
        payload["source_value_raw"],
        purpose="object_truth.mdm.source_value_raw.v1",
    )
    payload["source_link_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.reversible_source_link.v1",
    )
    return payload


def score_freshness(
    *,
    observed_at: Any | None,
    loaded_at: Any | None,
    as_of: Any,
    effective_at: Any | None = None,
    source_update_cadence_hours: float | None = None,
    source_declared_latency_hours: float | None = None,
    entity_activity_pattern: str | None = None,
    field_volatility: FieldVolatility = "moderate-change",
    override_reason: str | None = None,
) -> dict[str, Any]:
    """Score freshness independently from source authority."""

    volatility = _validate_member(field_volatility, FIELD_VOLATILITY_CLASSES, "field_volatility")
    as_of_dt = _parse_required_datetime(as_of, "as_of")
    evidence_dates = [
        parsed
        for parsed in (
            _parse_datetime(observed_at),
            _parse_datetime(loaded_at),
            _parse_datetime(effective_at),
        )
        if parsed is not None
    ]
    cadence_hours = _nonnegative_float(source_update_cadence_hours, "source_update_cadence_hours")
    latency_hours = _nonnegative_float(source_declared_latency_hours, "source_declared_latency_hours")
    baseline_hours = VOLATILITY_BASELINE_HOURS[volatility]
    allowed_age_hours = baseline_hours + cadence_hours + latency_hours
    if not evidence_dates:
        score = 0.0
        band: StalenessBand = "unknown"
        evidence_at = None
        age_hours = None
    else:
        evidence_dt = max(evidence_dates)
        evidence_at = _iso_datetime(evidence_dt)
        age_hours = max(0.0, (as_of_dt - evidence_dt).total_seconds() / 3600.0)
        score = round(max(0.0, 1.0 - (age_hours / (allowed_age_hours * 2.0))), 4)
        if age_hours <= allowed_age_hours * 0.25:
            band = "current"
        elif age_hours <= allowed_age_hours:
            band = "aging"
        else:
            band = "stale"
    payload = {
        "kind": "object_truth.mdm.freshness_score.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "freshness_score": score,
        "staleness_band": band,
        "override_reason": _optional_text(override_reason),
        "decay_basis": {
            "as_of": _iso_datetime(as_of_dt),
            "evidence_at": evidence_at,
            "age_hours": round(age_hours, 6) if age_hours is not None else None,
            "field_volatility": volatility,
            "volatility_baseline_hours": baseline_hours,
            "source_update_cadence_hours": cadence_hours,
            "source_declared_latency_hours": latency_hours,
            "allowed_age_hours": allowed_age_hours,
            "entity_activity_pattern": _optional_text(entity_activity_pattern),
        },
    }
    payload["freshness_digest"] = canonical_digest(payload, purpose="object_truth.mdm.freshness_score.v1")
    return payload


def build_source_authority_evidence(
    *,
    entity_type: EntityType,
    field_name: str,
    source_system: str,
    authority_rank: int,
    authority_scope: dict[str, Any],
    authority_reason: str,
    evidence_type: EvidenceType,
    evidence_reference: str,
    approved_by: str,
    approved_at: Any,
    review_interval_days: int,
    collection_mechanism: str | None = None,
    jurisdiction: str | None = None,
    business_domain: str | None = None,
    certification_status: str | None = None,
) -> dict[str, Any]:
    """Build field-aware source authority evidence."""

    approved_dt = _parse_required_datetime(approved_at, "approved_at")
    interval = _positive_int(review_interval_days, "review_interval_days")
    payload = {
        "kind": "object_truth.mdm.source_authority_evidence.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "entity_type": _validate_member(entity_type, ENTITY_TYPES, "entity_type"),
        "field_name": _required_text(field_name, "field_name"),
        "source_system": _required_text(source_system, "source_system"),
        "authority_rank": _positive_int(authority_rank, "authority_rank"),
        "authority_scope": canonical_value(_require_mapping(authority_scope, "authority_scope")),
        "authority_reason": _required_text(authority_reason, "authority_reason"),
        "evidence_type": _validate_member(evidence_type, EVIDENCE_TYPES, "evidence_type"),
        "evidence_reference": _required_text(evidence_reference, "evidence_reference"),
        "approved_by": _required_text(approved_by, "approved_by"),
        "approved_at": _iso_datetime(approved_dt),
        "review_interval_days": interval,
        "review_due_at": _iso_datetime(approved_dt + timedelta(days=interval)),
        "collection_mechanism": _optional_text(collection_mechanism),
        "jurisdiction": _optional_text(jurisdiction),
        "business_domain": _optional_text(business_domain),
        "certification_status": _optional_text(certification_status),
    }
    payload["authority_evidence_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.source_authority_evidence.v1",
    )
    return payload


def build_field_value_candidate(
    *,
    entity_type: EntityType,
    field_name: str,
    source_system: str,
    source_record_id: str,
    source_value_raw: Any,
    observed_at: Any,
    loaded_at: Any,
    as_of: Any,
    source_field: str | None = None,
    effective_at: Any | None = None,
    source_update_cadence_hours: float | None = None,
    source_declared_latency_hours: float | None = None,
    entity_activity_pattern: str | None = None,
    field_volatility: FieldVolatility = "moderate-change",
    normalization_rule: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one field candidate from one source system."""

    field = _required_text(field_name, "field_name")
    normalized = normalize_field_value(
        entity_type=entity_type,
        field_name=field,
        source_value_raw=source_value_raw,
        rule=normalization_rule,
    )
    freshness = score_freshness(
        observed_at=observed_at,
        effective_at=effective_at,
        loaded_at=loaded_at,
        as_of=as_of,
        source_update_cadence_hours=source_update_cadence_hours,
        source_declared_latency_hours=source_declared_latency_hours,
        entity_activity_pattern=entity_activity_pattern,
        field_volatility=field_volatility,
    )
    payload = {
        "kind": "object_truth.mdm.field_value_candidate.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "entity_type": _validate_member(entity_type, ENTITY_TYPES, "entity_type"),
        "field_name": field,
        "source_system": _required_text(source_system, "source_system"),
        "source_record_id": _required_text(source_record_id, "source_record_id"),
        "source_field": _optional_text(source_field) or field,
        "source_record_ref": f"{_required_text(source_system, 'source_system')}:{_required_text(source_record_id, 'source_record_id')}",
        "source_value_raw": normalized["source_value_raw"],
        "source_value_normalized": normalized["source_value_normalized"],
        "normalized_value_digest": normalized["normalized_value_digest"],
        "normalization_status": normalized["normalization_status"],
        "normalization_digest": normalized["normalization_digest"],
        "transform_chain": normalized["transform_chain"],
        "presence": "empty" if _is_empty(source_value_raw) else "present",
        "observed_at": _normalize_required_datetime(observed_at, "observed_at"),
        "effective_at": _normalize_optional_datetime(effective_at),
        "loaded_at": _normalize_required_datetime(loaded_at, "loaded_at"),
        "freshness": freshness,
    }
    payload["candidate_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.field_value_candidate.v1",
    )
    return payload


def compare_field_candidates(
    *,
    entity_type: EntityType,
    canonical_record_id: str,
    canonical_field: str,
    candidates: list[dict[str, Any]],
    authority_evidence: list[dict[str, Any]],
    as_of: Any,
    cluster_id: str | None = None,
    required: bool = True,
    owner_role: str = "data_steward",
) -> dict[str, Any]:
    """Compare source field candidates and explain canonical selection."""

    entity = _validate_member(entity_type, ENTITY_TYPES, "entity_type")
    field = _required_text(canonical_field, "canonical_field")
    as_of_text = _normalize_required_datetime(as_of, "as_of")
    normalized_candidates = sorted(
        (_coerce_field_candidate(item, entity_type=entity, field_name=field, as_of=as_of) for item in candidates),
        key=lambda item: str(item.get("candidate_digest") or ""),
    )
    authority_records = sorted(
        (_require_mapping(item, "authority_evidence") for item in authority_evidence),
        key=lambda item: (
            int(item.get("authority_rank") or 999999),
            str(item.get("source_system") or ""),
            str(item.get("authority_evidence_digest") or ""),
        ),
    )
    matching_authority = [
        item
        for item in authority_records
        if item.get("entity_type") == entity and item.get("field_name") == field
    ]
    annotated_candidates = [
        _candidate_with_authority(candidate, matching_authority)
        for candidate in normalized_candidates
    ]
    gaps: list[dict[str, Any]] = []
    for candidate in annotated_candidates:
        if candidate.get("normalization_status") == "non_normalizable":
            gaps.append(
                _comparison_gap(
                    entity_type=entity,
                    canonical_record_id=canonical_record_id,
                    cluster_id=cluster_id,
                    field_name=field,
                    gap_type="non-normalizable",
                    severity="medium",
                    detected_at=as_of_text,
                    owner_role=owner_role,
                    remediation_path="review normalization rule or preserve raw value",
                    closure_condition="candidate value normalizes safely or field is explicitly exempted",
                    evidence_refs=[str(candidate.get("candidate_digest"))],
                )
            )
    present_candidates = [
        item
        for item in annotated_candidates
        if item.get("presence") == "present" and item.get("normalization_status") != "non_normalizable"
    ]
    groups = _equivalence_groups(present_candidates)
    selected: dict[str, Any] | None = None
    selected_link: dict[str, Any] | None = None
    selection_state = "unresolved"
    selection_reason = "no_present_candidates"

    if not present_candidates:
        if required:
            gaps.append(
                _comparison_gap(
                    entity_type=entity,
                    canonical_record_id=canonical_record_id,
                    cluster_id=cluster_id,
                    field_name=field,
                    gap_type="missing-required",
                    severity="high",
                    detected_at=as_of_text,
                    owner_role=owner_role,
                    remediation_path="collect the required field from an authoritative source",
                    closure_condition="at least one authoritative non-empty candidate is observed",
                    evidence_refs=[],
                )
            )
    elif not matching_authority:
        selection_reason = "source_authority_policy_missing"
        gaps.append(
            _comparison_gap(
                entity_type=entity,
                canonical_record_id=canonical_record_id,
                cluster_id=cluster_id,
                field_name=field,
                gap_type="policy-missing",
                severity="high",
                detected_at=as_of_text,
                owner_role=owner_role,
                remediation_path="record source authority evidence for this entity field",
                closure_condition="field-level source authority evidence exists and is reviewable",
                evidence_refs=[str(item.get("candidate_digest")) for item in present_candidates],
            )
        )
    else:
        top_authority_rank = min(int(item["authority_rank"]) for item in matching_authority)
        top_authority_sources = {
            str(item["source_system"])
            for item in matching_authority
            if int(item["authority_rank"]) == top_authority_rank
        }
        top_candidate_pool = [
            item
            for item in present_candidates
            if item.get("source_system") in top_authority_sources
        ]
        if required and not top_candidate_pool:
            selection_reason = "missing_top_authoritative_source"
            gaps.append(
                _comparison_gap(
                    entity_type=entity,
                    canonical_record_id=canonical_record_id,
                    cluster_id=cluster_id,
                    field_name=field,
                    gap_type="missing-authoritative",
                    severity="high",
                    detected_at=as_of_text,
                    owner_role=owner_role,
                    remediation_path="collect the field from the highest-ranked source",
                    closure_condition="highest-ranked source provides a non-empty candidate",
                    evidence_refs=[
                        str(item.get("authority_evidence_digest"))
                        for item in matching_authority
                        if int(item["authority_rank"]) == top_authority_rank
                    ],
                )
            )
        elif len({str(item["normalized_value_digest"]) for item in top_candidate_pool}) > 1:
            selection_reason = "top_authority_conflict"
            gaps.append(
                _comparison_gap(
                    entity_type=entity,
                    canonical_record_id=canonical_record_id,
                    cluster_id=cluster_id,
                    field_name=field,
                    gap_type="conflicting-values",
                    severity="critical",
                    detected_at=as_of_text,
                    owner_role=owner_role,
                    remediation_path="manually adjudicate competing authoritative values",
                    closure_condition="one authoritative value is selected or source records are corrected",
                    evidence_refs=[str(item.get("candidate_digest")) for item in top_candidate_pool],
                )
            )
            gaps.append(
                _comparison_gap(
                    entity_type=entity,
                    canonical_record_id=canonical_record_id,
                    cluster_id=cluster_id,
                    field_name=field,
                    gap_type="manual-review-pending",
                    severity="high",
                    detected_at=as_of_text,
                    owner_role=owner_role,
                    remediation_path="route field conflict to data steward review",
                    closure_condition="review decision records selected canonical value and rejected candidates",
                    evidence_refs=[str(item.get("candidate_digest")) for item in top_candidate_pool],
                )
            )
        else:
            selected = sorted(top_candidate_pool, key=_candidate_selection_key)[0]
            selection_state = "selected"
            selection_reason = "top_field_authority"
            stale_tradeoff = _stale_authority_tradeoff(selected, present_candidates)
            if stale_tradeoff:
                selection_reason = "top_field_authority_with_stale_tradeoff"
                gaps.append(
                    _comparison_gap(
                        entity_type=entity,
                        canonical_record_id=canonical_record_id,
                        cluster_id=cluster_id,
                        field_name=field,
                        gap_type="stale-value",
                        severity="medium",
                        detected_at=as_of_text,
                        owner_role=owner_role,
                        remediation_path="refresh authoritative source or record explicit override",
                        closure_condition="authoritative value is refreshed or lower-authority fresh value is adjudicated",
                        evidence_refs=[
                            str(selected.get("candidate_digest")),
                            *[str(item.get("candidate_digest")) for item in stale_tradeoff],
                        ],
                    )
                )
            selected_link = build_reversible_source_link(
                canonical_record_id=canonical_record_id,
                canonical_field=field,
                source_system=str(selected["source_system"]),
                source_record_id=str(selected["source_record_id"]),
                source_field=str(selected["source_field"]),
                source_value_raw=selected["source_value_raw"],
                source_value_normalized=selected["source_value_normalized"],
                transform_chain=list(selected["transform_chain"]),
                selection_reason=selection_reason,
                authority_basis={
                    "authority_rank": selected.get("authority_rank"),
                    "authority_evidence_digest": selected.get("authority_evidence_digest"),
                    "freshness_score": selected.get("freshness", {}).get("freshness_score"),
                    "staleness_band": selected.get("freshness", {}).get("staleness_band"),
                },
                observed_at=selected["observed_at"],
                loaded_at=selected["loaded_at"],
            )

    conflict_flag = len(groups) > 1
    consensus_flag = len(groups) == 1 and bool(present_candidates)
    rejected = [
        _rejected_candidate(candidate, selected=selected, selection_reason=selection_reason)
        for candidate in present_candidates
        if selected is None or candidate.get("candidate_digest") != selected.get("candidate_digest")
    ]
    all_gaps = sorted(gaps, key=lambda item: str(item.get("gap_id")))
    payload = {
        "kind": "object_truth.mdm.field_comparison.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "entity_type": entity,
        "canonical_record_id": _required_text(canonical_record_id, "canonical_record_id"),
        "cluster_id": _optional_text(cluster_id),
        "canonical_field": field,
        "as_of": as_of_text,
        "candidate_count": len(normalized_candidates),
        "present_candidate_count": len(present_candidates),
        "candidates": [canonical_value(item) for item in annotated_candidates],
        "normalized_equivalence_groups": groups,
        "authority_rank_by_source": _authority_rank_by_source(annotated_candidates),
        "freshness_rank_by_source": _freshness_rank_by_source(annotated_candidates),
        "conflict_flag": conflict_flag,
        "consensus_flag": consensus_flag,
        "selection_state": selection_state,
        "selection_reason": selection_reason,
        "selected_canonical_value": (
            canonical_value(selected.get("source_value_normalized"))
            if selected is not None
            else None
        ),
        "selected_candidate_digest": selected.get("candidate_digest") if selected is not None else None,
        "selected_source_link": selected_link,
        "rejected_candidate_values": rejected,
        "typed_gaps": all_gaps,
    }
    payload["field_comparison_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.field_comparison.v1",
    )
    return payload


def build_typed_gap(
    *,
    entity_type: EntityType,
    field_name: str,
    gap_type: GapType,
    severity: GapSeverity,
    detected_at: Any,
    owner_role: str,
    remediation_path: str,
    closure_condition: str,
    canonical_record_id: str | None = None,
    cluster_id: str | None = None,
    evidence_refs: list[str] | None = None,
    gap_id: str | None = None,
) -> dict[str, Any]:
    """Build an actionable typed MDM gap."""

    canonical_id = _optional_text(canonical_record_id)
    resolved_cluster_id = _optional_text(cluster_id)
    if not canonical_id and not resolved_cluster_id:
        raise ObjectTruthOperationError(
            "object_truth.mdm.gap_target_missing",
            "typed gaps require canonical_record_id or cluster_id",
        )
    evidence = _normalized_text_list(evidence_refs or [], "evidence_refs")
    basis = {
        "entity_type": entity_type,
        "field_name": field_name,
        "gap_type": gap_type,
        "canonical_record_id": canonical_id,
        "cluster_id": resolved_cluster_id,
        "evidence_refs": evidence,
    }
    resolved_gap_id = (
        _optional_text(gap_id)
        or f"object_truth_gap.{canonical_digest(basis, purpose='object_truth.mdm.gap_id.v1')[:16]}"
    )
    payload = {
        "kind": "object_truth.mdm.typed_gap.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "gap_id": resolved_gap_id,
        "entity_type": _validate_member(entity_type, ENTITY_TYPES, "entity_type"),
        "canonical_record_id": canonical_id,
        "cluster_id": resolved_cluster_id,
        "field_name": _required_text(field_name, "field_name"),
        "gap_type": _validate_member(gap_type, GAP_TYPES, "gap_type"),
        "severity": _validate_member(severity, GAP_SEVERITIES, "severity"),
        "detected_at": _normalize_required_datetime(detected_at, "detected_at"),
        "owner_role": _required_text(owner_role, "owner_role"),
        "remediation_path": _required_text(remediation_path, "remediation_path"),
        "closure_condition": _required_text(closure_condition, "closure_condition"),
        "evidence_refs": evidence,
    }
    payload["gap_digest"] = canonical_digest(payload, purpose="object_truth.mdm.typed_gap.v1")
    return payload


def build_hierarchy_signal(
    *,
    entity_type: EntityType,
    signal_type: HierarchySignalType,
    source_system: str,
    source_record_id: str,
    observed_at: Any,
    child_record_id: str | None = None,
    parent_record_id: str | None = None,
    ultimate_parent_record_id: str | None = None,
    hierarchy_depth: int | None = None,
    authoritative: bool = False,
    rollup_eligible: bool | None = None,
    alternate_parent_candidates: list[str] | None = None,
    flattening_logic: str | None = None,
    flattening_authority: str | None = None,
    information_lost: list[str] | None = None,
    evidence_reference: str | None = None,
) -> dict[str, Any]:
    """Build hierarchy or flattening evidence without changing identity."""

    payload = {
        "kind": "object_truth.mdm.hierarchy_signal.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "entity_type": _validate_member(entity_type, ENTITY_TYPES, "entity_type"),
        "signal_type": _validate_member(signal_type, HIERARCHY_SIGNAL_TYPES, "signal_type"),
        "source_system": _required_text(source_system, "source_system"),
        "source_record_id": _required_text(source_record_id, "source_record_id"),
        "child_record_id": _optional_text(child_record_id),
        "parent_record_id": _optional_text(parent_record_id),
        "ultimate_parent_record_id": _optional_text(ultimate_parent_record_id),
        "hierarchy_depth": _optional_nonnegative_int(hierarchy_depth, "hierarchy_depth"),
        "authoritative": bool(authoritative),
        "rollup_eligible": bool(rollup_eligible) if rollup_eligible is not None else None,
        "alternate_parent_candidates": _normalized_text_list(
            alternate_parent_candidates or [],
            "alternate_parent_candidates",
        ),
        "flattening_logic": _optional_text(flattening_logic),
        "flattening_authority": _optional_text(flattening_authority),
        "information_lost": _normalized_text_list(information_lost or [], "information_lost"),
        "evidence_reference": _optional_text(evidence_reference),
        "observed_at": _normalize_required_datetime(observed_at, "observed_at"),
    }
    payload["hierarchy_signal_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.hierarchy_signal.v1",
    )
    return payload


def build_mdm_resolution_packet(
    *,
    client_ref: str,
    entity_type: EntityType,
    as_of: Any,
    identity_clusters: list[dict[str, Any]],
    field_comparisons: list[dict[str, Any]],
    normalization_rules: list[dict[str, Any]] | None = None,
    authority_evidence: list[dict[str, Any]] | None = None,
    hierarchy_signals: list[dict[str, Any]] | None = None,
    typed_gaps: list[dict[str, Any]] | None = None,
    packet_ref: str | None = None,
) -> dict[str, Any]:
    """Build a stable packet that can be persisted later by CQRS/storage work."""

    entity = _validate_member(entity_type, ENTITY_TYPES, "entity_type")
    clusters = _sort_records(identity_clusters, "identity_cluster_digest")
    comparisons = _sort_records(field_comparisons, "field_comparison_digest")
    rules = _sort_records(normalization_rules or [], "normalization_rule_digest")
    authority = _sort_records(authority_evidence or [], "authority_evidence_digest")
    hierarchy = _sort_records(hierarchy_signals or [], "hierarchy_signal_digest")
    gaps = _sort_records(typed_gaps or [], "gap_digest")
    basis = {
        "client_ref": client_ref,
        "entity_type": entity,
        "identity_clusters": [item.get("identity_cluster_digest") for item in clusters],
        "field_comparisons": [item.get("field_comparison_digest") for item in comparisons],
        "typed_gaps": [item.get("gap_digest") for item in gaps],
    }
    payload = {
        "kind": "object_truth.mdm.resolution_packet.v1",
        "schema_version": OBJECT_TRUTH_MDM_SCHEMA_VERSION,
        "packet_ref": _optional_text(packet_ref)
        or f"object_truth_mdm_packet.{canonical_digest(basis, purpose='object_truth.mdm.packet_ref.v1')[:16]}",
        "client_ref": _required_text(client_ref, "client_ref"),
        "entity_type": entity,
        "as_of": _normalize_required_datetime(as_of, "as_of"),
        "identity_clusters": clusters,
        "field_comparisons": comparisons,
        "normalization_rules": rules,
        "authority_evidence": authority,
        "hierarchy_signals": hierarchy,
        "typed_gaps": gaps,
    }
    payload["resolution_packet_digest"] = canonical_digest(
        payload,
        purpose="object_truth.mdm.resolution_packet.v1",
    )
    return payload


def stable_mdm_digest(value: Any, *, purpose: str = "object_truth.mdm") -> str:
    """Expose the stable digest contract for Phase 3 artifacts."""

    return canonical_digest(value, purpose=purpose)


def _cluster_thresholds(thresholds: dict[str, float] | None) -> dict[str, float]:
    raw = {**DEFAULT_CLUSTER_THRESHOLDS, **(thresholds or {})}
    auto_accept = _bounded_float(raw["auto_accept"], "auto_accept")
    manual_review = _bounded_float(raw["manual_review"], "manual_review")
    auto_reject = _bounded_float(raw["auto_reject"], "auto_reject")
    if not auto_accept >= manual_review >= auto_reject:
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_cluster_thresholds",
            "cluster thresholds must satisfy auto_accept >= manual_review >= auto_reject",
            details=raw,
        )
    return {
        "auto_accept": auto_accept,
        "manual_review": manual_review,
        "auto_reject": auto_reject,
    }


def _cluster_confidence(match_signals: list[dict[str, Any]], anti_match_signals: list[dict[str, Any]]) -> float:
    positive = sum(float(item.get("signal_score") or 0.0) for item in match_signals)
    penalty = sum(
        float(item.get("penalty") or 0.0)
        for item in anti_match_signals
        if not bool(item.get("blocking", False))
    )
    return round(max(0.0, min(1.0, positive) - min(1.0, penalty)), 6)


def _cluster_state(
    confidence: float,
    anti_match_signals: list[dict[str, Any]],
    thresholds: dict[str, float],
    *,
    member_count: int,
) -> ClusterState:
    if any(bool(item.get("blocking", False)) for item in anti_match_signals):
        return "split-required" if member_count > 1 else "rejected"
    if confidence >= thresholds["auto_accept"]:
        return "auto-accepted"
    if confidence >= thresholds["manual_review"]:
        return "review-required"
    if confidence <= thresholds["auto_reject"]:
        return "rejected"
    return "proposed"


def _default_normalization_steps(field_name: str) -> list[str]:
    normalized = field_name.strip().casefold()
    if "email" in normalized:
        return ["strip", "lowercase"]
    if "phone" in normalized or "mobile" in normalized:
        return ["strip", "digits_only", "normalize_us_phone"]
    if normalized in {"tax_id", "ein"} or normalized.endswith("_tax_id"):
        return ["strip", "uppercase", "alnum_only"]
    if "status" in normalized:
        return ["strip", "casefold", "status_synonym"]
    if normalized.endswith("_at") or "date" in normalized:
        return ["iso_datetime"]
    if "name" in normalized or normalized in {"legal_entity", "company"}:
        return ["strip", "collapse_whitespace", "remove_punctuation", "casefold", "remove_legal_suffix"]
    if "address" in normalized or normalized in {"city", "state", "country"}:
        return ["strip", "collapse_whitespace", "casefold"]
    if normalized.endswith("_id") or normalized == "id":
        return ["strip"]
    return ["strip", "collapse_whitespace"]


def _apply_normalization_step(value: Any, step: str) -> Any:
    if step == "canonicalize_json":
        return canonical_value(value)
    if step == "strip":
        return value.strip() if isinstance(value, str) else value
    if step == "collapse_whitespace":
        return re.sub(r"\s+", " ", value).strip() if isinstance(value, str) else value
    if step == "casefold":
        return value.casefold() if isinstance(value, str) else value
    if step == "lowercase":
        return value.lower() if isinstance(value, str) else value
    if step == "uppercase":
        return value.upper() if isinstance(value, str) else value
    if step == "remove_punctuation":
        return re.sub(r"[^\w\s]", " ", value) if isinstance(value, str) else value
    if step == "alnum_only":
        return re.sub(r"[^0-9A-Za-z]", "", value) if isinstance(value, str) else value
    if step == "digits_only":
        return re.sub(r"\D", "", value) if isinstance(value, str) else value
    if step == "normalize_us_phone":
        return _normalize_us_phone(value)
    if step == "remove_legal_suffix":
        return _remove_legal_suffix(value)
    if step == "iso_datetime":
        parsed = _parse_datetime(value)
        if parsed is None:
            raise ObjectTruthOperationError(
                "object_truth.mdm.datetime_not_normalizable",
                "value cannot be normalized as an ISO datetime",
                details={"value_type": type(value).__name__},
            )
        return _iso_datetime(parsed)
    if step == "status_synonym":
        return _status_synonym(value)
    raise ObjectTruthOperationError(
        "object_truth.mdm.unknown_normalization_step",
        "normalization step is not supported",
        details={"step": step},
    )


def _normalize_us_phone(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    if len(text) == 10:
        return f"+1{text}"
    if len(text) == 11 and text.startswith("1"):
        return f"+{text}"
    raise ObjectTruthOperationError(
        "object_truth.mdm.phone_not_normalizable",
        "phone values require 10 US digits or 11 digits starting with 1",
        details={"digit_count": len(text)},
    )


def _remove_legal_suffix(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    parts = value.split()
    while parts and parts[-1].casefold() in LEGAL_SUFFIXES:
        parts.pop()
    return " ".join(parts)


def _status_synonym(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    normalized = value.strip().casefold()
    if normalized in {"active", "enabled", "current", "open"}:
        return "active"
    if normalized in {"inactive", "disabled", "closed", "deactivated"}:
        return "inactive"
    if normalized in {"pending", "proposed", "draft"}:
        return "pending"
    return normalized


def _transform_record(
    *,
    step: str,
    before: Any,
    after: Any,
    lossy: bool,
    status: str,
    reason_code: str | None,
) -> dict[str, Any]:
    return {
        "step": step,
        "before_digest": canonical_digest(before, purpose="object_truth.mdm.transform.before.v1"),
        "after_digest": canonical_digest(after, purpose="object_truth.mdm.transform.after.v1"),
        "changed": canonical_value(before) != canonical_value(after),
        "lossy": bool(lossy),
        "status": status,
        "reason_code": reason_code,
    }


def _normalization_loss_risk(transform_chain: list[dict[str, Any]]) -> str:
    if any(item.get("status") == "failed" for item in transform_chain):
        return "review-required"
    if any(bool(item.get("lossy")) and bool(item.get("changed")) for item in transform_chain):
        return "lossy-source-preserved"
    return "none"


def _coerce_field_candidate(
    item: dict[str, Any],
    *,
    entity_type: EntityType,
    field_name: str,
    as_of: Any,
) -> dict[str, Any]:
    candidate = _require_mapping(item, "candidate")
    if candidate.get("kind") == "object_truth.mdm.field_value_candidate.v1":
        return canonical_value(candidate)
    return build_field_value_candidate(
        entity_type=entity_type,
        field_name=str(candidate.get("field_name") or field_name),
        source_system=str(candidate.get("source_system") or ""),
        source_record_id=str(candidate.get("source_record_id") or ""),
        source_field=_optional_text(candidate.get("source_field")),
        source_value_raw=candidate.get("source_value_raw"),
        observed_at=candidate.get("observed_at"),
        effective_at=candidate.get("effective_at"),
        loaded_at=candidate.get("loaded_at"),
        as_of=as_of,
        source_update_cadence_hours=candidate.get("source_update_cadence_hours"),
        source_declared_latency_hours=candidate.get("source_declared_latency_hours"),
        entity_activity_pattern=_optional_text(candidate.get("entity_activity_pattern")),
        field_volatility=candidate.get("field_volatility", "moderate-change"),
        normalization_rule=candidate.get("normalization_rule") if isinstance(candidate.get("normalization_rule"), dict) else None,
    )


def _candidate_with_authority(candidate: dict[str, Any], authority_records: list[dict[str, Any]]) -> dict[str, Any]:
    source_system = str(candidate.get("source_system") or "")
    matches = [
        item
        for item in authority_records
        if str(item.get("source_system") or "") == source_system
    ]
    resolved = dict(candidate)
    if not matches:
        resolved.update(
            {
                "authority_rank": None,
                "authority_evidence_digest": None,
                "authority_reason": None,
                "authority_scope": None,
            }
        )
        return resolved
    best = sorted(
        matches,
        key=lambda item: (
            int(item.get("authority_rank") or 999999),
            str(item.get("authority_evidence_digest") or ""),
        ),
    )[0]
    resolved.update(
        {
            "authority_rank": int(best["authority_rank"]),
            "authority_evidence_digest": best.get("authority_evidence_digest"),
            "authority_reason": best.get("authority_reason"),
            "authority_scope": canonical_value(best.get("authority_scope") or {}),
        }
    )
    return resolved


def _equivalence_groups(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        groups.setdefault(str(candidate["normalized_value_digest"]), []).append(candidate)
    result: list[dict[str, Any]] = []
    for digest, items in sorted(groups.items()):
        sorted_items = sorted(items, key=lambda item: str(item.get("candidate_digest") or ""))
        result.append(
            {
                "normalized_value_digest": digest,
                "representative_normalized_value": canonical_value(sorted_items[0].get("source_value_normalized")),
                "source_count": len({str(item.get("source_system")) for item in sorted_items}),
                "candidate_count": len(sorted_items),
                "sources": sorted({str(item.get("source_system")) for item in sorted_items}),
                "candidate_digests": [str(item.get("candidate_digest")) for item in sorted_items],
            }
        )
    return result


def _candidate_selection_key(candidate: dict[str, Any]) -> tuple[float, str, str, str]:
    freshness = candidate.get("freshness") if isinstance(candidate.get("freshness"), dict) else {}
    return (
        -float(freshness.get("freshness_score") or 0.0),
        str(candidate.get("source_system") or ""),
        str(candidate.get("source_record_id") or ""),
        str(candidate.get("candidate_digest") or ""),
    )


def _stale_authority_tradeoff(selected: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    freshness = selected.get("freshness") if isinstance(selected.get("freshness"), dict) else {}
    if freshness.get("staleness_band") != "stale":
        return []
    selected_rank = selected.get("authority_rank")
    selected_score = float(freshness.get("freshness_score") or 0.0)
    selected_digest = str(selected.get("normalized_value_digest") or "")
    conflicts: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_freshness = candidate.get("freshness") if isinstance(candidate.get("freshness"), dict) else {}
        candidate_rank = candidate.get("authority_rank")
        if str(candidate.get("normalized_value_digest") or "") == selected_digest:
            continue
        if candidate_rank is None or selected_rank is None or int(candidate_rank) <= int(selected_rank):
            continue
        if float(candidate_freshness.get("freshness_score") or 0.0) > selected_score:
            conflicts.append(candidate)
    return sorted(conflicts, key=lambda item: str(item.get("candidate_digest") or ""))


def _authority_rank_by_source(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        source = str(candidate.get("source_system") or "")
        current = rows.get(source)
        rank = candidate.get("authority_rank")
        if current is None or (rank is not None and (current.get("authority_rank") is None or rank < current["authority_rank"])):
            rows[source] = {
                "source_system": source,
                "authority_rank": rank,
                "authority_evidence_digest": candidate.get("authority_evidence_digest"),
                "authority_reason": candidate.get("authority_reason"),
            }
    return [rows[key] for key in sorted(rows)]


def _freshness_rank_by_source(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for candidate in candidates:
        freshness = candidate.get("freshness") if isinstance(candidate.get("freshness"), dict) else {}
        rows.append(
            {
                "source_system": candidate.get("source_system"),
                "source_record_id": candidate.get("source_record_id"),
                "candidate_digest": candidate.get("candidate_digest"),
                "freshness_score": freshness.get("freshness_score"),
                "staleness_band": freshness.get("staleness_band"),
                "evidence_at": (freshness.get("decay_basis") or {}).get("evidence_at")
                if isinstance(freshness.get("decay_basis"), dict)
                else None,
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            -float(item.get("freshness_score") or 0.0),
            str(item.get("source_system") or ""),
            str(item.get("source_record_id") or ""),
        ),
    )


def _rejected_candidate(
    candidate: dict[str, Any],
    *,
    selected: dict[str, Any] | None,
    selection_reason: str,
) -> dict[str, Any]:
    if selected is None:
        reason = selection_reason
    elif candidate.get("normalized_value_digest") == selected.get("normalized_value_digest"):
        reason = "normalized_duplicate"
    elif candidate.get("authority_rank") is None:
        reason = "no_source_authority_evidence"
    elif selected.get("authority_rank") is not None and int(candidate["authority_rank"]) > int(selected["authority_rank"]):
        reason = "lower_field_authority"
    else:
        reason = "not_selected"
    return {
        "candidate_digest": candidate.get("candidate_digest"),
        "source_system": candidate.get("source_system"),
        "source_record_id": candidate.get("source_record_id"),
        "source_value_raw": canonical_value(candidate.get("source_value_raw")),
        "source_value_normalized": canonical_value(candidate.get("source_value_normalized")),
        "normalized_value_digest": candidate.get("normalized_value_digest"),
        "rejection_reason": reason,
    }


def _comparison_gap(
    *,
    entity_type: EntityType,
    canonical_record_id: str,
    cluster_id: str | None,
    field_name: str,
    gap_type: GapType,
    severity: GapSeverity,
    detected_at: Any,
    owner_role: str,
    remediation_path: str,
    closure_condition: str,
    evidence_refs: list[str],
) -> dict[str, Any]:
    return build_typed_gap(
        entity_type=entity_type,
        field_name=field_name,
        gap_type=gap_type,
        severity=severity,
        detected_at=detected_at,
        owner_role=owner_role,
        remediation_path=remediation_path,
        closure_condition=closure_condition,
        canonical_record_id=canonical_record_id,
        cluster_id=cluster_id,
        evidence_refs=evidence_refs,
    )


def _sort_records(records: list[dict[str, Any]], digest_field: str) -> list[dict[str, Any]]:
    return [
        canonical_value(_require_mapping(item, "record"))
        for item in sorted(
            records,
            key=lambda item: str(item.get(digest_field) or canonical_digest(item, purpose=digest_field)),
        )
    ]


def _validate_member(value: Any, allowed: set[str], field_name: str) -> Any:
    text = _required_text(value, field_name)
    if text not in allowed:
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_enum_value",
            f"{field_name} is not supported",
            details={"field_name": field_name, "value": text, "allowed": sorted(allowed)},
        )
    return text


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ObjectTruthOperationError(
            "object_truth.mdm.mapping_required",
            f"{field_name} must be an object",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalized_text_list(values: list[str] | tuple[str, ...], field_name: str) -> list[str]:
    if not isinstance(values, (list, tuple)):
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_string_list",
            f"{field_name} must be a list of strings",
            details={"field_name": field_name, "value_type": type(values).__name__},
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def _required_text(value: Any, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ObjectTruthOperationError(
            "object_truth.mdm.required_text_missing",
            f"{field_name} is required",
            details={"field_name": field_name},
        )
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _bounded_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_float",
            f"{field_name} must be a number between 0 and 1",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    resolved = float(value)
    if resolved < 0.0 or resolved > 1.0:
        raise ObjectTruthOperationError(
            "object_truth.mdm.float_out_of_range",
            f"{field_name} must be between 0 and 1",
            details={"field_name": field_name, "value": resolved},
        )
    return round(resolved, 6)


def _nonnegative_float(value: Any, field_name: str) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_number",
            f"{field_name} must be a non-negative number",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    resolved = float(value)
    if resolved < 0:
        raise ObjectTruthOperationError(
            "object_truth.mdm.negative_number",
            f"{field_name} must be non-negative",
            details={"field_name": field_name, "value": resolved},
        )
    return resolved


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_integer",
            f"{field_name} must be a positive integer",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    resolved = int(value)
    if resolved <= 0:
        raise ObjectTruthOperationError(
            "object_truth.mdm.integer_not_positive",
            f"{field_name} must be positive",
            details={"field_name": field_name, "value": resolved},
        )
    return resolved


def _optional_nonnegative_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ObjectTruthOperationError(
            "object_truth.mdm.invalid_integer",
            f"{field_name} must be a non-negative integer",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    resolved = int(value)
    if resolved < 0:
        raise ObjectTruthOperationError(
            "object_truth.mdm.integer_negative",
            f"{field_name} must be non-negative",
            details={"field_name": field_name, "value": resolved},
        )
    return resolved


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_required_datetime(value: Any, field_name: str) -> datetime:
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ObjectTruthOperationError(
            "object_truth.mdm.datetime_required",
            f"{field_name} must be an ISO datetime",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    return parsed


def _normalize_required_datetime(value: Any, field_name: str) -> str:
    return _iso_datetime(_parse_required_datetime(value, field_name))


def _normalize_optional_datetime(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return _iso_datetime(parsed) if parsed is not None else None


def _iso_datetime(value: datetime) -> str:
    parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "ANTI_MATCH_SIGNAL_CLASSES",
    "CLUSTER_STATES",
    "DEFAULT_CLUSTER_THRESHOLDS",
    "ENTITY_TYPES",
    "EVIDENCE_TYPES",
    "FIELD_VOLATILITY_CLASSES",
    "GAP_SEVERITIES",
    "GAP_TYPES",
    "HIERARCHY_SIGNAL_TYPES",
    "MATCH_SIGNAL_CLASSES",
    "OBJECT_TRUTH_MDM_SCHEMA_VERSION",
    "build_anti_match_signal",
    "build_cluster_member",
    "build_field_value_candidate",
    "build_hierarchy_signal",
    "build_identity_cluster",
    "build_match_signal",
    "build_mdm_resolution_packet",
    "build_normalization_rule_record",
    "build_reversible_source_link",
    "build_source_authority_evidence",
    "build_typed_gap",
    "compare_field_candidates",
    "normalize_field_value",
    "score_freshness",
    "stable_mdm_digest",
]
