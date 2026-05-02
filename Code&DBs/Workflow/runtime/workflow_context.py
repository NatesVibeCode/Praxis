"""Workflow Context authority domain primitives.

This module keeps Workflow authoring independent from live client systems.
It compiles intent into explicit inferred context, can build deterministic
synthetic worlds, and evaluates truth-state guardrails before anything is
treated as deployable proof.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import json
import re
from typing import Any


CONTEXT_MODES = {"standalone", "inferred", "synthetic", "bound", "hybrid"}
IO_MODES = {"none", "inferred", "synthetic", "bound", "runtime_generated", "hybrid"}
TRUTH_STATES = {
    "none",
    "inferred",
    "synthetic",
    "documented",
    "anonymized_operational",
    "schema_bound",
    "observed",
    "verified",
    "promoted",
    "stale",
    "contradicted",
    "blocked",
}
EVIDENCE_TIERS = {
    "inferred",
    "synthetic",
    "documented",
    "anonymized_operational",
    "schema_bound",
    "observed",
    "verified",
    "promoted",
}
EVIDENCE_STATES = EVIDENCE_TIERS | {"stale", "contradicted", "blocked"}
SCENARIO_PACK_REFS = {
    "crm_sync",
    "duplicate_merge",
    "renewal_risk",
    "support_escalation",
    "invoice_failure",
    "permission_denied",
    "stale_import",
    "webhook_storm",
    "slack_approval",
}


class WorkflowContextError(ValueError):
    """Domain-level context authority failure with machine-readable detail."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any, *, length: int = 16) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()[:length]


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "context"


def _clean_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkflowContextError(
            "workflow_context.invalid_input",
            f"{field_name} must be a non-empty string",
            details={"field_name": field_name},
        )
    return value.strip()


def _clean_optional_text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise WorkflowContextError(
            "workflow_context.invalid_input",
            "optional text fields must be non-empty strings when supplied",
        )
    return value.strip()


def _clean_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise WorkflowContextError(
            "workflow_context.invalid_input",
            "expected a list of strings",
        )
    return [str(item).strip() for item in value if str(item).strip()]


def _clean_evidence(value: object) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise WorkflowContextError(
            "workflow_context.invalid_evidence",
            "evidence must be a list of JSON objects or refs",
        )
    evidence: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, str):
            evidence.append(
                {
                    "evidence_ref": item.strip(),
                    "evidence_tier": _infer_evidence_tier(item),
                }
            )
            continue
        if not isinstance(item, Mapping):
            raise WorkflowContextError(
                "workflow_context.invalid_evidence",
                "each evidence entry must be a string or object",
            )
        entry = dict(item)
        tier = str(entry.get("evidence_tier") or entry.get("truth_state") or "documented")
        if tier not in EVIDENCE_STATES:
            tier = "documented"
        entry["evidence_tier"] = tier
        evidence.append(entry)
    return evidence


def _infer_evidence_tier(evidence_ref: str) -> str:
    lowered = evidence_ref.lower()
    for tier in (
        "promoted",
        "verified",
        "contradicted",
        "blocked",
        "stale",
        "observed",
        "schema_bound",
        "anonymized_operational",
        "documented",
        "synthetic",
    ):
        if tier in lowered:
            return tier
    return "documented"


def _evidence_tier(item: Mapping[str, Any] | str) -> str:
    if isinstance(item, str):
        return _infer_evidence_tier(item)
    tier = str(
        item.get("evidence_tier")
        or item.get("truth_state")
        or _infer_evidence_tier(str(item.get("evidence_ref") or item.get("ref") or ""))
    )
    return tier if tier in EVIDENCE_STATES else "documented"


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _evidence_is_anonymized_or_synthetic(item: Mapping[str, Any] | str) -> bool:
    if isinstance(item, str):
        lowered = item.lower()
        return "anonymized" in lowered or "synthetic" in lowered
    tier = _evidence_tier(item)
    if tier in {"synthetic", "anonymized_operational"}:
        return True
    return any(
        _truthy_flag(item.get(flag))
        for flag in (
            "anonymized",
            "synthetic",
            "customer_data_removed",
            "redacted",
            "sampled",
        )
    )


def _evidence_can_prove_live(item: Mapping[str, Any] | str) -> bool:
    tier = _evidence_tier(item)
    if tier not in {"verified", "promoted"}:
        return False
    if _evidence_is_anonymized_or_synthetic(item):
        return False
    if isinstance(item, Mapping) and item.get("promotion_evidence_allowed") is False:
        return False
    return True


def _evidence_signals(evidence: Sequence[Any]) -> dict[str, Any]:
    tiers: list[str] = []
    verifier_statuses: list[str] = []
    contradiction_count = 0
    stale = False
    unknown_mutator_risk = False
    anonymized_or_synthetic_count = 0
    promotion_evidence_count = 0
    for item in evidence:
        if not isinstance(item, (Mapping, str)):
            continue
        tier = _evidence_tier(item)
        tiers.append(tier)
        if _evidence_is_anonymized_or_synthetic(item):
            anonymized_or_synthetic_count += 1
        if _evidence_can_prove_live(item):
            promotion_evidence_count += 1
        if isinstance(item, str):
            lowered = item.lower()
            stale = stale or "stale" in lowered
            unknown_mutator_risk = unknown_mutator_risk or "unknown_mutator" in lowered or "unknown writer" in lowered
            contradiction_count += 1 if "contradict" in lowered or "conflict" in lowered else 0
            continue
        ref_text = str(item.get("evidence_ref") or item.get("ref") or "").lower()
        freshness_state = str(item.get("freshness_state") or item.get("freshness") or "").lower()
        stale = stale or tier == "stale" or freshness_state == "stale" or "stale" in ref_text
        unknown_mutator_risk = unknown_mutator_risk or _truthy_flag(item.get("unknown_mutator_risk"))
        unknown_mutator_risk = unknown_mutator_risk or "unknown_mutator" in ref_text or "unknown writer" in ref_text
        conflict_state = str(item.get("conflict_state") or item.get("contradiction_state") or "").lower()
        if tier == "contradicted" or conflict_state in {"conflict", "contradicted"}:
            contradiction_count += 1
        elif _truthy_flag(item.get("contradicted")) or _truthy_flag(item.get("contradiction")):
            contradiction_count += 1
        verifier_status = str(item.get("verifier_status") or item.get("verification_status") or "").lower()
        if verifier_status in {"passed", "failed"}:
            verifier_statuses.append(verifier_status)
    verifier_status = "failed" if "failed" in verifier_statuses else "passed" if "passed" in verifier_statuses else None
    return {
        "evidence_tiers": sorted(set(tiers)),
        "freshness_state": "stale" if stale else None,
        "unknown_mutator_risk": unknown_mutator_risk,
        "contradiction_count": contradiction_count,
        "verifier_status": verifier_status,
        "anonymized_or_synthetic_count": anonymized_or_synthetic_count,
        "promotion_evidence_count": promotion_evidence_count,
    }


def _has_live_promotion_evidence(evidence: Sequence[Any]) -> bool:
    return any(
        _evidence_can_prove_live(item)
        for item in evidence
        if isinstance(item, (Mapping, str))
    )


def _pack_ref(
    *,
    workflow_ref: str | None,
    intent: str,
    graph: Mapping[str, Any],
    scenario_pack_refs: Sequence[str],
    seed: str,
) -> str:
    digest = _digest(
        {
            "workflow_ref": workflow_ref,
            "intent": intent,
            "graph": graph,
            "scenario_pack_refs": list(scenario_pack_refs),
            "seed": seed,
        },
        length=20,
    )
    return f"workflow_context:{_slug(workflow_ref or intent)[:48]}:{digest}"


def scenario_pack_registry() -> dict[str, dict[str, Any]]:
    """Return reusable scenario pack templates for standalone Workflow authoring."""

    return {
        "crm_sync": {
            "systems": ["Salesforce", "HubSpot"],
            "objects": ["Account", "Contact", "Opportunity"],
            "fields": ["external_id", "email", "owner_id", "updated_at"],
            "events": ["record_created", "record_updated", "dedupe_candidate_detected"],
            "actions": ["upsert_account", "sync_contact", "write_lineage_receipt"],
            "failures": ["field_mapping_missing", "source_authority_conflict"],
            "verifier_expectations": ["idempotent_sync", "lineage_edge_created"],
        },
        "duplicate_merge": {
            "systems": ["CRM", "Billing"],
            "objects": ["Account", "Customer", "IdentityCluster"],
            "fields": ["canonical_name", "domain", "billing_account_id"],
            "events": ["duplicate_candidate_found", "merge_review_requested"],
            "actions": ["score_identity_match", "propose_merge", "record_reversible_binding"],
            "failures": ["ambiguous_identity", "conflicting_source_authority"],
            "verifier_expectations": ["merge_is_reversible", "losing_record_not_deleted"],
        },
        "renewal_risk": {
            "systems": ["CRM", "Billing", "Support", "Slack"],
            "objects": ["Account", "Subscription", "Ticket", "WorkspaceUser"],
            "fields": ["arr", "renewal_date", "health_score", "ticket_severity", "owner_id"],
            "events": ["renewal_window_opened", "high_severity_ticket_created"],
            "actions": ["calculate_renewal_risk", "notify_owner", "open_success_task"],
            "failures": ["missing_owner", "unknown_contract_mutator", "stale_support_import"],
            "verifier_expectations": ["risk_score_explained", "notification_is_deduped"],
        },
        "support_escalation": {
            "systems": ["Support", "CRM", "Slack"],
            "objects": ["Ticket", "Account", "Escalation"],
            "fields": ["severity", "sla_deadline", "account_tier", "owner_id"],
            "events": ["sla_breach_near", "ticket_escalated"],
            "actions": ["route_escalation", "notify_channel", "record_owner_ack"],
            "failures": ["permission_denied", "missing_account_link"],
            "verifier_expectations": ["sla_timer_preserved", "ack_required_for_close"],
        },
        "invoice_failure": {
            "systems": ["Billing", "CRM", "Support"],
            "objects": ["Invoice", "Account", "PaymentMethod"],
            "fields": ["invoice_status", "failure_reason", "retry_at", "account_owner"],
            "events": ["payment_failed", "retry_window_elapsed"],
            "actions": ["create_collection_task", "notify_owner", "pause_renewal_offer"],
            "failures": ["card_declined", "billing_system_unavailable"],
            "verifier_expectations": ["no_duplicate_collection_tasks", "retry_schedule_visible"],
        },
        "permission_denied": {
            "systems": ["IdentityProvider", "TargetApp"],
            "objects": ["Principal", "PermissionGrant", "ActionAttempt"],
            "fields": ["actor_ref", "scope_ref", "required_permission"],
            "events": ["action_denied", "access_review_requested"],
            "actions": ["request_access", "record_policy_decision"],
            "failures": ["insufficient_scope", "expired_token"],
            "verifier_expectations": ["denied_write_has_no_side_effect"],
        },
        "stale_import": {
            "systems": ["DataWarehouse", "CRM"],
            "objects": ["ImportBatch", "Account", "FieldSnapshot"],
            "fields": ["imported_at", "watermark", "field_digest"],
            "events": ["watermark_lag_detected", "import_completed"],
            "actions": ["mark_context_stale", "request_refresh"],
            "failures": ["late_arriving_change", "unknown_mutator"],
            "verifier_expectations": ["stale_context_blocks_promotion"],
        },
        "webhook_storm": {
            "systems": ["WebhookProvider", "WorkflowRuntime"],
            "objects": ["WebhookEvent", "DeduplicationKey", "AutomationRun"],
            "fields": ["event_id", "idempotency_key", "received_at"],
            "events": ["webhook_received", "duplicate_event_suppressed"],
            "actions": ["dedupe_event", "rate_limit_followup", "emit_receipt"],
            "failures": ["out_of_order_delivery", "retry_storm"],
            "verifier_expectations": ["idempotency_key_replay_is_noop"],
        },
        "slack_approval": {
            "systems": ["Slack", "WorkflowRuntime"],
            "objects": ["ApprovalRequest", "Approver", "Decision"],
            "fields": ["channel_id", "approver_ref", "decision_status"],
            "events": ["approval_requested", "approval_decided", "approval_expired"],
            "actions": ["post_approval", "record_decision", "continue_or_block"],
            "failures": ["approver_missing", "approval_timeout"],
            "verifier_expectations": ["only_approved_path_continues"],
        },
    }


def infer_scenario_pack_refs(intent: str, graph: Mapping[str, Any] | None = None) -> list[str]:
    """Infer scenario packs from workflow intent and graph hints."""

    text = f"{intent} {_canonical_json(graph or {})}".lower()
    matches: list[str] = []
    keyword_map = {
        "renewal_risk": ("renewal", "churn", "risk", "health score"),
        "support_escalation": ("support", "ticket", "escalat", "sla"),
        "invoice_failure": ("invoice", "payment", "billing", "collection"),
        "crm_sync": ("crm", "salesforce", "hubspot", "sync"),
        "duplicate_merge": ("duplicate", "merge", "identity", "mdm"),
        "permission_denied": ("permission", "denied", "access", "scope"),
        "stale_import": ("stale", "import", "watermark", "warehouse"),
        "webhook_storm": ("webhook", "storm", "retry", "idempot"),
        "slack_approval": ("slack", "approval", "approver", "channel"),
    }
    for pack_ref, keywords in keyword_map.items():
        if any(keyword in text for keyword in keywords):
            matches.append(pack_ref)
    return matches or ["crm_sync"]


def compile_workflow_context(
    *,
    intent: str,
    workflow_ref: str | None = None,
    graph: Mapping[str, Any] | None = None,
    context_mode: str = "inferred",
    scenario_pack_refs: Sequence[str] | None = None,
    seed: str | None = None,
    source_prompt_ref: str | None = None,
    evidence: Sequence[Mapping[str, Any] | str] | None = None,
    unknown_mutator_risk: bool = False,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compile intent and optional graph into a Workflow Context pack."""

    clean_intent = _clean_text(intent, field_name="intent")
    clean_workflow_ref = _clean_optional_text(workflow_ref)
    clean_graph = dict(graph or {})
    clean_mode = str(context_mode or "inferred").strip()
    if clean_mode not in CONTEXT_MODES:
        raise WorkflowContextError(
            "workflow_context.invalid_context_mode",
            "context_mode is not allowed",
            details={"context_mode": clean_mode, "allowed": sorted(CONTEXT_MODES)},
        )
    inferred_packs = infer_scenario_pack_refs(clean_intent, clean_graph)
    requested_packs = _clean_string_list(scenario_pack_refs)
    pack_refs = requested_packs or inferred_packs
    unknown_packs = [pack_ref for pack_ref in pack_refs if pack_ref not in SCENARIO_PACK_REFS]
    if unknown_packs:
        raise WorkflowContextError(
            "workflow_context.unknown_scenario_pack",
            "unknown scenario pack requested",
            details={"unknown": unknown_packs, "allowed": sorted(SCENARIO_PACK_REFS)},
        )
    clean_seed = seed.strip() if isinstance(seed, str) and seed.strip() else _digest(
        {"intent": clean_intent, "graph": clean_graph, "scenario_pack_refs": pack_refs},
        length=12,
    )
    context_ref = _pack_ref(
        workflow_ref=clean_workflow_ref,
        intent=clean_intent,
        graph=clean_graph,
        scenario_pack_refs=pack_refs,
        seed=clean_seed,
    )
    evidence_entries = _clean_evidence(evidence)
    evidence_signals = _evidence_signals(evidence_entries)
    effective_unknown_mutator_risk = bool(unknown_mutator_risk or evidence_signals["unknown_mutator_risk"])
    initial_truth_state = "synthetic" if clean_mode in {"synthetic", "hybrid"} else "inferred"
    entities = _build_entities(
        context_ref=context_ref,
        scenario_pack_refs=pack_refs,
        graph=clean_graph,
        truth_state=initial_truth_state,
        context_mode=clean_mode,
    )
    blockers = _initial_blockers(
        scenario_pack_refs=pack_refs,
        entities=entities,
        unknown_mutator_risk=effective_unknown_mutator_risk,
    )
    verifier_expectations = _verifier_expectations(pack_refs)
    confidence = compute_confidence(
        truth_state=initial_truth_state,
        evidence=evidence_entries,
        blockers=blockers,
        unknown_mutator_risk=effective_unknown_mutator_risk,
        verifier_status=evidence_signals["verifier_status"],
        freshness_state=evidence_signals["freshness_state"],
        contradiction_count=int(evidence_signals["contradiction_count"]),
    )
    synthetic_world = None
    if clean_mode in {"synthetic", "hybrid"}:
        synthetic_world = build_synthetic_world(
            context_ref=context_ref,
            seed=clean_seed,
            entities=entities,
            scenario_pack_refs=pack_refs,
        )
    pack = {
        "context_ref": context_ref,
        "workflow_ref": clean_workflow_ref,
        "context_mode": clean_mode,
        "truth_state": initial_truth_state,
        "seed": clean_seed,
        "intent": clean_intent,
        "graph_ref": f"workflow_context_graph:{_digest(clean_graph, length=20)}",
        "source_prompt_ref": _clean_optional_text(source_prompt_ref),
        "scenario_pack_refs": list(pack_refs),
        "materialized_from": {
            "intent": clean_intent,
            "workflow_ref": clean_workflow_ref,
            "graph": clean_graph,
            "inferred_scenario_pack_refs": inferred_packs,
            "requested_scenario_pack_refs": requested_packs,
        },
        "entities": entities,
        "evidence_refs": evidence_entries,
        "blockers": blockers,
        "verifier_expectations": verifier_expectations,
        "unknown_mutator_risk": effective_unknown_mutator_risk,
        "confidence": confidence,
        "confidence_score": confidence["score"],
        "confidence_state": confidence["state"],
        "synthetic_world": synthetic_world,
        "guardrail": guardrail_check(
            {
                "truth_state": initial_truth_state,
                "confidence": confidence,
                "blockers": blockers,
                "unknown_mutator_risk": effective_unknown_mutator_risk,
                "evidence_refs": evidence_entries,
            }
        ),
        "review_packet": build_review_packet(
            context_ref=context_ref,
            truth_state=initial_truth_state,
            confidence=confidence,
            blockers=blockers,
            guardrail=None,
            binding=None,
        ),
        "metadata": dict(metadata or {}),
    }
    pack["review_packet"] = build_review_packet(
        context_ref=context_ref,
        truth_state=initial_truth_state,
        confidence=confidence,
        blockers=blockers,
        guardrail=pack["guardrail"],
        binding=None,
    )
    return pack


def _build_entities(
    *,
    context_ref: str,
    scenario_pack_refs: Sequence[str],
    graph: Mapping[str, Any],
    truth_state: str,
    context_mode: str,
) -> list[dict[str, Any]]:
    registry = scenario_pack_registry()
    raw_entities: list[dict[str, Any]] = []
    for pack_ref in scenario_pack_refs:
        pack = registry[pack_ref]
        for system in pack["systems"]:
            raw_entities.append(
                {
                    "entity_kind": "system",
                    "label": system,
                    "payload": {"scenario_pack_ref": pack_ref},
                }
            )
        for obj in pack["objects"]:
            raw_entities.append(
                {
                    "entity_kind": "object",
                    "label": obj,
                    "payload": {"scenario_pack_ref": pack_ref},
                }
            )
        for field in pack["fields"]:
            raw_entities.append(
                {
                    "entity_kind": "field",
                    "label": field,
                    "payload": {"scenario_pack_ref": pack_ref},
                }
            )
        for event in pack["events"]:
            raw_entities.append(
                {
                    "entity_kind": "event",
                    "label": event,
                    "payload": {"scenario_pack_ref": pack_ref},
                }
            )
        for action in pack["actions"]:
            raw_entities.append(
                {
                    "entity_kind": "action",
                    "label": action,
                    "payload": {"scenario_pack_ref": pack_ref},
                }
            )
        for failure in pack["failures"]:
            raw_entities.append(
                {
                    "entity_kind": "failure",
                    "label": failure,
                    "payload": {"scenario_pack_ref": pack_ref},
                }
            )
    for index, node in enumerate(_graph_nodes(graph)):
        label = str(node.get("title") or node.get("label") or node.get("id") or f"node_{index}")
        raw_entities.append(
            {
                "entity_kind": "workflow_node",
                "label": label,
                "payload": {
                    "node_id": node.get("id"),
                    "node": dict(node),
                },
            }
        )
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in raw_entities:
        key = (str(raw["entity_kind"]), str(raw["label"]).lower())
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = raw
            continue
        existing_payload = dict(existing.get("payload") or {})
        existing_payload.setdefault("merged_from", []).append(raw.get("payload") or {})
        existing["payload"] = existing_payload

    entities: list[dict[str, Any]] = []
    for index, raw in enumerate(deduped.values()):
        entity_kind = str(raw["entity_kind"])
        label = str(raw["label"])
        entity_ref = f"{context_ref}:entity:{entity_kind}:{_slug(label)}:{_digest([entity_kind, label, index], length=8)}"
        entity_truth_state = truth_state
        io_mode = "synthetic" if context_mode in {"synthetic", "hybrid"} else "inferred"
        entities.append(
            {
                "entity_ref": entity_ref,
                "context_ref": context_ref,
                "entity_kind": entity_kind,
                "label": label,
                "truth_state": entity_truth_state,
                "io_mode": io_mode,
                "context_pill": entity_truth_state,
                "payload": dict(raw.get("payload") or {}),
                "evidence_refs": [],
                "confidence_score": 0.28 if entity_truth_state == "synthetic" else 0.18,
            }
        )
    return entities


def _graph_nodes(graph: Mapping[str, Any]) -> list[dict[str, Any]]:
    nodes = graph.get("nodes") or graph.get("steps") or []
    if not isinstance(nodes, Sequence) or isinstance(nodes, (str, bytes)):
        return []
    return [dict(node) for node in nodes if isinstance(node, Mapping)]


def _initial_blockers(
    *,
    scenario_pack_refs: Sequence[str],
    entities: Sequence[Mapping[str, Any]],
    unknown_mutator_risk: bool,
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    object_count = sum(1 for item in entities if item.get("entity_kind") == "object")
    system_count = sum(1 for item in entities if item.get("entity_kind") == "system")
    if not object_count:
        blockers.append(
            {
                "blocker_ref": "workflow_context.blocker.no_objects",
                "severity": "hard",
                "reason_code": "workflow_context.no_objects",
                "message": "No workflow objects were inferred.",
            }
        )
    if not system_count:
        blockers.append(
            {
                "blocker_ref": "workflow_context.blocker.no_systems",
                "severity": "soft",
                "reason_code": "workflow_context.no_systems",
                "message": "No source systems were inferred.",
            }
        )
    if unknown_mutator_risk:
        blockers.append(
            {
                "blocker_ref": "workflow_context.blocker.unknown_mutator",
                "severity": "hard",
                "reason_code": "workflow_context.unknown_mutator_risk",
                "message": "Untracked systems may mutate source objects.",
            }
        )
    if "stale_import" in scenario_pack_refs:
        blockers.append(
            {
                "blocker_ref": "workflow_context.blocker.stale_import_possible",
                "severity": "soft",
                "reason_code": "workflow_context.stale_import_possible",
                "message": "Imported evidence may lag live source systems.",
            }
        )
    return blockers


def _verifier_expectations(scenario_pack_refs: Sequence[str]) -> list[dict[str, Any]]:
    registry = scenario_pack_registry()
    expectations: list[dict[str, Any]] = []
    for pack_ref in scenario_pack_refs:
        for expectation in registry[pack_ref]["verifier_expectations"]:
            expectations.append(
                {
                    "verifier_ref": f"verifier.workflow_context.{pack_ref}.{expectation}",
                    "scenario_pack_ref": pack_ref,
                    "expectation": expectation,
                    "required_before": "promoted",
                }
            )
    return expectations


def build_synthetic_world(
    *,
    context_ref: str,
    seed: str,
    entities: Sequence[Mapping[str, Any]],
    scenario_pack_refs: Sequence[str],
) -> dict[str, Any]:
    """Create a deterministic executable fake world from context entities."""

    object_entities = [item for item in entities if item.get("entity_kind") == "object"]
    event_entities = [item for item in entities if item.get("entity_kind") == "event"]
    action_entities = [item for item in entities if item.get("entity_kind") == "action"]
    failure_entities = [item for item in entities if item.get("entity_kind") == "failure"]
    records: list[dict[str, Any]] = []
    for obj in object_entities:
        label = str(obj.get("label"))
        for index in range(1, 4):
            record_id = f"synthetic:{_slug(label)}:{_digest([seed, label, index], length=10)}"
            records.append(
                {
                    "record_id": record_id,
                    "object_label": label,
                    "synthetic": True,
                    "synthetic_seed": seed,
                    "fields": {
                        "name": f"Synthetic {label} {index}",
                        "status": _deterministic_choice(seed, label, index, ["new", "active", "at_risk"]),
                        "priority": _deterministic_choice(seed, label, index, ["low", "medium", "high"]),
                        "external_id": record_id,
                    },
                }
            )
    world_ref = f"{context_ref}:synthetic_world:{_digest([context_ref, seed], length=16)}"
    synthetic_events = [
        {
            "event_ref": f"synthetic_event:{_slug(str(item.get('label')))}:{_digest([seed, item.get('label')], length=8)}",
            "label": item.get("label"),
            "synthetic": True,
        }
        for item in event_entities
    ]
    synthetic_actions = [
        {
            "action_ref": f"synthetic_action:{_slug(str(item.get('label')))}:{_digest([seed, item.get('label')], length=8)}",
            "label": item.get("label"),
            "synthetic": True,
            "side_effect_mode": "virtual_only",
        }
        for item in action_entities
    ]
    synthetic_failures = [
        {
            "failure_ref": f"synthetic_failure:{_slug(str(item.get('label')))}:{_digest([seed, item.get('label')], length=8)}",
            "label": item.get("label"),
            "synthetic": True,
        }
        for item in failure_entities
    ]
    virtual_lab = build_virtual_lab_packets_for_synthetic_world(
        context_ref=context_ref,
        world_ref=world_ref,
        seed=seed,
        scenario_pack_refs=scenario_pack_refs,
        records=records,
        actions=synthetic_actions,
        failures=synthetic_failures,
    )
    return {
        "world_ref": world_ref,
        "context_ref": context_ref,
        "seed": seed,
        "synthetic": True,
        "scenario_pack_refs": list(scenario_pack_refs),
        "records": records,
        "events": synthetic_events,
        "actions": synthetic_actions,
        "failures": synthetic_failures,
        "permissions": {
            "live_writes_allowed": False,
            "customer_data_allowed": False,
            "promotion_evidence_allowed": False,
        },
        "virtual_lab": virtual_lab,
    }


def _deterministic_choice(seed: str, label: str, index: int, choices: Sequence[str]) -> str:
    position = int(_digest([seed, label, index], length=8), 16) % len(choices)
    return choices[position]


def build_virtual_lab_packets_for_synthetic_world(
    *,
    context_ref: str,
    world_ref: str,
    seed: str,
    scenario_pack_refs: Sequence[str],
    records: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    failures: Sequence[Mapping[str, Any]] | None = None,
    clock_start: str = "2026-01-01T00:00:00Z",
) -> dict[str, Any]:
    """Project a synthetic Workflow Context world into Virtual Lab packets.

    Workflow Context owns the fake world. Virtual Lab owns execution. This
    adapter creates only deterministic packets that callers can submit through
    the existing Virtual Lab CQRS operations.
    """

    if not records:
        raise WorkflowContextError(
            "workflow_context.synthetic_world_empty",
            "synthetic worlds require at least one record before Virtual Lab simulation",
        )
    from runtime.virtual_lab.state import (
        SeedManifestEntry,
        build_environment_revision,
        build_seed_manifest,
        object_states_from_seed_manifest,
        virtual_lab_digest,
    )

    environment_id = f"virtual_lab.env.workflow_context.{_digest(context_ref, length=20)}"
    revision_id = f"virtual_lab_revision.workflow_context.{_digest([context_ref, world_ref, seed], length=20)}"
    entries = []
    for index, record in enumerate(records):
        record_id = _clean_text(record.get("record_id"), field_name="record.record_id")
        object_label = _clean_text(record.get("object_label"), field_name="record.object_label")
        base_state = {
            "synthetic": True,
            "synthetic_seed": seed,
            "context_ref": context_ref,
            "world_ref": world_ref,
            "record_id": record_id,
            "object_label": object_label,
            "fields": dict(record.get("fields") or {}),
        }
        entries.append(
            SeedManifestEntry(
                object_id=record_id,
                instance_id="primary",
                object_truth_ref=f"workflow_context.synthetic_record.{_digest([context_ref, record_id], length=20)}",
                object_truth_version=f"synthetic.{_digest([seed, record_id], length=12)}",
                projection_version="workflow_context.synthetic_virtual_lab.v1",
                seed_parameters={
                    "context_ref": context_ref,
                    "world_ref": world_ref,
                    "scenario_pack_refs": list(scenario_pack_refs),
                    "record_ordinal": index,
                },
                base_state=base_state,
            )
        )
    seed_manifest = build_seed_manifest(entries)
    config = {
        "seed": seed,
        "context_ref": context_ref,
        "world_ref": world_ref,
        "scenario_pack_refs": list(scenario_pack_refs),
        "runtime": "authority.virtual_lab",
    }
    policy = {
        "synthetic": True,
        "live_writes_allowed": False,
        "customer_data_allowed": False,
        "promotion_evidence_allowed": False,
        "authority_owner": "authority.virtual_lab",
        "source_authority": "authority.workflow_context",
    }
    revision = build_environment_revision(
        environment_id=environment_id,
        revision_id=revision_id,
        revision_reason="workflow_context.synthetic_world",
        seed_manifest=seed_manifest,
        config=config,
        policy=policy,
        created_at=clock_start,
        created_by="authority.workflow_context",
        metadata={
            "context_ref": context_ref,
            "world_ref": world_ref,
            "synthetic": True,
        },
    )
    object_states = [item.to_json() for item in object_states_from_seed_manifest(revision)]
    simulation_actions = _virtual_lab_simulation_actions(
        context_ref=context_ref,
        seed=seed,
        records=records,
        actions=actions,
    )
    automation_rules = _virtual_lab_automation_rules(
        context_ref=context_ref,
        seed=seed,
        records=records,
        simulation_actions=simulation_actions,
    )
    scenario = {
        "scenario_id": f"workflow_context.synthetic.{_digest([context_ref, world_ref, seed], length=20)}",
        "initial_state": {
            "revision": revision.to_json(),
            "object_states": object_states,
        },
        "actions": simulation_actions,
        "config": {
            "seed": seed,
            "clock_start": clock_start,
            "clock_step_seconds": 1,
            "max_actions": max(10, len(simulation_actions) + len(automation_rules) + 5),
            "max_automation_firings": max(5, len(automation_rules) + 2),
            "max_recursion_depth": 4,
        },
        "automation_rules": automation_rules,
        "assertions": _virtual_lab_assertions(simulation_actions),
        "verifiers": [
            {
                "verifier_id": "verifier.workflow_context.synthetic.no_blockers",
                "verifier_kind": "no_blockers",
                "severity": "error",
            },
            {
                "verifier_id": "verifier.workflow_context.synthetic.assertions_passed",
                "verifier_kind": "all_assertions_passed",
                "severity": "error",
            },
            {
                "verifier_id": "verifier.workflow_context.synthetic.object_patched",
                "verifier_kind": "trace_contains_event_type",
                "event_type": "object.patched",
                "min_count": 1,
                "severity": "error",
            },
        ],
        "metadata": {
            "context_ref": context_ref,
            "world_ref": world_ref,
            "synthetic": True,
            "scenario_pack_refs": list(scenario_pack_refs),
            "failure_refs": [item.get("failure_ref") for item in failures or []],
        },
    }
    return {
        "environment_revision": revision.to_json(),
        "object_states": object_states,
        "state_record_payload": {
            "environment_revision": revision.to_json(),
            "object_states": object_states,
            "events": [],
            "command_receipts": [],
            "typed_gaps": [],
            "observed_by_ref": "authority.workflow_context",
            "source_ref": context_ref,
        },
        "simulation_scenario": scenario,
        "simulation_run_payload": {
            "scenario": scenario,
            "task_contract_ref": f"task_environment_contract.workflow_context.{_digest(context_ref, length=16)}",
            "integration_action_contract_refs": [],
            "automation_snapshot_refs": [],
            "observed_by_ref": "authority.workflow_context",
            "source_ref": context_ref,
        },
        "digests": {
            "config_digest": virtual_lab_digest(config, purpose="workflow_context.synthetic_virtual_lab_config.v1"),
            "policy_digest": virtual_lab_digest(policy, purpose="workflow_context.synthetic_virtual_lab_policy.v1"),
        },
    }


def _virtual_lab_simulation_actions(
    *,
    context_ref: str,
    seed: str,
    records: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not actions:
        actions = [{"label": "touch_synthetic_context", "action_ref": "synthetic_action.touch"}]
    simulation_actions: list[dict[str, Any]] = []
    for index, action in enumerate(actions):
        record = records[index % len(records)]
        label = str(action.get("label") or f"synthetic_action_{index + 1}")
        action_id = f"workflow_context.synthetic_action.{_slug(label)}.{_digest([context_ref, seed, label, index], length=12)}"
        simulation_actions.append(
            {
                "action_id": action_id,
                "action_kind": "patch_object",
                "object_id": record["record_id"],
                "instance_id": "primary",
                "payload": {
                    "patch": {
                        "workflow_context": {
                            "synthetic": True,
                            "simulated_action": label,
                            "action_ref": action.get("action_ref"),
                            "action_index": index,
                        }
                    }
                },
                "actor": {
                    "actor_id": "workflow_context.synthetic_runner",
                    "actor_type": "agent",
                },
                "metadata": {
                    "context_ref": context_ref,
                    "side_effect_mode": "virtual_only",
                },
            }
        )
    return simulation_actions


def _virtual_lab_automation_rules(
    *,
    context_ref: str,
    seed: str,
    records: Sequence[Mapping[str, Any]],
    simulation_actions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if len(records) < 2 or not simulation_actions:
        return []
    first_action = simulation_actions[0]
    first_label = first_action["payload"]["patch"]["workflow_context"]["simulated_action"]
    target = records[1]
    return [
        {
            "rule_id": f"workflow_context.synthetic_rule.followup.{_digest([context_ref, seed, first_label], length=12)}",
            "name": "Synthetic follow-up automation",
            "predicate": {
                "predicate_kind": "payload_field_equals",
                "field_path": ["overlay_patch", "workflow_context", "simulated_action"],
                "expected": first_label,
            },
            "effects": [
                {
                    "action_id": f"workflow_context.synthetic_automation.followup.{_digest([context_ref, seed, target['record_id']], length=12)}",
                    "action_kind": "patch_object",
                    "object_id": target["record_id"],
                    "instance_id": "primary",
                    "payload": {
                        "patch": {
                            "workflow_context": {
                                "synthetic": True,
                                "automation_fired": True,
                                "triggered_by_action": first_label,
                            }
                        }
                    },
                    "actor": {
                        "actor_id": "workflow_context.synthetic_automation",
                        "actor_type": "system",
                    },
                    "metadata": {
                        "context_ref": context_ref,
                        "side_effect_mode": "virtual_only",
                    },
                }
            ],
            "priority": 10,
            "status": "active",
            "max_firings": 1,
            "metadata": {
                "context_ref": context_ref,
                "synthetic": True,
            },
        }
    ]


def _virtual_lab_assertions(simulation_actions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    assertions = [
        {
            "assertion_id": "assertion.workflow_context.synthetic.no_blockers",
            "assertion_kind": "no_blockers",
            "severity": "error",
        },
        {
            "assertion_id": "assertion.workflow_context.synthetic.object_patched",
            "assertion_kind": "event_count_at_least",
            "event_type": "object.patched",
            "min_count": 1,
            "severity": "error",
        },
    ]
    if simulation_actions:
        first_action = simulation_actions[0]
        assertions.append(
            {
                "assertion_id": "assertion.workflow_context.synthetic.first_action_visible",
                "assertion_kind": "final_object_field_equals",
                "object_id": first_action["object_id"],
                "instance_id": first_action.get("instance_id") or "primary",
                "field_path": ["workflow_context", "simulated_action"],
                "expected": first_action["payload"]["patch"]["workflow_context"]["simulated_action"],
                "severity": "error",
            }
        )
    return assertions


def compute_confidence(
    *,
    truth_state: str,
    evidence: Sequence[Mapping[str, Any] | str] | None = None,
    blockers: Sequence[Mapping[str, Any]] | None = None,
    unknown_mutator_risk: bool = False,
    verifier_status: str | None = None,
    freshness_state: str | None = None,
    contradiction_count: int = 0,
) -> dict[str, Any]:
    """Compute confidence from evidence, freshness, verifier, and mutator risk."""

    if truth_state not in TRUTH_STATES:
        raise WorkflowContextError(
            "workflow_context.invalid_truth_state",
            "truth_state is not allowed",
            details={"truth_state": truth_state, "allowed": sorted(TRUTH_STATES)},
        )
    weights = {
        "none": 0.0,
        "inferred": 0.18,
        "synthetic": 0.28,
        "documented": 0.42,
        "anonymized_operational": 0.50,
        "schema_bound": 0.66,
        "observed": 0.78,
        "verified": 0.92,
        "promoted": 0.96,
        "stale": 0.36,
        "contradicted": 0.20,
        "blocked": 0.10,
    }
    evidence_entries = list(evidence or [])
    evidence_signals = _evidence_signals(evidence_entries)
    effective_unknown_mutator_risk = bool(unknown_mutator_risk or evidence_signals["unknown_mutator_risk"])
    effective_freshness_state = freshness_state or evidence_signals["freshness_state"]
    effective_verifier_status = verifier_status or evidence_signals["verifier_status"]
    effective_contradiction_count = max(int(contradiction_count), int(evidence_signals["contradiction_count"]))
    base = weights[truth_state]
    evidence_scores = []
    for item in evidence_entries:
        if not isinstance(item, (Mapping, str)):
            continue
        tier = _evidence_tier(item)
        score_value = weights.get(tier, 0.42)
        if tier in {"verified", "promoted"} and not _evidence_can_prove_live(item):
            score_value = min(score_value, weights["anonymized_operational"])
        evidence_scores.append(score_value)
    score = max([base, *evidence_scores]) if evidence_scores else base
    hard_blockers = [
        item for item in blockers or []
        if str(item.get("severity") or "").lower() == "hard"
    ]
    soft_blockers = [
        item for item in blockers or []
        if str(item.get("severity") or "").lower() == "soft"
    ]
    penalties = 0.0
    if hard_blockers:
        penalties += 0.30
    if soft_blockers:
        penalties += min(0.15, 0.05 * len(soft_blockers))
    if effective_unknown_mutator_risk:
        penalties += 0.20
    if effective_freshness_state == "stale" or truth_state == "stale":
        penalties += 0.20
    if effective_contradiction_count or truth_state == "contradicted":
        penalties += min(0.45, 0.25 + 0.10 * effective_contradiction_count)
    if effective_verifier_status == "failed":
        penalties += 0.30
    if effective_verifier_status == "passed":
        score = max(score, weights["verified"])
    final_score = max(0.0, min(1.0, round(score - penalties, 4)))
    if final_score >= 0.82 and effective_verifier_status == "passed":
        state = "verified"
    elif final_score >= 0.70:
        state = "high"
    elif final_score >= 0.40:
        state = "medium"
    else:
        state = "low"
    return {
        "score": final_score,
        "state": state,
        "inputs": {
            "truth_state": truth_state,
            "evidence_count": len(evidence_entries),
            "evidence_tiers": evidence_signals["evidence_tiers"],
            "promotion_evidence_count": evidence_signals["promotion_evidence_count"],
            "anonymized_or_synthetic_evidence_count": evidence_signals["anonymized_or_synthetic_count"],
            "hard_blocker_count": len(hard_blockers),
            "soft_blocker_count": len(soft_blockers),
            "unknown_mutator_risk": effective_unknown_mutator_risk,
            "verifier_status": effective_verifier_status,
            "freshness_state": effective_freshness_state,
            "contradiction_count": effective_contradiction_count,
        },
    }


def guardrail_check(
    pack: Mapping[str, Any],
    *,
    target_truth_state: str | None = None,
    risk_disposition: str | None = None,
    requested_action: str | None = None,
) -> dict[str, Any]:
    """Evaluate allowed next actions and no-go conditions for a context pack."""

    truth_state = str(pack.get("truth_state") or "none")
    if truth_state not in TRUTH_STATES:
        raise WorkflowContextError(
            "workflow_context.invalid_truth_state",
            "current truth_state is not allowed",
            details={"truth_state": truth_state},
        )
    if target_truth_state is not None and target_truth_state not in TRUTH_STATES:
        raise WorkflowContextError(
            "workflow_context.invalid_truth_state",
            "target truth_state is not allowed",
            details={"target_truth_state": target_truth_state},
        )
    blockers = list(pack.get("blockers") or [])
    confidence = dict(pack.get("confidence") or {})
    if not confidence and "confidence_score" in pack:
        confidence = {"score": pack.get("confidence_score"), "state": pack.get("confidence_state")}
    evidence = list(pack.get("evidence_refs") or [])
    evidence_signals = _evidence_signals(evidence)
    unknown_mutator_risk = bool(pack.get("unknown_mutator_risk") or evidence_signals["unknown_mutator_risk"])
    allowed_actions = _allowed_actions_for_state(truth_state)
    no_go: list[dict[str, Any]] = []
    hard_blockers = [item for item in blockers if str(item.get("severity") or "").lower() == "hard"]
    if hard_blockers:
        no_go.append(
            {
                "reason_code": "workflow_context.hard_blockers_present",
                "message": "Hard blockers must be resolved or accepted before live trust boundaries.",
                "blocker_refs": [item.get("blocker_ref") for item in hard_blockers],
            }
        )
    if truth_state in {"stale", "contradicted", "blocked"}:
        no_go.append(
            {
                "reason_code": f"workflow_context.{truth_state}",
                "message": f"Context in {truth_state} state cannot be promoted.",
            }
        )
    if unknown_mutator_risk and risk_disposition != "accepted":
        no_go.append(
            {
                "reason_code": "workflow_context.unknown_mutator_risk",
                "message": "Unknown mutator risk requires an accepted-risk receipt before promotion.",
            }
        )
    if target_truth_state == "promoted":
        if truth_state in {"none", "inferred", "synthetic"}:
            no_go.append(
                {
                    "reason_code": "workflow_context.synthetic_or_inferred_cannot_promote",
                    "message": "Inferred or synthetic context can build and simulate but cannot promote live.",
                }
            )
        has_verified_evidence = _has_evidence_tier(evidence, {"verified", "promoted"})
        if not has_verified_evidence:
            no_go.append(
                {
                    "reason_code": "workflow_context.verified_evidence_required",
                    "message": "Promotion requires verified evidence.",
                }
            )
        elif not _has_live_promotion_evidence(evidence):
            no_go.append(
                {
                    "reason_code": "workflow_context.live_evidence_required",
                    "message": "Promotion requires verified non-synthetic and non-anonymized evidence.",
                }
            )
        if evidence_signals["freshness_state"] == "stale":
            no_go.append(
                {
                    "reason_code": "workflow_context.stale_evidence",
                    "message": "Stale evidence cannot prove promotion without refresh.",
                }
            )
        if int(evidence_signals["contradiction_count"]):
            no_go.append(
                {
                    "reason_code": "workflow_context.contradicted_evidence",
                    "message": "Contradicted evidence must be resolved before promotion.",
                    "contradiction_count": evidence_signals["contradiction_count"],
                }
            )
        if float(confidence.get("score") or 0.0) < 0.82:
            no_go.append(
                {
                    "reason_code": "workflow_context.confidence_too_low",
                    "message": "Promotion requires computed confidence >= 0.82.",
                    "confidence_score": confidence.get("score"),
                }
            )
    if requested_action == "live_write" and truth_state != "promoted":
        no_go.append(
            {
                "reason_code": "workflow_context.live_write_requires_promoted",
                "message": "Live writes require promoted context.",
            }
        )
    allowed = not no_go if target_truth_state == "promoted" or requested_action == "live_write" else True
    review_required = bool(target_truth_state in {"promoted", "verified"} or risk_disposition == "accepted")
    return {
        "allowed": allowed,
        "truth_state": truth_state,
        "target_truth_state": target_truth_state,
        "requested_action": requested_action,
        "allowed_next_actions": allowed_actions,
        "review_required": review_required,
        "no_go_conditions": no_go,
        "safe_next_llm_actions": _safe_next_llm_actions(truth_state, no_go),
    }


def _allowed_actions_for_state(truth_state: str) -> list[str]:
    table = {
        "none": ["infer_context", "attach_documented_evidence"],
        "inferred": ["build_synthetic_context", "attach_documented_evidence", "propose_binding", "revise_assumptions"],
        "synthetic": ["simulate", "attach_documented_evidence", "propose_binding", "revise_synthetic_world"],
        "documented": ["bind_candidate", "request_schema", "verify"],
        "anonymized_operational": ["bind_candidate", "verify", "request_schema"],
        "schema_bound": ["observe", "verify", "propose_binding"],
        "observed": ["verify", "propose_binding", "monitor_freshness"],
        "verified": ["promote", "monitor_freshness", "package_review"],
        "promoted": ["monitor", "detect_drift", "mark_stale"],
        "stale": ["refresh_evidence", "revise_context", "block_promotion"],
        "contradicted": ["resolve_contradiction", "revise_context", "block_promotion"],
        "blocked": ["resolve_blocker", "accept_risk", "revise_context"],
    }
    return table[truth_state]


def _safe_next_llm_actions(truth_state: str, no_go: Sequence[Mapping[str, Any]]) -> list[str]:
    if truth_state in {"inferred", "synthetic"}:
        return ["continue_building", "run_synthetic_simulation", "generate_review_packet"]
    if no_go:
        return ["revise_context", "collect_more_evidence", "queue_review_item"]
    return ["continue_verification", "prepare_binding_packet"]


def _has_evidence_tier(evidence: Sequence[Any], tiers: set[str]) -> bool:
    for item in evidence:
        if isinstance(item, (Mapping, str)) and _evidence_tier(item) in tiers:
            return True
    return False


def transition_context_pack(
    pack: Mapping[str, Any],
    *,
    to_truth_state: str,
    transition_reason: str,
    evidence: Sequence[Mapping[str, Any] | str] | None = None,
    risk_disposition: str | None = None,
    decision_ref: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return an updated pack and transition record after guardrail approval."""

    from_state = str(pack.get("truth_state") or "none")
    reason = _clean_text(transition_reason, field_name="transition_reason")
    evidence_entries = [*list(pack.get("evidence_refs") or []), *_clean_evidence(evidence)]
    evidence_signals = _evidence_signals(evidence_entries)
    effective_unknown_mutator_risk = bool(pack.get("unknown_mutator_risk") or evidence_signals["unknown_mutator_risk"])
    confidence_unknown_mutator_risk = effective_unknown_mutator_risk and risk_disposition != "accepted"
    candidate = dict(pack)
    candidate["evidence_refs"] = evidence_entries
    candidate["unknown_mutator_risk"] = effective_unknown_mutator_risk
    guardrail = guardrail_check(
        candidate,
        target_truth_state=to_truth_state,
        risk_disposition=risk_disposition,
    )
    if to_truth_state == "promoted" and not guardrail["allowed"]:
        raise WorkflowContextError(
            "workflow_context.transition_blocked",
            "truth-state transition rejected by guardrail",
            details={"guardrail": guardrail},
        )
    updated_blockers = list(pack.get("blockers") or [])
    if to_truth_state in {"verified", "promoted"}:
        updated_blockers = [
            item for item in updated_blockers
            if str(item.get("severity") or "").lower() != "hard"
        ]
    confidence = compute_confidence(
        truth_state=to_truth_state,
        evidence=evidence_entries,
        blockers=updated_blockers,
        unknown_mutator_risk=confidence_unknown_mutator_risk,
        verifier_status="passed" if to_truth_state in {"verified", "promoted"} else evidence_signals["verifier_status"],
        freshness_state="stale" if to_truth_state == "stale" else evidence_signals["freshness_state"],
        contradiction_count=max(1 if to_truth_state == "contradicted" else 0, int(evidence_signals["contradiction_count"])),
    )
    updated = dict(pack)
    updated.update(
        {
            "truth_state": to_truth_state,
            "context_mode": _context_mode_after_transition(str(pack.get("context_mode") or "inferred"), to_truth_state),
            "evidence_refs": evidence_entries,
            "blockers": updated_blockers,
            "unknown_mutator_risk": confidence_unknown_mutator_risk,
            "confidence": confidence,
            "confidence_score": confidence["score"],
            "confidence_state": confidence["state"],
            "guardrail": guardrail_check(
                {
                    **updated,
                    "truth_state": to_truth_state,
                    "evidence_refs": evidence_entries,
                    "blockers": updated_blockers,
                    "confidence": confidence,
                },
                risk_disposition=risk_disposition,
            ),
        }
    )
    transition_ref = (
        f"{updated['context_ref']}:transition:"
        f"{_slug(from_state)}_to_{_slug(to_truth_state)}:{_digest([reason, evidence_entries], length=10)}"
    )
    transition = {
        "transition_ref": transition_ref,
        "context_ref": updated["context_ref"],
        "from_truth_state": from_state,
        "to_truth_state": to_truth_state,
        "transition_reason": reason,
        "decision_ref": _clean_optional_text(decision_ref),
        "risk_disposition": _clean_optional_text(risk_disposition),
        "evidence_refs": evidence_entries,
        "guardrail": guardrail,
    }
    updated["review_packet"] = build_review_packet(
        context_ref=str(updated["context_ref"]),
        truth_state=to_truth_state,
        confidence=confidence,
        blockers=updated_blockers,
        guardrail=updated["guardrail"],
        binding=None,
    )
    return updated, transition


def _context_mode_after_transition(current_mode: str, truth_state: str) -> str:
    if truth_state in {"schema_bound", "observed", "verified", "promoted"}:
        if current_mode == "synthetic":
            return "hybrid"
        return "bound"
    return current_mode if current_mode in CONTEXT_MODES else "inferred"


def build_binding(
    *,
    pack: Mapping[str, Any],
    entity: Mapping[str, Any],
    target_ref: str,
    target_authority_domain: str = "authority.object_truth",
    evidence: Sequence[Mapping[str, Any] | str] | None = None,
    risk_level: str = "medium",
    binding_state: str = "proposed",
    reversible: bool = True,
    reviewed_by_ref: str | None = None,
) -> dict[str, Any]:
    """Build a reversible context binding proposal or accepted binding."""

    context_ref = _clean_text(pack.get("context_ref"), field_name="context_ref")
    entity_ref = _clean_text(entity.get("entity_ref"), field_name="entity_ref")
    clean_target_ref = _clean_text(target_ref, field_name="target_ref")
    clean_domain = _clean_text(target_authority_domain, field_name="target_authority_domain")
    clean_risk = str(risk_level or "medium").strip().lower()
    if clean_risk not in {"low", "medium", "high", "critical"}:
        raise WorkflowContextError(
            "workflow_context.invalid_binding_risk",
            "binding risk_level is not allowed",
            details={"risk_level": clean_risk},
        )
    clean_state = str(binding_state or "proposed").strip().lower()
    if clean_state not in {"proposed", "accepted", "rejected", "revoked"}:
        raise WorkflowContextError(
            "workflow_context.invalid_binding_state",
            "binding_state is not allowed",
            details={"binding_state": clean_state},
        )
    requires_review = clean_risk in {"high", "critical"} or clean_domain != "authority.object_truth"
    if requires_review and clean_state == "accepted" and not reviewed_by_ref:
        raise WorkflowContextError(
            "workflow_context.binding_review_required",
            "high-risk or non-Object Truth bindings require human review before acceptance",
            details={"risk_level": clean_risk, "target_authority_domain": clean_domain},
        )
    evidence_entries = _clean_evidence(evidence)
    binding_evidence = evidence_entries or list(pack.get("evidence_refs") or [])
    evidence_signals = _evidence_signals(binding_evidence)
    confidence = compute_confidence(
        truth_state="schema_bound" if clean_domain == "authority.object_truth" else "documented",
        evidence=binding_evidence,
        blockers=list(pack.get("blockers") or []),
        unknown_mutator_risk=bool(pack.get("unknown_mutator_risk") or evidence_signals["unknown_mutator_risk"]),
    )
    binding_ref = (
        f"{context_ref}:binding:{_slug(str(entity.get('label') or entity_ref))}:"
        f"{_digest([entity_ref, clean_domain, clean_target_ref], length=12)}"
    )
    return {
        "binding_ref": binding_ref,
        "context_ref": context_ref,
        "entity_ref": entity_ref,
        "target_ref": clean_target_ref,
        "target_authority_domain": clean_domain,
        "binding_state": clean_state,
        "risk_level": clean_risk,
        "requires_review": requires_review,
        "reversible": bool(reversible),
        "reviewed_by_ref": _clean_optional_text(reviewed_by_ref),
        "confidence_score": confidence["score"],
        "confidence": confidence,
        "evidence_refs": evidence_entries,
        "guardrail": guardrail_check(
            pack,
            requested_action="bind_sensitive" if requires_review else "bind",
        ),
    }


def build_review_packet(
    *,
    context_ref: str,
    truth_state: str,
    confidence: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
    guardrail: Mapping[str, Any] | None,
    binding: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build the compact review packet the LLM queues at trust boundaries."""

    queue_items: list[dict[str, Any]] = []
    queued_keys: set[tuple[str, str | None, str | None]] = set()
    queued_reason_codes: set[str] = set()

    def _queue(
        *,
        decision_type: str,
        reason_code: str | None = None,
        blocked_actions: Sequence[str] = (),
        blocker_ref: Any = None,
        binding_ref: Any = None,
    ) -> None:
        key = (decision_type, reason_code, str(binding_ref or blocker_ref or ""))
        if key in queued_keys:
            return
        queued_keys.add(key)
        if reason_code:
            queued_reason_codes.add(reason_code)
        queue_items.append(
            {
                "decision_type": decision_type,
                "required": True,
                "reason_code": reason_code,
                "block_scope": "trust_boundary_only",
                "blocked_actions": list(blocked_actions),
                **({"blocker_ref": blocker_ref} if blocker_ref else {}),
                **({"binding_ref": binding_ref} if binding_ref else {}),
            }
        )

    guardrail_payload = dict(guardrail or {})
    target_truth_state = guardrail_payload.get("target_truth_state")
    requested_action = guardrail_payload.get("requested_action")
    if truth_state in {"verified", "promoted"} or target_truth_state in {"verified", "promoted"}:
        _queue(
            decision_type="promotion_or_live_trust_boundary",
            reason_code="workflow_context.real_world_trust_boundary",
            blocked_actions=("promote_context", "deploy_to_live_sandbox", "live_write"),
        )
    if requested_action == "live_write":
        _queue(
            decision_type="live_effect_decision",
            reason_code="workflow_context.live_effect_requires_review",
            blocked_actions=("live_write", "customer_visible_effect"),
        )
    hard_blocker_queued = False
    for blocker in blockers:
        if str(blocker.get("severity") or "").lower() == "hard":
            hard_blocker_queued = True
            _queue(
                decision_type="accepted_risk_or_blocker_resolution",
                reason_code=str(blocker.get("reason_code") or "workflow_context.hard_blocker"),
                blocked_actions=("accept_risk", "promote_context", "live_write"),
                blocker_ref=blocker.get("blocker_ref"),
            )
    for condition in list(guardrail_payload.get("no_go_conditions") or []):
        if not isinstance(condition, Mapping):
            continue
        reason_code = str(condition.get("reason_code") or "")
        if reason_code == "workflow_context.hard_blockers_present" and hard_blocker_queued:
            continue
        if reason_code in queued_reason_codes:
            continue
        if reason_code in {
            "workflow_context.unknown_mutator_risk",
            "workflow_context.stale_evidence",
            "workflow_context.contradicted_evidence",
            "workflow_context.hard_blockers_present",
        }:
            _queue(
                decision_type="accepted_risk_or_blocker_resolution",
                reason_code=reason_code,
                blocked_actions=("accept_risk", "promote_context", "live_write"),
                blocker_ref=condition.get("blocker_refs") or condition.get("blocker_ref"),
            )
    if binding and binding.get("requires_review"):
        _queue(
            decision_type="sensitive_binding_review",
            reason_code="workflow_context.sensitive_binding_review_required",
            blocked_actions=("accept_binding", "promote_context", "live_write"),
            binding_ref=binding.get("binding_ref"),
        )
        if binding.get("target_authority_domain") != "authority.object_truth":
            _queue(
                decision_type="source_authority_decision",
                reason_code="workflow_context.source_authority_requires_review",
                blocked_actions=("declare_source_authority", "accept_binding", "promote_context"),
                binding_ref=binding.get("binding_ref"),
            )
    safe_next_actions = list(guardrail_payload.get("safe_next_llm_actions") or _safe_next_llm_actions(truth_state, []))
    blocked_actions = sorted(
        {
            str(action)
            for item in queue_items
            for action in list(item.get("blocked_actions") or [])
            if str(action).strip()
        }
    )
    autopilot_states = {"none", "inferred", "synthetic", "documented", "anonymized_operational", "schema_bound", "observed", "verified"}
    return {
        "packet_ref": f"{context_ref}:review_packet:{_digest([truth_state, confidence, blockers, binding], length=12)}",
        "context_ref": context_ref,
        "truth_state": truth_state,
        "confidence": dict(confidence),
        "queue_items": queue_items,
        "queued_decision_count": len(queue_items),
        "autopilot_allowed": truth_state in autopilot_states,
        "autopilot_scope": {
            "can_continue_without_review": safe_next_actions,
            "blocked_until_review": blocked_actions,
            "block_scope": "trust_boundary_only" if queue_items else "none",
        },
        "safe_next_llm_actions": safe_next_actions,
        "human_review_blocks_all_work": False,
    }


__all__ = [
    "CONTEXT_MODES",
    "IO_MODES",
    "SCENARIO_PACK_REFS",
    "TRUTH_STATES",
    "WorkflowContextError",
    "build_binding",
    "build_review_packet",
    "build_synthetic_world",
    "compile_workflow_context",
    "compute_confidence",
    "guardrail_check",
    "infer_scenario_pack_refs",
    "scenario_pack_registry",
    "transition_context_pack",
]
