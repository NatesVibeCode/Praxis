"""Explicit planning artifacts over workflow build authority state.

This module projects the current build workspace into the first shared planning
artifacts for the operating-model path:

- CandidateResolutionManifest: deterministic proposal layer
- ReviewablePlan: explicit approval/review layer

The source of truth stays where it already belongs:
- definition/build state in the workflow build record
- review provenance in workflow_build_review_decisions

This module is intentionally a projector. It does not introduce a second
planner stack and it does not mutate workflow state.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from runtime.build_authority import build_authority_bundle
from runtime.build_review_decisions import scrub_review_state_for_persistence
from runtime.definition_compile_kernel import materialize_definition
from storage.postgres.workflow_build_review_repository import (
    list_latest_workflow_build_review_decisions,
)


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _stable_id(prefix: str, payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}_{hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:16]}"


def _candidate_approval_state(
    *,
    candidate_ref: str,
    decision: dict[str, Any] | None,
) -> str:
    if not isinstance(decision, dict):
        return "proposed"
    decision_name = _text(decision.get("decision")).lower()
    approved_ref = _text(decision.get("candidate_ref"))
    if decision_name == "approve":
        if approved_ref and approved_ref == candidate_ref:
            return "approved"
        if approved_ref:
            return "superseded"
        return "proposed"
    if decision_name == "reject":
        if approved_ref and approved_ref == candidate_ref:
            return "rejected"
        return "proposed"
    return "proposed"


def _slot_approval_state(decision: dict[str, Any] | None) -> str:
    if not isinstance(decision, dict):
        return "unapproved"
    decision_name = _text(decision.get("decision")).lower()
    if decision_name == "approve":
        return "approved"
    if decision_name == "reject":
        return "rejected"
    if decision_name == "defer":
        return "deferred"
    return "unapproved"


def _slot_candidate_resolution_state(
    *,
    binding: dict[str, Any],
    blocking_issue_ids: list[str],
) -> str:
    state = _text(binding.get("state")).lower()
    candidate_targets = (
        binding.get("candidate_targets")
        if isinstance(binding.get("candidate_targets"), list)
        else []
    )
    if blocking_issue_ids and state == "stale":
        return "blocked"
    if state == "captured" and not candidate_targets:
        return "unresolved"
    if state in {"suggested", "accepted", "rejected", "stale"} or candidate_targets:
        return "candidate_set"
    return "unresolved"


def _review_provenance(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None
    return {
        "review_decision_id": _text(record.get("review_decision_id")) or None,
        "decision": _text(record.get("decision")) or None,
        "actor_type": _text(record.get("actor_type")) or None,
        "actor_ref": _text(record.get("actor_ref")) or None,
        "approval_mode": _text(record.get("approval_mode")) or None,
        "rationale": _text(record.get("rationale")) or None,
        "decided_at": _text(record.get("decided_at")) or None,
        "source_subpath": _text(record.get("source_subpath")) or None,
    }


def _build_binding_slots(
    *,
    binding_ledger: list[dict[str, Any]],
    issue_ids_by_binding: dict[str, list[str]],
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []
    for binding in binding_ledger:
        if not isinstance(binding, dict):
            continue
        binding_id = _text(binding.get("binding_id"))
        if not binding_id:
            continue
        latest_decision = latest_by_target.get(("binding", binding_id))
        candidate_targets = (
            binding.get("candidate_targets")
            if isinstance(binding.get("candidate_targets"), list)
            else []
        )
        candidates: list[dict[str, Any]] = []
        for index, target in enumerate(candidate_targets, start=1):
            if not isinstance(target, dict):
                continue
            candidate_ref = _text(target.get("target_ref")) or f"{binding_id}:candidate:{index}"
            candidates.append(
                {
                    "candidate_ref": candidate_ref,
                    "rank": index,
                    "label": _text(target.get("label")) or candidate_ref,
                    "kind": _text(target.get("kind")) or "reference",
                    "candidate_approval_state": _candidate_approval_state(
                        candidate_ref=candidate_ref,
                        decision=latest_decision,
                    ),
                    "payload": _json_clone(target),
                }
            )
        slots.append(
            {
                "slot_ref": binding_id,
                "kind": _text(binding.get("source_kind")) or "reference",
                "required": True,
                "source_label": _text(binding.get("source_label")) or binding_id,
                "candidate_resolution_state": _slot_candidate_resolution_state(
                    binding=binding,
                    blocking_issue_ids=issue_ids_by_binding.get(binding_id, []),
                ),
                "approval_state": _slot_approval_state(latest_decision),
                "top_ranked_ref": candidates[0]["candidate_ref"] if candidates else None,
                "approved_ref": _text(latest_decision.get("candidate_ref")) or None
                if isinstance(latest_decision, dict) and _text(latest_decision.get("decision")).lower() == "approve"
                else None,
                "blocking_issue_ids": issue_ids_by_binding.get(binding_id, []),
                "candidate_count": len(candidates),
                "candidates": candidates,
                "freshness": _json_clone(binding.get("freshness"))
                if isinstance(binding.get("freshness"), dict)
                else None,
                "review_provenance": _review_provenance(latest_decision),
                "rationale": _text(binding.get("rationale")) or None,
                "source_node_ids": _string_list(binding.get("source_node_ids")),
            }
        )
    return slots


def _build_import_evidence(
    *,
    import_snapshots: list[dict[str, Any]],
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for snapshot in import_snapshots:
        if not isinstance(snapshot, dict):
            continue
        snapshot_id = _text(snapshot.get("snapshot_id"))
        if not snapshot_id:
            continue
        latest_decision = latest_by_target.get(("import_snapshot", snapshot_id))
        evidence.append(
            {
                "snapshot_ref": snapshot_id,
                "binding_ref": _text(snapshot.get("binding_id")) or None,
                "source_kind": _text(snapshot.get("source_kind")) or "net_request",
                "source_locator": _text(snapshot.get("source_locator")) or None,
                "candidate_resolution_state": (
                    "blocked"
                    if _text(snapshot.get("approval_state")) == "stale"
                    else "candidate_set"
                ),
                "approval_state": _slot_approval_state(latest_decision),
                "top_ranked_ref": (
                    _text((snapshot.get("admitted_targets") or [{}])[0].get("target_ref"))
                    if isinstance(snapshot.get("admitted_targets"), list) and snapshot.get("admitted_targets")
                    else None
                ),
                "captured_at": _text(snapshot.get("captured_at")) or None,
                "stale_after_at": _text(snapshot.get("stale_after_at")) or None,
                "review_provenance": _review_provenance(latest_decision),
                "requested_shape": _json_clone(snapshot.get("requested_shape"))
                if isinstance(snapshot.get("requested_shape"), dict)
                else {},
            }
        )
    return evidence


def _build_workflow_shape_candidates(
    *,
    build_graph: dict[str, Any] | None,
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(build_graph, dict):
        return []
    graph_id = _text(build_graph.get("graph_id"))
    if not graph_id:
        return []
    latest_decision = latest_by_target.get(("workflow_shape", graph_id))
    return [
        {
            "candidate_ref": graph_id,
            "kind": "build_graph",
            "approval_state": _slot_approval_state(latest_decision),
            "review_provenance": _review_provenance(latest_decision),
            "summary": {
                "node_count": len(build_graph.get("nodes") or []),
                "edge_count": len(build_graph.get("edges") or []),
                "projection_state": _text((build_graph.get("projection_status") or {}).get("state"))
                or None,
            },
        }
    ]


def _latest_review_decisions(
    conn: Any | None,
    *,
    workflow_id: str | None,
    definition_revision: str | None,
) -> list[dict[str, Any]]:
    if conn is None or not workflow_id or not definition_revision:
        return []
    return list_latest_workflow_build_review_decisions(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )


def build_candidate_resolution_manifest(
    *,
    definition: dict[str, Any],
    workflow_id: str | None = None,
    conn: Any | None = None,
    compiled_spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    scrubbed_definition = scrub_review_state_for_persistence(materialized)
    authority_bundle = build_authority_bundle(scrubbed_definition, compiled_spec=compiled_spec)
    definition_revision = _text(materialized.get("definition_revision")) or None
    latest_records = _latest_review_decisions(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
    latest_by_target = {
        (_text(record.get("target_kind")), _text(record.get("target_ref"))): record
        for record in latest_records
        if isinstance(record, dict)
    }
    issue_ids_by_binding: dict[str, list[str]] = {}
    for issue in authority_bundle.get("build_issues", []):
        if not isinstance(issue, dict):
            continue
        binding_id = _text(issue.get("binding_id"))
        issue_id = _text(issue.get("issue_id"))
        if binding_id and issue_id:
            issue_ids_by_binding.setdefault(binding_id, []).append(issue_id)

    binding_slots = _build_binding_slots(
        binding_ledger=authority_bundle.get("binding_ledger") or [],
        issue_ids_by_binding=issue_ids_by_binding,
        latest_by_target=latest_by_target,
    )
    open_required_slots = [
        slot
        for slot in binding_slots
        if slot.get("required") and slot.get("approval_state") != "approved"
    ]
    blocking_issues = [
        issue
        for issue in authority_bundle.get("build_issues", [])
        if isinstance(issue, dict) and _text(issue.get("severity")) == "blocking"
    ]
    hard_blocking_issues = [
        issue
        for issue in blocking_issues
        if _text(issue.get("kind")) not in {"binding_gate"}
    ]
    execution_readiness = (
        "blocked"
        if hard_blocking_issues
        else "review_required"
        if open_required_slots
        else "ready"
    )
    manifest_payload = {
        "workflow_id": workflow_id,
        "definition_revision": definition_revision,
        "binding_slots": binding_slots,
        "import_evidence": _build_import_evidence(
            import_snapshots=authority_bundle.get("import_snapshots") or [],
            latest_by_target=latest_by_target,
        ),
        "workflow_shape_candidates": _build_workflow_shape_candidates(
            build_graph=authority_bundle.get("build_graph")
            if isinstance(authority_bundle.get("build_graph"), dict)
            else None,
            latest_by_target=latest_by_target,
        ),
        "capability_bundle_candidates": [],
        "blocking_issues": _json_clone(hard_blocking_issues),
        "review_gates": _json_clone(
            [
                issue
                for issue in blocking_issues
                if _text(issue.get("kind")) == "binding_gate"
            ]
        ),
        "required_confirmations": [
            {
                "slot_ref": slot["slot_ref"],
                "reason": "Explicit approval is required before execution can proceed.",
            }
            for slot in open_required_slots
        ],
        "overall_confidence": None,
        "execution_readiness": execution_readiness,
        "rationale": (
            "Deterministic candidate resolution produced proposals only; "
            "explicit review approval is still required before hardening."
        ),
        "projection_status": _json_clone(authority_bundle.get("projection_status") or {}),
    }
    return {
        "manifest_version": 1,
        "manifest_id": _stable_id("candidate_manifest", manifest_payload),
        **manifest_payload,
    }


def build_reviewable_plan(
    *,
    definition: dict[str, Any],
    workflow_id: str | None = None,
    conn: Any | None = None,
    compiled_spec: dict[str, Any] | None = None,
    candidate_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    definition_revision = _text(materialized.get("definition_revision")) or None
    latest_records = _latest_review_decisions(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
    manifest = (
        candidate_manifest
        if isinstance(candidate_manifest, dict)
        else build_candidate_resolution_manifest(
            definition=materialized,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=compiled_spec,
        )
    )
    approval_records: list[dict[str, Any]] = []
    approved_binding_refs: list[dict[str, str]] = []
    approved_bundle_refs: list[str] = []
    approved_workflow_shape_ref: str | None = None
    deferred_slot_refs: list[str] = []
    proposal_requests: list[dict[str, Any]] = []
    widening_ops: list[dict[str, Any]] = []

    for record in latest_records:
        if not isinstance(record, dict):
            continue
        decision = _text(record.get("decision")).lower()
        target_kind = _text(record.get("target_kind"))
        target_ref = _text(record.get("target_ref"))
        approval_records.append(
            {
                "review_decision_id": _text(record.get("review_decision_id")) or None,
                "target_kind": target_kind,
                "target_ref": target_ref,
                "decision": decision,
                "candidate_ref": _text(record.get("candidate_ref")) or None,
                "candidate_payload": _json_clone(record.get("candidate_payload")),
                "approved_by": _text(record.get("actor_ref")) or None,
                "approved_at": _text(record.get("decided_at")) or None,
                "approval_mode": _text(record.get("approval_mode")) or None,
                "review_actor": {
                    "actor_type": _text(record.get("actor_type")) or None,
                    "actor_ref": _text(record.get("actor_ref")) or None,
                },
                "rationale": _text(record.get("rationale")) or None,
            }
        )
        if decision == "approve":
            if target_kind == "binding":
                approved_binding_refs.append(
                    {
                        "slot_ref": target_ref,
                        "candidate_ref": _text(record.get("candidate_ref")) or target_ref,
                    }
                )
            elif target_kind == "capability_bundle":
                approved_bundle_refs.append(target_ref)
            elif target_kind == "workflow_shape":
                approved_workflow_shape_ref = target_ref
        elif decision == "defer":
            deferred_slot_refs.append(target_ref)
        elif decision == "widen":
            widening_ops.append(
                {
                    "target_kind": target_kind,
                    "target_ref": target_ref,
                    "requested_by": {
                        "actor_type": _text(record.get("actor_type")) or None,
                        "actor_ref": _text(record.get("actor_ref")) or None,
                    },
                    "requested_at": _text(record.get("decided_at")) or None,
                    "operation": _json_clone(record.get("candidate_payload")),
                    "rationale": _text(record.get("rationale")) or None,
                }
            )
        elif decision == "proposal_request":
            proposal_requests.append(
                {
                    "target_kind": target_kind,
                    "target_ref": target_ref,
                    "candidate_ref": _text(record.get("candidate_ref")) or None,
                    "proposal_payload": _json_clone(record.get("candidate_payload")),
                    "requested_by": {
                        "actor_type": _text(record.get("actor_type")) or None,
                        "actor_ref": _text(record.get("actor_ref")) or None,
                    },
                    "requested_at": _text(record.get("decided_at")) or None,
                    "rationale": _text(record.get("rationale")) or None,
                }
            )

    status = "accepted"
    if proposal_requests:
        status = "needs_proposals"
    elif widening_ops:
        status = "needs_widening"
    elif manifest.get("execution_readiness") == "blocked":
        status = "blocked"
    elif manifest.get("execution_readiness") != "ready":
        status = "needs_review"
    elif deferred_slot_refs:
        status = "accepted_with_deferred_noncritical_slots"

    review_payload = {
        "workflow_id": workflow_id,
        "definition_revision": definition_revision,
        "manifest_id": manifest.get("manifest_id"),
        "approved_binding_refs": approved_binding_refs,
        "approved_bundle_refs": approved_bundle_refs,
        "approved_workflow_shape_ref": approved_workflow_shape_ref,
        "proposal_requests": proposal_requests,
        "widening_ops": widening_ops,
        "deferred_slot_refs": sorted(set(deferred_slot_refs)),
        "approval_records": approval_records,
        "status": status,
        "required_unapproved_slots": [
            slot["slot_ref"]
            for slot in manifest.get("binding_slots", [])
            if isinstance(slot, dict)
            and slot.get("required")
            and slot.get("approval_state") != "approved"
        ],
    }
    return {
        "review_version": 1,
        "review_id": _stable_id("review_plan", review_payload),
        **review_payload,
    }


__all__ = [
    "build_candidate_resolution_manifest",
    "build_reviewable_plan",
]
