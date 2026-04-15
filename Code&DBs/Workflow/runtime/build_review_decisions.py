"""DB-backed review authority for workflow build planning."""

from __future__ import annotations

import json
from typing import Any

from runtime.event_log import CHANNEL_BUILD_STATE, EVENT_REVIEW_DECISION, emit
from storage.postgres.workflow_build_planning_repository import (
    load_default_workflow_build_review_policy,
    load_review_policy_definition,
    load_workflow_build_review_session,
)
from storage.postgres.workflow_build_review_repository import (
    get_latest_workflow_build_review_decision,
    list_latest_workflow_build_review_decisions,
    record_workflow_build_review_decision,
)


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def record_build_review_decision(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
    target_kind: str,
    target_ref: str,
    decision: str,
    actor_type: str | None = None,
    actor_ref: str | None = None,
    approval_mode: str | None = None,
    rationale: str | None = None,
    source_subpath: str | None = None,
    slot_ref: str | None = None,
    review_group_ref: str | None = None,
    authority_scope: str | None = None,
    supersedes_decision_ref: str | None = None,
    candidate_ref: str | None = None,
    candidate_payload: object | None = None,
) -> dict[str, Any]:
    normalized_actor_type = _text(actor_type).lower() or "human"
    normalized_decision = _text(decision).lower()
    if hasattr(conn, "fetchrow") and hasattr(conn, "execute"):
        try:
            session = load_workflow_build_review_session(
                conn,
                workflow_id=workflow_id,
                definition_revision=definition_revision,
                review_group_ref=review_group_ref,
            )
        except Exception:
            session = None
        policy = None
        policy_ref = _text((session or {}).get("review_policy_ref"))
        if policy_ref:
            try:
                policy = load_review_policy_definition(conn, review_policy_ref=policy_ref)
            except Exception:
                policy = None
        if policy is None:
            try:
                policy = load_default_workflow_build_review_policy(conn)
            except Exception:
                policy = None
        if isinstance(policy, dict):
            allowed_actor_types = {
                _text(item).lower()
                for item in (policy.get("allowed_actor_types_json") or [])
                if _text(item)
            }
            if allowed_actor_types and normalized_actor_type not in allowed_actor_types:
                raise ValueError(
                    f"review policy forbids actor_type '{normalized_actor_type}' for workflow build review"
                )
            if normalized_decision == "defer" and not bool(policy.get("defer_allowed")):
                raise ValueError("review policy forbids defer decisions for this workflow build review")
            if normalized_decision == "widen" and not bool(policy.get("widen_allowed")):
                raise ValueError("review policy forbids widen decisions for this workflow build review")
            if normalized_decision == "proposal_request" and not bool(policy.get("proposal_request_allowed")):
                raise ValueError("review policy forbids proposal_request decisions for this workflow build review")
    record = record_workflow_build_review_decision(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
        target_kind=target_kind,
        target_ref=target_ref,
        decision=decision,
        actor_type=normalized_actor_type,
        actor_ref=actor_ref,
        approval_mode=approval_mode,
        rationale=rationale,
        source_subpath=source_subpath,
        slot_ref=slot_ref,
        review_group_ref=review_group_ref,
        authority_scope=authority_scope,
        supersedes_decision_ref=supersedes_decision_ref,
        candidate_ref=candidate_ref,
        candidate_payload=candidate_payload,
    )
    emit(
        conn,
        channel=CHANNEL_BUILD_STATE,
        event_type=EVENT_REVIEW_DECISION,
        entity_id=workflow_id,
        entity_kind="workflow",
        payload=_json_clone(record),
        emitted_by="runtime.build_review_decisions",
    )
    return record


def build_review_decision_undo_receipt(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
    target_kind: str,
    target_ref: str,
    slot_ref: str | None = None,
) -> dict[str, Any]:
    previous = get_latest_workflow_build_review_decision(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
        target_kind=target_kind,
        target_ref=target_ref,
        slot_ref=slot_ref,
    )
    body: dict[str, Any] = {
        "target_kind": target_kind,
        "target_ref": target_ref,
    }
    if _text(slot_ref):
        body["slot_ref"] = _text(slot_ref)
    if previous is None:
        body.update(
            {
                "decision": "revoke",
                "approval_mode": "undo_restore",
                "rationale": "Undo restore to the prior unapproved build-review state.",
            }
        )
    else:
        body.update(
            {
                "decision": previous["decision"],
                "candidate_ref": previous.get("candidate_ref"),
                "candidate_payload": _json_clone(previous.get("candidate_payload")),
                "slot_ref": previous.get("slot_ref"),
                "review_group_ref": previous.get("review_group_ref"),
                "authority_scope": previous.get("authority_scope"),
                "supersedes_decision_ref": previous.get("supersedes_decision_ref"),
                "review_actor_type": previous.get("actor_type"),
                "review_actor_ref": previous.get("actor_ref"),
                "approval_mode": previous.get("approval_mode"),
                "rationale": previous.get("rationale"),
            }
        )
    return {
        "workflow_id": workflow_id,
        "steps": [
            {
                "subpath": "review_decisions",
                "body": body,
            }
        ],
    }


def _scrub_binding_review_state(definition: dict[str, Any]) -> dict[str, Any]:
    cloned = _json_clone(definition if isinstance(definition, dict) else {})
    cloned.pop("review_state", None)
    bindings = cloned.get("binding_ledger")
    if isinstance(bindings, list):
        next_bindings: list[dict[str, Any]] = []
        for entry in bindings:
            if not isinstance(entry, dict):
                continue
            binding = _json_clone(entry)
            prior_state = _text(binding.get("state"))
            candidate_targets = binding.get("candidate_targets") if isinstance(binding.get("candidate_targets"), list) else []
            if prior_state in {"accepted", "rejected"}:
                binding["state"] = "suggested" if candidate_targets else "captured"
                binding["rationale"] = (
                    "Explicit review approval is required before the binding can execute."
                    if candidate_targets
                    else "Needs an accepted authority target before planning can run cleanly."
                )
            binding["accepted_target"] = None
            next_bindings.append(binding)
        cloned["binding_ledger"] = next_bindings

    snapshots = cloned.get("import_snapshots")
    if isinstance(snapshots, list):
        next_snapshots: list[dict[str, Any]] = []
        for entry in snapshots:
            if not isinstance(entry, dict):
                continue
            snapshot = _json_clone(entry)
            snapshot["approval_state"] = "staged"
            snapshot["admitted_targets"] = []
            next_snapshots.append(snapshot)
        cloned["import_snapshots"] = next_snapshots
    build_graph = cloned.get("build_graph")
    if isinstance(build_graph, dict):
        sanitized_graph = _json_clone(build_graph)
        sanitized_graph.pop("approval_state", None)
        sanitized_graph.pop("review_provenance", None)
        cloned["build_graph"] = sanitized_graph
    return cloned


def _bind_candidate_payload(record: dict[str, Any], current: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = record.get("candidate_payload")
    if isinstance(payload, dict) and payload:
        return _json_clone(payload)
    candidate_ref = _text(record.get("candidate_ref"))
    if candidate_ref and isinstance(current, dict):
        for candidate in current.get("candidate_targets") if isinstance(current.get("candidate_targets"), list) else []:
            if not isinstance(candidate, dict):
                continue
            if _text(candidate.get("target_ref")) == candidate_ref:
                return _json_clone(candidate)
    if candidate_ref:
        return {
            "target_ref": candidate_ref,
            "label": candidate_ref,
            "kind": "reference",
        }
    return None


def _review_record_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_decision_id": _text(record.get("review_decision_id")) or None,
        "review_group_ref": _text(record.get("review_group_ref")) or None,
        "target_kind": _text(record.get("target_kind")) or None,
        "target_ref": _text(record.get("target_ref")) or None,
        "slot_ref": _text(record.get("slot_ref")) or None,
        "decision": _text(record.get("decision")) or None,
        "actor_type": _text(record.get("actor_type")) or None,
        "actor_ref": _text(record.get("actor_ref")) or None,
        "authority_scope": _text(record.get("authority_scope")) or None,
        "approval_mode": _text(record.get("approval_mode")) or None,
        "rationale": _text(record.get("rationale")) or None,
        "source_subpath": _text(record.get("source_subpath")) or None,
        "supersedes_decision_ref": _text(record.get("supersedes_decision_ref")) or None,
        "candidate_ref": _text(record.get("candidate_ref")) or None,
        "candidate_payload": _json_clone(record.get("candidate_payload")),
        "decided_at": _text(record.get("decided_at")) or None,
        "created_at": _text(record.get("created_at")) or None,
    }


def _workflow_shape_review_ref(build_graph: dict[str, Any]) -> str | None:
    definition_revision = _text(build_graph.get("definition_revision"))
    if definition_revision:
        return f"workflow_shape:{definition_revision}"
    graph_id = _text(build_graph.get("graph_id"))
    return graph_id or None


def effective_workflow_build_review_state(
    conn: Any,
    *,
    workflow_id: str,
    definition_revision: str,
) -> dict[str, Any]:
    latest_records = list_latest_workflow_build_review_decisions(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
    latest_by_target = {
        (_text(record.get("target_kind")), _text(record.get("target_ref"))): record
        for record in latest_records
        if isinstance(record, dict)
    }
    approval_records = [
        _review_record_payload(record)
        for record in latest_records
        if isinstance(record, dict)
    ]
    approved_binding_refs: list[str] = []
    approved_import_snapshot_refs: list[str] = []
    approved_bundle_refs: list[str] = []
    approved_workflow_shape_ref: str | None = None
    proposal_requests: list[dict[str, Any]] = []
    widening_ops: list[dict[str, Any]] = []
    for record in latest_records:
        if not isinstance(record, dict):
            continue
        decision = _text(record.get("decision")).lower()
        target_kind = _text(record.get("target_kind"))
        target_ref = _text(record.get("target_ref"))
        if decision == "approve":
            if target_kind == "binding":
                approved_binding_refs.append(target_ref)
            elif target_kind == "import_snapshot":
                approved_import_snapshot_refs.append(target_ref)
            elif target_kind == "capability_bundle":
                approved_bundle_refs.append(target_ref)
            elif target_kind == "workflow_shape":
                approved_workflow_shape_ref = target_ref
        elif decision == "proposal_request":
            proposal_requests.append(_review_record_payload(record))
        elif decision == "widen":
            widening_ops.append(_review_record_payload(record))
    review_group_ref = (
        _text(latest_records[0].get("review_group_ref"))
        if latest_records and isinstance(latest_records[0], dict)
        else None
    )
    return {
        "review_group_ref": review_group_ref,
        "latest_records": latest_records,
        "latest_by_target": latest_by_target,
        "approval_records": approval_records,
        "approved_binding_refs": approved_binding_refs,
        "approved_import_snapshot_refs": approved_import_snapshot_refs,
        "approved_bundle_refs": approved_bundle_refs,
        "approved_workflow_shape_ref": approved_workflow_shape_ref,
        "proposal_requests": proposal_requests,
        "widening_ops": widening_ops,
    }


def materialize_reviewed_build_definition(
    conn: Any,
    *,
    workflow_id: str,
    definition: dict[str, Any],
    compiled_spec: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    from runtime.build_authority import apply_authority_bundle

    base_definition = definition if isinstance(definition, dict) else {}
    definition_revision = _text(base_definition.get("definition_revision"))
    if not definition_revision:
        return apply_authority_bundle(base_definition, compiled_spec=compiled_spec), False

    effective_state = effective_workflow_build_review_state(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
    latest_records = effective_state["latest_records"]
    if not latest_records:
        return apply_authority_bundle(base_definition, compiled_spec=compiled_spec), False

    materialized = apply_authority_bundle(
        _scrub_binding_review_state(base_definition),
        compiled_spec=compiled_spec,
    )
    binding_index = {
        _text(entry.get("binding_id")): entry
        for entry in materialized.get("binding_ledger", [])
        if isinstance(entry, dict) and _text(entry.get("binding_id"))
    }
    snapshot_index = {
        _text(entry.get("snapshot_id")): entry
        for entry in materialized.get("import_snapshots", [])
        if isinstance(entry, dict) and _text(entry.get("snapshot_id"))
    }

    for record in latest_records:
        target_kind = _text(record.get("target_kind"))
        target_ref = _text(record.get("target_ref"))
        decision = _text(record.get("decision"))
        if target_kind == "binding" and target_ref in binding_index:
            binding = binding_index[target_ref]
            if decision == "approve":
                binding["state"] = "accepted"
                binding["accepted_target"] = _bind_candidate_payload(record, binding)
                binding["rationale"] = _text(record.get("rationale")) or "Explicitly approved through build review."
            elif decision == "reject":
                binding["state"] = "rejected"
                binding["accepted_target"] = None
                binding["rationale"] = _text(record.get("rationale")) or "Explicitly rejected through build review."
            elif decision == "revoke":
                candidate_targets = binding.get("candidate_targets") if isinstance(binding.get("candidate_targets"), list) else []
                binding["state"] = "suggested" if candidate_targets else "captured"
                binding["accepted_target"] = None
                binding["rationale"] = (
                    _text(record.get("rationale")) or "Review approval revoked; explicit approval is required again."
                )
        elif target_kind == "import_snapshot" and target_ref in snapshot_index:
            snapshot = snapshot_index[target_ref]
            if decision == "approve":
                candidate_payload = _bind_candidate_payload(record, None)
                snapshot["approval_state"] = "admitted"
                snapshot["admitted_targets"] = [candidate_payload] if isinstance(candidate_payload, dict) else []
            elif decision == "revoke":
                snapshot["approval_state"] = "staged"
                snapshot["admitted_targets"] = []
            elif decision == "reject":
                snapshot["approval_state"] = "staged"
                snapshot["admitted_targets"] = []
    build_graph = materialized.get("build_graph") if isinstance(materialized.get("build_graph"), dict) else None
    if isinstance(build_graph, dict):
        graph_id = _text(build_graph.get("graph_id"))
        review_ref = _workflow_shape_review_ref(build_graph)
        if review_ref or graph_id:
            workflow_shape_record = None
            if review_ref:
                workflow_shape_record = effective_state["latest_by_target"].get(("workflow_shape", review_ref))
            if workflow_shape_record is None and graph_id:
                workflow_shape_record = effective_state["latest_by_target"].get(("workflow_shape", graph_id))
            decision = _text((workflow_shape_record or {}).get("decision")).lower()
            build_graph["approval_state"] = "approved" if decision == "approve" else "unapproved"
            build_graph["review_provenance"] = (
                _review_record_payload(workflow_shape_record)
                if isinstance(workflow_shape_record, dict)
                else None
            )
    materialized["review_state"] = {
        "review_group_ref": effective_state["review_group_ref"],
        "approval_records": _json_clone(effective_state["approval_records"]),
        "approved_binding_refs": _json_clone(effective_state["approved_binding_refs"]),
        "approved_import_snapshot_refs": _json_clone(effective_state["approved_import_snapshot_refs"]),
        "approved_bundle_refs": _json_clone(effective_state["approved_bundle_refs"]),
        "approved_workflow_shape_ref": effective_state["approved_workflow_shape_ref"],
        "proposal_requests": _json_clone(effective_state["proposal_requests"]),
        "widening_ops": _json_clone(effective_state["widening_ops"]),
    }
    return materialized, True


def scrub_review_state_for_persistence(definition: dict[str, Any]) -> dict[str, Any]:
    return _scrub_binding_review_state(definition if isinstance(definition, dict) else {})


__all__ = [
    "build_review_decision_undo_receipt",
    "effective_workflow_build_review_state",
    "materialize_reviewed_build_definition",
    "record_build_review_decision",
    "scrub_review_state_for_persistence",
]
