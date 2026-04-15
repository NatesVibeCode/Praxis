"""DB-backed review authority for workflow build planning."""

from __future__ import annotations

import json
from typing import Any

from runtime.event_log import CHANNEL_BUILD_STATE, EVENT_REVIEW_DECISION, emit
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
    candidate_ref: str | None = None,
    candidate_payload: object | None = None,
) -> dict[str, Any]:
    record = record_workflow_build_review_decision(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
        target_kind=target_kind,
        target_ref=target_ref,
        decision=decision,
        actor_type=actor_type,
        actor_ref=actor_ref,
        approval_mode=approval_mode,
        rationale=rationale,
        source_subpath=source_subpath,
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
) -> dict[str, Any]:
    previous = get_latest_workflow_build_review_decision(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
        target_kind=target_kind,
        target_ref=target_ref,
    )
    body: dict[str, Any] = {
        "target_kind": target_kind,
        "target_ref": target_ref,
    }
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

    latest_records = list_latest_workflow_build_review_decisions(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
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
    return apply_authority_bundle(materialized, compiled_spec=compiled_spec), True


def scrub_review_state_for_persistence(definition: dict[str, Any]) -> dict[str, Any]:
    return _scrub_binding_review_state(definition if isinstance(definition, dict) else {})


__all__ = [
    "build_review_decision_undo_receipt",
    "materialize_reviewed_build_definition",
    "record_build_review_decision",
    "scrub_review_state_for_persistence",
]
