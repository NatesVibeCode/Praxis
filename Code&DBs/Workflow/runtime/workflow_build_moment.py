"""Runtime-owned Canvas build moment assembly."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from runtime.build_authority import apply_authority_bundle, build_authority_bundle
from runtime.build_planning_contract import (
    build_candidate_resolution_manifest,
    build_intent_brief,
    build_reviewable_plan,
)
from runtime.build_review_decisions import materialize_reviewed_build_definition
from runtime.operating_model_planner import current_compiled_spec
from runtime.payload_coercion import (
    coerce_isoformat,
    coerce_text,
    parse_json_field,
)


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _serialize_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _serialize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_json(item) for item in value]
    if is_dataclass(value):
        return {key: _serialize_json(item) for key, item in asdict(value).items()}
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _normalized_execution_manifest(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    if "tool_allowlist" in value and "verify_refs" in value and "materialized_spec" in value:
        return _json_clone(value)
    tool_allowlist = value.get("tool_allowlist_json")
    if not isinstance(tool_allowlist, dict):
        return None
    materialized_spec = value.get("materialized_spec_json")
    policy_gates = value.get("policy_gates_json")
    hardening_report = value.get("hardening_report_json")
    return {
        "execution_manifest_ref": coerce_text(value.get("execution_manifest_ref")) or None,
        "workflow_id": coerce_text(value.get("workflow_id")) or None,
        "definition_revision": coerce_text(value.get("definition_revision")) or None,
        "manifest_ref": coerce_text(value.get("manifest_ref")) or None,
        "review_group_ref": coerce_text(value.get("review_group_ref")) or None,
        "approved_bundle_refs": [
            item
            for item in (
                _json_clone(value.get("approved_bundle_refs_json"))
                if isinstance(value.get("approved_bundle_refs_json"), list)
                else []
            )
            if coerce_text(item)
        ],
        "tool_allowlist": _json_clone(tool_allowlist),
        "verify_refs": [
            item
            for item in (
                _json_clone(value.get("verify_refs_json"))
                if isinstance(value.get("verify_refs_json"), list)
                else []
            )
            if coerce_text(item)
        ],
        "materialized_spec": _json_clone(materialized_spec) if isinstance(materialized_spec, dict) else {},
        "policy_gates": _json_clone(policy_gates) if isinstance(policy_gates, dict) else {},
        "hardening_report": _json_clone(hardening_report) if isinstance(hardening_report, dict) else {},
    }


def load_latest_workflow_build_execution_manifest(
    conn: Any,
    *,
    workflow_id: str | None,
    definition_revision: str | None,
) -> dict[str, Any] | None:
    normalized_workflow_id = coerce_text(workflow_id)
    normalized_definition_revision = coerce_text(definition_revision)
    if not normalized_workflow_id or not normalized_definition_revision:
        return None
    try:
        from storage.postgres.workflow_build_planning_repository import (
            load_latest_workflow_build_execution_manifest as _load_latest_workflow_build_execution_manifest,
        )
    except Exception:
        return None
    try:
        manifest = _load_latest_workflow_build_execution_manifest(
            conn,
            workflow_id=normalized_workflow_id,
            definition_revision=normalized_definition_revision,
        )
    except Exception:
        return None
    return _normalized_execution_manifest(manifest)


def build_workflow_build_moment(
    row: dict[str, Any],
    *,
    conn: Any | None = None,
    definition: dict[str, Any] | None = None,
    materialized_spec: dict[str, Any] | None = None,
    build_bundle: dict[str, Any] | None = None,
    planning_notes: list[str] | None = None,
    intent_brief: dict[str, Any] | None = None,
    execution_manifest: dict[str, Any] | None = None,
    progressive_build: dict[str, Any] | None = None,
    undo_receipt: dict[str, Any] | None = None,
    mutation_event_id: int | None = None,
    compile_preview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    workflow_id = coerce_text(row.get("id")) or None
    effective_definition = parse_json_field(definition if definition is not None else row.get("definition")) or {}
    effective_compiled_spec = parse_json_field(
        materialized_spec if materialized_spec is not None else row.get("materialized_spec")
    )
    effective_build_bundle = build_bundle if isinstance(build_bundle, dict) else None

    if effective_build_bundle is None:
        current_plan = current_compiled_spec(effective_definition, effective_compiled_spec)
        if conn is not None:
            effective_definition, _ = materialize_reviewed_build_definition(
                conn,
                workflow_id=workflow_id,
                definition=effective_definition,
                materialized_spec=current_plan,
            )
        else:
            effective_definition = apply_authority_bundle(
                effective_definition,
                materialized_spec=current_plan,
            )
        effective_compiled_spec = current_plan
        effective_build_bundle = build_authority_bundle(
            effective_definition,
            materialized_spec=current_plan,
        )
        if not isinstance(intent_brief, dict):
            intent_brief = build_intent_brief(
                definition=effective_definition,
                workflow_id=workflow_id,
                conn=conn,
            )
        if not isinstance(execution_manifest, dict) and conn is not None:
            execution_manifest = load_latest_workflow_build_execution_manifest(
                conn,
                workflow_id=workflow_id,
                definition_revision=coerce_text(effective_definition.get("definition_revision")) or None,
            )
    elif not isinstance(intent_brief, dict):
        intent_brief = build_intent_brief(
            definition=effective_definition,
            workflow_id=workflow_id,
            conn=conn,
        )

    blocking_issues = [
        issue
        for issue in effective_build_bundle.get("build_issues", [])
        if isinstance(issue, dict) and coerce_text(issue.get("severity")) == "blocking"
    ]
    candidate_resolution_manifest = build_candidate_resolution_manifest(
        definition=effective_definition,
        workflow_id=workflow_id,
        conn=conn,
        materialized_spec=effective_compiled_spec,
    )
    reviewable_plan = build_reviewable_plan(
        definition=effective_definition,
        workflow_id=workflow_id,
        conn=conn,
        materialized_spec=effective_compiled_spec,
        candidate_manifest=candidate_resolution_manifest,
    )
    review_state = effective_definition.get("review_state")
    effective_progressive_build = (
        progressive_build
        if isinstance(progressive_build, dict)
        else effective_definition.get("progressive_build")
    )
    return {
        "workflow": {
            "id": row["id"],
            "name": row.get("name"),
            "description": row.get("description"),
            "version": int(row.get("version") or 1),
            "updated_at": coerce_isoformat(row.get("updated_at")),
        },
        "intent_brief": intent_brief or {},
        "definition": effective_definition,
        "materialized_spec": effective_compiled_spec,
        "planning_notes": planning_notes or [],
        "build_state": coerce_text(effective_build_bundle.get("projection_status", {}).get("state")) or "blocked",
        "build_blockers": blocking_issues,
        "build_graph": effective_build_bundle.get("build_graph"),
        "binding_ledger": effective_build_bundle.get("binding_ledger") or [],
        "import_snapshots": effective_build_bundle.get("import_snapshots") or [],
        "authority_attachments": effective_build_bundle.get("authority_attachments") or [],
        "review_state": _serialize_json(review_state) if isinstance(review_state, dict) else {},
        "build_issues": effective_build_bundle.get("build_issues") or [],
        "projection_status": effective_build_bundle.get("projection_status") or {},
        "materialized_spec_projection": effective_build_bundle.get("materialized_spec_projection"),
        "candidate_resolution_manifest": candidate_resolution_manifest,
        "reviewable_plan": reviewable_plan,
        "execution_manifest": execution_manifest,
        "progressive_build": (
            _serialize_json(effective_progressive_build)
            if isinstance(effective_progressive_build, dict)
            else None
        ),
        "compile_preview": compile_preview if isinstance(compile_preview, dict) else None,
        "undo_receipt": undo_receipt,
        "mutation_event_id": mutation_event_id,
    }


__all__ = [
    "build_workflow_build_moment",
    "load_latest_workflow_build_execution_manifest",
]
