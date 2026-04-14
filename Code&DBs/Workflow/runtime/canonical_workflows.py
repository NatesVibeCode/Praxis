"""Canonical workflow runtime ownership for mutation surfaces."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Any
import uuid

from storage.postgres.workflow_runtime_repository import (
    delete_workflow_record,
    load_workflow_record,
    persist_workflow_build_record,
    persist_workflow_record,
    reconcile_workflow_triggers,
    record_system_event,
    record_workflow_invocation,
    update_workflow_trigger_record,
    update_workflow_record,
    upsert_workflow_trigger_record,
)


class WorkflowRuntimeBoundaryError(RuntimeError):
    """Raised when canonical workflow runtime ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


_UNSET = object()
_TRIGGER_MANUAL_ROUTE = "trigger"
_TRIGGER_SCHEDULE_ROUTE = "trigger/schedule"
_TRIGGER_WEBHOOK_ROUTE = "trigger/webhook"
_WEBHOOK_TRIGGER_EVENT_TYPE = "db.webhook_events.insert"


def _is_trigger_route(route: str) -> bool:
    normalized = _text(route)
    return normalized in {
        _TRIGGER_MANUAL_ROUTE,
        _TRIGGER_SCHEDULE_ROUTE,
        _TRIGGER_WEBHOOK_ROUTE,
    }


def _build_graph_trigger_intent(
    node: dict[str, Any],
    *,
    index: int,
    existing_trigger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route = _text(node.get("route"))
    node_id = _text(node.get("node_id") or node.get("id"))
    trigger_config = node.get("trigger") if isinstance(node.get("trigger"), dict) else {}
    payload = dict(existing_trigger or {})
    payload["id"] = _text(payload.get("id")) or f"trigger-{index:03d}"
    payload["title"] = _text(node.get("title")) or _text(payload.get("title")) or f"Trigger {index}"
    payload["summary"] = (
        _text(node.get("summary"))
        or _text(payload.get("summary"))
        or _text(node.get("title"))
        or payload["id"]
    )
    payload["source_node_id"] = node_id
    payload["source_block_ids"] = [
        source_id
        for source_id in (node.get("source_block_ids") or [])
        if isinstance(source_id, str)
    ]
    payload["reference_slugs"] = [
        slug
        for slug in (payload.get("reference_slugs") or [])
        if isinstance(slug, str) and slug.strip()
    ]
    if isinstance(trigger_config.get("filter"), dict):
        payload["filter"] = dict(trigger_config.get("filter") or {})
    else:
        payload["filter"] = dict(payload.get("filter") or {}) if isinstance(payload.get("filter"), dict) else {}

    if route == _TRIGGER_SCHEDULE_ROUTE:
        payload["event_type"] = "schedule"
        cron_expression = _text(trigger_config.get("cron_expression")) or _text(payload.get("cron_expression")) or "@daily"
        if cron_expression:
            payload["cron_expression"] = cron_expression
        else:
            payload.pop("cron_expression", None)
    elif route == _TRIGGER_WEBHOOK_ROUTE:
        payload["event_type"] = _WEBHOOK_TRIGGER_EVENT_TYPE
        payload.pop("cron_expression", None)
        source_ref = _text(trigger_config.get("source_ref")) or _text(payload.get("source_ref"))
        if source_ref:
            payload["source_ref"] = source_ref
        else:
            payload.pop("source_ref", None)
    else:
        payload["event_type"] = "manual"
        payload.pop("cron_expression", None)
        source_ref = _text(trigger_config.get("source_ref")) or _text(payload.get("source_ref"))
        if source_ref:
            payload["source_ref"] = source_ref
        else:
            payload.pop("source_ref", None)

    return payload


def commit_workflow(
    conn: Any,
    *,
    title: str,
    definition: dict[str, Any],
    compiled_spec: dict[str, Any] | None,
    workflow_id: str | None = None,
) -> dict[str, Any]:
    from runtime.operating_model_planner import current_compiled_spec

    normalized_title = _text(title)
    if not normalized_title:
        raise WorkflowRuntimeBoundaryError("title is required")
    if not isinstance(definition, dict):
        raise WorkflowRuntimeBoundaryError("definition is required and must be an object")
    if compiled_spec is not None and not isinstance(compiled_spec, dict):
        raise WorkflowRuntimeBoundaryError("compiled_spec must be an object")

    normalized_workflow_id = _text(workflow_id) or ("wf_" + uuid.uuid4().hex[:12])
    persisted_compiled_spec = current_compiled_spec(definition, compiled_spec)
    description_source = definition.get("compiled_prose") or definition.get("source_prose") or normalized_title
    description = (
        description_source[:200]
        if isinstance(description_source, str)
        else str(description_source)[:200]
    )
    persist_workflow_record(
        conn,
        workflow_id=normalized_workflow_id,
        name=normalized_title,
        description=description,
        definition=definition,
        compiled_spec=persisted_compiled_spec,
    )
    triggers = reconcile_workflow_triggers(
        conn,
        workflow_id=normalized_workflow_id,
        compiled_spec=persisted_compiled_spec,
    )
    return {
        "workflow_id": normalized_workflow_id,
        "status": "committed",
        "title": normalized_title,
        "jobs": len(persisted_compiled_spec.get("jobs", [])) if isinstance(persisted_compiled_spec, dict) else 0,
        "triggers": len(triggers),
        "has_current_plan": persisted_compiled_spec is not None,
    }


def save_workflow(
    conn: Any,
    *,
    workflow_id: str | None,
    body: dict[str, Any],
) -> dict[str, Any]:
    from runtime.operating_model_planner import current_compiled_spec

    if workflow_id is None:
        normalized_workflow_id = _text(body.get("id")) or ("wf_" + uuid.uuid4().hex[:12])
        definition = body.get("definition")
        if not isinstance(definition, dict):
            raise WorkflowRuntimeBoundaryError("definition is required and must be an object")
        row = persist_workflow_record(
            conn,
            workflow_id=normalized_workflow_id,
            name=_text(body.get("name")),
            description=_text(body.get("description")),
            definition=definition,
            compiled_spec=current_compiled_spec(definition, body.get("compiled_spec")),
            tags=body.get("tags"),
            is_template=body.get("is_template"),
        )
        reconcile_workflow_triggers(
            conn,
            workflow_id=normalized_workflow_id,
            compiled_spec=_parse_json_field(row.get("compiled_spec")),
        )
        return row

    normalized_workflow_id = _text(workflow_id)
    if not normalized_workflow_id:
        raise WorkflowRuntimeBoundaryError("workflow id is required")
    current_row = load_workflow_record(conn, workflow_id=normalized_workflow_id)
    if current_row is None:
        raise WorkflowRuntimeBoundaryError(f"Workflow not found: {normalized_workflow_id}", status_code=404)

    should_refresh_compiled_spec = "definition" in body or "compiled_spec" in body
    persisted_compiled_spec: dict[str, Any] | None | object = _UNSET
    if should_refresh_compiled_spec:
        current_definition = (
            body["definition"]
            if "definition" in body and isinstance(body.get("definition"), dict)
            else _parse_json_field(current_row.get("definition")) or {}
        )
        current_compiled_spec_row = (
            body.get("compiled_spec")
            if "compiled_spec" in body
            else _parse_json_field(current_row.get("compiled_spec"))
        )
        persisted_compiled_spec = current_compiled_spec(
            current_definition,
            current_compiled_spec_row,
        )

    kwargs: dict[str, Any] = {}
    if "name" in body:
        kwargs["name"] = _text(body.get("name"))
    if "description" in body:
        kwargs["description"] = body.get("description")
    if "definition" in body:
        kwargs["definition"] = body.get("definition")
    if persisted_compiled_spec is not _UNSET:
        kwargs["compiled_spec"] = persisted_compiled_spec
    if "tags" in body:
        kwargs["tags"] = body.get("tags") or []
    if "is_template" in body:
        kwargs["is_template"] = body.get("is_template")

    row = update_workflow_record(
        conn,
        workflow_id=normalized_workflow_id,
        **kwargs,
    )
    if row is None:
        raise WorkflowRuntimeBoundaryError(f"Workflow not found: {normalized_workflow_id}", status_code=404)
    if persisted_compiled_spec is not _UNSET:
        reconcile_workflow_triggers(
            conn,
            workflow_id=normalized_workflow_id,
            compiled_spec=persisted_compiled_spec,
        )
    return row


def save_workflow_trigger(
    conn: Any,
    *,
    body: dict[str, Any],
) -> dict[str, Any]:
    normalized_workflow_id = _text(body.get("workflow_id"))
    if not normalized_workflow_id:
        raise WorkflowRuntimeBoundaryError("workflow_id is required")
    normalized_event_type = _text(body.get("event_type"))
    if not normalized_event_type:
        raise WorkflowRuntimeBoundaryError("event_type is required")

    workflow_row = load_workflow_record(conn, workflow_id=normalized_workflow_id)
    if workflow_row is None:
        raise WorkflowRuntimeBoundaryError(f"Workflow not found: {normalized_workflow_id}", status_code=404)

    row = upsert_workflow_trigger_record(
        conn,
        trigger_id=_text(body.get("id")) or ("trg_" + uuid.uuid4().hex[:12]),
        workflow_id=normalized_workflow_id,
        event_type=normalized_event_type,
        trigger_filter=body.get("filter", {}),
        cron_expression=body.get("cron_expression"),
        enabled=body.get("enabled", True),
    )
    row["workflow_name"] = workflow_row.get("name")
    return row


def update_workflow_trigger(
    conn: Any,
    *,
    trigger_id: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    normalized_trigger_id = _text(trigger_id)
    if not normalized_trigger_id:
        raise WorkflowRuntimeBoundaryError("trigger id is required")
    if not body:
        raise WorkflowRuntimeBoundaryError("No trigger fields provided for update")

    update_kwargs: dict[str, Any] = {}
    workflow_row: dict[str, Any] | None = None

    if "workflow_id" in body:
        normalized_workflow_id = _text(body.get("workflow_id"))
        if not normalized_workflow_id:
            raise WorkflowRuntimeBoundaryError("workflow_id must be a non-empty string")
        workflow_row = load_workflow_record(conn, workflow_id=normalized_workflow_id)
        if workflow_row is None:
            raise WorkflowRuntimeBoundaryError(f"Workflow not found: {normalized_workflow_id}", status_code=404)
        update_kwargs["workflow_id"] = normalized_workflow_id
    if "event_type" in body:
        normalized_event_type = _text(body.get("event_type"))
        if not normalized_event_type:
            raise WorkflowRuntimeBoundaryError("event_type must be a non-empty string")
        update_kwargs["event_type"] = normalized_event_type
    if "filter" in body:
        update_kwargs["trigger_filter"] = body.get("filter")
    if "cron_expression" in body:
        update_kwargs["cron_expression"] = body.get("cron_expression")
    if "enabled" in body:
        update_kwargs["enabled"] = body["enabled"]

    row = update_workflow_trigger_record(
        conn,
        trigger_id=normalized_trigger_id,
        **update_kwargs,
    )
    if row is None:
        raise WorkflowRuntimeBoundaryError(f"Trigger not found: {normalized_trigger_id}", status_code=404)
    if workflow_row is None:
        workflow_row = load_workflow_record(conn, workflow_id=str(row.get("workflow_id") or ""))
    row["workflow_name"] = workflow_row.get("name") if isinstance(workflow_row, dict) else None
    return row


def mutate_workflow_build(
    conn: Any,
    *,
    workflow_id: str,
    subpath: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    from runtime.build_authority import (
        admit_import_snapshot,
        attach_authority,
        stage_import_snapshot,
        upsert_binding,
    )

    row = load_workflow_record(conn, workflow_id=workflow_id)
    if row is None:
        raise WorkflowRuntimeBoundaryError(f"Workflow not found: {workflow_id}", status_code=404)
    definition = _parse_json_field(row.get("definition")) or {}

    if subpath == "attachments":
        node_id = _text(body.get("node_id"))
        authority_kind = _text(body.get("authority_kind"))
        authority_ref = _text(body.get("authority_ref"))
        role = _text(body.get("role")) or "input"
        if not node_id or not authority_kind or not authority_ref:
            raise WorkflowRuntimeBoundaryError("node_id, authority_kind, and authority_ref are required")
        definition = attach_authority(
            definition,
            node_id=node_id,
            authority_kind=authority_kind,
            authority_ref=authority_ref,
            role=role,
            label=_text(body.get("label")) or None,
            promote_to_state=bool(body.get("promote_to_state")),
        )
    elif subpath.startswith("bindings/") and subpath.endswith("/accept"):
        binding_id = subpath[len("bindings/") : -len("/accept")].strip("/")
        accepted_target = body.get("accepted_target")
        if not binding_id or not isinstance(accepted_target, dict):
            raise WorkflowRuntimeBoundaryError("accepted_target is required")
        definition = upsert_binding(
            definition,
            binding_id=binding_id,
            state="accepted",
            accepted_target=accepted_target,
            candidate_targets=body.get("candidate_targets") if isinstance(body.get("candidate_targets"), list) else None,
            rationale=_text(body.get("rationale")) or "Accepted in build workspace.",
        )
    elif subpath.startswith("bindings/") and subpath.endswith("/reject"):
        binding_id = subpath[len("bindings/") : -len("/reject")].strip("/")
        if not binding_id:
            raise WorkflowRuntimeBoundaryError("binding id is required")
        definition = upsert_binding(
            definition,
            binding_id=binding_id,
            state="rejected",
            rationale=_text(body.get("rationale")) or "Rejected in build workspace.",
        )
    elif subpath.startswith("bindings/") and subpath.endswith("/replace"):
        binding_id = subpath[len("bindings/") : -len("/replace")].strip("/")
        accepted_target = body.get("accepted_target")
        if not binding_id or not isinstance(accepted_target, dict):
            raise WorkflowRuntimeBoundaryError("accepted_target is required")
        definition = upsert_binding(
            definition,
            binding_id=binding_id,
            state="accepted",
            accepted_target=accepted_target,
            candidate_targets=body.get("candidate_targets") if isinstance(body.get("candidate_targets"), list) else None,
            rationale=_text(body.get("rationale")) or "Replaced in build workspace.",
        )
    elif subpath == "imports":
        source_locator = _text(body.get("source_locator"))
        if not source_locator:
            raise WorkflowRuntimeBoundaryError("source_locator is required")
        definition = stage_import_snapshot(
            definition,
            node_id=_text(body.get("node_id")) or None,
            source_kind=_text(body.get("source_kind")) or "net",
            source_locator=source_locator,
            requested_shape=body.get("requested_shape") if isinstance(body.get("requested_shape"), dict) else None,
            payload=body.get("payload"),
            freshness_ttl=int(body.get("freshness_ttl") or 3600),
        )
    elif subpath.startswith("imports/") and subpath.endswith("/admit"):
        snapshot_id = subpath[len("imports/") : -len("/admit")].strip("/")
        admitted_target = body.get("admitted_target")
        if not snapshot_id or not isinstance(admitted_target, dict):
            raise WorkflowRuntimeBoundaryError("admitted_target is required")
        definition = admit_import_snapshot(
            definition,
            snapshot_id=snapshot_id,
            admitted_target=admitted_target,
        )
    elif subpath == "materialize-here":
        node_id = _text(body.get("node_id"))
        if not node_id:
            raise WorkflowRuntimeBoundaryError("node_id is required")
        source_locator = _text(body.get("source_locator"))
        snapshot_id = _text(body.get("snapshot_id"))
        admitted_target = body.get("admitted_target") if isinstance(body.get("admitted_target"), dict) else None
        if source_locator:
            definition = stage_import_snapshot(
                definition,
                node_id=node_id,
                source_kind=_text(body.get("source_kind")) or "net",
                source_locator=source_locator,
                requested_shape=body.get("requested_shape") if isinstance(body.get("requested_shape"), dict) else None,
                payload=body.get("payload"),
                freshness_ttl=int(body.get("freshness_ttl") or 3600),
            )
            if not snapshot_id:
                snapshots = definition.get("import_snapshots") if isinstance(definition.get("import_snapshots"), list) else []
                if snapshots:
                    snapshot_id = _text(snapshots[-1].get("snapshot_id"))
        if snapshot_id and admitted_target is not None:
            definition = admit_import_snapshot(
                definition,
                snapshot_id=snapshot_id,
                admitted_target=admitted_target,
            )
        authority_kind = _text(body.get("authority_kind"))
        authority_ref = _text(body.get("authority_ref")) or _text((admitted_target or {}).get("target_ref"))
        if authority_kind and authority_ref:
            definition = attach_authority(
                definition,
                node_id=node_id,
                authority_kind=authority_kind,
                authority_ref=authority_ref,
                role=_text(body.get("role")) or "input",
                label=_text(body.get("label")) or _text((admitted_target or {}).get("label")) or None,
                promote_to_state=bool(body.get("promote_to_state")),
            )
    elif subpath == "build_graph":
        nodes = body.get("nodes") if isinstance(body.get("nodes"), list) else []
        edges = body.get("edges") if isinstance(body.get("edges"), list) else []
        node_routes = {
            _text(node.get("node_id") or node.get("id")): _text(node.get("route"))
            for node in nodes
            if isinstance(node, dict) and _text(node.get("node_id") or node.get("id"))
        }

        # Build dependency map from sequence edges (skip gate/state edges)
        incoming: dict[str, list[str]] = {}
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            if _text(edge.get("kind") or "") == "authority_gate":
                continue
            to_id = _text(edge.get("to_node_id"))
            from_id = _text(edge.get("from_node_id"))
            if _is_trigger_route(node_routes.get(from_id, "")) or _is_trigger_route(node_routes.get(to_id, "")):
                continue
            if to_id and from_id:
                incoming.setdefault(to_id, []).append(from_id)

        # Preserve existing phase metadata (prompts, inputs, outputs, etc.)
        existing_phases: dict[str, dict[str, Any]] = {}
        existing_setup = definition.get("execution_setup") if isinstance(definition.get("execution_setup"), dict) else {}
        for phase in (existing_setup.get("phases") or []):
            if isinstance(phase, dict) and _text(phase.get("step_id")):
                existing_phases[_text(phase.get("step_id"))] = dict(phase)
        existing_triggers: dict[str, dict[str, Any]] = {}
        for trigger in definition.get("trigger_intent", []) if isinstance(definition.get("trigger_intent"), list) else []:
            if not isinstance(trigger, dict):
                continue
            source_node_id = _text(trigger.get("source_node_id"))
            if source_node_id:
                existing_triggers[source_node_id] = dict(trigger)

        draft_flow: list[dict[str, Any]] = []
        new_phases: list[dict[str, Any]] = []
        new_triggers: list[dict[str, Any]] = []
        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            node_id = _text(node.get("node_id") or node.get("id"))
            if not node_id:
                continue
            if _text(node.get("kind") or "step") not in ("step", ""):
                continue  # skip gate and state nodes
            route = _text(node.get("route"))
            if _is_trigger_route(route):
                new_triggers.append(
                    _build_graph_trigger_intent(
                        node,
                        index=len(new_triggers) + 1,
                        existing_trigger=existing_triggers.get(node_id),
                    )
                )
                continue
            draft_flow.append({
                "id": node_id,
                "order": i,
                "title": _text(node.get("title")) or f"Step {i + 1}",
                "summary": _text(node.get("summary")) or "",
                "depends_on": incoming.get(node_id, []),
                "source_block_ids": [s for s in (node.get("source_block_ids") or []) if isinstance(s, str)],
            })
            phase = dict(existing_phases.get(node_id, {}))
            phase["step_id"] = node_id
            if route:
                phase["agent_route"] = route
            if phase.get("agent_route"):
                new_phases.append(phase)

        definition["draft_flow"] = draft_flow
        definition["trigger_intent"] = new_triggers
        if not isinstance(definition.get("execution_setup"), dict):
            definition["execution_setup"] = {}
        definition["execution_setup"]["phases"] = new_phases

        # Persist edge gate rules (from UI gate chips on sequence edges)
        edge_gates: list[dict[str, Any]] = []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            gate = edge.get("gate")
            if not isinstance(gate, dict) or not _text(gate.get("family")):
                continue
            from_id = _text(edge.get("from_node_id"))
            to_id = _text(edge.get("to_node_id"))
            if from_id and to_id:
                edge_gates.append({
                    "edge_id": _text(edge.get("edge_id")) or f"edge-{from_id}-{to_id}",
                    "from_node_id": from_id,
                    "to_node_id": to_id,
                    "family": _text(gate.get("family")),
                    "label": _text(gate.get("label")) or "",
                })
        definition["execution_setup"]["edge_gates"] = edge_gates
    else:
        raise WorkflowRuntimeBoundaryError(
            f"Unknown build endpoint: /api/workflows/{workflow_id}/build/{subpath}",
            status_code=404,
        )

    hydrated_definition, compiled_spec, build_bundle, planning_notes = _rebuild_workflow_build(
        definition,
        workflow_name=_text(row.get("name")) or workflow_id,
        conn=conn,
    )
    persisted_row = persist_workflow_build_record(
        conn,
        workflow_id=workflow_id,
        workflow_name=_text(row.get("name")) or workflow_id,
        existing_description=_text(row.get("description")) or None,
        definition=hydrated_definition,
        compiled_spec=compiled_spec,
    )
    reconcile_workflow_triggers(
        conn,
        workflow_id=workflow_id,
        compiled_spec=compiled_spec,
    )

    # Emit to the service bus
    from runtime.event_log import emit, CHANNEL_BUILD_STATE, EVENT_MUTATION
    emit(
        conn,
        channel=CHANNEL_BUILD_STATE,
        event_type=EVENT_MUTATION,
        entity_id=workflow_id,
        entity_kind="workflow",
        payload={"subpath": subpath},
        emitted_by="mutate_workflow_build",
    )

    return {
        "row": persisted_row,
        "definition": hydrated_definition,
        "compiled_spec": compiled_spec,
        "build_bundle": build_bundle,
        "planning_notes": planning_notes,
    }


def _rebuild_workflow_build(
    definition: dict[str, Any],
    *,
    workflow_name: str,
    conn: Any,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any], list[str]]:
    from runtime.build_authority import apply_authority_bundle, build_authority_bundle
    from runtime.operating_model_planner import PlanningBlockedError, plan_definition

    planning_notes: list[str] = []
    compiled_spec: dict[str, Any] | None = None
    bundle = build_authority_bundle(definition)
    if _text(bundle.get("projection_status", {}).get("state")) == "ready":
        try:
            plan_result = plan_definition(definition, title=workflow_name, conn=conn)
            candidate_spec = plan_result.get("compiled_spec")
            if isinstance(candidate_spec, dict):
                compiled_spec = candidate_spec
            planning_notes = [
                note
                for note in plan_result.get("planning_notes", [])
                if isinstance(note, str) and note.strip()
            ]
        except PlanningBlockedError as exc:
            planning_notes = [str(exc)]
    hydrated_definition = apply_authority_bundle(definition, compiled_spec=compiled_spec)
    bundle = build_authority_bundle(hydrated_definition, compiled_spec=compiled_spec)
    return hydrated_definition, compiled_spec, bundle, planning_notes


def delete_workflow(
    conn: Any,
    *,
    workflow_id: str,
) -> dict[str, Any]:
    normalized_workflow_id = _text(workflow_id)
    if not normalized_workflow_id:
        raise WorkflowRuntimeBoundaryError("workflow_id required")
    deleted = delete_workflow_record(conn, workflow_id=normalized_workflow_id)
    if not deleted:
        raise WorkflowRuntimeBoundaryError(f"Workflow not found: {normalized_workflow_id}", status_code=404)
    return {"deleted": True, "workflow_id": normalized_workflow_id}


def trigger_workflow_manually(
    subsystems: Any,
    *,
    workflow_id: str,
    repo_root: Path,
) -> dict[str, Any]:
    from runtime.operating_model_planner import current_compiled_spec, missing_execution_plan_message

    normalized_workflow_id = _text(workflow_id)
    if not normalized_workflow_id:
        raise WorkflowRuntimeBoundaryError("workflow_id required")

    pg = subsystems.get_pg_conn()
    workflow_row = load_workflow_record(pg, workflow_id=normalized_workflow_id)
    if workflow_row is None:
        raise WorkflowRuntimeBoundaryError(f"Workflow not found: {normalized_workflow_id}", status_code=404)

    definition_row = _parse_json_field(workflow_row.get("definition")) or {}
    compiled_spec_row = _parse_json_field(workflow_row.get("compiled_spec"))
    spec = current_compiled_spec(definition_row, compiled_spec_row)
    if spec is None:
        raise WorkflowRuntimeBoundaryError(missing_execution_plan_message(workflow_row.get("name")))

    spec_to_submit = _json_clone(spec)
    spec_to_submit["packet_provenance"] = {
        "source_kind": "workflow_trigger",
        "workflow_row": dict(workflow_row),
        "definition_row": definition_row,
        "compiled_spec_row": spec,
    }
    record_system_event(
        pg,
        event_type="manual",
        source_id=str(workflow_row["id"]),
        source_type="user",
        payload={
            "workflow_id": workflow_row["id"],
            "workflow_name": workflow_row["name"],
            "trigger_depth": 0,
        },
    )
    result = _submit_spec_via_service_bus(
        pg,
        spec=spec_to_submit,
        spec_name=str(spec_to_submit.get("name") or workflow_row["name"]),
        repo_root=repo_root,
        requested_by_kind="http",
        requested_by_ref="workflow_trigger",
    )
    if result.get("error") or not result.get("run_id"):
        raise WorkflowRuntimeBoundaryError(
            str(result.get("error_detail") or result.get("error") or "workflow trigger dispatch failed"),
            status_code=500,
        )
    record_workflow_invocation(pg, workflow_id=str(workflow_row["id"]))
    return {
        "triggered": True,
        "workflow_id": workflow_row["id"],
        "workflow_name": workflow_row["name"],
        "run_id": result["run_id"],
    }


def _submit_spec_via_service_bus(
    conn: Any,
    *,
    spec: dict[str, Any],
    spec_name: str,
    repo_root: Path,
    requested_by_kind: str,
    requested_by_ref: str,
) -> dict[str, Any]:
    from runtime.control_commands import (
        render_workflow_submit_response,
        request_workflow_submit_command,
    )

    temp_dir = repo_root / "artifacts" / "workflow"
    temp_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".queue.json",
        dir=str(temp_dir),
        delete=False,
    ) as handle:
        json.dump(spec, handle, default=str)
        spec_path = handle.name

    try:
        command = request_workflow_submit_command(
            conn,
            requested_by_kind=requested_by_kind,
            requested_by_ref=requested_by_ref,
            spec_path=os.path.relpath(spec_path, str(repo_root)),
            repo_root=str(repo_root),
        )
        return render_workflow_submit_response(
            command,
            spec_name=spec_name,
            total_jobs=len(spec.get("jobs", [])),
        )
    finally:
        os.unlink(spec_path)


__all__ = [
    "WorkflowRuntimeBoundaryError",
    "commit_workflow",
    "delete_workflow",
    "mutate_workflow_build",
    "save_workflow",
    "save_workflow_trigger",
    "trigger_workflow_manually",
    "update_workflow_trigger",
]
