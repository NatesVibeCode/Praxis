"""Catalog-backed operator MCP tools."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..subsystems import _subs

_OPERATOR_READ_LIMIT_MAX = 500


def _parse_iso_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 datetime string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return parsed


def _structured_runtime_error(exc: Exception, *, operation_name: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": str(exc),
        "error_code": getattr(exc, "reason_code", f"{operation_name}.failed"),
        "operation_name": operation_name,
    }
    details = getattr(exc, "details", None)
    if isinstance(details, dict) and details:
        payload["details"] = details
    return payload


def _structured_input_error(exc: Exception, *, operation_name: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": str(exc),
        "error_code": f"{operation_name}.invalid_input",
        "operation_name": operation_name,
    }


def _bounded_limit(
    params: dict[str, Any],
    *,
    default: int,
    maximum: int = _OPERATOR_READ_LIMIT_MAX,
) -> int:
    raw = params.get("limit", default)
    if raw is None or raw == "":
        raw = default
    if isinstance(raw, bool):
        raise ValueError("limit must be a positive integer")
    if isinstance(raw, int):
        limit = raw
    elif isinstance(raw, str):
        text = raw.strip()
        if not text or not text.lstrip("-").isdigit():
            raise ValueError("limit must be a positive integer")
        limit = int(text)
    else:
        raise ValueError("limit must be a positive integer")
    if limit <= 0:
        raise ValueError("limit must be greater than zero")
    return min(limit, maximum)


def _execute_catalog_tool(*, operation_name: str, payload: dict[str, Any]) -> dict:
    try:
        result = execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload=payload,
        )
        if isinstance(result, dict) and "ok" not in result:
            result["ok"] = True
        return result
    except Exception as exc:
        return _structured_runtime_error(exc, operation_name=operation_name)


def _optional_sequence_payload(params: dict[str, Any], field_name: str) -> Any:
    """Avoid leaking ``None`` into tuple-backed operator command contracts."""

    value = params.get(field_name)
    return [] if value is None else value


def execute_operation_from_subsystems(*args: Any, **kwargs: Any) -> Any:
    from runtime.operation_catalog_gateway import (
        execute_operation_from_subsystems as _execute_operation_from_subsystems,
    )

    return _execute_operation_from_subsystems(*args, **kwargs)


def _bug_query_default_open_only_backlog() -> bool:
    from runtime.primitive_contracts import bug_query_default_open_only_backlog

    return bug_query_default_open_only_backlog()


def tool_praxis_status_snapshot(params: dict) -> dict:
    """Read the canonical workflow status snapshot."""

    return _execute_catalog_tool(
        operation_name="operator.status_snapshot",
        payload={"since_hours": params.get("since_hours", 24)},
    )


def tool_praxis_orient(params: dict) -> dict:
    """Return the canonical orientation payload for a fresh agent or operator.

    Delegates to ``surfaces.api.handlers.workflow_admin._handle_orient``, the
    single HTTP authority behind POST /orient. One implementation, one shape,
    no drift between MCP and HTTP consumers.
    """

    from surfaces.api.handlers.workflow_admin import _handle_orient

    body = dict(params) if isinstance(params, dict) else {}
    return _handle_orient(_subs, body)


def tool_praxis_metrics_reset(params: dict) -> dict:
    """Reset observability metrics through explicit operator maintenance authority."""

    return _execute_catalog_tool(
        operation_name="operator.metrics_reset",
        payload={
            "confirm": bool(params.get("confirm", False)),
            "before_date": params.get("before_date"),
        },
    )


def tool_praxis_bug_replay_provenance_backfill(params: dict) -> dict:
    """Backfill replay provenance from authoritative bug and receipt state."""

    operation_name = "operator.bug_replay_provenance_backfill"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "open_only": bool(
                params.get("open_only", _bug_query_default_open_only_backlog())
            ),
            "receipt_limit": params.get("receipt_limit", 1),
        },
    )


def tool_praxis_semantic_bridges_backfill(params: dict) -> dict:
    """Replay semantic bridges from canonical operator authority."""

    as_of = params.get("as_of")
    return _execute_catalog_tool(
        operation_name="operator.semantic_bridges_backfill",
        payload={
            "include_object_relations": bool(
                params.get("include_object_relations", True)
            ),
            "include_operator_decisions": bool(
                params.get("include_operator_decisions", True)
            ),
            "include_roadmap_items": bool(params.get("include_roadmap_items", True)),
            "as_of": (
                _parse_iso_datetime(as_of, field_name="as_of")
                if as_of is not None
                else None
            ),
        },
    )


def tool_praxis_semantic_projection_refresh(params: dict) -> dict:
    """Refresh the semantic projection through explicit operator maintenance authority."""

    operation_name = "operator.semantic_projection_refresh"
    as_of = params.get("as_of")
    try:
        limit = _bounded_limit(params, default=100)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "as_of": (
                _parse_iso_datetime(as_of, field_name="as_of")
                if as_of is not None
                else None
            ),
        },
    )


def tool_praxis_run_status(params: dict) -> dict:
    """Read one run-scoped operator status view."""

    return _execute_catalog_tool(
        operation_name="operator.run_status",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_run_scoreboard(params: dict) -> dict:
    """Read one run-scoped cutover scoreboard."""

    return _execute_catalog_tool(
        operation_name="operator.run_scoreboard",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_run_graph(params: dict) -> dict:
    """Read one run-scoped workflow graph."""

    return _execute_catalog_tool(
        operation_name="operator.run_graph",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_graph_projection(params: dict) -> dict:
    """Read the cross-domain operator graph projection."""

    as_of = params.get("as_of")
    return _execute_catalog_tool(
        operation_name="operator.graph_projection",
        payload={
            "as_of": (
                _parse_iso_datetime(as_of, field_name="as_of")
                if as_of is not None
                else None
            ),
        },
    )


def tool_praxis_ui_experience_graph(params: dict) -> dict:
    """Read the LLM-facing app experience graph."""

    return _execute_catalog_tool(
        operation_name="operator.ui_experience_graph",
        payload={
            "focus": params.get("focus"),
            "surface_name": params.get("surface_name"),
            "limit": params.get("limit", 80),
        },
    )


def tool_praxis_run_lineage(params: dict) -> dict:
    """Read one run-scoped lineage view."""

    return _execute_catalog_tool(
        operation_name="operator.run_lineage",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_issue_backlog(params: dict) -> dict:
    """Read the canonical operator issue backlog."""

    operation_name = "operator.issue_backlog"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "open_only": bool(
                params.get("open_only", _bug_query_default_open_only_backlog())
            ),
            "status": params.get("status"),
        },
    )


def tool_praxis_operator_ideas(params: dict) -> dict:
    """Record, resolve, promote, or list pre-commitment operator ideas."""

    operation_name = "operator.ideas"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "action": params.get("action", "list"),
            "idea_id": params.get("idea_id"),
            "idea_key": params.get("idea_key"),
            "title": params.get("title"),
            "summary": params.get("summary"),
            "source_kind": params.get("source_kind", "operator"),
            "source_ref": params.get("source_ref"),
            "owner_ref": params.get("owner_ref"),
            "decision_ref": params.get("decision_ref"),
            "status": params.get("status"),
            "resolution_summary": params.get("resolution_summary"),
            "roadmap_item_id": params.get("roadmap_item_id"),
            "promoted_by": params.get("promoted_by"),
            "opened_at": params.get("opened_at"),
            "resolved_at": params.get("resolved_at"),
            "promoted_at": params.get("promoted_at"),
            "created_at": params.get("created_at"),
            "updated_at": params.get("updated_at"),
            "idea_ids": _optional_sequence_payload(params, "idea_ids"),
            "open_only": bool(
                params.get("open_only", _bug_query_default_open_only_backlog())
            ),
            "limit": limit,
        },
    )


def tool_praxis_replay_ready_bugs(params: dict) -> dict:
    """Read the replay-ready bug backlog."""

    operation_name = "operator.replay_ready_bugs"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
        },
    )


def tool_praxis_operator_write(params: dict) -> dict:
    """Preview, validate, or commit roadmap rows through the shared operator-write gate."""

    operation_name = "operator.roadmap_write"
    action = str(params.get("action") or "").strip() or "preview"
    title = str(params.get("title") or "").strip()
    intent_brief = str(params.get("intent_brief") or "").strip()
    roadmap_item_id = str(params.get("roadmap_item_id") or "").strip() or None
    phase_order = str(params.get("phase_order") or "").strip() or None
    if not action:
        return _structured_input_error(
            ValueError("action is required and cannot be empty"), operation_name=operation_name
        )
    if not title and roadmap_item_id is None:
        return _structured_input_error(
            ValueError(
                "title is required unless roadmap_item_id is provided for update"
            ),
            operation_name=operation_name,
        )
    if not intent_brief and roadmap_item_id is None:
        return _structured_input_error(
            ValueError(
                "intent_brief is required unless roadmap_item_id is provided for update"
            ),
            operation_name=operation_name,
        )

    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "action": action,
            "title": title or None,
            "intent_brief": intent_brief or None,
            "template": params.get("template", "single_capability"),
            "priority": params.get("priority", "p2"),
            "parent_roadmap_item_id": params.get("parent_roadmap_item_id"),
            "slug": params.get("slug"),
            "depends_on": _optional_sequence_payload(params, "depends_on"),
            "source_bug_id": params.get("source_bug_id"),
            "source_idea_id": params.get("source_idea_id"),
            "registry_paths": _optional_sequence_payload(params, "registry_paths"),
            "decision_ref": params.get("decision_ref"),
            "item_kind": params.get("item_kind"),
            "status": params.get("status"),
            "lifecycle": params.get("lifecycle"),
            "tier": params.get("tier"),
            "phase_ready": params.get("phase_ready"),
            "approval_tag": params.get("approval_tag"),
            "reference_doc": params.get("reference_doc"),
            "outcome_gate": params.get("outcome_gate"),
            "proof_kind": params.get("proof_kind"),
            "roadmap_item_id": roadmap_item_id,
            "phase_order": phase_order,
        },
    )


def tool_praxis_operator_decisions(params: dict) -> dict:
    """Record or list canonical operator decisions through operator_decisions."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "list":
        operation_name = "operator.decision_list"
        as_of = params.get("as_of")
        try:
            limit = _bounded_limit(params, default=100)
        except ValueError as exc:
            return _structured_input_error(exc, operation_name=operation_name)
        return execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload={
                "decision_kind": params.get("decision_kind"),
                "decision_scope_kind": params.get("decision_scope_kind"),
                "decision_scope_ref": params.get("decision_scope_ref"),
                "as_of": (
                    _parse_iso_datetime(as_of, field_name="as_of")
                    if as_of is not None
                    else None
                ),
                "limit": limit,
            },
        )
    if action != "record":
        return {"error": "Unknown action. Supported actions: list, record"}
    effective_from = params.get("effective_from")
    effective_to = params.get("effective_to")
    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.decision_record",
        payload={
            "decision_key": str(params.get("decision_key") or ""),
            "decision_kind": str(params.get("decision_kind") or ""),
            "decision_status": str(params.get("decision_status") or "decided"),
            "title": str(params.get("title") or ""),
            "rationale": str(params.get("rationale") or ""),
            "decided_by": str(params.get("decided_by") or ""),
            "decision_source": str(params.get("decision_source") or ""),
            "decision_scope_kind": params.get("decision_scope_kind"),
            "decision_scope_ref": params.get("decision_scope_ref"),
            "effective_from": (
                _parse_iso_datetime(effective_from, field_name="effective_from")
                if effective_from is not None
                else None
            ),
            "effective_to": (
                _parse_iso_datetime(effective_to, field_name="effective_to")
                if effective_to is not None
                else None
            ),
        },
    )


def tool_praxis_operator_relations(params: dict) -> dict:
    """Record canonical functional areas and cross-object semantic relations."""

    action = str(params.get("action") or "").strip().lower()
    if action == "record_functional_area":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.functional_area_record",
            payload={
                "area_slug": str(params.get("area_slug") or ""),
                "title": str(params.get("title") or ""),
                "summary": str(params.get("summary") or ""),
                "area_status": str(params.get("area_status") or "active"),
                "created_at": params.get("created_at"),
                "updated_at": params.get("updated_at"),
            },
        )
    if action == "record_relation":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.object_relation_record",
            payload={
                "relation_kind": str(params.get("relation_kind") or ""),
                "source_kind": str(params.get("source_kind") or ""),
                "source_ref": str(params.get("source_ref") or ""),
                "target_kind": str(params.get("target_kind") or ""),
                "target_ref": str(params.get("target_ref") or ""),
                "relation_status": str(params.get("relation_status") or "active"),
                "relation_metadata": params.get("relation_metadata"),
                "bound_by_decision_id": params.get("bound_by_decision_id"),
                "created_at": params.get("created_at"),
                "updated_at": params.get("updated_at"),
            },
        )
    return {
        "error": (
            "Unknown action. Supported actions: record_functional_area, record_relation"
        )
    }


def tool_praxis_semantic_assertions(params: dict) -> dict:
    """Register predicates, record or retract assertions, and query semantic authority."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "list":
        operation_name = "semantic_assertions.list"
        as_of = params.get("as_of")
        try:
            limit = _bounded_limit(params, default=100)
        except ValueError as exc:
            return _structured_input_error(exc, operation_name=operation_name)
        return execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload={
                "predicate_slug": params.get("predicate_slug"),
                "subject_kind": params.get("subject_kind"),
                "subject_ref": params.get("subject_ref"),
                "object_kind": params.get("object_kind"),
                "object_ref": params.get("object_ref"),
                "source_kind": params.get("source_kind"),
                "source_ref": params.get("source_ref"),
                "active_only": bool(params.get("active_only", True)),
                "as_of": (
                    _parse_iso_datetime(as_of, field_name="as_of")
                    if as_of is not None
                    else None
                ),
                "limit": limit,
            },
        )
    if action == "register_predicate":
        created_at = params.get("created_at")
        updated_at = params.get("updated_at")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.register_predicate",
            payload={
                "predicate_slug": str(params.get("predicate_slug") or ""),
                "subject_kind_allowlist": params.get("subject_kind_allowlist") or (),
                "object_kind_allowlist": params.get("object_kind_allowlist") or (),
                "cardinality_mode": str(params.get("cardinality_mode") or "many"),
                "predicate_status": str(params.get("predicate_status") or "active"),
                "description": params.get("description"),
                "created_at": (
                    _parse_iso_datetime(created_at, field_name="created_at")
                    if created_at is not None
                    else None
                ),
                "updated_at": (
                    _parse_iso_datetime(updated_at, field_name="updated_at")
                    if updated_at is not None
                    else None
                ),
            },
        )
    if action == "record_assertion":
        valid_from = params.get("valid_from")
        valid_to = params.get("valid_to")
        created_at = params.get("created_at")
        updated_at = params.get("updated_at")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.record",
            payload={
                "predicate_slug": str(params.get("predicate_slug") or ""),
                "subject_kind": str(params.get("subject_kind") or ""),
                "subject_ref": str(params.get("subject_ref") or ""),
                "object_kind": str(params.get("object_kind") or ""),
                "object_ref": str(params.get("object_ref") or ""),
                "qualifiers_json": params.get("qualifiers_json"),
                "source_kind": str(params.get("source_kind") or ""),
                "source_ref": str(params.get("source_ref") or ""),
                "evidence_ref": params.get("evidence_ref"),
                "bound_decision_id": params.get("bound_decision_id"),
                "valid_from": (
                    _parse_iso_datetime(valid_from, field_name="valid_from")
                    if valid_from is not None
                    else None
                ),
                "valid_to": (
                    _parse_iso_datetime(valid_to, field_name="valid_to")
                    if valid_to is not None
                    else None
                ),
                "assertion_status": str(params.get("assertion_status") or "active"),
                "semantic_assertion_id": params.get("semantic_assertion_id"),
                "created_at": (
                    _parse_iso_datetime(created_at, field_name="created_at")
                    if created_at is not None
                    else None
                ),
                "updated_at": (
                    _parse_iso_datetime(updated_at, field_name="updated_at")
                    if updated_at is not None
                    else None
                ),
            },
        )
    if action == "retract_assertion":
        retracted_at = params.get("retracted_at")
        updated_at = params.get("updated_at")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.retract",
            payload={
                "semantic_assertion_id": str(params.get("semantic_assertion_id") or ""),
                "retracted_at": (
                    _parse_iso_datetime(retracted_at, field_name="retracted_at")
                    if retracted_at is not None
                    else None
                ),
                "updated_at": (
                    _parse_iso_datetime(updated_at, field_name="updated_at")
                    if updated_at is not None
                    else None
                ),
            },
        )
    return {
        "error": (
            "Unknown action. Supported actions: "
            "list, register_predicate, record_assertion, retract_assertion"
        )
    }


def tool_praxis_operator_native_primary_cutover_gate(params: dict) -> dict:
    """Admit one native primary cutover gate through operator-control persistence."""

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.native_primary_cutover_gate",
        payload={
            "decided_by": params.get("decided_by", ""),
            "decision_source": params.get("decision_source", ""),
            "rationale": params.get("rationale", ""),
            "roadmap_item_id": params.get("roadmap_item_id"),
            "workflow_class_id": params.get("workflow_class_id"),
            "schedule_definition_id": params.get("schedule_definition_id"),
            "title": params.get("title"),
            "gate_name": params.get("gate_name"),
            "gate_policy": params.get("gate_policy"),
            "required_evidence": params.get("required_evidence"),
            "decided_at": params.get("decided_at"),
            "opened_at": params.get("opened_at"),
            "created_at": params.get("created_at"),
            "updated_at": params.get("updated_at"),
        },
    )


def tool_praxis_operator_architecture_policy(params: dict) -> dict:
    """Record one bounded architecture-policy decision through operator control."""

    effective_from = params.get("effective_from")
    effective_to = params.get("effective_to")
    decided_at = params.get("decided_at")
    created_at = params.get("created_at")
    updated_at = params.get("updated_at")
    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.architecture_policy_record",
        payload={
            "authority_domain": params.get("authority_domain", ""),
            "policy_slug": params.get("policy_slug", ""),
            "title": params.get("title", ""),
            "rationale": params.get("rationale", ""),
            "decided_by": params.get("decided_by", ""),
            "decision_source": params.get("decision_source", ""),
            "effective_from": (
                _parse_iso_datetime(effective_from, field_name="effective_from")
                if effective_from is not None
                else None
            ),
            "effective_to": (
                _parse_iso_datetime(effective_to, field_name="effective_to")
                if effective_to is not None
                else None
            ),
            "decided_at": (
                _parse_iso_datetime(decided_at, field_name="decided_at")
                if decided_at is not None
                else None
            ),
            "created_at": (
                _parse_iso_datetime(created_at, field_name="created_at")
                if created_at is not None
                else None
            ),
            "updated_at": (
                _parse_iso_datetime(updated_at, field_name="updated_at")
                if updated_at is not None
                else None
            ),
        },
    )


def tool_praxis_operator_closeout(params: dict) -> dict:
    """Preview or commit proof-backed bug and roadmap closeout through the shared gate."""

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.work_item_closeout",
        payload={
            "action": params.get("action", "preview"),
            "bug_ids": _optional_sequence_payload(params, "bug_ids"),
            "roadmap_item_ids": _optional_sequence_payload(params, "roadmap_item_ids"),
        },
    )


def tool_praxis_operator_roadmap_view(params: dict) -> dict:
    """Read one roadmap subtree, dependency edges, and semantic-first external neighbors."""

    root_roadmap_item_id = str(params.get("root_roadmap_item_id", "")).strip()
    if not root_roadmap_item_id:
        rows = _subs.get_pg_conn().execute(
            """
            SELECT roadmap_item_id
              FROM roadmap_items
             WHERE parent_roadmap_item_id IS NULL
             ORDER BY
                 CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                 updated_at DESC,
                 created_at DESC
             LIMIT 1
            """
        )
        if not rows:
            return {"error": "root_roadmap_item_id is required and no roadmap roots were found"}
        root_roadmap_item_id = str(rows[0].get("roadmap_item_id") or "").strip()
    if not root_roadmap_item_id:
        return {"error": "failed to resolve a default roadmap root"}

    operation_name = "operator.roadmap_tree"
    try:
        semantic_neighbor_limit = _bounded_limit(params, default=5, maximum=200)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "root_roadmap_item_id": root_roadmap_item_id,
            "semantic_neighbor_limit": semantic_neighbor_limit,
        },
    )


def tool_praxis_circuits(params: dict) -> dict:
    """Inspect or override provider circuit breakers through operator-control authority."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "history":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.circuit_history",
            payload={
                "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
            },
        )

    if action == "list":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.circuit_states",
            payload={
                "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
            },
        )

    provider_slug = str(params.get("provider_slug") or "").strip().lower()
    if not provider_slug:
        return {"error": "provider_slug is required for circuit override actions"}

    if action not in {"open", "close", "reset"}:
        return {"error": "Unknown action. Supported actions: list, history, open, close, reset"}

    effective_to = params.get("effective_to")
    effective_from = params.get("effective_from")
    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.circuit_override",
        payload={
            "provider_slug": provider_slug,
            "override_state": {
                "open": "open",
                "close": "closed",
                "reset": "reset",
            }[action],
            "effective_to": (
                _parse_iso_datetime(effective_to, field_name="effective_to")
                if effective_to is not None
                else None
            ),
            "reason_code": str(params.get("reason_code") or "operator_control"),
            "rationale": params.get("rationale"),
            "effective_from": (
                _parse_iso_datetime(effective_from, field_name="effective_from")
                if effective_from is not None
                else None
            ),
            "decided_by": params.get("decided_by"),
            "decision_source": params.get("decision_source"),
        },
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_orient": (
        tool_praxis_orient,
        {
            "description": (
                "Fresh-agent orientation: returns the canonical orient payload "
                "(standing orders, authority envelope, tool guidance, recent activity, "
                "endpoints, health). The single best first call for any LLM agent or "
                "operator waking up cold against Praxis. Delegates to the same authority "
                "that serves POST /orient so HTTP and MCP consumers see identical shape.\n\n"
                "USE WHEN: starting a new session, onboarding a new agent, or re-anchoring after long idle.\n\n"
                "DO NOT USE: for deep subsystem inspection — use cluster-specific tools instead."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
            "cli": {
                "surface": "operations",
                "tier": "curated",
                "recommended_alias": "orient",
                "when_to_use": "Wake up against Praxis and get standing orders, authority envelope, tool guidance, and endpoints in one call.",
                "when_not_to_use": "Do not use it for deep subsystem inspection; call cluster-specific tools instead.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Orient", "input": {}},
                ],
            },
        },
    ),
    "praxis_status_snapshot": (
        tool_praxis_status_snapshot,
        {
            "description": "Read the canonical workflow status snapshot — pass rate, failure mix, queue depth, and in-flight run summaries from receipt authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "since_hours": {"type": "integer", "description": "Lookback window in hours.", "default": 24},
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Inspect workflow pass rate, failure mix, and in-flight run summaries from canonical receipts.",
                "when_not_to_use": "Do not use it for deep run inspection or workflow launch.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Show 24h status", "input": {"since_hours": 24}},
                ],
            },
        },
    ),
    "praxis_metrics_reset": (
        tool_praxis_metrics_reset,
        {
            "description": "Reset observability metrics through explicit operator maintenance authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "confirm": {
                        "type": "boolean",
                        "description": "Required for destructive maintenance actions.",
                        "default": False,
                    },
                    "before_date": {
                        "type": "string",
                        "description": "ISO date for surgical reset (only delete data before this date).",
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Reset polluted quality metrics or routing counters through one explicit maintenance operation.",
                "when_not_to_use": "Do not use it for ordinary observability reads.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Reset metrics with confirmation", "input": {"confirm": True}},
                ],
            },
        },
    ),
    "praxis_bug_replay_provenance_backfill": (
        tool_praxis_bug_replay_provenance_backfill,
        {
            "description": "Backfill replay provenance from canonical bug and receipt authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Optional scan limit.",
                        "minimum": 0,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": (
                            "When true, only scan unresolved bugs. Default is sourced from "
                            "runtime.primitive_contracts.bug_query_default_open_only_backlog() so "
                            "operator-facing bug surfaces share one authority (closes BUG-BAEC85C1)."
                        ),
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "receipt_limit": {
                        "type": "integer",
                        "description": "Receipt context lookback for replay provenance backfill.",
                        "minimum": 1,
                        "default": 1,
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Backfill replay provenance without bundling unrelated maintenance actions into one selector tool.",
                "when_not_to_use": "Do not use it for read-only bug backlog inspection.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Backfill replay provenance", "input": {"open_only": True}},
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.bug.replay_backfill_result"]},
            },
        },
    ),
    "praxis_semantic_bridges_backfill": (
        tool_praxis_semantic_bridges_backfill,
        {
            "description": "Replay semantic bridges from canonical operator authority into semantic assertions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 cutoff for semantic bridge replay.",
                    },
                    "include_object_relations": {
                        "type": "boolean",
                        "description": "Replay operator_object_relations into semantic assertions.",
                        "default": True,
                    },
                    "include_operator_decisions": {
                        "type": "boolean",
                        "description": "Replay operator_decisions into semantic assertions.",
                        "default": True,
                    },
                    "include_roadmap_items": {
                        "type": "boolean",
                        "description": "Replay roadmap semantic fields into semantic assertions.",
                        "default": True,
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Rebuild semantic bridge authority from canonical operator sources.",
                "when_not_to_use": "Do not use it for semantic reads; use praxis_semantic_assertions instead.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Backfill semantic bridges", "input": {"include_object_relations": True}},
                ],
            },
        },
    ),
    "praxis_semantic_projection_refresh": (
        tool_praxis_semantic_projection_refresh,
        {
            "description": "Refresh the semantic projection through explicit operator maintenance authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum events to consume in one refresh.",
                        "minimum": 1,
                        "default": 100,
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 cutoff for projection refresh.",
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Consume semantic projection events through one explicit maintenance operation.",
                "when_not_to_use": "Do not use it for read-only graph inspection.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Refresh semantic projection", "input": {"limit": 100}},
                ],
            },
        },
    ),
    "praxis_run_status": (
        tool_praxis_run_status,
        {
            "description": "Read one run-scoped operator status view.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect operator status for one workflow run.",
                "when_not_to_use": "Do not use it for whole-system pass-rate summaries.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run status", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_run_scoreboard": (
        tool_praxis_run_scoreboard,
        {
            "description": "Read one run-scoped cutover scoreboard.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect cutover readiness for one workflow run.",
                "when_not_to_use": "Do not use it for workflow launch or global status.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run scoreboard", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_run_graph": (
        tool_praxis_run_graph,
        {
            "description": "Read one run-scoped workflow graph.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect workflow topology for one run.",
                "when_not_to_use": "Do not use it for cross-domain operator graph inspection.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run graph", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_graph_projection": (
        tool_praxis_graph_projection,
        {
            "description": "Read the cross-domain operator graph projection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 timestamp for the projection snapshot.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect the semantic-first operator graph across domains.",
                "when_not_to_use": "Do not use it for run-scoped workflow topology.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read operator graph projection", "input": {"as_of": "2026-04-16T20:05:00+00:00"}},
                ],
            },
        },
    ),
    "praxis_ui_experience_graph": (
        tool_praxis_ui_experience_graph,
        {
            "description": "Read the LLM-facing Praxis app experience graph: surfaces, controls, authority sources, relationships, and source-file anchors.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "focus": {
                        "type": "string",
                        "description": "Optional text filter such as moon, dashboard, run, chat, gate, release, or navigation.",
                    },
                    "surface_name": {
                        "type": "string",
                        "description": "Optional exact surface id/name such as build, dashboard, chat, manifests, atlas, or moon.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum items per section, capped at 250.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect the app UI experience before changing React, CSS, or surface catalog behavior.",
                "when_not_to_use": "Do not use it for run-scoped execution topology or raw knowledge-graph traversal.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read Moon UI graph", "input": {"surface_name": "build"}},
                    {"title": "Find release controls", "input": {"focus": "release", "limit": 40}},
                ],
            },
        },
    ),
    "praxis_run_lineage": (
        tool_praxis_run_lineage,
        {
            "description": "Read one run-scoped lineage view.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect graph lineage and operator frames for one run.",
                "when_not_to_use": "Do not use it for whole-system summaries.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run lineage", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_issue_backlog": (
        tool_praxis_issue_backlog,
        {
            "description": "Read the canonical operator issue backlog.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return.",
                        "minimum": 1,
                        "default": 50,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": (
                            "When true, exclude resolved issues. Default is sourced from "
                            "runtime.primitive_contracts.bug_query_default_open_only_backlog() so "
                            "operator-facing bug surfaces share one authority (closes BUG-BAEC85C1)."
                        ),
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "status": {
                        "type": "string",
                        "description": "Optional issue status filter.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect the canonical upstream issue backlog before bug promotion.",
                "when_not_to_use": "Do not use it to mutate issue or bug state.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read issue backlog", "input": {"limit": 25}},
                ],
            },
        },
    ),
    "praxis_operator_ideas": (
        tool_praxis_operator_ideas,
        {
            "description": (
                "Record, resolve, promote, or list pre-commitment operator ideas. "
                "Ideas are upstream of roadmap commitment: they may be rejected, "
                "superseded, archived, or promoted into existing roadmap items, "
                "but roadmap itself does not gain a canceled state."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "file", "resolve", "promote"],
                        "default": "list",
                    },
                    "idea_id": {"type": "string"},
                    "idea_key": {"type": "string"},
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "source_kind": {
                        "type": "string",
                        "description": "Origin of the idea, such as operator, conversation, receipt, or research.",
                        "default": "operator",
                    },
                    "source_ref": {"type": "string"},
                    "owner_ref": {"type": "string"},
                    "decision_ref": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": ["open", "promoted", "rejected", "superseded", "archived"],
                        "description": "Filter for list or terminal status for resolve.",
                    },
                    "resolution_summary": {"type": "string"},
                    "roadmap_item_id": {
                        "type": "string",
                        "description": "Existing roadmap item to link when action='promote'.",
                    },
                    "promoted_by": {"type": "string"},
                    "opened_at": {"type": "string"},
                    "resolved_at": {"type": "string"},
                    "promoted_at": {"type": "string"},
                    "created_at": {"type": "string"},
                    "updated_at": {"type": "string"},
                    "idea_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, list only open ideas unless status is supplied.",
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum idea rows to return for list.",
                        "minimum": 1,
                        "default": 50,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Capture pre-commitment ideas or promote them into roadmap without polluting roadmap with canceled items.",
                "when_not_to_use": "Do not use it to create committed roadmap work; use praxis_operator_write after the idea is ready for roadmap.",
                "risks": {
                    "default": "read",
                    "actions": {
                        "list": "read",
                        "file": "write",
                        "resolve": "write",
                        "promote": "write",
                    },
                },
                "examples": [
                    {
                        "title": "List open ideas",
                        "input": {"action": "list", "limit": 25},
                    },
                    {
                        "title": "File an idea",
                        "input": {
                            "action": "file",
                            "title": "First-class ideas authority",
                            "summary": "Pre-commitment intake for roadmap candidates.",
                        },
                    },
                    {
                        "title": "Reject an idea",
                        "input": {
                            "action": "resolve",
                            "idea_id": "operator_idea.example",
                            "status": "rejected",
                            "resolution_summary": "No longer fits the operator model.",
                        },
                    },
                ],
            },
        },
    ),
    "praxis_replay_ready_bugs": (
        tool_praxis_replay_ready_bugs,
        {
            "description": "Read the replay-ready bug backlog from authoritative provenance.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return.",
                        "minimum": 1,
                        "default": 50,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect replayable bugs without bundling that read behind a selector view.",
                "when_not_to_use": "Do not use it to trigger replay backfill.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read replay-ready bugs", "input": {"limit": 25}},
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.bug.replay_ready_list"]},
            },
        },
    ),
    "praxis_operator_write": (
        tool_praxis_operator_write,
        {
            "description": (
                "Preview, validate, or commit roadmap rows through the shared operator-write validation gate.\n\n"
                "USE WHEN: you want to add a roadmap item or a packaged roadmap program without raw SQL. "
                "This gate auto-generates ids, keys, dependency ids, and phase ordering, then returns a preview "
                "before commit.\n\n"
                "EXAMPLE: praxis_operator_write(action='preview', title='Unified operator write gate', "
                "intent_brief='Single preview-first validation gate for roadmap writes', "
                "parent_roadmap_item_id='roadmap_item.authority.cleanup', template='hard_cutover_program')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["preview", "validate", "commit"],
                        "default": "preview",
                    },
                    "title": {"type": "string"},
                    "intent_brief": {"type": "string"},
                    "template": {
                        "type": "string",
                        "enum": ["single_capability", "hard_cutover_program"],
                        "default": "single_capability",
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["p1", "p2"],
                        "default": "p2",
                    },
                    "parent_roadmap_item_id": {"type": "string"},
                    "slug": {"type": "string"},
                    "depends_on": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "source_bug_id": {"type": "string"},
                    "source_idea_id": {"type": "string"},
                    "registry_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "decision_ref": {"type": "string"},
                    "item_kind": {
                        "type": "string",
                        "enum": ["capability", "initiative"],
                    },
                    "status": {
                        "type": "string",
                        "enum": ["active", "completed", "done"],
                    },
                    "lifecycle": {
                        "type": "string",
                        "enum": ["planned", "claimed", "completed", "retired"],
                        "description": (
                            "Roadmap commitment lifecycle. Use praxis_operator_ideas for pre-commitment ideas. "
                            "Set to 'retired' (with roadmap_item_id) to mark a misfiled/superseded row without "
                            "proof-backed closeout."
                        ),
                    },
                    "tier": {"type": "string"},
                    "phase_ready": {"type": "boolean"},
                    "approval_tag": {"type": "string"},
                    "reference_doc": {"type": "string"},
                    "outcome_gate": {"type": "string"},
                    "proof_kind": {
                        "type": "string",
                        "enum": ["capability_delivered_by_decision_filing"],
                        "description": (
                            "Opt-in proof contract for capability rows whose deliverable IS a "
                            "filed operator_decision (e.g. standing-order policy filings). When "
                            "set, closeout requires only that the decision_ref points at a "
                            "decided operator_decision row, not source_bug + validates_fix proof."
                        ),
                    },
                    "roadmap_item_id": {
                        "type": "string",
                        "description": (
                            "Target an existing roadmap row for update/retire/re-parent. When "
                            "provided, the tool runs in update mode: existing values are preserved "
                            "unless overridden, template children are NOT regenerated, and slug/title/"
                            "intent_brief become optional. Combine with parent_roadmap_item_id to "
                            "re-parent, with lifecycle='retired' to retire, or with phase_order to "
                            "reorder under siblings."
                        ),
                    },
                    "phase_order": {
                        "type": "string",
                        "description": (
                            "Explicit phase_order override (e.g. '33.1'). When omitted, phase_order "
                            "is auto-assigned from sibling insertion order."
                        ),
                    },
                },
                "required": [],
            },
        },
    ),
    "praxis_operator_decisions": (
        tool_praxis_operator_decisions,
        {
            "description": (
                "List or record canonical operator decisions through the shared operator_decisions table.\n\n"
                "USE WHEN: you need durable, queryable operator decisions such as architecture policy rows, "
                "and you want them stored as first-class control authority instead of hidden in prose.\n\n"
                "EXAMPLE: praxis_operator_decisions(action='list', decision_kind='architecture_policy')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "record"],
                        "default": "list",
                    },
                    "decision_key": {"type": "string"},
                    "decision_kind": {"type": "string"},
                    "decision_status": {"type": "string", "default": "decided"},
                    "title": {"type": "string"},
                    "rationale": {"type": "string"},
                    "decided_by": {"type": "string"},
                    "decision_source": {"type": "string"},
                    "decision_scope_kind": {"type": "string"},
                    "decision_scope_ref": {"type": "string"},
                    "effective_from": {"type": "string"},
                    "effective_to": {"type": "string"},
                    "as_of": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100,
                    },
                },
            },
        },
    ),
    "praxis_operator_relations": (
        tool_praxis_operator_relations,
        {
            "description": (
                "Record canonical functional areas and cross-object semantic relations.\n\n"
                "USE WHEN: a bug, roadmap item, repo path, document, workflow target, or decision "
                "needs one explicit semantic edge instead of hidden tags or prose.\n\n"
                "ACTIONS:\n"
                "  'record_functional_area' — create or update one functional area row\n"
                "  'record_relation' — create or update one cross-object relation row\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_relations(action='record_functional_area', area_slug='checkout', title='Checkout', summary='Shared checkout semantics')\n"
                "  praxis_operator_relations(action='record_relation', relation_kind='grouped_in', source_kind='roadmap_item', source_ref='roadmap_item.checkout', target_kind='functional_area', target_ref='checkout')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["record_functional_area", "record_relation"],
                    },
                    "area_slug": {"type": "string"},
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "area_status": {
                        "type": "string",
                        "enum": ["active", "inactive"],
                        "default": "active",
                    },
                    "relation_kind": {"type": "string"},
                    "source_kind": {
                        "type": "string",
                        "enum": [
                            "issue",
                            "bug",
                            "roadmap_item",
                            "operator_decision",
                            "cutover_gate",
                            "workflow_class",
                            "schedule_definition",
                            "workflow_run",
                            "document",
                            "repo_path",
                            "functional_area",
                        ],
                    },
                    "source_ref": {"type": "string"},
                    "target_kind": {
                        "type": "string",
                        "enum": [
                            "issue",
                            "bug",
                            "roadmap_item",
                            "operator_decision",
                            "cutover_gate",
                            "workflow_class",
                            "schedule_definition",
                            "workflow_run",
                            "document",
                            "repo_path",
                            "functional_area",
                        ],
                    },
                    "target_ref": {"type": "string"},
                    "relation_status": {
                        "type": "string",
                        "enum": ["active", "inactive"],
                        "default": "active",
                    },
                    "relation_metadata": {
                        "type": "object",
                        "description": "Optional structured context for the relation.",
                    },
                    "bound_by_decision_id": {"type": "string"},
                    "created_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "updated_at": {"type": "string", "description": "ISO-8601 datetime string"},
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_semantic_assertions": (
        tool_praxis_semantic_assertions,
        {
            "description": (
                "Register semantic predicates, record or retract semantic assertions, and query the canonical semantic substrate.\n\n"
                "USE WHEN: semantics should become typed authority rows with explicit provenance and validity "
                "instead of hidden metadata fields or prose.\n\n"
                "ACTIONS:\n"
                "  'list' — query semantic assertions through the CQRS read path\n"
                "  'register_predicate' — register or update one predicate vocabulary row\n"
                "  'record_assertion' — record one semantic assertion row and emit a semantic bus event\n"
                "  'retract_assertion' — retract one semantic assertion row and emit a semantic bus event\n\n"
                "EXAMPLES:\n"
                "  praxis_semantic_assertions(action='register_predicate', predicate_slug='grouped_in', subject_kind_allowlist=['bug'], object_kind_allowlist=['functional_area'])\n"
                "  praxis_semantic_assertions(action='record_assertion', predicate_slug='grouped_in', subject_kind='bug', subject_ref='bug.checkout.1', object_kind='functional_area', object_ref='functional_area.checkout', source_kind='operator', source_ref='nate')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list",
                            "register_predicate",
                            "record_assertion",
                            "retract_assertion",
                        ],
                        "default": "list",
                    },
                    "predicate_slug": {"type": "string"},
                    "subject_kind_allowlist": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "object_kind_allowlist": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "cardinality_mode": {
                        "type": "string",
                        "enum": [
                            "many",
                            "single_active_per_subject",
                            "single_active_per_edge",
                        ],
                        "default": "many",
                    },
                    "predicate_status": {
                        "type": "string",
                        "enum": ["active", "inactive"],
                        "default": "active",
                    },
                    "description": {"type": "string"},
                    "subject_kind": {"type": "string"},
                    "subject_ref": {"type": "string"},
                    "object_kind": {"type": "string"},
                    "object_ref": {"type": "string"},
                    "qualifiers_json": {"type": "object"},
                    "source_kind": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "evidence_ref": {"type": "string"},
                    "bound_decision_id": {"type": "string"},
                    "valid_from": {"type": "string"},
                    "valid_to": {"type": "string"},
                    "assertion_status": {
                        "type": "string",
                        "enum": ["active", "superseded", "retracted"],
                        "default": "active",
                    },
                    "semantic_assertion_id": {"type": "string"},
                    "retracted_at": {"type": "string"},
                    "created_at": {"type": "string"},
                    "updated_at": {"type": "string"},
                    "as_of": {"type": "string"},
                    "active_only": {"type": "boolean", "default": True},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100,
                    },
                },
            },
        },
    ),
    "praxis_operator_native_primary_cutover_gate": (
        tool_praxis_operator_native_primary_cutover_gate,
        {
            "description": (
                "Admit a native primary cutover gate into operator-control decision and gate authority tables.\n\n"
                "USE WHEN: you need a tracked cutover decision for one target (roadmap item, workflow class, or "
                "schedule definition) with optional policy/evidence payloads.\n\n"
                "EXAMPLE: praxis_operator_native_primary_cutover_gate(\n"
                "  decided_by='operator-auto',\n"
                "  decision_source='runbook',\n"
                "  rationale='manual rollout hold ended',\n"
                "  roadmap_item_id='roadmap_item.platform.deploy',\n"
                "  gate_policy={'rollout_window':'canary'},\n"
                "  required_evidence={'checks':['operator-readiness']}\n"
                ")"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "decided_by": {
                        "type": "string",
                        "description": "Operator principal or service taking action.",
                    },
                    "decision_source": {
                        "type": "string",
                        "description": "Source system or artifact for this cutover decision.",
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Human-readable justification for opening the gate.",
                    },
                    "roadmap_item_id": {"type": "string"},
                    "workflow_class_id": {"type": "string"},
                    "schedule_definition_id": {"type": "string"},
                    "title": {"type": "string"},
                    "gate_name": {"type": "string"},
                    "gate_policy": {
                        "type": "object",
                        "description": "Optional policy envelope attached to the gate.",
                    },
                    "required_evidence": {
                        "type": "object",
                        "description": "Optional evidence envelope attached to the gate.",
                    },
                    "decided_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "opened_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "created_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "updated_at": {"type": "string", "description": "ISO-8601 datetime string"},
                },
                "required": ["decided_by", "decision_source", "rationale"],
            },
        },
    ),
    "praxis_operator_architecture_policy": (
        tool_praxis_operator_architecture_policy,
        {
            "description": (
                "Record a durable architecture-policy decision in operator authority.\n\n"
                "USE WHEN: explicit operator or CTO guidance should become a typed, queryable "
                "decision row instead of living only in chat, docs, or migration folklore.\n\n"
                "EXAMPLE: praxis_operator_architecture_policy(\n"
                "  authority_domain='decision_tables',\n"
                "  policy_slug='db-native-authority',\n"
                "  title='Decision tables are DB-native authority',\n"
                "  decided_by='nate',\n"
                "  decision_source='cto.guidance',\n"
                "  rationale='Authority, durable state, and orchestration belong in DB primitives.'\n"
                ")"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "authority_domain": {
                        "type": "string",
                        "description": "Typed authority domain scope, for example decision_tables.",
                    },
                    "policy_slug": {
                        "type": "string",
                        "description": "Stable slug for the architecture policy inside the domain.",
                    },
                    "title": {
                        "type": "string",
                        "description": "Short title for the policy decision.",
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Durable reason for the policy decision.",
                    },
                    "decided_by": {
                        "type": "string",
                        "description": "Principal or operator recording the decision.",
                    },
                    "decision_source": {
                        "type": "string",
                        "description": "Source artifact or authority lane for the decision.",
                    },
                    "effective_from": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime when the policy becomes effective.",
                    },
                    "effective_to": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime when the policy expires.",
                    },
                    "decided_at": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the decision timestamp.",
                    },
                    "created_at": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the row creation timestamp.",
                    },
                    "updated_at": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the row update timestamp.",
                    },
                },
                "required": [
                    "authority_domain",
                    "policy_slug",
                    "title",
                    "rationale",
                    "decided_by",
                    "decision_source",
                ],
            },
        },
    ),
    "praxis_operator_closeout": (
        tool_praxis_operator_closeout,
        {
            "description": (
                "Preview or commit proof-backed bug and roadmap closeout through the shared reconciliation gate.\n\n"
                "USE WHEN: you want to safely close bugs and linked roadmap items from explicit validates_fix evidence "
                "without mutating truth from inference alone. Preview returns candidates and skips; commit applies only "
                "the proof-backed subset.\n\n"
                "EXAMPLE: praxis_operator_closeout(action='preview', bug_ids=['bug.operator.fix.123'])"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["preview", "commit"],
                        "default": "preview",
                    },
                    "bug_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "roadmap_item_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
    ),
    "praxis_operator_roadmap_view": (
        tool_praxis_operator_roadmap_view,
        {
            "description": (
                "Read one roadmap subtree and its dependency edges from DB-backed authority.\n\n"
                "USE WHEN: you want the full package view for a roadmap item, including generated child waves, "
                "derived roadmap item clusters, external dependency edges, canonical semantic neighbors, "
                "and a rendered markdown outline.\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_roadmap_view()\n"
                "  praxis_operator_roadmap_view(root_roadmap_item_id='roadmap_item.authority.cleanup.unified.operator.write.validation.gate')\n"
                "  praxis_operator_roadmap_view(root_roadmap_item_id='roadmap_item.authority.cleanup.unified.operator.write.validation.gate', semantic_neighbor_limit=8)"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "root_roadmap_item_id": {"type": "string"},
                    "semantic_neighbor_limit": {
                        "type": "integer",
                        "description": "How many external roadmap neighbors to include from canonical semantic assertions.",
                        "default": 5,
                        "minimum": 0,
                    },
                },
            },
        },
    ),
    "praxis_circuits": (
        tool_praxis_circuits,
        {
            "description": (
                "Inspect effective circuit-breaker state or apply a durable manual override for one provider.\n\n"
                "ACTIONS:\n"
                "  'list'  — show effective state, runtime state, and any active manual override metadata\n"
                "  'history' — show append-only override decision history from operator authority\n"
                "  'open'  — force the breaker open for one provider until reset or effective_to\n"
                "  'close' — force the breaker closed for one provider until reset or effective_to\n"
                "  'reset' — clear the manual override and return to runtime-managed breaker behavior\n\n"
                "USE WHEN: you need operator control over provider traffic without mutating in-memory state by hand."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "history", "open", "close", "reset"],
                        "default": "list",
                    },
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider slug for open, close, reset, or to filter list/history output.",
                    },
                    "effective_to": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime when the manual override expires.",
                    },
                    "effective_from": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the decision timestamp.",
                    },
                    "reason_code": {
                        "type": "string",
                        "description": "Operator reason code stored on the decision row.",
                        "default": "operator_control",
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Human-readable rationale for the override.",
                    },
                    "decided_by": {
                        "type": "string",
                        "description": "Principal applying the override.",
                    },
                    "decision_source": {
                        "type": "string",
                        "description": "Source artifact or workflow applying the override.",
                    },
                },
            },
        },
    ),
}
