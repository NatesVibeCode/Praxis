"""Tools: praxis_operator_view, praxis_status."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems

from ..subsystems import _subs


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


def _compat_status_snapshot(*, since_hours: int) -> dict[str, Any]:
    from runtime import receipt_store
    from runtime.quality_views import load_failure_category_zones

    conn = _subs.get_pg_conn()
    totals = receipt_store.receipt_stats(since_hours=since_hours, conn=conn).get("totals", {})
    receipt_count = int(totals.get("receipts") or 0)
    records = (
        receipt_store.list_receipts(limit=receipt_count, since_hours=since_hours)
        if receipt_count > 0
        else []
    )
    total = len(records)
    succeeded = sum(1 for record in records if record.status == "succeeded")
    failure_counts: dict[str, int] = {}
    for record in records:
        if record.failure_code:
            failure_counts[record.failure_code] = failure_counts.get(record.failure_code, 0) + 1
    top_failures = dict(
        sorted(failure_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    )
    pass_rate = (succeeded / total) if total > 0 else 0.0

    zone_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    zone_authority_ready = True
    zone_authority_error: str | None = None
    try:
        zone_lookup = load_failure_category_zones(conn, consumer="operator.status_snapshot")
        for record in records:
            payload = record.to_dict()
            failure_classification = payload.get("failure_classification")
            if isinstance(failure_classification, dict):
                category = str(failure_classification.get("category") or "").strip()
            else:
                category = str(payload.get("failure_category") or "").strip()
            if not category:
                continue
            category_counts[category] = category_counts.get(category, 0) + 1
            zone = zone_lookup.get(category, "internal")
            zone_counts[zone] = zone_counts.get(zone, 0) + 1
    except Exception as exc:
        zone_authority_ready = False
        zone_authority_error = str(exc)

    external_failures = zone_counts.get("external", 0)
    adjusted_denominator = total - external_failures
    adjusted_pass_rate = None
    if zone_authority_ready:
        adjusted_pass_rate = (
            (succeeded / adjusted_denominator) if adjusted_denominator > 0 else 0.0
        )

    in_flight = []
    try:
        running_rows = conn.execute(
            """SELECT run_id, current_state, requested_at, request_envelope
            FROM workflow_runs
            WHERE current_state = 'running'
            ORDER BY requested_at DESC LIMIT 10""",
        )
        now = datetime.now(timezone.utc)
        for row in running_rows:
            envelope = row["request_envelope"]
            if not isinstance(envelope, dict):
                envelope = {}
            outbox_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM workflow_outbox WHERE run_id = $1 AND authority_table = 'receipts'",
                row["run_id"],
            )
            completed = int(outbox_count[0]["cnt"]) if outbox_count else 0
            elapsed = None
            if row["requested_at"]:
                elapsed = round((now - row["requested_at"]).total_seconds(), 1)
            total_jobs = int(envelope.get("total_jobs") or 0)
            if total_jobs > 0 and completed >= total_jobs:
                continue
            in_flight.append(
                {
                    "run_id": row["run_id"],
                    "workflow_name": envelope.get("name") or envelope.get("spec_name", ""),
                    "total_jobs": total_jobs,
                    "completed_jobs": completed,
                    "elapsed_seconds": elapsed,
                }
            )
    except Exception:
        pass

    result: dict[str, Any] = {
        "total_workflows": total,
        "pass_rate": round(pass_rate, 4),
        "adjusted_pass_rate": (
            round(adjusted_pass_rate, 4)
            if adjusted_pass_rate is not None
            else None
        ),
        "failure_breakdown": {
            "by_zone": zone_counts,
            "by_category": category_counts,
        },
        "top_failure_codes": top_failures,
        "since_hours": since_hours,
        "zone_authority_ready": zone_authority_ready,
        "observability_state": "ready" if zone_authority_ready else "degraded",
        "queue_depth": 0,
        "queue_depth_status": "unknown",
        "queue_depth_pending": 0,
        "queue_depth_ready": 0,
        "queue_depth_claimed": 0,
        "queue_depth_running": 0,
        "queue_depth_total": 0,
        "queue_depth_warning_threshold": 0,
        "queue_depth_critical_threshold": 0,
        "queue_depth_utilization_pct": 0.0,
        "queue_depth_error": "queue depth unavailable in compatibility mode",
    }
    if zone_authority_error:
        result["errors"] = [
            {
                "code": "failure_category_zones_lookup_failed",
                "message": zone_authority_error,
            }
        ]
    if in_flight:
        result["in_flight_workflows"] = in_flight
    return result


def tool_praxis_status(params: dict) -> dict:
    """Recent workflow status from canonical receipts, with categorized failure breakdown."""
    since_hours = params.get("since_hours", 24)
    payload = {"since_hours": since_hours}
    try:
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.status_snapshot",
            payload=payload,
        )
    except Exception as exc:
        try:
            conn = _subs.get_pg_conn()
        except Exception:
            return {"error": str(exc)}
        if not hasattr(conn, "fetchrow"):
            return _compat_status_snapshot(since_hours=max(1, int(since_hours or 24)))
        return {"error": str(exc)}


def tool_praxis_maintenance(params: dict) -> dict:
    """Run explicit operator maintenance actions outside observability surfaces."""

    action = params.get("action", "")
    operation_name = {
        "reset_metrics": "operator.metrics_reset",
        "backfill_bug_replay_provenance": "operator.bug_replay_provenance_backfill",
        "backfill_semantic_bridges": "operator.semantic_bridges_backfill",
        "refresh_semantic_projection": "operator.semantic_projection_refresh",
    }.get(action)
    if operation_name is None:
        return {
            "error": (
                "Unknown maintenance action. Supported actions: reset_metrics, "
                "backfill_bug_replay_provenance, backfill_semantic_bridges, "
                "refresh_semantic_projection"
            )
        }
    as_of = params.get("as_of")
    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "confirm": bool(params.get("confirm", False)),
            "before_date": params.get("before_date"),
            "limit": params.get("limit"),
            "open_only": bool(params.get("open_only", True)),
            "receipt_limit": params.get("receipt_limit", 1),
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


def tool_praxis_operator_view(params: dict) -> dict:
    """Observability views: operator status, scoreboard, workflow graph, operator graph, semantics, lineage, issue backlog, and replay-ready bugs."""

    try:
        view = str(params.get("view") or "status").strip().lower()
        operation_name = {
            "status": "operator.run_status",
            "scoreboard": "operator.run_scoreboard",
            "graph": "operator.run_graph",
            "operator_graph": "operator.graph_projection",
            "semantics": "semantic_assertions.list",
            "lineage": "operator.run_lineage",
            "issue_backlog": "operator.issue_backlog",
            "replay_ready_bugs": "operator.replay_ready_bugs",
        }.get(view)
        if operation_name is None:
            raise ValueError(
                "Unknown view. Options: status, scoreboard, graph, operator_graph, "
                "semantics, lineage, issue_backlog, replay_ready_bugs"
            )
        payload: dict[str, Any] = {}
        if view in {"status", "scoreboard", "graph", "lineage"}:
            payload["run_id"] = params.get("run_id")
        if view == "operator_graph":
            payload["as_of"] = (
                _parse_iso_datetime(params.get("as_of"), field_name="as_of")
                if params.get("as_of") is not None
                else None
            )
        if view == "semantics":
            payload = {
                "predicate_slug": params.get("predicate_slug"),
                "subject_kind": params.get("subject_kind"),
                "subject_ref": params.get("subject_ref"),
                "object_kind": params.get("object_kind"),
                "object_ref": params.get("object_ref"),
                "source_kind": params.get("source_kind"),
                "source_ref": params.get("source_ref"),
                "active_only": bool(params.get("active_only", True)),
                "as_of": (
                    _parse_iso_datetime(params.get("as_of"), field_name="as_of")
                    if params.get("as_of") is not None
                    else None
                ),
                "limit": int(params.get("limit", 50) or 50),
            }
        if view == "issue_backlog":
            payload = {
                "limit": int(params.get("limit", 50) or 50),
                "open_only": bool(params.get("open_only", True)),
                "status": params.get("status"),
            }
        if view == "replay_ready_bugs":
            if bool(params.get("refresh_backfill", False)):
                raise ValueError(
                    "replay_ready_bugs is read-only; use maintenance backfill instead"
                )
            payload = {
                "limit": int(params.get("limit", 50) or 50),
            }
        return execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload=payload,
        )
    except Exception as exc:
        return {"error": str(exc)}


def tool_praxis_operator_write(params: dict) -> dict:
    """Preview, validate, or commit roadmap rows through the shared operator-write gate."""

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.roadmap_write",
        payload={
            "action": params.get("action", "preview"),
            "title": params.get("title", ""),
            "intent_brief": params.get("intent_brief", ""),
            "template": params.get("template", "single_capability"),
            "priority": params.get("priority", "p2"),
            "parent_roadmap_item_id": params.get("parent_roadmap_item_id"),
            "slug": params.get("slug"),
            "depends_on": params.get("depends_on"),
            "source_bug_id": params.get("source_bug_id"),
            "registry_paths": params.get("registry_paths"),
            "decision_ref": params.get("decision_ref"),
            "item_kind": params.get("item_kind"),
            "tier": params.get("tier"),
            "phase_ready": params.get("phase_ready"),
            "approval_tag": params.get("approval_tag"),
            "reference_doc": params.get("reference_doc"),
            "outcome_gate": params.get("outcome_gate"),
        },
    )


def tool_praxis_operator_decisions(params: dict) -> dict:
    """Record or list canonical operator decisions through operator_decisions."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "list":
        as_of = params.get("as_of")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.decision_list",
            payload={
                "decision_kind": params.get("decision_kind"),
                "decision_scope_kind": params.get("decision_scope_kind"),
                "decision_scope_ref": params.get("decision_scope_ref"),
                "as_of": (
                    _parse_iso_datetime(as_of, field_name="as_of")
                    if as_of is not None
                    else None
                ),
                "limit": int(params.get("limit", 100) or 100),
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
        as_of = params.get("as_of")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.list",
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
                "limit": int(params.get("limit", 100) or 100),
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
            "bug_ids": params.get("bug_ids"),
            "roadmap_item_ids": params.get("roadmap_item_ids"),
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

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.roadmap_tree",
        payload={
            "root_roadmap_item_id": root_roadmap_item_id,
            "semantic_neighbor_limit": int(params.get("semantic_neighbor_limit", 5) or 5),
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
    "praxis_status": (
        tool_praxis_status,
        {
            "description": (
                "Quick snapshot of how workflows are performing — total runs, pass/fail rate, "
                "categorized failure breakdown by zone (external/config/internal), and adjusted "
                "pass rate that excludes external provider failures.\n\n"
                "USE WHEN: you want a quick health check on workflow activity. 'How are things going?' "
                "'What's the pass rate?' 'Any failures today?' 'What's our real system quality?'\n\n"
                "EXAMPLE: praxis_status()  or  praxis_status(since_hours=4)\n\n"
                "DO NOT USE: for deep inspection of a specific run (use praxis_workflow action='inspect'), "
                "or for full system health (use praxis_health)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "since_hours": {"type": "integer", "description": "Lookback window in hours.", "default": 24},
                },
            },
        },
    ),
    "praxis_maintenance": (
        tool_praxis_maintenance,
        {
            "description": (
                "Run explicit operator maintenance actions that mutate observability aggregates.\n\n"
                "USE WHEN: you need to clean polluted quality metrics or routing counters without "
                "mixing destructive actions into read-only status surfaces.\n\n"
                "EXAMPLES:\n"
                "  praxis_maintenance(action='reset_metrics', confirm=true)\n"
                "  praxis_maintenance(action='backfill_bug_replay_provenance')\n"
                "  praxis_maintenance(action='backfill_semantic_bridges')\n"
                "  praxis_maintenance(action='refresh_semantic_projection')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Maintenance action to run.",
                        "enum": [
                            "reset_metrics",
                            "backfill_bug_replay_provenance",
                            "backfill_semantic_bridges",
                            "refresh_semantic_projection",
                        ],
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Required for destructive maintenance actions.",
                        "default": False,
                    },
                    "before_date": {
                        "type": "string",
                        "description": "ISO date for surgical reset (only delete data before this date).",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Optional scan limit for bug replay provenance backfill.",
                        "minimum": 0,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, only scan unresolved bugs during replay provenance backfill.",
                        "default": True,
                    },
                    "receipt_limit": {
                        "type": "integer",
                        "description": "Receipt context lookback for replay provenance backfill.",
                        "minimum": 1,
                        "default": 1,
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 cutoff for semantic bridge replay or semantic projection refresh.",
                    },
                    "include_object_relations": {
                        "type": "boolean",
                        "description": "When action='backfill_semantic_bridges', replay operator_object_relations into semantic assertions.",
                        "default": True,
                    },
                    "include_operator_decisions": {
                        "type": "boolean",
                        "description": "When action='backfill_semantic_bridges', replay operator_decisions into semantic assertions.",
                        "default": True,
                    },
                    "include_roadmap_items": {
                        "type": "boolean",
                        "description": "When action='backfill_semantic_bridges', replay roadmap_items semantic fields into semantic assertions.",
                        "default": True,
                    },
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_operator_view": (
        tool_praxis_operator_view,
        {
            "description": (
                "Render detailed operator observability views — deeper than praxis_status.\n\n"
                "VIEWS:\n"
                "  'status'     — operator status with outbox depth, subscription state, watermark drift\n"
                "  'scoreboard' — cutover readiness: which gates are proven, which are blocked\n"
                "  'graph'      — run-scoped workflow graph topology for one workflow run\n"
                "  'operator_graph' — cross-domain operator graph projection sourced from semantic assertions first,\n"
                "                   with legacy relation rows only as compatibility input\n"
                "  'semantics'  — unified semantic assertion inspection over semantic_current_assertions\n"
                "  'lineage'    — graph lineage for one run, including operator frames\n"
                "  'issue_backlog' — canonical upstream issue intake rows before bug promotion\n"
                "  'replay_ready_bugs' — replayable bug backlog from already-authoritative provenance\n\n"
                "USE WHEN: you need detailed operational insight beyond pass/fail rates. "
                "'Show me the workflow graph for this run.' 'Show me the operator graph.' "
                "'Show me the semantic links.' "
                "'What's the cutover status?' 'Show the issue backlog.' "
                "'Which bugs can I replay right now?'\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_view(view='graph', run_id='run_123')\n"
                "  praxis_operator_view(view='operator_graph')\n"
                "  praxis_operator_view(view='semantics', predicate_slug='grouped_in')\n"
                "  praxis_operator_view(view='lineage', run_id='run_123')\n"
                "  praxis_operator_view(view='issue_backlog')\n"
                "  praxis_operator_view(view='replay_ready_bugs')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "view": {
                        "type": "string",
                        "description": "View to render: 'status', 'scoreboard', 'graph', 'operator_graph', 'semantics', 'lineage', 'issue_backlog', or 'replay_ready_bugs'.",
                        "enum": ["status", "scoreboard", "graph", "operator_graph", "semantics", "lineage", "issue_backlog", "replay_ready_bugs"],
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Required for run-scoped views: 'status', 'scoreboard', 'graph', and 'lineage'. Not used by 'operator_graph' or 'semantics'.",
                    },
                    "status": {
                        "type": "string",
                        "description": "Optional issue status filter for issue_backlog, e.g. open or resolved.",
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, exclude resolved issues from issue_backlog.",
                        "default": True,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return for issue_backlog, replay_ready_bugs, or semantics.",
                        "minimum": 1,
                        "default": 50,
                    },
                    "predicate_slug": {
                        "type": "string",
                        "description": "Optional semantic predicate filter for view='semantics'.",
                    },
                    "subject_kind": {
                        "type": "string",
                        "description": "Optional semantic subject kind filter for view='semantics'.",
                    },
                    "subject_ref": {
                        "type": "string",
                        "description": "Optional semantic subject ref filter for view='semantics'.",
                    },
                    "object_kind": {
                        "type": "string",
                        "description": "Optional semantic object kind filter for view='semantics'.",
                    },
                    "object_ref": {
                        "type": "string",
                        "description": "Optional semantic object ref filter for view='semantics'.",
                    },
                    "source_kind": {
                        "type": "string",
                        "description": "Optional semantic source kind filter for view='semantics'.",
                    },
                    "source_ref": {
                        "type": "string",
                        "description": "Optional semantic source ref filter for view='semantics'.",
                    },
                    "active_only": {
                        "type": "boolean",
                        "description": "When view='semantics', prefer semantic_current_assertions instead of historical reads.",
                        "default": True,
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 as-of timestamp for historical semantic inspection or operator_graph projection reads.",
                    },
                },
                "required": ["view"],
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
                    "registry_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "decision_ref": {"type": "string"},
                    "item_kind": {
                        "type": "string",
                        "enum": ["capability", "initiative"],
                    },
                    "tier": {"type": "string"},
                    "phase_ready": {"type": "boolean"},
                    "approval_tag": {"type": "string"},
                    "reference_doc": {"type": "string"},
                    "outcome_gate": {"type": "string"},
                },
                "required": ["title", "intent_brief"],
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
                "external dependency edges, canonical semantic neighbors, and a rendered markdown outline.\n\n"
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
