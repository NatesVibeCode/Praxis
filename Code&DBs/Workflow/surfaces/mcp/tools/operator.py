"""Tools: praxis_operator_view, praxis_status."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems
from surfaces.api.handlers import workflow_query_core
from storage.postgres.workflow_runtime_repository import reset_observability_metrics

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


def tool_praxis_status(params: dict) -> dict:
    """Recent workflow status from canonical receipts, with categorized failure breakdown."""
    from runtime.receipt_store import list_receipts

    conn = _subs.get_pg_conn()

    since_hours = params.get("since_hours", 24)

    records = list_receipts(limit=5000, since_hours=since_hours)
    total = len(records)
    succeeded = sum(1 for record in records if record.status == "succeeded")
    failure_counts: dict[str, int] = {}
    for record in records:
        if record.failure_code:
            failure_counts[record.failure_code] = failure_counts.get(record.failure_code, 0) + 1
    top_failures = dict(sorted(failure_counts.items(), key=lambda item: (-item[1], item[0]))[:10])

    pass_rate = (succeeded / total) if total > 0 else 0.0

    # ── Categorized failure breakdown ─────────────────────────────────
    zone_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    try:
        zone_lookup = {
            str(row["category"]): str(row["zone"])
            for row in conn.execute("SELECT category, zone FROM failure_category_zones")
            if row.get("category")
        }
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
    except Exception:
        pass  # Graceful degradation if zone table missing

    # Adjusted pass rate: exclude external (provider/network) failures from denominator
    external_failures = zone_counts.get("external", 0)
    adjusted_denominator = total - external_failures
    adjusted_pass_rate = (succeeded / adjusted_denominator) if adjusted_denominator > 0 else 0.0

    # In-flight workflows from workflow_runs
    in_flight = []
    try:
        running_rows = conn.execute(
            """SELECT run_id, current_state, requested_at, request_envelope
            FROM workflow_runs
            WHERE current_state = 'running'
            ORDER BY requested_at DESC LIMIT 10""",
        )
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        for r in running_rows:
            envelope = r["request_envelope"] if isinstance(r["request_envelope"], dict) else _json.loads(r["request_envelope"])
            # Count completed jobs in outbox
            outbox_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM workflow_outbox WHERE run_id = $1 AND authority_table = 'receipts'",
                r["run_id"],
            )
            completed = int(outbox_count[0]["cnt"]) if outbox_count else 0
            elapsed = None
            if r["requested_at"]:
                elapsed = round((_dt.now(_tz.utc) - r["requested_at"]).total_seconds(), 1)
            total_jobs = envelope.get("total_jobs", 0)
            # Skip stale runs where all jobs already finished
            if total_jobs > 0 and completed >= total_jobs:
                continue
            in_flight.append({
                "run_id": r["run_id"],
                "workflow_name": envelope.get("name") or envelope.get("spec_name", ""),
                "total_jobs": total_jobs,
                "completed_jobs": completed,
                "elapsed_seconds": elapsed,
            })
    except Exception:
        pass

    result: dict[str, Any] = {
        "total_workflows": total,
        "pass_rate": round(pass_rate, 4),
        "adjusted_pass_rate": round(adjusted_pass_rate, 4),
        "failure_breakdown": {
            "by_zone": zone_counts,
            "by_category": category_counts,
        },
        "top_failure_codes": top_failures,
        "since_hours": since_hours,
    }
    if in_flight:
        result["in_flight_workflows"] = in_flight
    return result


def tool_praxis_maintenance(params: dict) -> dict:
    """Run explicit operator maintenance actions outside observability surfaces."""

    action = params.get("action", "")
    if action == "backfill_bug_replay_provenance":
        bug_tracker = _subs.get_bug_tracker()
        limit_raw = params.get("limit")
        limit = None if limit_raw in (None, "") else max(0, int(limit_raw))
        return {
            "backfill": bug_tracker.bulk_backfill_replay_provenance(
                limit=limit,
                open_only=bool(params.get("open_only", True)),
                receipt_limit=max(1, int(params.get("receipt_limit", 1) or 1)),
            )
        }
    if action != "reset_metrics":
        return {
            "error": (
                "Unknown maintenance action. Supported actions: reset_metrics, "
                "backfill_bug_replay_provenance"
            )
        }
    if not params.get("confirm"):
        return {
            "error": (
                "Pass confirm=true to reset metrics. This truncates quality_rollups, "
                "agent_profiles, failure_catalog and zeros routing counters."
            )
        }
    return reset_observability_metrics(
        _subs.get_pg_conn(),
        before_date=params.get("before_date"),
    )


def tool_praxis_operator_view(params: dict) -> dict:
    """Observability views: operator status, scoreboard, graph, lineage, issue backlog, and replay-ready bugs."""

    try:
        return workflow_query_core.handle_operator_view(_subs, params)
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
    control = OperatorControlFrontdoor()
    if action == "record_functional_area":
        return control.record_functional_area(
            area_slug=str(params.get("area_slug") or ""),
            title=str(params.get("title") or ""),
            summary=str(params.get("summary") or ""),
            area_status=str(params.get("area_status") or "active"),
            created_at=params.get("created_at"),
            updated_at=params.get("updated_at"),
        )
    if action == "record_relation":
        return control.record_operator_object_relation(
            relation_kind=str(params.get("relation_kind") or ""),
            source_kind=str(params.get("source_kind") or ""),
            source_ref=str(params.get("source_ref") or ""),
            target_kind=str(params.get("target_kind") or ""),
            target_ref=str(params.get("target_ref") or ""),
            relation_status=str(params.get("relation_status") or "active"),
            relation_metadata=params.get("relation_metadata"),
            bound_by_decision_id=params.get("bound_by_decision_id"),
            created_at=params.get("created_at"),
            updated_at=params.get("updated_at"),
        )
    return {
        "error": (
            "Unknown action. Supported actions: record_functional_area, record_relation"
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
    return OperatorControlFrontdoor().record_architecture_policy_decision(
        authority_domain=params.get("authority_domain", ""),
        policy_slug=params.get("policy_slug", ""),
        title=params.get("title", ""),
        rationale=params.get("rationale", ""),
        decided_by=params.get("decided_by", ""),
        decision_source=params.get("decision_source", ""),
        effective_from=(
            _parse_iso_datetime(effective_from, field_name="effective_from")
            if effective_from is not None
            else None
        ),
        effective_to=(
            _parse_iso_datetime(effective_to, field_name="effective_to")
            if effective_to is not None
            else None
        ),
        decided_at=(
            _parse_iso_datetime(decided_at, field_name="decided_at")
            if decided_at is not None
            else None
        ),
        created_at=(
            _parse_iso_datetime(created_at, field_name="created_at")
            if created_at is not None
            else None
        ),
        updated_at=(
            _parse_iso_datetime(updated_at, field_name="updated_at")
            if updated_at is not None
            else None
        ),
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
    """Read one roadmap subtree and its dependency edges from DB-backed authority."""

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
        provider_slug = str(params.get("provider_slug") or "").strip().lower()
        rows = _subs.get_pg_conn().execute(
            """
            SELECT
                operator_decision_id,
                decision_key,
                decision_kind,
                decision_status,
                rationale,
                decided_by,
                decision_source,
                effective_from,
                effective_to,
                decided_at,
                created_at,
                updated_at,
                decision_scope_kind,
                decision_scope_ref
            FROM operator_decisions
            WHERE decision_scope_kind = 'provider'
              AND decision_kind IN (
                    'circuit_breaker_reset',
                    'circuit_breaker_force_open',
                    'circuit_breaker_force_closed'
              )
              AND ($1::text = '' OR decision_scope_ref = $1)
            ORDER BY decided_at DESC, created_at DESC, operator_decision_id DESC
            """,
            provider_slug,
        )
        history: list[dict[str, object]] = []
        for row in rows:
            decision_key = str(row.get("decision_key") or "")
            row_provider_slug = str(row.get("decision_scope_ref") or "").strip().lower()
            history.append(
                {
                    "provider_slug": row_provider_slug,
                    "operator_decision_id": str(row.get("operator_decision_id") or ""),
                    "decision_key": decision_key,
                    "decision_kind": str(row.get("decision_kind") or ""),
                    "decision_status": str(row.get("decision_status") or ""),
                    "rationale": str(row.get("rationale") or ""),
                    "decided_by": str(row.get("decided_by") or ""),
                    "decision_source": str(row.get("decision_source") or ""),
                    "effective_from": row.get("effective_from").isoformat() if row.get("effective_from") is not None else None,
                    "effective_to": row.get("effective_to").isoformat() if row.get("effective_to") is not None else None,
                    "decided_at": row.get("decided_at").isoformat() if row.get("decided_at") is not None else None,
                    "created_at": row.get("created_at").isoformat() if row.get("created_at") is not None else None,
                    "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") is not None else None,
                    "decision_scope_kind": str(row.get("decision_scope_kind") or ""),
                    "decision_scope_ref": row_provider_slug,
                }
            )
        return {"history": history}

    if action == "list":
        from runtime.circuit_breaker import get_circuit_breakers

        try:
            payload = get_circuit_breakers().all_states()
        except Exception as exc:
            return {"error": str(exc)}
        provider_slug = str(params.get("provider_slug") or "").strip().lower()
        if provider_slug:
            return {
                "circuits": (
                    {provider_slug: payload[provider_slug]}
                    if provider_slug in payload
                    else {}
                )
            }
        return {"circuits": payload}

    provider_slug = str(params.get("provider_slug") or "").strip().lower()
    if not provider_slug:
        return {"error": "provider_slug is required for circuit override actions"}

    if action not in {"open", "close", "reset"}:
        return {"error": "Unknown action. Supported actions: list, history, open, close, reset"}

    effective_to = params.get("effective_to")
    effective_from = params.get("effective_from")
    return OperatorControlFrontdoor().set_circuit_breaker_override(
        provider_slug=provider_slug,
        override_state={
            "open": "open",
            "close": "closed",
            "reset": "reset",
        }[action],
        effective_to=(
            _parse_iso_datetime(effective_to, field_name="effective_to")
            if effective_to is not None
            else None
        ),
        reason_code=str(params.get("reason_code") or "operator_control"),
        rationale=params.get("rationale"),
        effective_from=(
            _parse_iso_datetime(effective_from, field_name="effective_from")
            if effective_from is not None
            else None
        ),
        decided_by=params.get("decided_by"),
        decision_source=params.get("decision_source"),
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
                "  praxis_maintenance(action='backfill_bug_replay_provenance')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Maintenance action to run.",
                        "enum": ["reset_metrics", "backfill_bug_replay_provenance"],
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
                "  'graph'      — full operator graph projection: bugs, roadmap items, decisions, "
                "gates, and their connections\n"
                "  'lineage'    — graph lineage for one run, including operator frames\n"
                "  'issue_backlog' — canonical upstream issue intake rows before bug promotion\n"
                "  'replay_ready_bugs' — replayable bug backlog with optional safe provenance refresh\n\n"
                "USE WHEN: you need detailed operational insight beyond pass/fail rates. "
                "'Show me the operator graph.' 'What's the cutover status?' 'Show the issue backlog.' "
                "'Which bugs can I replay right now?'\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_view(view='graph', run_id='run_123')\n"
                "  praxis_operator_view(view='lineage', run_id='run_123')\n"
                "  praxis_operator_view(view='issue_backlog')\n"
                "  praxis_operator_view(view='replay_ready_bugs')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "view": {
                        "type": "string",
                        "description": "View to render: 'status', 'scoreboard', 'graph', 'lineage', 'issue_backlog', or 'replay_ready_bugs'.",
                        "enum": ["status", "scoreboard", "graph", "lineage", "issue_backlog", "replay_ready_bugs"],
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Required for run-scoped views: 'status', 'scoreboard', 'graph', and 'lineage'.",
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
                        "description": "Maximum rows to return for issue_backlog or replay_ready_bugs.",
                        "minimum": 1,
                        "default": 50,
                    },
                    "refresh_backfill": {
                        "type": "boolean",
                        "description": "When true, refresh safe replay provenance before listing replay-ready bugs.",
                        "default": True,
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
                "external dependency edges, and a rendered markdown outline.\n\n"
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
                        "description": "How many semantically similar external roadmap items to include (set 0 to disable).",
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
