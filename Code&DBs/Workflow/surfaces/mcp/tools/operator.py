"""Tools: praxis_operator_view, praxis_status."""
from __future__ import annotations

from typing import Any

from surfaces.api import operator_read, operator_write
from surfaces.api.handlers import workflow_query_core
from storage.postgres.workflow_runtime_repository import reset_observability_metrics

from ..subsystems import _subs


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
    """Observability views: operator status, scoreboard, graph, lineage, and replay-ready bugs."""

    try:
        return workflow_query_core.handle_operator_view(_subs, params)
    except Exception as exc:
        return {"error": str(exc)}


def tool_praxis_operator_write(params: dict) -> dict:
    """Preview, validate, or commit roadmap rows through the shared operator-write gate."""

    return operator_write.roadmap_write(
        action=params.get("action", "preview"),
        title=params.get("title", ""),
        intent_brief=params.get("intent_brief", ""),
        template=params.get("template", "single_capability"),
        priority=params.get("priority", "p2"),
        parent_roadmap_item_id=params.get("parent_roadmap_item_id"),
        slug=params.get("slug"),
        depends_on=params.get("depends_on"),
        source_bug_id=params.get("source_bug_id"),
        registry_paths=params.get("registry_paths"),
        decision_ref=params.get("decision_ref"),
        item_kind=params.get("item_kind"),
        tier=params.get("tier"),
        phase_ready=params.get("phase_ready"),
        approval_tag=params.get("approval_tag"),
        reference_doc=params.get("reference_doc"),
        outcome_gate=params.get("outcome_gate"),
    )


def tool_praxis_operator_closeout(params: dict) -> dict:
    """Preview or commit proof-backed bug and roadmap closeout through the shared gate."""

    return operator_write.reconcile_work_item_closeout(
        action=params.get("action", "preview"),
        bug_ids=params.get("bug_ids"),
        roadmap_item_ids=params.get("roadmap_item_ids"),
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

    return operator_read.query_roadmap_tree(
        root_roadmap_item_id=root_roadmap_item_id,
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
                "  'replay_ready_bugs' — replayable bug backlog with optional safe provenance refresh\n\n"
                "USE WHEN: you need detailed operational insight beyond pass/fail rates. "
                "'Show me the operator graph.' 'What's the cutover status?' 'Which bugs can I replay right now?'\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_view(view='graph', run_id='run_123')\n"
                "  praxis_operator_view(view='lineage', run_id='run_123')\n"
                "  praxis_operator_view(view='replay_ready_bugs')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "view": {
                        "type": "string",
                        "description": "View to render: 'status', 'scoreboard', 'graph', 'lineage', or 'replay_ready_bugs'.",
                        "enum": ["status", "scoreboard", "graph", "lineage", "replay_ready_bugs"],
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Required for run-scoped views: 'status', 'scoreboard', 'graph', and 'lineage'.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum replay-ready bugs to return for replay_ready_bugs.",
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
                "  praxis_operator_roadmap_view(root_roadmap_item_id='roadmap_item.authority.cleanup.unified.operator.write.validation.gate')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "root_roadmap_item_id": {"type": "string"},
                },
            },
        },
    ),
}
