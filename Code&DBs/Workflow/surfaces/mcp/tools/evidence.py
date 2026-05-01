"""Tools: praxis_receipts, praxis_constraints, praxis_friction, praxis_action_fingerprints."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..runtime_context import get_current_workflow_mcp_context
from ..subsystems import _subs


def tool_praxis_receipts(params: dict) -> dict:
    """Search canonical workflow receipts + token burn analytics."""
    from runtime.receipt_store import receipt_stats, search_receipts

    action = params.get("action", "search")

    if action == "search":
        query = params.get("query", "")
        if not query:
            return {"error": "query is required for search"}
        status = params.get("status") or None
        agent = params.get("agent") or None
        limit = params.get("limit", 20)

        # In a workflow session, scope to the current workflow unless the caller
        # explicitly passes workflow_id=None to opt out.
        workflow_id: str | None = None
        if "workflow_id" in params:
            workflow_id = params.get("workflow_id") or None
        else:
            ctx = get_current_workflow_mcp_context()
            if ctx and ctx.workflow_id:
                workflow_id = ctx.workflow_id

        results = search_receipts(
            query,
            status=status,
            agent=agent,
            limit=limit,
            workflow_id=workflow_id,
            conn=_subs.get_pg_conn(),
        )
        return {"results": [record.to_search_result() for record in results], "count": len(results)}

    if action == "token_burn":
        if get_current_workflow_mcp_context() is not None:
            return {"error": "praxis_receipts action='token_burn' is not permitted inside a workflow session."}
        since_hours = params.get("since_hours", 24)
        return {"token_burn": receipt_stats(since_hours=since_hours, conn=_subs.get_pg_conn())}

    return {"error": f"Unknown receipts action: {action}"}


def _session_write_scope() -> list[str]:
    """Return the write_scope for the current workflow MCP session, if any."""
    from runtime.workflow.job_runtime_context import load_workflow_job_runtime_context
    ctx = get_current_workflow_mcp_context()
    if not ctx or not ctx.run_id:
        return []
    try:
        record = load_workflow_job_runtime_context(
            _subs.get_pg_conn(),
            run_id=ctx.run_id,
            job_label=ctx.job_label,
        )
        if record:
            shard = record.get("execution_context_shard") or {}
            return list(shard.get("write_scope") or [])
    except Exception:
        pass
    return []


def tool_praxis_constraints(params: dict) -> dict:
    """List or search mined failure constraints."""
    action = params.get("action", "list")
    ledger = _subs.get_constraint_ledger()

    if action == "list":
        # In a workflow session, auto-scope to the job's write_scope unless
        # the caller explicitly sets write_paths to opt out.
        if "write_paths" not in params:
            write_scope = _session_write_scope()
            if write_scope:
                items = ledger.get_for_scope(write_scope)
                if not items:
                    return {"count": 0, "message": "No constraints match your write scope.", "scoped_to": write_scope}
                return {
                    "count": len(items),
                    "scoped_to": write_scope,
                    "constraints": [
                        {"pattern": c.pattern, "text": c.constraint_text, "confidence": round(c.confidence, 3)}
                        for c in items
                    ],
                }

        min_conf = params.get("min_confidence", 0.5)
        items = ledger.list_all(min_confidence=min_conf)
        if not items:
            return {"count": 0, "message": "No mined constraints yet."}
        return {
            "count": len(items),
            "constraints": [
                {
                    "constraint_id": c.constraint_id,
                    "pattern": c.pattern,
                    "text": c.constraint_text,
                    "confidence": round(c.confidence, 3),
                    "mined_from": list(c.mined_from_jobs)[:5],
                }
                for c in items
            ],
        }

    if action == "for_scope":
        paths = params.get("write_paths", [])
        if not paths:
            return {"error": "write_paths list is required for for_scope"}
        items = ledger.get_for_scope(paths)
        if not items:
            return {"count": 0, "message": "No constraints match these paths."}
        return {
            "count": len(items),
            "constraints": [
                {"pattern": c.pattern, "text": c.constraint_text, "confidence": round(c.confidence, 3)}
                for c in items
            ],
        }

    return {"error": f"Unknown constraints action: {action}"}


def tool_praxis_friction(params: dict) -> dict:
    """Friction ledger: record events; inspect stats, lists, patterns."""
    action = params.get("action", "stats")

    if action == "record":
        # Writes go through the catalog gateway so each event leaves an
        # authority_operation_receipts row + an authority_events row
        # (event_type='friction.recorded', linked by receipt_id). Per
        # architecture-policy::agent-behavior::cqrs-wizard-before-cqrs-edits
        # this is a thin delegation, not a hidden write behind the read tool.
        from runtime.operation_catalog_gateway import (
            execute_operation_from_subsystems,
        )

        payload = {
            key: value
            for key, value in dict(params or {}).items()
            if key != "action" and value is not None
        }
        return execute_operation_from_subsystems(
            _subs,
            operation_name="friction_record",
            payload=payload,
        )

    # Friction stats and unscoped lists expose system-wide guardrail activity.
    # Block them inside a workflow session. Recording is per-event evidence —
    # writes are scope-safe and pass through above.
    if get_current_workflow_mcp_context() is not None:
        return {"error": "praxis_friction is not permitted inside a workflow session."}

    ledger = _subs.get_friction_ledger()

    if action == "stats":
        include_test = params.get("include_test", False)
        task_mode = (params.get("task_mode") or "").strip().lower() or None
        stats = ledger.stats(include_test=include_test, task_mode=task_mode)
        if stats.total == 0:
            return {"total": 0, "message": "No friction events recorded."}
        patterns = ledger.cluster_patterns(since_hours=24)
        result: dict = {
            "total": stats.total,
            "by_type": stats.by_type,
            "by_source": stats.by_source,
            "bounce_rate_24h": round(ledger.bounce_rate(since_hours=24, include_test=include_test), 4),
        }
        if task_mode:
            result["task_mode"] = task_mode
        if patterns:
            result["patterns"] = [
                {"pattern": p["pattern"], "count": p["count"], "sources": p["sources"]}
                for p in patterns
            ]
        return result

    if action == "list":
        limit = params.get("limit", 20)
        source = params.get("source") or None
        include_test = params.get("include_test", False)
        task_mode = (params.get("task_mode") or "").strip().lower() or None
        events = ledger.list_events(
            source=source, limit=limit, include_test=include_test, task_mode=task_mode,
        )
        if not events:
            return {"count": 0, "message": "No friction events found."}
        return {
            "count": len(events),
            "events": [
                {
                    "event_id": e.event_id,
                    "type": e.friction_type.value,
                    "source": e.source,
                    "job_label": e.job_label,
                    "message": e.message[:200],
                    "timestamp": e.timestamp.isoformat(),
                    "task_mode": e.task_mode,
                }
                for e in events
            ],
        }

    if action == "patterns":
        limit = params.get("limit", 20)
        scan_limit = params.get("scan_limit", 500)
        source = params.get("source") or None
        include_test = params.get("include_test", False)
        promotion_threshold = params.get("promotion_threshold", 3)
        since_hours = params.get("since_hours")
        since = None
        if since_hours is not None:
            since = datetime.now(timezone.utc) - timedelta(hours=float(since_hours))
        patterns = ledger.patterns(
            source=source,
            since=since,
            limit=limit,
            scan_limit=scan_limit,
            include_test=include_test,
            promotion_threshold=promotion_threshold,
        )
        if not patterns:
            return {"count": 0, "message": "No friction patterns found."}
        return {
            "count": len(patterns),
            "patterns": [pattern.to_json() for pattern in patterns],
        }

    return {"error": f"Unknown friction action: {action}"}


def tool_praxis_action_fingerprints(params: dict) -> dict:
    """Record raw shell/edit/write/read action shapes through the gateway."""
    action = params.get("action", "record")

    if action != "record":
        return {"error": f"Unknown action fingerprint action: {action}"}

    from runtime.operation_catalog_gateway import (
        execute_operation_from_subsystems,
    )

    payload = {
        key: value
        for key, value in dict(params or {}).items()
        if key != "action" and value is not None
    }
    return execute_operation_from_subsystems(
        _subs,
        operation_name="action_fingerprint_record",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_receipts": (
        tool_praxis_receipts,
        {
            "description": (
                "Search through past workflow results and analyze costs. Every workflow run produces "
                "receipts — this tool lets you search them by keyword and analyze token/cost spending.\n\n"
                "USE WHEN: you want to find past workflow results, check what an agent produced, "
                "or analyze token burn and costs.\n\n"
                "EXAMPLES:\n"
                "  Find past work:     praxis_receipts(action='search', query='catalog runtime')\n"
                "  Failed jobs only:   praxis_receipts(action='search', query='import error', status='failed')\n"
                "  By agent:           praxis_receipts(action='search', query='build', agent='openai/gpt-5.4')\n"
                "  Cost analytics:     praxis_receipts(action='token_burn', since_hours=24)\n\n"
                "DO NOT USE: for live workflow status (use praxis_workflow action='status'), or for "
                "knowledge graph search (use praxis_recall)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'search' or 'token_burn' (cost/token analytics).",
                        "enum": ["search", "token_burn"],
                    },
                    "query": {"type": "string", "description": "Free-text search query (for search action)."},
                    "status": {"type": "string", "description": "Optional status filter: succeeded, failed, error."},
                    "agent": {"type": "string", "description": "Optional agent slug filter."},
                    "limit": {"type": "integer", "description": "Max results to return.", "default": 20},
                    "since_hours": {"type": "integer", "description": "Lookback window for token_burn.", "default": 24},
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_constraints": (
        tool_praxis_constraints,
        {
            "description": (
                "View automatically-mined constraints from past workflow failures. The system learns "
                "rules like 'files in runtime/ must include imports' from repeated failures.\n\n"
                "USE WHEN: you want to see what the system has learned from failures, or check if "
                "specific files have known constraints before launching a workflow that touches them.\n\n"
                "EXAMPLES:\n"
                "  List all constraints:  praxis_constraints(action='list')\n"
                "  Check before writing:  praxis_constraints(action='for_scope', write_paths=['runtime/workflow/unified.py'])"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'list' or 'for_scope'.",
                        "enum": ["list", "for_scope"],
                    },
                    "min_confidence": {"type": "number", "description": "Min confidence threshold (for list).", "default": 0.5},
                    "write_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "File paths to match constraints against (for for_scope).",
                    },
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_friction": (
        tool_praxis_friction,
        {
            "description": (
                "Read or write the friction ledger — every guardrail bounce, warning, or hard "
                "failure (scope violations, secret leaks, policy bounces, JIT trigger matches).\n\n"
                "USE WHEN: you want to understand what's being blocked by governance, identify "
                "recurring friction patterns, audit guardrail activity, or record a new friction "
                "event from a per-harness PreToolUse hook.\n\n"
                "EXAMPLES:\n"
                "  Overview stats:     praxis_friction(action='stats')\n"
                "  Stats by mode:      praxis_friction(action='stats', task_mode='release')\n"
                "  Recent events:      praxis_friction(action='list', limit=10)\n"
                "  Repeated failures:  praxis_friction(action='patterns', source='cli.workflow')\n"
                "  From one source:    praxis_friction(action='list', source='governance')\n"
                "  Record JIT match:   praxis_friction(action='record', event_type='WARN_ONLY', source='preact_orient_hook', subject_ref='Bash', decision_keys=['architecture-policy::...'])"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'stats', 'list', 'patterns', or 'record'.",
                        "enum": ["stats", "list", "patterns", "record"],
                    },
                    "source": {"type": "string", "description": "Filter by source (for list/patterns); origin tag (for record)."},
                    "limit": {"type": "integer", "description": "Max events or patterns to return.", "default": 20},
                    "scan_limit": {"type": "integer", "description": "Max recent events to scan for pattern grouping.", "default": 500},
                    "since_hours": {"type": "number", "description": "Only include events from the last N hours."},
                    "promotion_threshold": {"type": "integer", "description": "Pattern count that marks promotion_candidate=true.", "default": 3},
                    "include_test": {"type": "boolean", "description": "Include test friction events (excluded by default).", "default": False},
                    "task_mode": {"type": "string", "description": "Filter stats/list to a single task mode, or tag the recorded event with one (chat/build/release/incident/...)."},
                    "event_type": {"type": "string", "description": "Friction type for record: GUARDRAIL_BOUNCE, WARN_ONLY, or HARD_FAILURE.", "enum": ["GUARDRAIL_BOUNCE", "WARN_ONLY", "HARD_FAILURE"]},
                    "subject_kind": {"type": "string", "description": "Optional kind label for the subject of the recorded event (e.g. 'agent_action')."},
                    "subject_ref": {"type": "string", "description": "Subject identifier (typically the tool name) for the recorded event."},
                    "job_label": {"type": "string", "description": "Override for friction_events.job_label; defaults to subject_ref."},
                    "decision_keys": {"type": "array", "items": {"type": "string"}, "description": "Operator-decision keys this firing matched (for record)."},
                    "metadata": {"type": "object", "description": "Free-form context (subject text, harness, matched_decisions) for record."},
                    "message": {"type": "string", "description": "Optional explicit ledger message for record; defaults to a structured JSON envelope."},
                    "is_test": {"type": "boolean", "description": "Mark a recorded event as synthetic (test fixture).", "default": False},
                },
                "required": ["action"],
                "x-action-requirements": {
                    "record": {"required": ["event_type", "source"]}
                },
            },
        },
    ),
    "praxis_action_fingerprints": (
        tool_praxis_action_fingerprints,
        {
            "description": (
                "Record raw shell/edit/write/read action shapes into the action fingerprint ledger.\n\n"
                "USE WHEN: a harness hook needs to persist one raw tool invocation so recurrent"
                " shell or file actions can surface as tool opportunities.\n\n"
                "EXAMPLES:\n"
                "  Record shell action: praxis_action_fingerprints(action='record', tool_name='local_shell', source_surface='codex:host', tool_input={'command':['pytest','tests/test_x.py','-q']})\n"
                "  Record file read:    praxis_action_fingerprints(action='record', tool_name='read_file', source_surface='gemini:host', tool_input={'file_path':'Code&DBs/Workflow/runtime/foo.py'})"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'record'.",
                        "enum": ["record"],
                    },
                    "tool_name": {
                        "type": "string",
                        "description": "Raw harness tool name, e.g. local_shell, Bash, apply_patch, read_file.",
                    },
                    "tool_input": {
                        "type": "object",
                        "description": "Raw tool payload from the harness hook.",
                    },
                    "source_surface": {
                        "type": "string",
                        "description": "Origin tag, e.g. codex:host, claude-code:host, gemini:host.",
                    },
                    "session_ref": {
                        "type": "string",
                        "description": "Optional harness session identifier.",
                    },
                    "payload_meta": {
                        "type": "object",
                        "description": "Additional bounded metadata to store alongside the shape row.",
                    },
                },
                "required": ["action", "tool_name", "tool_input", "source_surface"],
            },
        },
    ),
}
