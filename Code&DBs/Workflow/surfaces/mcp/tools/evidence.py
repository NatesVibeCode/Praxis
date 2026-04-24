"""Tools: praxis_receipts, praxis_constraints, praxis_friction."""
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
    """Friction ledger: guardrail bounces, warnings, hard failures."""
    action = params.get("action", "stats")

    # Friction stats and unscoped lists expose system-wide guardrail activity.
    # Block both inside a workflow session.
    if get_current_workflow_mcp_context() is not None:
        return {"error": "praxis_friction is not permitted inside a workflow session."}

    ledger = _subs.get_friction_ledger()

    if action == "stats":
        include_test = params.get("include_test", False)
        stats = ledger.stats(include_test=include_test)
        if stats.total == 0:
            return {"total": 0, "message": "No friction events recorded."}
        patterns = ledger.cluster_patterns(since_hours=24)
        result: dict = {
            "total": stats.total,
            "by_type": stats.by_type,
            "by_source": stats.by_source,
            "bounce_rate_24h": round(ledger.bounce_rate(since_hours=24, include_test=include_test), 4),
        }
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
        events = ledger.list_events(source=source, limit=limit, include_test=include_test)
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
                "View the friction ledger — a record of every time a guardrail blocked or warned "
                "about an action (scope violations, secret leaks, policy bounces).\n\n"
                "USE WHEN: you want to understand what's being blocked by governance, identify "
                "recurring friction patterns, or audit guardrail activity.\n\n"
                "EXAMPLES:\n"
                "  Overview stats:     praxis_friction(action='stats')\n"
                "  Recent events:      praxis_friction(action='list', limit=10)\n"
                "  Repeated failures:  praxis_friction(action='patterns', source='cli.workflow')\n"
                "  From one source:    praxis_friction(action='list', source='governance')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'stats', 'list', or 'patterns'.",
                        "enum": ["stats", "list", "patterns"],
                    },
                    "source": {"type": "string", "description": "Filter by source (for list/patterns)."},
                    "limit": {"type": "integer", "description": "Max events or patterns to return.", "default": 20},
                    "scan_limit": {"type": "integer", "description": "Max recent events to scan for pattern grouping.", "default": 500},
                    "since_hours": {"type": "number", "description": "Only include events from the last N hours."},
                    "promotion_threshold": {"type": "integer", "description": "Pattern count that marks promotion_candidate=true.", "default": 3},
                    "include_test": {"type": "boolean", "description": "Include test friction events (excluded by default).", "default": False},
                },
                "required": ["action"],
            },
        },
    ),
}
