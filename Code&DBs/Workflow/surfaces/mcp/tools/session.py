"""Tools: praxis_session, praxis_heartbeat, praxis_decompose, praxis_research."""
from __future__ import annotations

from typing import Any

from ..subsystems import _subs


def tool_praxis_heartbeat(params: dict) -> dict:
    """Run heartbeat cycle or view last status."""
    action = params.get("action", "status")

    if action == "run":
        runner = _subs.get_heartbeat_runner()
        result = runner.run_once()
        from runtime.heartbeat_runner import summarize_cycle_result
        return summarize_cycle_result(result)

    if action == "status":
        from runtime.heartbeat_runner import latest_heartbeat_status
        snapshot = latest_heartbeat_status(conn=_subs.get_pg_conn())
        if snapshot is None:
            return {"message": "No heartbeat cycles have run yet."}
        return {"latest_cycle": snapshot.cycle_id, "summary": dict(snapshot.summary)}

    return {"error": f"Unknown heartbeat action: {action}"}


def tool_praxis_session(params: dict) -> dict:
    """Session carry-forward packs: list, load, validate."""
    action = params.get("action", "latest")
    mgr = _subs.get_session_carry_mgr()
    from runtime.session_carry import (
        filter_pack_for_effective_provider_catalog,
        load_effective_provider_job_catalog_for_carry,
        pack_to_summary_dict,
    )

    try:
        effective_catalog = load_effective_provider_job_catalog_for_carry(
            _subs.get_pg_conn()
        )
    except Exception as exc:
        return {
            "error_code": "session_provider_catalog_unavailable",
            "error": f"provider catalog unavailable for session carry-forward: {exc}",
        }

    if action == "latest":
        pack = mgr.latest()
        if pack is None:
            return {"message": "No carry-forward packs saved yet."}
        pack = filter_pack_for_effective_provider_catalog(
            pack,
            effective_provider_job_catalog=effective_catalog,
        )
        return pack_to_summary_dict(pack)

    if action == "validate":
        pack_id = params.get("pack_id", "")
        if not pack_id:
            pack = mgr.latest()
        else:
            pack = mgr.load(pack_id)
        if pack is None:
            return {"message": "Pack not found."}
        pack = filter_pack_for_effective_provider_catalog(
            pack,
            effective_provider_job_catalog=effective_catalog,
        )
        issues = mgr.validate(pack)
        if not issues:
            return {"valid": True, "pack": pack_to_summary_dict(pack)}
        return {"valid": False, "pack": pack_to_summary_dict(pack), "issues": issues}

    return {"error": f"Unknown session action: {action}"}


def tool_praxis_decompose(params: dict) -> dict:
    """Decompose an objective into micro-sprints."""
    objective = params.get("objective", "")
    if not objective:
        return {"error": "objective is required"}
    scope_files = params.get("scope_files", [])

    try:
        from runtime.sprint_decomposer import SprintDecomposer
        decomposer = SprintDecomposer()
        sprints = decomposer.decompose(objective, scope_files)
        if not sprints:
            return {"message": "Could not decompose into sprints.", "sprints": []}
        critical = decomposer.critical_path(sprints)
        total_est = decomposer.total_estimate(sprints)
        return {
            "total_sprints": len(sprints),
            "total_estimate_minutes": total_est,
            "critical_path": [s.label for s in critical],
            "sprints": [
                {
                    "label": s.label,
                    "complexity": s.complexity.value,
                    "depends_on": list(s.depends_on),
                    "estimate_minutes": s.estimated_minutes,
                    "files": list(s.file_targets)[:10],
                }
                for s in sprints
            ],
        }
    except Exception as e:
        return {"error": str(e)}


def tool_praxis_research(params: dict) -> dict:
    """Research sessions: search local knowledge, compile briefs."""
    action = params.get("action", "search")

    try:
        from memory.research_runtime import ResearchExecutor

        if action == "search":
            query = params.get("query", "")
            if not query:
                return {"error": "query is required for search"}
            engine = _subs.get_memory_engine()
            executor = ResearchExecutor(engine)
            result = executor.search_local(query)
            if not result.hits:
                return {"count": 0, "message": "No results found."}
            return {
                "count": len(result.hits),
                "hits": [
                    {"title": h.title, "source": h.source, "snippet": h.snippet[:200]}
                    for h in result.hits[:20]
                ],
            }

        return {"error": f"Unknown research action: {action}"}
    except Exception as e:
        return {"error": str(e)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_heartbeat": (
        tool_praxis_heartbeat,
        {
            "description": (
                "Run or check the knowledge graph maintenance cycle. The heartbeat syncs receipts, "
                "bugs, constraints, and friction events into the knowledge graph, mines relationships "
                "between entities, generates daily/weekly rollups, and archives stale nodes.\n\n"
                "USE WHEN: the knowledge graph seems stale, or you want to trigger a maintenance "
                "cycle after a batch of workflow runs.\n\n"
                "EXAMPLES:\n"
                "  Run maintenance:  praxis_heartbeat(action='run')\n"
                "  Check last run:   praxis_heartbeat(action='status')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'run' (execute cycle) or 'status' (last results).",
                        "enum": ["run", "status"],
                    },
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_session": (
        tool_praxis_session,
        {
            "description": (
                "View or validate session carry-forward packs — compressed context snapshots that "
                "help new sessions pick up where previous ones left off.\n\n"
                "USE WHEN: starting a new session and want to load prior context, or verifying "
                "that a session pack is intact.\n\n"
                "EXAMPLES:\n"
                "  Get latest pack:   praxis_session(action='latest')\n"
                "  Validate a pack:   praxis_session(action='validate', pack_id='pack_abc123')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'latest' or 'validate'.",
                        "enum": ["latest", "validate"],
                    },
                    "pack_id": {"type": "string", "description": "Pack ID (for validate). Defaults to latest."},
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_research": (
        tool_praxis_research,
        {
            "kind": "search",
            "description": (
                "Search the knowledge graph specifically for research findings and analysis results. "
                "Lighter-weight than praxis_recall — focused on retrieving prior research.\n\n"
                "USE WHEN: you want to check if someone already researched a topic.\n\n"
                "EXAMPLE: praxis_research(action='search', query='provider routing performance')\n\n"
                "DO NOT USE: for general knowledge search (use praxis_recall), or for code search (use praxis_discover)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'search'.",
                        "enum": ["search"],
                    },
                    "query": {"type": "string", "description": "Search query."},
                },
                "required": ["action", "query"],
            },
        },
    ),
    "praxis_decompose": (
        tool_praxis_decompose,
        {
            "description": (
                "Break down a large objective into small, workflow-ready micro-sprints. Returns each "
                "sprint with estimated complexity, dependencies between sprints, and the critical path.\n\n"
                "USE WHEN: you have a big task and need to plan how to break it into workflow jobs.\n\n"
                "EXAMPLES:\n"
                "  praxis_decompose(objective='Add real-time notifications to the workflow runtime')\n"
                "  praxis_decompose(objective='Consolidate operator surfaces', "
                "scope_files=['surfaces/api/operator_read.py', 'surfaces/api/operator_write.py'])\n\n"
                "DO NOT USE: for running the work (use praxis_workflow after decomposing)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "objective": {"type": "string", "description": "The objective to decompose."},
                    "scope_files": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional file paths to scope the decomposition.",
                    },
                },
                "required": ["objective"],
            },
        },
    ),
}
