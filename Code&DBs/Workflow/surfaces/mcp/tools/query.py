"""Tools: praxis_query — the natural-language router."""
from __future__ import annotations

from typing import Any

from ..subsystems import _subs
from surfaces.api.handlers import workflow_query_core


def tool_praxis_query(params: dict) -> dict:
    """Natural language query surface — routes to the shared query core."""
    try:
        return workflow_query_core.handle_query(_subs, dict(params))
    except Exception as exc:
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_query": (
        tool_praxis_query,
        {
            "description": (
                "Ask any question about the system in plain English. This is the best starting point "
                "when you're unsure which tool to use — it automatically routes your question to the "
                "right subsystem. Think of it as a router, not as the deep authority for every domain.\n\n"
                "USE WHEN: the user asks a question and you're not sure which specific tool handles it.\n\n"
                "EXAMPLES:\n"
                "  'what is the current pass rate?'         → workflow status\n"
                "  'what is failing right now?'             → recent failure evidence\n"
                "  'are there any open bugs?'               → bug tracker\n"
                "  'which agent performs best?'             → leaderboard\n"
                "  'what failed recently?'                  → failure analysis\n"
                "  'how much did we spend on tokens today?' → receipt analytics\n"
                "  'what does TaskAssembler do?'            → knowledge graph search\n"
                "  'find retry logic with exponential backoff' → code discovery\n"
                "  'data dictionary'                        → browsable table schema + valid values\n"
                "  'schema for workflow_runs'               → detailed table schema\n"
                "  'import path for SchemaProjector'        → exact import statement\n"
                "  'test command for runtime/compiler.py'   → pytest command + test files\n\n"
                "ROUTES TO: status, bugs, quality metrics, failure analysis, agent leaderboard, "
                "code discovery, receipt search, constraints, friction, artifacts, heartbeat, "
                "governance, health, data dictionary, import resolver, test commands, "
                "or knowledge graph (fallback).\n\n"
                "DO NOT USE: when you already know which specific tool to call, or when you need "
                "an exact static architecture scan (`workflow architecture scan`)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "Natural language question about the system."},
                },
                "required": ["question"],
            },
        },
    ),
}
