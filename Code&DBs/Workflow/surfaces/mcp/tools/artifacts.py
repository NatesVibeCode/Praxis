"""Tools: praxis_artifacts."""
from __future__ import annotations

from typing import Any

from surfaces.placeholder_ids import is_demo_placeholder, placeholder_error

from ..subsystems import _subs


def _resolve_sandbox_id(params: dict) -> tuple[str, dict | None]:
    requested = str(params.get("sandbox_id", "") or "").strip()
    if is_demo_placeholder("sandbox_id", requested):
        return "", placeholder_error("sandbox_id", requested)
    if not requested:
        return "", {
            "error": "sandbox_id is required for list",
            "reason_code": "sandbox_id.required",
        }
    return requested, None


def tool_praxis_artifacts(params: dict) -> dict:
    """Sandbox artifact store: list, search, diff, stats."""
    action = params.get("action", "stats")
    store = _subs.get_artifact_store()

    if action == "stats":
        s = store.stats()
        if s["total_artifacts"] == 0:
            return {"message": "No artifacts captured yet."}
        return s

    if action == "list":
        sandbox_id, error_payload = _resolve_sandbox_id(params)
        if not sandbox_id:
            return error_payload or {"error": "sandbox_id is required for list"}
        items = store.list_by_sandbox(sandbox_id)
        if not items:
            return {"sandbox_id": sandbox_id, "count": 0, "message": f"No artifacts for sandbox {sandbox_id}."}
        payload = {
            "sandbox_id": sandbox_id,
            "count": len(items),
            "artifacts": [
                {
                    "artifact_id": a.artifact_id,
                    "file_path": a.file_path,
                    "byte_count": a.byte_count,
                    "line_count": a.line_count,
                    "captured_at": a.captured_at.isoformat(),
                }
                for a in items
            ],
        }
        return payload

    if action == "search":
        query = params.get("query", "")
        if not query:
            return {"error": "query is required for search"}
        items = store.search(query, limit=params.get("limit", 20))
        if not items:
            return {"count": 0, "message": "No matching artifacts."}
        return {
            "count": len(items),
            "artifacts": [
                {
                    "artifact_id": a.artifact_id,
                    "file_path": a.file_path,
                    "sandbox_id": a.sandbox_id,
                    "byte_count": a.byte_count,
                }
                for a in items
            ],
        }

    if action == "diff":
        id_a = params.get("artifact_id_a", "")
        id_b = params.get("artifact_id_b", "")
        if not id_a or not id_b:
            return {"error": "artifact_id_a and artifact_id_b are required"}
        return store.diff(id_a, id_b)

    return {"error": f"Unknown artifacts action: {action}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_artifacts": (
        tool_praxis_artifacts,
        {
            "description": (
                "Browse and compare files produced by workflow sandbox runs. Each workflow job can "
                "write artifacts (code, logs, reports) — this tool lets you find, search, and diff them.\n\n"
                "USE WHEN: you want to see what a workflow job produced, search for specific output, "
                "or compare two versions of an artifact.\n\n"
                "EXAMPLES:\n"
                "  Overall stats:     praxis_artifacts(action='stats')\n"
                "  List by sandbox:   praxis_artifacts(action='list', sandbox_id='sandbox_20260423_001')\n"
                "  Search outputs:    praxis_artifacts(action='search', query='migration schema')\n"
                "  Compare versions:  praxis_artifacts(action='diff', artifact_id_a='art_1', artifact_id_b='art_2')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'stats', 'list', 'search', or 'diff'.",
                        "enum": ["stats", "list", "search", "diff"],
                    },
                    "sandbox_id": {"type": "string", "description": "Sandbox ID (required for list)."},
                    "query": {"type": "string", "description": "Search query (for search)."},
                    "artifact_id_a": {"type": "string", "description": "First artifact ID (for diff)."},
                    "artifact_id_b": {"type": "string", "description": "Second artifact ID (for diff)."},
                    "limit": {"type": "integer", "description": "Max results.", "default": 20},
                },
                "required": ["action"],
            },
        },
    ),
}
