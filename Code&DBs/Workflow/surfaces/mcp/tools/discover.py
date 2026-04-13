"""Tools: praxis_discover — functional synonym detection via vector similarity.

Lets agents find existing infrastructure before building new code.
Uses pgvector embeddings over AST-extracted behavioral fingerprints.
"""
from __future__ import annotations

import json
import re
from typing import Any

from ..subsystems import _subs


def tool_praxis_discover(params: dict) -> dict:
    """Search for functionally similar code in the codebase.

    Actions:
        search  — find modules/classes/functions that solve a given problem
        reindex — re-index the codebase (after code changes)
        stats   — show index statistics
    """
    action = params.get("action", "search")
    indexer = _subs.get_module_indexer()

    if action == "search":
        query = params.get("query", "").strip()
        if not query:
            return {"error": "query is required for search"}

        limit = params.get("limit", 10)
        kind = params.get("kind")  # module, class, function, or None for all
        threshold = params.get("threshold", 0.3)

        raw_results = indexer.search(
            query=query,
            limit=limit,
            kind=kind,
            threshold=threshold,
        )

        # Clean results for readability — strip internal scoring and AST noise
        clean = []
        for r in raw_results:
            entry: dict = {
                "name": r.get("name", ""),
                "kind": r.get("kind", ""),
                "path": r.get("module_path", "").replace("Code&DBs/Workflow/", ""),
                "similarity": round(r.get("cosine_similarity", 0), 2),
            }
            # Use docstring if available, fall back to summary
            doc = (r.get("docstring_preview") or "").strip()
            if doc:
                entry["description"] = doc[:200]
            else:
                # Clean up AST-generated summary noise
                summary = r.get("summary", "")
                # Strip "Module 'x'. " / "Class 'x'. " / "Function 'x'. " prefix
                for prefix in ("Module", "Class", "Function"):
                    marker = f"{prefix} '{r.get('name', '')}'."
                    if summary.startswith(marker):
                        summary = summary[len(marker):].strip()
                # Strip "Interacts with database tables: ..." noise
                summary = re.sub(r'Interacts with database tables:.*?\.', '', summary).strip()
                summary = re.sub(r'I/O patterns:.*?\.', '', summary).strip()
                summary = re.sub(r'Uses:.*?\.', '', summary).strip()
                summary = re.sub(r'Works with:.*?\.', '', summary).strip()
                if summary:
                    entry["description"] = summary[:200]

            if r.get("signature"):
                entry["signature"] = r["signature"]

            clean.append(entry)

        return {
            "query": query,
            "results": clean,
            "count": len(clean),
        }

    elif action == "reindex":
        force = params.get("force", False)
        subdirs = params.get("subdirs")
        result = indexer.index_codebase(subdirs=subdirs, force=force)
        return {
            "action": "reindex",
            "result": result,
        }

    elif action == "stats":
        return {
            "action": "stats",
            **indexer.stats(),
        }

    return {"error": f"Unknown action: {action}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_discover": (
        tool_praxis_discover,
        {
            "description": (
                "Find existing code that already does what you need — BEFORE writing new code. "
                "Uses vector embeddings over AST-extracted behavioral fingerprints to find "
                "functionally similar modules, classes, and functions even when naming differs.\n\n"
                "SEARCH BEFORE YOU BUILD: Before implementing ANY new function, module, class, "
                "utility, or pattern, call this tool first. The codebase is large and has extensive "
                "existing infrastructure — duplicating what already exists wastes time and creates "
                "maintenance burden. Describe the *behavior* you need, not the name you'd give it. "
                "Also use praxis_recall for architectural decisions and praxis_query for DB-level "
                "searches (receipts, bugs, constraints). After code changes, call "
                "praxis_discover(action='reindex') to update the index.\n\n"
                "USE WHEN: you're about to build something and want to check if it already exists, "
                "or you need to find code that handles a specific concern.\n\n"
                "EXAMPLES:\n"
                "  praxis_discover(query='retry logic with exponential backoff')\n"
                "  praxis_discover(query='Postgres connection pooling')\n"
                "  praxis_discover(query='JSON-RPC transport', kind='class')\n"
                "  praxis_discover(action='reindex')  # after making code changes\n"
                "  praxis_discover(action='stats')    # how many items are indexed\n\n"
                "DO NOT USE: for searching knowledge/decisions/bugs (use praxis_recall), or for "
                "full-text search over workflow results (use praxis_receipts)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": (
                            "'search' (find similar code), "
                            "'reindex' (re-index codebase after changes), "
                            "'stats' (show index statistics)."
                        ),
                        "enum": ["search", "reindex", "stats"],
                        "default": "search",
                    },
                    "query": {
                        "type": "string",
                        "description": (
                            "Natural language description of what you need. "
                            "E.g. 'durable message delivery with retry', "
                            "'checkpoint-based event replay', "
                            "'subprocess management for background jobs'."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 10).",
                        "default": 10,
                    },
                    "kind": {
                        "type": "string",
                        "description": "Filter by code unit kind.",
                        "enum": ["module", "class", "function", "subsystem"],
                    },
                    "threshold": {
                        "type": "number",
                        "description": "Minimum cosine similarity (0-1, default 0.3).",
                        "default": 0.3,
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Force full re-index even if source unchanged (for reindex action).",
                        "default": False,
                    },
                    "subdirs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Directories to scan (relative to repo root, for reindex action).",
                    },
                },
            },
        },
    ),
}
