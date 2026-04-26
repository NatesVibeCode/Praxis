"""Tools: praxis_discover — hybrid code retrieval for behavioral reuse.

Lets agents find existing infrastructure before building new code.
Uses pgvector embeddings over AST-extracted behavioral fingerprints plus
Postgres full-text search, fused into one ranked result set.
"""
from __future__ import annotations

import json
import re
from typing import Any

from runtime.interpretive_context import (
    attach_interpretive_context_to_items,
    discover_result_candidates,
)
from runtime.workspace_paths import strip_workflow_prefix

from ..subsystems import _subs


def tool_praxis_discover(params: dict, _progress_emitter=None) -> dict:
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
        include_interpretive_context = params.get("include_interpretive_context", True)
        max_context_results = params.get("max_context_results", 5)
        max_context_fields = params.get("max_context_fields", 6)

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
                "path": strip_workflow_prefix(r.get("module_path", "")),
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

        context_warning = None
        if include_interpretive_context:
            try:
                clean = attach_interpretive_context_to_items(
                    _subs.get_pg_conn(),
                    clean,
                    candidate_fn=discover_result_candidates,
                    max_context_items=int(
                        5 if max_context_results is None else max_context_results
                    ),
                    max_fields_per_object=int(
                        6 if max_context_fields is None else max_context_fields
                    ),
                )
            except Exception as exc:
                context_warning = f"interpretive-context attachment failed: {type(exc).__name__}: {exc}"

        payload: dict[str, Any] = {
            "ok": True,
            "query": query,
            "results": clean,
            "count": len(clean),
        }
        if context_warning:
            payload["warning"] = context_warning
        return payload

    elif action == "reindex":
        force = params.get("force", False)
        subdirs = params.get("subdirs")
        if _progress_emitter:
            _progress_emitter.log("Reindexing codebase — scanning files...")
            _progress_emitter.emit(progress=0, total=1, message="Scanning")
        result = indexer.index_codebase(subdirs=subdirs, force=force)
        if _progress_emitter:
            count = result.get("indexed", result.get("count", "?")) if isinstance(result, dict) else "?"
            if isinstance(result, dict) and str(result.get("observability_state") or "complete") != "complete":
                error_count = len(tuple(result.get("errors") or ()))
                _progress_emitter.emit(
                    progress=1,
                    total=1,
                    message=f"Degraded — {count} entities indexed, {error_count} errors",
                )
            else:
                _progress_emitter.emit(progress=1, total=1, message=f"Done — {count} entities indexed")
        return {
            "ok": True,
            "action": "reindex",
            "result": result,
        }

    elif action == "stats":
        return {
            "ok": True,
            "action": "stats",
            **indexer.stats(),
        }

    elif action == "stale-check":
        sample = int(params.get("sample_limit", 50))
        return {
            "ok": True,
            "action": "stale-check",
            **indexer.stale_check(sample_limit=sample),
        }

    return {"error": f"Unknown action: {action}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_discover": (
        tool_praxis_discover,
        {
            "kind": "search",
            "description": (
                "Find existing code that already does what you need — BEFORE writing new code. "
                "Uses hybrid retrieval: vector embeddings over AST-extracted behavioral fingerprints "
                "plus Postgres full-text search, fused with reciprocal rank fusion so you get both "
                "semantic and exact-ish matches even when naming differs.\n\n"
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
                "  praxis_discover(query='rate limit backoff', kind='function')\n"
                "  praxis_discover(query='Postgres connection pooling')\n"
                "  praxis_discover(query='JSON-RPC transport', kind='class')\n"
                "  praxis_discover(query='import-linter contract loader', kind='module')\n"
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
                            "'stats' (show index statistics), "
                            "'stale-check' (count files whose source has drifted from the index)."
                        ),
                        "enum": ["search", "reindex", "stats", "stale-check"],
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
                    "include_interpretive_context": {
                        "type": "boolean",
                        "description": (
                            "Attach bounded interpretive data-dictionary context "
                            "when a result maps to a cataloged object."
                        ),
                        "default": True,
                    },
                    "max_context_results": {
                        "type": "integer",
                        "description": (
                            "Maximum search results that may receive attached "
                            "interpretive context."
                        ),
                        "default": 5,
                    },
                    "max_context_fields": {
                        "type": "integer",
                        "description": (
                            "Maximum fields per attached dictionary object."
                        ),
                        "default": 6,
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
                    "sample_limit": {
                        "type": "integer",
                        "description": "Max stale/missing paths to include in stale-check output (default 50).",
                        "default": 50,
                    },
                },
            },
        },
    ),
}
