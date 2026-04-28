"""Tools: praxis_search — the canonical federated search entry point.

Dispatches through ``operation_catalog_gateway.execute_operation_from_subsystems``
so every search call records a read receipt in
``authority_operation_receipts`` at the same architectural tier as
every other CQRS query. The gateway loads the
``runtime.operations.queries.search.FederatedSearchQuery`` Pydantic
model from ``operation_catalog_registry`` and invokes
``handle_federated_search`` — registered by migration 278.

When the catalog binding is unavailable (e.g., fresh clone before
migrations land), falls back to direct handler invocation so the tool
keeps working — but the receipt path is the canonical one.
"""
from __future__ import annotations

from fnmatch import fnmatch
from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems
from runtime.operations.queries.search import (
    FederatedSearchQuery,
    handle_federated_search,
)

from ..subsystems import _subs
from ..runtime_context import get_current_workflow_mcp_context


_OPERATION_NAME = "search.federated"
_SCOPE_PATH_KEYS = (
    "resolved_read_scope",
    "declared_read_scope",
    "write_scope",
    "test_scope",
    "blast_radius",
)


def _normalize_paths(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _path_is_inside(candidate: str, scope_path: str) -> bool:
    candidate = candidate.strip().strip("/")
    scope_path = scope_path.strip().strip("/")
    if not candidate or not scope_path:
        return False
    if candidate == scope_path:
        return True
    if any(ch in candidate for ch in "*?[]"):
        return fnmatch(scope_path, candidate)
    if any(ch in scope_path for ch in "*?[]"):
        return fnmatch(candidate, scope_path)
    return candidate.startswith(scope_path.rstrip("/") + "/")


def _intersect_requested_paths_with_shard(
    requested_paths: list[str],
    shard_paths: list[str],
) -> list[str]:
    if not shard_paths:
        return requested_paths
    if not requested_paths:
        return shard_paths
    scoped: list[str] = []
    for requested in requested_paths:
        for shard_path in shard_paths:
            if _path_is_inside(shard_path, requested):
                if shard_path not in scoped:
                    scoped.append(shard_path)
            elif _path_is_inside(requested, shard_path):
                if requested not in scoped:
                    scoped.append(requested)
    return scoped


def _workflow_search_shard_paths() -> list[str]:
    context = get_current_workflow_mcp_context()
    if context is None or not isinstance(context.access_policy, dict):
        return []
    scoped: list[str] = []
    for key in _SCOPE_PATH_KEYS:
        for path in _normalize_paths(context.access_policy.get(key)):
            if path not in scoped:
                scoped.append(path)
    return scoped


def _apply_workflow_search_scope(payload: dict[str, Any]) -> dict[str, Any]:
    shard_paths = _workflow_search_shard_paths()
    if not shard_paths:
        return payload
    scoped_payload = dict(payload)
    raw_scope = scoped_payload.get("scope")
    scope = dict(raw_scope) if isinstance(raw_scope, dict) else {}
    requested_paths = _normalize_paths(scope.get("paths"))
    scoped_paths = _intersect_requested_paths_with_shard(requested_paths, shard_paths)
    if not scoped_paths:
        return {
            "ok": False,
            "error": "workflow MCP search scope is outside the admitted shard",
            "reason_code": "workflow_mcp.search_scope_outside_shard",
            "requested_paths": requested_paths,
            "admitted_paths": shard_paths,
        }
    scope["paths"] = scoped_paths
    scoped_payload["scope"] = scope
    return scoped_payload


def _suggested_refinements(
    results: list[dict[str, Any]], envelope_meta: dict[str, Any]
) -> list[dict[str, Any]]:
    """Inspect hit distribution and propose narrowing knobs."""

    if len(results) < 5:
        return []
    suggestions: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    prefix_counts: dict[str, int] = {}
    sources_queried = envelope_meta.get("sources_queried") or []
    for row in results:
        src = str(row.get("source") or "")
        source_counts[src] = source_counts.get(src, 0) + 1
        path = str(row.get("path") or "")
        if path:
            top_prefix = path.split("/", 1)[0]
            prefix_counts[top_prefix] = prefix_counts.get(top_prefix, 0) + 1
    total = len(results)
    if len(sources_queried) > 1 and source_counts:
        dominant_source, count = max(source_counts.items(), key=lambda x: x[1])
        if count / total >= 0.7:
            suggestions.append(
                {
                    "kind": "narrow_sources",
                    "rationale": f"{int(count / total * 100)}% of hits from '{dominant_source}'",
                    "apply": {"sources": [dominant_source]},
                }
            )
    if prefix_counts:
        dominant_prefix, count = max(prefix_counts.items(), key=lambda x: x[1])
        if count / total >= 0.6 and dominant_prefix:
            suggestions.append(
                {
                    "kind": "narrow_paths",
                    "rationale": f"{int(count / total * 100)}% of code hits in '{dominant_prefix}/'",
                    "apply": {"scope": {"paths": [f"{dominant_prefix}/**"]}},
                }
            )
    return suggestions


def _attach_refinements(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or "results" not in payload:
        return payload
    meta = payload.get("_meta") or {}
    suggestions = _suggested_refinements(payload.get("results") or [], meta)
    if suggestions:
        meta = dict(meta)
        meta["suggested_refinements"] = suggestions
        payload["_meta"] = meta
    return payload


def tool_praxis_search(params: dict, _progress_emitter=None) -> dict:
    """Dispatch federated search through the operation catalog gateway."""

    payload = _apply_workflow_search_scope(dict(params or {}))
    if payload.get("reason_code") == "workflow_mcp.search_scope_outside_shard":
        return payload
    try:
        result = execute_operation_from_subsystems(
            _subs,
            operation_name=_OPERATION_NAME,
            payload=payload,
        )
    except Exception as exc:
        # Fall back to direct handler invocation when the gateway binding
        # is unavailable (fresh clone, migration not yet applied) so the
        # tool keeps working. The receipt path is the canonical one.
        try:
            query = FederatedSearchQuery(**payload)
        except Exception as parse_exc:
            return {"ok": False, "error": str(parse_exc)}
        try:
            direct = handle_federated_search(query, _subs)
        except Exception as handler_exc:
            return {"ok": False, "error": str(handler_exc)}
        direct.setdefault("_meta", {})["dispatch_path"] = "direct_fallback"
        direct["_meta"]["gateway_error"] = f"{type(exc).__name__}: {exc}"
        return _attach_refinements(direct)

    if isinstance(result, dict):
        meta = result.setdefault("_meta", {})
        meta.setdefault("dispatch_path", "gateway")
        return _attach_refinements(result)
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_search": (
        tool_praxis_search,
        {
            "kind": "search",
            "description": (
                "Canonical federated search. Returns the data you'd otherwise "
                "reach for bash to fetch — line-context code matches, "
                "regex/exact/semantic modes, path-glob scoping, time bounds, "
                "freshness signal, source-tagged ranked results across code "
                "(today) and knowledge/bugs/receipts/git/files/db (rolling out).\n\n"
                "USE WHEN: you need data from the codebase, knowledge graph, bugs, "
                "receipts, decisions, git history, or files. Prefer this over "
                "praxis_discover/praxis_recall/praxis_query for new code paths.\n\n"
                "EXAMPLES:\n"
                "  praxis_search(query='retry logic with exponential backoff')\n"
                "  praxis_search(query='subprocess.', mode='exact', "
                "scope={'paths':['Code&DBs/Workflow/runtime/**/*.py']}, "
                "shape='context', context_lines=3)\n"
                "  praxis_search(query='/class.*Authority/', mode='regex', "
                "scope={'paths':['Code&DBs/Workflow/runtime/**/*.py']})\n\n"
                "DO NOT USE: for write operations or to launch work — those still "
                "live on subsystem-specific tools."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "What to search for. Natural language for semantic, "
                            "literal text for exact (or wrap in quotes), "
                            "/regex/ for regex."
                        ),
                    },
                    "mode": {
                        "type": "string",
                        "description": (
                            "Match mode. 'auto' picks based on query shape: "
                            "/regex/ -> regex, 'quoted' -> exact, otherwise semantic."
                        ),
                        "enum": ["auto", "semantic", "exact", "regex"],
                        "default": "auto",
                    },
                    "sources": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Sources to query. Today: code, knowledge, decisions, "
                            "research, bugs, receipts. Adding next: git_history, "
                            "files, db, data_dictionary, lineage."
                        ),
                        "default": ["code"],
                    },
                    "scope": {
                        "type": "object",
                        "description": (
                            "Narrow the universe before ranking. paths/exclude_paths "
                            "are glob patterns. since_iso/until_iso bound by mtime "
                            "(code/files) or commit time (git). type_slug filters by "
                            "data-dictionary type. entity_kind filters code units "
                            "(module/class/function/subsystem)."
                        ),
                        "properties": {
                            "paths": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "exclude_paths": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "since_iso": {"type": "string"},
                            "until_iso": {"type": "string"},
                            "type_slug": {"type": "string"},
                            "exclude_terms": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "entity_kind": {
                                "type": "string",
                                "enum": ["module", "class", "function", "subsystem"],
                            },
                        },
                    },
                    "shape": {
                        "type": "string",
                        "description": (
                            "How much data per hit. 'match' = single line, "
                            "'context' = +/- context_lines around match (default), "
                            "'full' = whole file/record."
                        ),
                        "enum": ["match", "context", "full"],
                        "default": "context",
                    },
                    "context_lines": {
                        "type": "integer",
                        "description": (
                            "Lines of context around each match when shape='context'."
                        ),
                        "default": 5,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return.",
                        "default": 20,
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Opaque pagination cursor from prior _meta.next_cursor.",
                    },
                    "explain": {
                        "type": "boolean",
                        "description": (
                            "Per-row explain block: matched terms, vector vs FTS "
                            "contribution, graph hops, dedup decisions."
                        ),
                        "default": False,
                    },
                    "auto_reindex_if_stale": {
                        "type": "boolean",
                        "description": (
                            "Lazy reindex when the on-disk source has drifted past "
                            "stale_threshold files. Default true so the operator "
                            "never has to remember to run reindex manually."
                        ),
                        "default": True,
                    },
                    "stale_threshold": {
                        "type": "integer",
                        "description": (
                            "Drift count above which auto_reindex_if_stale fires."
                        ),
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    ),
}
