"""Knowledge graph + operator_decisions source plugin.

Wraps ``surfaces._recall.search_recall_results`` so the federated recall
authority is reachable through ``praxis_search``. ``scope.type_slug``
maps to the underlying ``entity_type`` filter (decisions, patterns,
modules, etc.).

Filters out machine-generated event-log entries (``hard_failure:*``,
``verification:*``, ``workflow_<hex>``, ``receipt:*``) that pollute the
``fact`` entity_type — these were dominating semantic search ranking
because their generic short names matched many queries (BUG-4E6A2081).
Callers who want event-log search can opt in via
``scope.extras.include_event_log_facts=True``.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from surfaces._recall import search_recall_results
from surfaces.mcp.tools._search_envelope import (
    SOURCE_DECISIONS,
    SOURCE_KNOWLEDGE,
    SOURCE_RESEARCH,
    SearchEnvelope,
)


_SOURCE_TYPE_OVERRIDE = {
    SOURCE_DECISIONS: "decision",
    SOURCE_RESEARCH: "research",
}


# Machine-generated entity names that pollute semantic search.
# These are derived from workflow runs, verification events, hard
# failures, receipts — not human-curated knowledge. Filtered by default
# from federated search; opt back in via
# ``scope.extras.include_event_log_facts=True``.
_EVENT_LOG_NAME_PATTERNS = (
    re.compile(r"^hard_failure[:\s]"),
    re.compile(r"^verification[:\s]"),
    re.compile(r"^workflow_[a-f0-9]{8,}"),
    re.compile(r"^receipt[:\s]"),
    re.compile(r"^wave\d+_"),
)


def _is_event_log_noise(name: str, entity_type: str) -> bool:
    if entity_type and entity_type.lower() not in {"fact", ""}:
        return False
    if not name:
        return False
    return any(p.match(name) for p in _EVENT_LOG_NAME_PATTERNS)


def _exclude_term_hit(text: str, exclude_terms) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def search_knowledge(
    *,
    envelope: SearchEnvelope,
    subsystems: Any,
    source_label: str = SOURCE_KNOWLEDGE,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run a recall query and shape its results into the search envelope."""

    entity_type: str | None = _SOURCE_TYPE_OVERRIDE.get(source_label)
    if entity_type is None and envelope.scope.type_slug:
        entity_type = envelope.scope.type_slug

    # Bypass switch for callers that genuinely want event-log facts.
    extras = envelope.scope.extras or {}
    include_event_log = bool(extras.get("include_event_log_facts", False))

    try:
        rows = search_recall_results(
            subsystems,
            query=envelope.query,
            entity_type=entity_type,
            limit=envelope.limit * 2,  # over-fetch to absorb the filter
        )
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    results: list[dict[str, Any]] = []
    filtered_event_log = 0
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        content = str(row.get("content") or "")
        name = str(row.get("name") or "")
        if _exclude_term_hit(f"{content} {name}", envelope.scope.exclude_terms):
            continue
        if not include_event_log and _is_event_log_noise(name, str(row.get("type") or "")):
            filtered_event_log += 1
            continue
        results.append(
            {
                "source": source_label,
                "name": name,
                "type": row.get("type"),
                "entity_id": row.get("entity_id"),
                "match_text": content[:400] if content else name,
                "score": float(row.get("score") or 0.0),
                "found_via": row.get("found_via") or "knowledge_graph",
                "knowledge_source": row.get("source"),
                "_explain": {
                    "knowledge_score": float(row.get("score") or 0.0),
                    "found_via": row.get("found_via"),
                    "provenance": row.get("provenance"),
                },
            }
        )
        if len(results) >= envelope.limit:
            break

    freshness = {
        "status": "complete",
        "rows_considered": len(rows),
        "rows_filtered_event_log": filtered_event_log,
        "entity_type_filter": entity_type,
        "include_event_log_facts": include_event_log,
    }
    return results, freshness


__all__ = ["search_knowledge"]
