"""Knowledge graph + operator_decisions source plugin.

Wraps ``surfaces._recall.search_recall_results`` so the federated recall
authority is reachable through ``praxis_search``. ``scope.type_slug``
maps to the underlying ``entity_type`` filter (decisions, patterns,
modules, etc.).
"""
from __future__ import annotations

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

    try:
        rows = search_recall_results(
            subsystems,
            query=envelope.query,
            entity_type=entity_type,
            limit=envelope.limit,
        )
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    results: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        content = str(row.get("content") or "")
        name = str(row.get("name") or "")
        if _exclude_term_hit(f"{content} {name}", envelope.scope.exclude_terms):
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

    freshness = {
        "status": "complete",
        "rows_considered": len(rows),
        "entity_type_filter": entity_type,
    }
    return results, freshness


__all__ = ["search_knowledge"]
