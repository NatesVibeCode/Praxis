"""Tool: praxis_authority_memory_refresh.

Projects authoritative FK relationships from authority tables
(roadmap_items, operator_object_relations, workflow_build_intents) into
memory_edges as ``authority_class='canonical'`` edges so discover, recall,
and the system atlas see the real structure instead of only enrichment.

Runtime: runtime/authority_memory_projection.py
Migration: 158_authority_memory_projection_vocabulary.sql
"""
from __future__ import annotations

import asyncio
from typing import Any

from runtime.authority_memory_projection import refresh_authority_memory_projection


def tool_praxis_authority_memory_refresh(params: dict, _progress_emitter=None) -> dict:
    """Run one idempotent refresh of the authority-to-memory projection."""
    result = asyncio.run(refresh_authority_memory_projection())
    return result.to_json()


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_authority_memory_refresh": (
        tool_praxis_authority_memory_refresh,
        {
            "description": (
                "Project authority FKs into memory_edges so the knowledge graph reflects "
                "actual structure. Upserts canonical-class edges for roadmap parent_of, "
                "roadmap resolves_bug, operator_object_relations mirror, and "
                "workflow_build_intent implements_build. Idempotent; safe to re-run.\n\n"
                "USE WHEN: after bulk roadmap edits, new operator_object_relations, or "
                "when `praxis workflow discover` returns shallow results and you suspect "
                "the memory graph is stale.\n\n"
                "EXAMPLE: praxis_authority_memory_refresh()\n\n"
                "DO NOT USE: as a real-time sync mechanism — it's scan-based. For per-"
                "event projection, wait until authority write paths emit outbox events "
                "and a true cursor subscriber is wired."
            ),
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": (
                    "Refresh the authority-to-memory projection after bulk authority "
                    "writes so discover and recall see current structure."
                ),
                "when_not_to_use": (
                    "Do not use it for reading the graph; use praxis_discover or "
                    "praxis_recall."
                ),
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Refresh authority projection", "input": {}},
                ],
            },
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
        },
    ),
}
