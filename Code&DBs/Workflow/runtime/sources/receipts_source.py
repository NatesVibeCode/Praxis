"""Receipts source plugin.

Wraps ``runtime.receipt_store.search_receipts``. The receipt search
already accepts query/status/agent/workflow_id; the source plugin
exposes them to praxis_search via ``scope.extras``. Relevance scoring
is token-overlap (BUG-6E719C54) so receipts are cross-source
comparable to semantic-scored sources.
"""
from __future__ import annotations

from typing import Any

from runtime.receipt_store import search_receipts as _search_receipts
from runtime.sources._relevance import query_tokens, token_overlap_score
from surfaces.mcp.tools._search_envelope import SOURCE_RECEIPTS, SearchEnvelope


def _exclude_term_hit(text: str, exclude_terms) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def _record_to_row(
    record: Any, *, exclude_terms, tokens: list[str]
) -> dict[str, Any] | None:
    summary = getattr(record, "summary", "") or ""
    agent = getattr(record, "agent", "") or ""
    workflow_id = getattr(record, "workflow_id", "") or ""
    if _exclude_term_hit(f"{summary} {agent} {workflow_id}", exclude_terms):
        return None
    score = token_overlap_score(tokens, f"{summary} {agent} {workflow_id}")
    return {
        "source": SOURCE_RECEIPTS,
        "entity_id": getattr(record, "receipt_id", ""),
        "name": summary[:120],
        "match_text": summary[:400],
        "status": getattr(record, "status", ""),
        "agent": agent,
        "workflow_id": workflow_id,
        "score": score,
        "found_via": "receipt_store.token_overlap",
    }


def search_receipts(
    *,
    envelope: SearchEnvelope,
    subsystems: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Search receipts via runtime.receipt_store.search_receipts."""

    extras = envelope.scope.extras or {}
    try:
        records = _search_receipts(
            envelope.query,
            limit=envelope.limit,
            status=extras.get("status"),
            agent=extras.get("agent"),
            workflow_id=extras.get("workflow_id"),
        )
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    tokens = query_tokens(envelope.query)
    rows = [
        row
        for record in records
        if (row := _record_to_row(record, exclude_terms=envelope.scope.exclude_terms, tokens=tokens))
        is not None
    ]
    rows = [r for r in rows if r.get("score", 0) > 0]
    return rows, {
        "status": "complete",
        "rows_considered": len(records),
        "rows_relevant": len(rows),
    }


__all__ = ["search_receipts"]
