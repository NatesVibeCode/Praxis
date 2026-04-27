"""Bugs source plugin.

Wraps ``BugTracker.search`` so existing bug authority becomes a search
source. Status/severity/category accepted via ``scope.extras`` so the
common envelope stays small while power filters remain reachable.
Relevance scoring is token-overlap from ``runtime.sources._relevance``
so bugs are cross-source comparable to semantic-scored sources and
don't crowd them out (BUG-6E719C54).
"""
from __future__ import annotations

from typing import Any

from runtime.sources._relevance import query_tokens, token_overlap_score
from surfaces.mcp.tools._search_envelope import SOURCE_BUGS, SearchEnvelope


def _exclude_term_hit(text: str, exclude_terms) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def _bug_relevant(bug: Any, tokens: list[str]) -> bool:
    """Guard against BugTracker.search returning everything when nothing matches.

    Without this filter the bug source returns the entire open-bug list
    for any unmatched query (BUG-9475EEB0).
    """
    if not tokens:
        return True
    title = (getattr(bug, "title", "") or "").lower()
    description = (getattr(bug, "description", "") or "").lower()
    haystack = f"{title} {description}"
    return any(token in haystack for token in tokens)


def _bug_to_row(
    bug: Any, *, exclude_terms, tokens: list[str]
) -> dict[str, Any] | None:
    title = getattr(bug, "title", "") or ""
    description = getattr(bug, "description", "") or ""
    if _exclude_term_hit(f"{title} {description}", exclude_terms):
        return None
    bug_id = getattr(bug, "bug_id", "") or ""
    status_value = getattr(getattr(bug, "status", None), "value", None) or str(
        getattr(bug, "status", "") or ""
    )
    severity_value = getattr(getattr(bug, "severity", None), "value", None) or str(
        getattr(bug, "severity", "") or ""
    )
    category_value = getattr(getattr(bug, "category", None), "value", None) or str(
        getattr(bug, "category", "") or ""
    )
    score = token_overlap_score(tokens, f"{title} {description}")
    return {
        "source": SOURCE_BUGS,
        "entity_id": bug_id,
        "name": title,
        "match_text": title,
        "status": status_value,
        "severity": severity_value,
        "category": category_value,
        "description": description[:400] if description else "",
        "score": score,
        "found_via": "bug_tracker.token_overlap",
    }


def search_bugs(
    *,
    envelope: SearchEnvelope,
    subsystems: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Search bugs through the runtime BugTracker."""

    try:
        bug_tracker = subsystems.get_bug_tracker()
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    extras = envelope.scope.extras or {}
    try:
        bugs = bug_tracker.search(
            envelope.query,
            limit=envelope.limit,
            status=extras.get("status"),
            severity=extras.get("severity"),
            category=extras.get("category"),
            tags=tuple(extras.get("tags") or ()) or None,
            exclude_tags=tuple(extras.get("exclude_tags") or ()) or None,
            open_only=bool(extras.get("open_only", False)),
        )
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    tokens = query_tokens(envelope.query)
    relevant_bugs = [b for b in bugs if _bug_relevant(b, tokens)]
    rows = [
        row
        for bug in relevant_bugs
        if (
            row := _bug_to_row(
                bug, exclude_terms=envelope.scope.exclude_terms, tokens=tokens
            )
        )
        is not None
    ]
    # Drop zero-overlap rows so we don't pollute federated rank.
    rows = [r for r in rows if r.get("score", 0) > 0]
    return rows, {
        "status": "complete",
        "rows_considered": len(bugs),
        "rows_relevant": len(rows),
    }


__all__ = ["search_bugs"]
