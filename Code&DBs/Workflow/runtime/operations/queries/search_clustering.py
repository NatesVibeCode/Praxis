"""Cluster construction for federated search responses.

Turns a flat ranked hit list into topic-anchor clusters where each
cluster carries its full graph neighborhood — typed-edge-linked items
in ``related`` (capped at 3 per source with total count + fetch hint)
and signal-only counts in ``also`` for whatever didn't fit.

Walks the existing typed-edge tables:

- ``authority_object_registry.source_decision_ref`` → decision ↔ code/op
- ``roadmap_items.decision_ref``                    → decision ↔ roadmap
- ``bugs.decision_ref``                              → decision ↔ bugs

Plus semantic-neighbor pickup above a similarity floor for items the
typed edges miss. Empty state surfaces an explicit message and emits
``typed_gap(reason_code='retrieval.no_match')`` per the
"retrieval is the filter" standing order.
"""
from __future__ import annotations

import re
from typing import Any

from runtime.typed_gap_events import emit_typed_gap


_DEFAULT_CLUSTER_LIMIT = 5
_DEFAULT_RELATED_PER_SOURCE = 3
_DEFAULT_SIMILARITY_FLOOR = 0.5


def _canonical_key(hit: dict) -> str:
    """Stable key used to dedup hits across sources."""

    eid = str(hit.get("entity_id") or "").strip()
    if eid:
        return f"{hit.get('source', '?')}:{eid}"
    name = str(hit.get("name") or "").strip().lower()
    path = str(hit.get("path") or "").strip().lower()
    if path:
        return f"{hit.get('source', '?')}:path:{path}"
    if name:
        return f"{hit.get('source', '?')}:name:{name[:80]}"
    return f"{hit.get('source', '?')}:hash:{hash(repr(hit)) & 0xffff:x}"


_TOPIC_NORMALIZE = re.compile(r"[^a-z0-9]+")


def _topic_key(hit: dict) -> str:
    """Looser key used to detect that two hits are about the same topic.

    Strips punctuation/case from the name so 'Token budgets are not workflow
    execution authority' and 'token budget authority' collapse.
    """

    text = str(hit.get("name") or hit.get("match_text") or "").strip().lower()
    if not text:
        return _canonical_key(hit)
    normalized = _TOPIC_NORMALIZE.sub(" ", text).strip()
    tokens = normalized.split()
    if not tokens:
        return _canonical_key(hit)
    return " ".join(tokens[:8])


def _resolve_code_to_decisions(
    *, name: str | None, path: str | None, conn: Any, limit: int
) -> list[dict[str, Any]]:
    if conn is None or not (name or path):
        return []
    object_name = (name or "").strip()
    if not object_name:
        return []
    try:
        rows = conn.execute(
            """
            SELECT od.decision_key, od.title, od.decision_kind
              FROM authority_object_registry aor
              JOIN operator_decisions od ON od.decision_key = aor.source_decision_ref
             WHERE aor.object_name = $1
                OR aor.object_ref = $1
             LIMIT $2
            """,
            object_name,
            int(limit),
        )
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows or []:
        out.append(
            {
                "source": "decisions",
                "name": r.get("title") if hasattr(r, "get") else r["title"],
                "entity_id": r.get("decision_key") if hasattr(r, "get") else r["decision_key"],
                "decision_kind": r.get("decision_kind") if hasattr(r, "get") else r["decision_kind"],
                "score": 1.0,
                "found_via": "typed_edge.source_decision_ref",
            }
        )
    return out


def _resolve_decision_to_code(
    *, decision_key: str, conn: Any, limit: int
) -> list[dict[str, Any]]:
    if conn is None or not decision_key:
        return []
    try:
        rows = conn.execute(
            """
            SELECT object_name, object_kind, object_ref
              FROM authority_object_registry
             WHERE source_decision_ref = $1
             LIMIT $2
            """,
            decision_key,
            int(limit),
        )
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows or []:
        get = r.get if hasattr(r, "get") else r.__getitem__
        out.append(
            {
                "source": "code",
                "name": get("object_name"),
                "kind": get("object_kind"),
                "entity_id": get("object_ref"),
                "score": 1.0,
                "found_via": "typed_edge.source_decision_ref",
            }
        )
    return out


def _resolve_decision_to_roadmap(
    *, decision_key: str, conn: Any, limit: int
) -> list[dict[str, Any]]:
    if conn is None or not decision_key:
        return []
    try:
        rows = conn.execute(
            """
            SELECT roadmap_item_id, roadmap_key, title, status, lifecycle, priority
              FROM roadmap_items
             WHERE decision_ref = $1
             LIMIT $2
            """,
            decision_key,
            int(limit),
        )
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows or []:
        get = r.get if hasattr(r, "get") else r.__getitem__
        out.append(
            {
                "source": "roadmap",
                "name": get("title"),
                "entity_id": get("roadmap_key"),
                "status": get("status"),
                "lifecycle": get("lifecycle"),
                "priority": get("priority"),
                "score": 1.0,
                "found_via": "typed_edge.decision_ref",
            }
        )
    return out


def _resolve_decision_to_bugs(
    *, decision_key: str, conn: Any, limit: int
) -> list[dict[str, Any]]:
    if conn is None or not decision_key:
        return []
    try:
        rows = conn.execute(
            """
            SELECT bug_id, bug_key, title, status, severity
              FROM bugs
             WHERE decision_ref = $1
             LIMIT $2
            """,
            decision_key,
            int(limit),
        )
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows or []:
        get = r.get if hasattr(r, "get") else r.__getitem__
        out.append(
            {
                "source": "bugs",
                "name": get("title"),
                "entity_id": get("bug_key") or get("bug_id"),
                "status": get("status"),
                "severity": get("severity"),
                "score": 1.0,
                "found_via": "typed_edge.decision_ref",
            }
        )
    return out


def _typed_edges_for_anchor(
    anchor: dict, *, conn: Any, per_source_limit: int
) -> dict[str, list[dict[str, Any]]]:
    """Walk typed edges from the anchor to find authoritative neighbors."""

    src = str(anchor.get("source") or "")
    related: dict[str, list[dict[str, Any]]] = {}
    if src == "decisions":
        decision_key = str(anchor.get("entity_id") or "").strip()
        if decision_key:
            related["code"] = _resolve_decision_to_code(
                decision_key=decision_key, conn=conn, limit=per_source_limit * 2
            )
            related["roadmap"] = _resolve_decision_to_roadmap(
                decision_key=decision_key, conn=conn, limit=per_source_limit * 2
            )
            related["bugs"] = _resolve_decision_to_bugs(
                decision_key=decision_key, conn=conn, limit=per_source_limit * 2
            )
    elif src == "code":
        related["decisions"] = _resolve_code_to_decisions(
            name=anchor.get("name"),
            path=anchor.get("path"),
            conn=conn,
            limit=per_source_limit * 2,
        )
    return {k: v for k, v in related.items() if v}


def _shrink_related(
    related: dict[str, list[dict[str, Any]]],
    *,
    per_source_cap: int,
    envelope_query: str,
) -> dict[str, dict[str, Any]]:
    """Cap each source at N items + attach count + fetch hint."""

    shrunk: dict[str, dict[str, Any]] = {}
    for src, items in related.items():
        if not items:
            continue
        total = len(items)
        kept = items[:per_source_cap]
        block: dict[str, Any] = {"items": kept, "count": total}
        if total > per_source_cap:
            block["fetch_hint"] = {
                "operation_name": f"search.{src}",
                "payload": {
                    "query": envelope_query,
                    "limit": min(50, total),
                },
                "preview": f"{total - per_source_cap} more in '{src}'",
            }
        shrunk[src] = block
    return shrunk


def _build_also(
    *,
    raw_hits: list[dict[str, Any]],
    consumed_keys: set[str],
    envelope_query: str,
    similarity_floor: float,
) -> dict[str, dict[str, Any]]:
    """Items not absorbed into any cluster — count + preview + fetch hint."""

    by_source: dict[str, list[dict[str, Any]]] = {}
    for hit in raw_hits:
        if _canonical_key(hit) in consumed_keys:
            continue
        if float(hit.get("score") or 0.0) < similarity_floor:
            continue
        by_source.setdefault(str(hit.get("source") or "?"), []).append(hit)

    also: dict[str, dict[str, Any]] = {}
    for src, items in by_source.items():
        if not items:
            continue
        top = items[0]
        preview = (
            top.get("name")
            or top.get("match_text")
            or ""
        )
        also[src] = {
            "count": len(items),
            "preview": str(preview)[:120],
            "fetch_hint": {
                "operation_name": f"search.{src}",
                "payload": {
                    "query": envelope_query,
                    "limit": min(50, len(items)),
                },
            },
        }
    return also


def _suggest_for_empty(
    *, envelope, sources_status: dict[str, str]
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    if envelope.mode == "exact" or envelope.mode == "regex":
        suggestions.append(
            {
                "kind": "broaden_mode",
                "rationale": "exact/regex returned 0 — semantic mode may match",
                "apply": {"mode": "semantic"},
            }
        )
    if envelope.scope.paths:
        suggestions.append(
            {
                "kind": "drop_path_filter",
                "rationale": "scope.paths may be over-narrow",
                "apply": {"scope": {"paths": []}},
            }
        )
    if len(envelope.sources) <= 2:
        suggestions.append(
            {
                "kind": "expand_sources",
                "rationale": "try more sources for broader coverage",
                "apply": {
                    "sources": list(
                        {*envelope.sources, "code", "knowledge", "decisions"}
                    )
                },
            }
        )
    return suggestions


def _emit_no_match_gap(
    *, envelope, conn: Any, scope_label: str
) -> str | None:
    if conn is None:
        return None
    try:
        return emit_typed_gap(
            conn,
            gap_kind="retrieval",
            missing_type="search_result",
            reason_code="retrieval.no_match",
            legal_repair_actions=["broaden_query", "drop_path_filter", "expand_sources"],
            source_ref="search.federated",
            context={
                "query": envelope.query,
                "mode_resolved": envelope.mode,
                "sources": list(envelope.sources),
                "scope_paths": list(envelope.scope.paths or ()),
                "scope_label": scope_label,
            },
        )
    except Exception:
        return None


def build_clusters(
    *,
    envelope,
    raw_hits: list[dict[str, Any]],
    sources_status: dict[str, str],
    subsystems: Any,
    cluster_limit: int = _DEFAULT_CLUSTER_LIMIT,
    per_source_cap: int = _DEFAULT_RELATED_PER_SOURCE,
    similarity_floor: float = _DEFAULT_SIMILARITY_FLOOR,
) -> dict[str, Any]:
    """Build the topic-anchor cluster shape from a flat federated hit list."""

    conn = None
    try:
        conn = subsystems.get_pg_conn()
    except Exception:
        conn = None

    # Sort raw hits by score, stable across sources
    raw_sorted = sorted(
        raw_hits, key=lambda h: -float(h.get("score") or 0.0)
    )

    # Pick anchors — top distinct topics
    anchors: list[dict[str, Any]] = []
    seen_topics: set[str] = set()
    for hit in raw_sorted:
        topic = _topic_key(hit)
        if topic in seen_topics:
            continue
        seen_topics.add(topic)
        anchors.append(hit)
        if len(anchors) >= cluster_limit:
            break

    # Build clusters
    consumed_keys: set[str] = set()
    clusters: list[dict[str, Any]] = []
    for anchor in anchors:
        consumed_keys.add(_canonical_key(anchor))
        # Typed-edge walks
        typed_related = _typed_edges_for_anchor(
            anchor, conn=conn, per_source_limit=per_source_cap
        )
        # Semantic-neighbor pickup from raw hits, above the floor,
        # and not on the same source as the anchor (those are competitors)
        semantic_related: dict[str, list[dict[str, Any]]] = {}
        anchor_src = str(anchor.get("source") or "")
        for hit in raw_sorted:
            key = _canonical_key(hit)
            if key in consumed_keys:
                continue
            if float(hit.get("score") or 0.0) < similarity_floor:
                continue
            hsrc = str(hit.get("source") or "")
            if hsrc == anchor_src:
                continue  # don't pull same-source items into related
            semantic_related.setdefault(hsrc, []).append(hit)
            consumed_keys.add(key)

        # Merge typed + semantic, typed first (authoritative)
        merged: dict[str, list[dict[str, Any]]] = {}
        for src, items in typed_related.items():
            merged[src] = list(items)
        for src, items in semantic_related.items():
            existing = merged.setdefault(src, [])
            existing_keys = {_canonical_key(x) for x in existing}
            for item in items:
                if _canonical_key(item) not in existing_keys:
                    existing.append(item)

        related_block = _shrink_related(
            merged,
            per_source_cap=per_source_cap,
            envelope_query=envelope.query,
        )

        clusters.append(
            {
                "anchor": str(
                    anchor.get("name")
                    or anchor.get("match_text")
                    or anchor.get("path")
                    or ""
                )[:120],
                "primary": anchor,
                "related": related_block,
                "score": float(anchor.get("score") or 0.0),
            }
        )

    # "also" — the residual that didn't make it into any cluster
    also = _build_also(
        raw_hits=raw_sorted,
        consumed_keys=consumed_keys,
        envelope_query=envelope.query,
        similarity_floor=similarity_floor,
    )

    # Empty state — both whole-query and per-source
    payload: dict[str, Any] = {
        "clusters": clusters,
        "anchor_count": len(clusters),
    }
    if also:
        payload["also"] = also

    if not clusters:
        gap_id = _emit_no_match_gap(
            envelope=envelope, conn=conn, scope_label="whole_query"
        )
        payload["empty_state"] = {
            "reason_code": "retrieval.no_match",
            "message": (
                f"No matches for '{envelope.query}' across sources "
                f"{list(envelope.sources)}"
            ),
            "sources_attempted": list(envelope.sources),
            "suggestions": _suggest_for_empty(
                envelope=envelope, sources_status=sources_status
            ),
            "typed_gap_emitted": gap_id,
        }
    else:
        per_source_empty = _per_source_empty_states(
            envelope=envelope,
            sources_status=sources_status,
            raw_hits=raw_hits,
            conn=conn,
        )
        if per_source_empty:
            payload["source_empty_states"] = per_source_empty

    return payload


def _per_source_empty_states(
    *,
    envelope,
    sources_status: dict[str, str],
    raw_hits: list[dict[str, Any]],
    conn: Any,
) -> dict[str, Any]:
    """Per-source 'nothing matched' signals when the user targeted a source."""

    hit_sources = {str(h.get("source") or "") for h in raw_hits}
    out: dict[str, Any] = {}
    for src in envelope.sources:
        if src in hit_sources:
            continue
        status = sources_status.get(src) or "unknown"
        if status not in ("ok", "complete"):
            # Source failed for environmental reasons — don't double-flag
            out[src] = {
                "reason_code": f"retrieval.{status}",
                "message": f"Source '{src}' returned status='{status}'",
            }
            continue
        out[src] = {
            "reason_code": "retrieval.no_match",
            "message": f"No matches in source '{src}' for '{envelope.query}'",
            "suggestions": _suggest_for_empty(
                envelope=envelope, sources_status=sources_status
            ),
        }
    return out


__all__ = ["build_clusters"]
