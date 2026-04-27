"""Trace-walk query handler — return the cause tree for any anchor.

Phase 1 of causal tracing. The gateway now stamps every receipt with a
``correlation_id`` (same UUID for the whole trace) and an optional
``cause_receipt_id`` (the receipt that caused this one). This handler
walks those edges and returns a tree.

Anchor types accepted (exactly one):
    receipt_id     — exact receipt UUID
    event_id       — UUID of an authority_events row; resolved to its receipt
    correlation_id — fetch the entire trace by correlation
    run_id         — workflow run id; resolved via authority_events payloads
    bug_id         — bug id; resolved via bugs.discovered_in_receipt_id then
                     bug_evidence_links as fallback

For each anchor we emit:
    root          — the receipt at the root of the trace tree
    nodes         — every receipt in the trace (one row per receipt)
    edges         — (parent_receipt_id, child_receipt_id) pairs
    events        — authority_events rows in the same correlation
    orphan_count  — receipts in the correlation that have no path back
                    to root (Phase 2/3 will close this gap)

The walk is intentionally bounded by ``correlation_id`` — that's the
cheapest way to grab the whole tree without recursive CTE overhead, and
it degrades gracefully on receipts predating migration 290 (those rows
have NULL correlation_id and will not appear in any trace).

Anchor robustness: ``bugs.discovered_in_receipt_id`` and many
``bug_evidence_links.evidence_ref`` rows still store *legacy* receipt
identifiers (e.g. ``receipt:workflow_<hex>:<n>:<n>``) that predate the
CQRS receipts table and are NOT UUIDs. ``_looks_like_uuid`` guards
every UUID-typed lookup so a non-UUID value resolves cleanly to
``trace.no_correlation`` instead of letting asyncpg's
InvalidTextRepresentation bubble up through the gateway as a generic
failure.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field, model_validator


_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _looks_like_uuid(value: Any) -> bool:
    return isinstance(value, str) and bool(_UUID_PATTERN.match(value))


class QueryTraceWalk(BaseModel):
    """Input to ``trace.walk``.

    Provide exactly one of ``receipt_id``, ``event_id``, ``correlation_id``,
    ``run_id``, or ``bug_id``.
    """

    receipt_id: str | None = Field(default=None)
    event_id: str | None = Field(default=None)
    correlation_id: str | None = Field(default=None)
    run_id: str | None = Field(default=None)
    bug_id: str | None = Field(default=None)

    @model_validator(mode="after")
    def _exactly_one_anchor(self) -> "QueryTraceWalk":
        anchors = [
            self.receipt_id,
            self.event_id,
            self.correlation_id,
            self.run_id,
            self.bug_id,
        ]
        provided = [a for a in anchors if a]
        if len(provided) != 1:
            raise ValueError(
                "trace.walk requires exactly one of receipt_id, event_id, "
                "correlation_id, run_id, or bug_id"
            )
        return self


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "items"):
        return dict(row.items())
    return dict(row)  # type: ignore[arg-type]


def _correlation_for_receipt(conn: Any, receipt_id: str) -> str | None:
    if not _looks_like_uuid(receipt_id):
        return None
    rows = conn.execute(
        "SELECT correlation_id FROM authority_operation_receipts "
        "WHERE receipt_id = $1::uuid LIMIT 1",
        receipt_id,
    )
    for row in rows or ():
        value = _row_dict(row).get("correlation_id")
        return str(value) if value else None
    return None


def _resolve_via_event(conn: Any, event_id: str) -> str | None:
    if not _looks_like_uuid(event_id):
        return None
    rows = conn.execute(
        "SELECT correlation_id, receipt_id FROM authority_events "
        "WHERE event_id = $1::uuid LIMIT 1",
        event_id,
    )
    for row in rows or ():
        data = _row_dict(row)
        value = data.get("correlation_id")
        if value:
            return str(value)
        receipt_id = data.get("receipt_id")
        if receipt_id:
            return _correlation_for_receipt(conn, str(receipt_id))
        return None
    return None


def _resolve_via_run(conn: Any, run_id: str) -> str | None:
    """Resolve a workflow run_id to a correlation_id.

    The receipts table has no direct run_id column. We search
    authority_events.event_payload for the run id under either
    ``run_id`` or ``workflow_run_id`` and use the most recent matching
    event's correlation_id. As a fallback, follow ``bugs.discovered_in_run_id``
    to a discovered_in_receipt_id and read its correlation.
    """

    rows = conn.execute(
        """
        SELECT correlation_id, receipt_id
        FROM authority_events
        WHERE event_payload->>'run_id' = $1
           OR event_payload->>'workflow_run_id' = $1
        ORDER BY event_sequence DESC
        LIMIT 1
        """,
        run_id,
    )
    for row in rows or ():
        data = _row_dict(row)
        value = data.get("correlation_id")
        if value:
            return str(value)
        receipt_id = data.get("receipt_id")
        if receipt_id:
            return _correlation_for_receipt(conn, str(receipt_id))

    fallback = conn.execute(
        """
        SELECT discovered_in_receipt_id
        FROM bugs
        WHERE discovered_in_run_id = $1
          AND discovered_in_receipt_id IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
        """,
        run_id,
    )
    for row in fallback or ():
        receipt_id = _row_dict(row).get("discovered_in_receipt_id")
        if receipt_id:
            return _correlation_for_receipt(conn, str(receipt_id))
    return None


def _resolve_via_bug(conn: Any, bug_id: str) -> str | None:
    """Resolve a bug_id to a correlation_id.

    Primary path: ``bugs.correlation_id`` (populated by ``bug_tracker.file_bug``
    from CURRENT_CALLER_CONTEXT at filing time, migration 293). Fallback
    paths: ``bugs.discovered_in_receipt_id`` and ``bug_evidence_links``
    where ``evidence_kind='receipt'`` — both only resolve when the
    referenced receipt is UUID-shaped (post-CQRS).
    """

    rows = conn.execute(
        """
        SELECT correlation_id, discovered_in_receipt_id
        FROM bugs
        WHERE bug_id = $1
        LIMIT 1
        """,
        bug_id,
    )
    for row in rows or ():
        data = _row_dict(row)
        correlation_id = data.get("correlation_id")
        if correlation_id:
            return str(correlation_id)
        receipt_id = data.get("discovered_in_receipt_id")
        if receipt_id:
            corr = _correlation_for_receipt(conn, str(receipt_id))
            if corr:
                return corr

    fallback = conn.execute(
        """
        SELECT evidence_ref
        FROM bug_evidence_links
        WHERE bug_id = $1
          AND evidence_kind = 'receipt'
        ORDER BY created_at DESC
        LIMIT 5
        """,
        bug_id,
    )
    for row in fallback or ():
        receipt_id = _row_dict(row).get("evidence_ref")
        if receipt_id:
            corr = _correlation_for_receipt(conn, str(receipt_id))
            if corr:
                return corr
    return None


def _resolve_correlation_id(
    conn: Any, query: QueryTraceWalk
) -> str | None:
    if query.correlation_id:
        return query.correlation_id if _looks_like_uuid(query.correlation_id) else None
    if query.receipt_id:
        return _correlation_for_receipt(conn, query.receipt_id)
    if query.event_id:
        return _resolve_via_event(conn, query.event_id)
    if query.run_id:
        return _resolve_via_run(conn, query.run_id)
    if query.bug_id:
        return _resolve_via_bug(conn, query.bug_id)
    return None


def _fetch_trace_receipts(conn: Any, correlation_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            receipt_id,
            cause_receipt_id,
            correlation_id,
            operation_ref,
            operation_name,
            operation_kind,
            execution_status,
            result_status,
            error_code,
            duration_ms,
            created_at
        FROM authority_operation_receipts
        WHERE correlation_id = $1::uuid
        ORDER BY created_at ASC, receipt_id ASC
        """,
        correlation_id,
    )
    return [_row_dict(row) for row in rows or ()]


def _fetch_trace_events(conn: Any, correlation_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            event_id,
            event_sequence,
            event_type,
            aggregate_ref,
            operation_ref,
            receipt_id,
            causation_event_id,
            correlation_id,
            emitted_at
        FROM authority_events
        WHERE correlation_id = $1::uuid
        ORDER BY event_sequence ASC
        """,
        correlation_id,
    )
    return [_row_dict(row) for row in rows or ()]


def _serialize_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _serialize_value(val) for key, val in row.items()}


def _build_tree(
    receipts: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, list[dict[str, str]], int]:
    by_id = {str(r["receipt_id"]): r for r in receipts}
    edges: list[dict[str, str]] = []
    children_of: dict[str | None, list[str]] = {}
    for r in receipts:
        cause = r.get("cause_receipt_id")
        cause_id = str(cause) if cause else None
        children_of.setdefault(cause_id, []).append(str(r["receipt_id"]))
        if cause_id is not None:
            edges.append({"cause_receipt_id": cause_id, "receipt_id": str(r["receipt_id"])})

    roots = children_of.get(None, [])
    orphan_count = 0
    if not roots and receipts:
        # No NULL-cause row in this correlation — every receipt has a parent
        # outside this correlation. Treat earliest as the local root and
        # mark the rest as orphans for visibility.
        roots = [str(receipts[0]["receipt_id"])]
        orphan_count = len(receipts) - 1
    elif len(roots) > 1:
        # Multiple NULL-cause receipts in one correlation. Pre-Phase-2 this
        # happens when an entry-point spawns a sibling entry. Pick the
        # earliest as the canonical root; the rest become orphan-ish nodes
        # but all stay in the tree under their own subtree.
        orphan_count = len(roots) - 1

    if not roots:
        return None, edges, orphan_count

    root_id = roots[0]
    return by_id.get(root_id), edges, orphan_count


async def handle_query_trace_walk(
    query: QueryTraceWalk, subsystems: Any
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    correlation_id = _resolve_correlation_id(conn, query)
    if not correlation_id:
        return {
            "ok": False,
            "reason_code": "trace.no_correlation",
            "message": (
                "Anchor did not resolve to a correlation_id. The receipt or "
                "event predates migration 290, or the anchor does not exist."
            ),
            "anchor": {
                "receipt_id": query.receipt_id,
                "event_id": query.event_id,
                "correlation_id": query.correlation_id,
                "run_id": query.run_id,
                "bug_id": query.bug_id,
            },
        }

    receipts = _fetch_trace_receipts(conn, correlation_id)
    events = _fetch_trace_events(conn, correlation_id)

    if not receipts:
        return {
            "ok": True,
            "correlation_id": correlation_id,
            "root": None,
            "nodes": [],
            "edges": [],
            "events": [_serialize_row(e) for e in events],
            "orphan_count": 0,
            "node_count": 0,
            "anchor": {
                "receipt_id": query.receipt_id,
                "event_id": query.event_id,
                "correlation_id": query.correlation_id,
                "run_id": query.run_id,
                "bug_id": query.bug_id,
            },
        }

    root, edges, orphan_count = _build_tree(receipts)

    return {
        "ok": True,
        "correlation_id": correlation_id,
        "root": _serialize_row(root) if root else None,
        "nodes": [_serialize_row(r) for r in receipts],
        "edges": edges,
        "events": [_serialize_row(e) for e in events],
        "orphan_count": orphan_count,
        "node_count": len(receipts),
        "anchor": {
            "receipt_id": query.receipt_id,
            "event_id": query.event_id,
            "correlation_id": query.correlation_id,
        },
    }


__all__ = [
    "QueryTraceWalk",
    "handle_query_trace_walk",
]
