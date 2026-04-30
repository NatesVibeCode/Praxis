"""Authority operation receipts source plugin.

Reads from ``authority_operation_receipts`` — the gateway audit ledger
written by ``runtime.operation_catalog_gateway`` for every dispatched
operation. Distinct from ``runtime.sources.receipts_source`` which
searches workflow execution receipts (``receipts`` table).

Supports per-source extras for the audit-lens questions:
    extras.transport_kind     — cli / mcp / http / workflow / heartbeat /
                                internal / sandbox / test / unknown
    extras.execution_status   — completed / replayed / failed
    extras.operation_kind     — command / query
    extras.operation_name     — exact match against operation_name
    extras.caller_ref         — exact match against caller_ref
    extras.since_hours        — int, restricts to last N hours

The relevance score is a token-overlap pass over the operation_name +
caller_ref + error_detail concatenation, so audit hits compete with
other sources on the same numeric scale.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from runtime.sources._relevance import query_tokens, token_overlap_score
from surfaces.mcp.tools._search_envelope import (
    SOURCE_AUTHORITY_RECEIPTS,
    SearchEnvelope,
)


def _exclude_term_hit(text: str, exclude_terms) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def _row_to_result(row: dict[str, Any], *, exclude_terms, tokens: list[str]) -> dict[str, Any] | None:
    operation_name = str(row.get("operation_name") or "")
    caller_ref = str(row.get("caller_ref") or "")
    transport_kind = str(row.get("transport_kind") or "")
    error_detail = str(row.get("error_detail") or "")
    haystack = " ".join(filter(None, [operation_name, caller_ref, transport_kind, error_detail]))
    if _exclude_term_hit(haystack, exclude_terms):
        return None
    score = token_overlap_score(tokens, haystack) if tokens else 1.0
    summary_parts = [operation_name]
    if transport_kind:
        summary_parts.append(f"via {transport_kind}")
    if caller_ref:
        summary_parts.append(f"caller={caller_ref}")
    summary = " · ".join(summary_parts)
    return {
        "source": SOURCE_AUTHORITY_RECEIPTS,
        "entity_id": str(row.get("receipt_id") or ""),
        "name": summary[:120],
        "match_text": summary[:400],
        "operation_name": operation_name,
        "operation_kind": row.get("operation_kind"),
        "transport_kind": transport_kind or None,
        "caller_ref": caller_ref or None,
        "execution_status": row.get("execution_status"),
        "result_status": row.get("result_status"),
        "error_code": row.get("error_code"),
        "error_detail": error_detail or None,
        "duration_ms": row.get("duration_ms"),
        "correlation_id": str(row.get("correlation_id") or "") or None,
        "cause_receipt_id": str(row.get("cause_receipt_id") or "") or None,
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "score": score,
        "found_via": "authority_operation_receipts.token_overlap",
    }


def _fetch_authority_receipts(
    conn: Any,
    *,
    query: str,
    limit: int,
    transport_kind: str | None,
    execution_status: str | None,
    operation_kind: str | None,
    operation_name: str | None,
    caller_ref: str | None,
    since_hours: int | None,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    where: list[str] = []
    if query:
        params.append(query)
        idx = len(params)
        where.append(
            f"(operation_name ILIKE '%' || ${idx} || '%' "
            f"OR caller_ref ILIKE '%' || ${idx} || '%' "
            f"OR COALESCE(error_detail, '') ILIKE '%' || ${idx} || '%')"
        )
    if transport_kind:
        params.append(transport_kind.strip().lower())
        where.append(f"transport_kind = ${len(params)}")
    if execution_status:
        params.append(execution_status.strip().lower())
        where.append(f"execution_status = ${len(params)}")
    if operation_kind:
        params.append(operation_kind.strip().lower())
        where.append(f"operation_kind = ${len(params)}")
    if operation_name:
        params.append(operation_name.strip())
        where.append(f"operation_name = ${len(params)}")
    if caller_ref:
        params.append(caller_ref.strip())
        where.append(f"caller_ref = ${len(params)}")
    if since_hours and since_hours > 0:
        params.append(datetime.now(timezone.utc) - timedelta(hours=since_hours))
        where.append(f"created_at >= ${len(params)}")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(max(int(limit), 1))
    sql = (
        "SELECT receipt_id::text AS receipt_id, "
        "operation_ref, operation_name, operation_kind, "
        "transport_kind, caller_ref, "
        "execution_status, result_status, error_code, error_detail, "
        "duration_ms, "
        "correlation_id::text AS correlation_id, "
        "cause_receipt_id::text AS cause_receipt_id, "
        "created_at "
        "FROM authority_operation_receipts "
        f"{where_clause} "
        f"ORDER BY created_at DESC LIMIT ${len(params)}"
    )
    rows = conn.execute(sql, *params)
    return [dict(row) for row in rows or ()]


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    return candidate or None


def search_authority_receipts(
    *,
    envelope: SearchEnvelope,
    subsystems: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    extras = envelope.scope.extras or {}
    try:
        conn = subsystems.get_pg_conn()
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    try:
        rows = _fetch_authority_receipts(
            conn,
            query=envelope.query,
            limit=envelope.limit,
            transport_kind=_coerce_text(extras.get("transport_kind")),
            execution_status=_coerce_text(extras.get("execution_status")),
            operation_kind=_coerce_text(extras.get("operation_kind")),
            operation_name=_coerce_text(extras.get("operation_name")),
            caller_ref=_coerce_text(extras.get("caller_ref")),
            since_hours=_coerce_int(extras.get("since_hours")),
        )
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    tokens = query_tokens(envelope.query)
    results: list[dict[str, Any]] = []
    for row in rows:
        result = _row_to_result(row, exclude_terms=envelope.scope.exclude_terms, tokens=tokens)
        if result is not None and result.get("score", 0) > 0:
            results.append(result)

    return results, {
        "status": "complete",
        "rows_considered": len(rows),
        "rows_relevant": len(results),
    }


__all__ = ["search_authority_receipts"]
