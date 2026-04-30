"""Authority compliance receipts source plugin.

Reads from ``authority_compliance_receipts`` — the policy-enforcement
audit trail written by the reject-path triggers in migration 296+298.
A row exists for every UPDATE/DELETE/TRUNCATE that hit a registered
``policy_definitions`` rule, including the rejected ones (the
dblink-to-self pattern in migration 298 records the attempted mutation
even when the parent transaction rolls back).

Supports per-source extras for the policy-audit lens:
    extras.target_table   — exact match against target_table
    extras.outcome        — admit / reject
    extras.operation      — INSERT / UPDATE / DELETE / TRUNCATE
    extras.policy_id      — exact match against policy_id
    extras.since_hours    — int, restricts to last N hours
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from runtime.sources._relevance import query_tokens, token_overlap_score
from surfaces.mcp.tools._search_envelope import (
    SOURCE_COMPLIANCE_RECEIPTS,
    SearchEnvelope,
)


def _exclude_term_hit(text: str, exclude_terms) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def _row_to_result(row: dict[str, Any], *, exclude_terms, tokens: list[str]) -> dict[str, Any] | None:
    target_table = str(row.get("target_table") or "")
    operation = str(row.get("operation") or "")
    outcome = str(row.get("outcome") or "")
    rejected_reason = str(row.get("rejected_reason") or "")
    policy_id = str(row.get("policy_id") or "")
    decision_key = str(row.get("decision_key") or "")
    haystack = " ".join(
        filter(None, [target_table, operation, outcome, rejected_reason, policy_id, decision_key])
    )
    if _exclude_term_hit(haystack, exclude_terms):
        return None
    score = token_overlap_score(tokens, haystack) if tokens else 1.0
    summary = f"{outcome.upper()} {operation} on {target_table}"
    if rejected_reason and outcome == "reject":
        summary = f"{summary} — {rejected_reason[:80]}"
    return {
        "source": SOURCE_COMPLIANCE_RECEIPTS,
        "entity_id": str(row.get("compliance_receipt_id") or ""),
        "name": summary[:160],
        "match_text": summary[:400],
        "policy_id": policy_id or None,
        "decision_key": decision_key or None,
        "target_table": target_table or None,
        "operation": operation or None,
        "outcome": outcome or None,
        "rejected_reason": rejected_reason or None,
        "subject_pk": row.get("subject_pk"),
        "operation_receipt_id": str(row.get("operation_receipt_id") or "") or None,
        "correlation_id": str(row.get("correlation_id") or "") or None,
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "score": score,
        "found_via": "authority_compliance_receipts.token_overlap",
    }


def _fetch_compliance_receipts(
    conn: Any,
    *,
    query: str,
    limit: int,
    target_table: str | None,
    outcome: str | None,
    operation: str | None,
    policy_id: str | None,
    since_hours: int | None,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    where: list[str] = []
    if query:
        params.append(query)
        idx = len(params)
        where.append(
            f"(target_table ILIKE '%' || ${idx} || '%' "
            f"OR COALESCE(rejected_reason, '') ILIKE '%' || ${idx} || '%' "
            f"OR policy_id ILIKE '%' || ${idx} || '%' "
            f"OR decision_key ILIKE '%' || ${idx} || '%')"
        )
    if target_table:
        params.append(target_table.strip())
        where.append(f"target_table = ${len(params)}")
    if outcome:
        params.append(outcome.strip().lower())
        where.append(f"outcome = ${len(params)}")
    if operation:
        params.append(operation.strip().upper())
        where.append(f"operation = ${len(params)}")
    if policy_id:
        params.append(policy_id.strip())
        where.append(f"policy_id = ${len(params)}")
    if since_hours and since_hours > 0:
        params.append(datetime.now(timezone.utc) - timedelta(hours=since_hours))
        where.append(f"created_at >= ${len(params)}")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(max(int(limit), 1))
    sql = (
        "SELECT compliance_receipt_id::text AS compliance_receipt_id, "
        "policy_id, decision_key, target_table, operation, outcome, "
        "rejected_reason, subject_pk, "
        "operation_receipt_id::text AS operation_receipt_id, "
        "correlation_id::text AS correlation_id, "
        "created_at "
        "FROM authority_compliance_receipts "
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


def search_compliance_receipts(
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
        rows = _fetch_compliance_receipts(
            conn,
            query=envelope.query,
            limit=envelope.limit,
            target_table=_coerce_text(extras.get("target_table")),
            outcome=_coerce_text(extras.get("outcome")),
            operation=_coerce_text(extras.get("operation")),
            policy_id=_coerce_text(extras.get("policy_id")),
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


__all__ = ["search_compliance_receipts"]
