"""Query handlers for the Policy Authority subsystem (P4.2.c).

Surfaces two read paths:
  - policy.list                — list active policy_definitions rows
  - compliance.list_receipts   — list authority_compliance_receipts rows
                                 (currently empty; populated when reject-
                                 path receipts ship in a follow-up)

Both are gateway-dispatched queries with `idempotency_policy=read_only`,
so identical inputs replay the cached receipt-backed result. Posture is
`observe` — strictly read.

The handlers follow the existing pattern in
`runtime.operations.queries.circuits` and friends: a Pydantic input
model + a `handle_query_*` function that takes (query, subsystems) and
returns a dict.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class QueryPolicyList(BaseModel):
    """Filters for policy_definitions list.

    All filters are optional. Empty filter = all active policies.
    """
    target_table: str | None = None
    enforcement_kind: str | None = None
    include_retired: bool = False

    @field_validator("target_table", "enforcement_kind", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("policy filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("enforcement_kind")
    @classmethod
    def _enforcement_kind_known(cls, value: str | None) -> str | None:
        if value is None:
            return None
        allowed = {"insert_reject", "update_reject", "delete_reject", "truncate_reject"}
        if value not in allowed:
            raise ValueError(
                f"enforcement_kind must be one of {sorted(allowed)}; got {value!r}"
            )
        return value


class QueryComplianceReceipts(BaseModel):
    """Filters for authority_compliance_receipts list."""
    policy_id: str | None = None
    target_table: str | None = None
    outcome: str | None = None  # 'admit' | 'reject'
    correlation_id: str | None = None
    limit: int = 100

    @field_validator("policy_id", "target_table", "correlation_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("compliance filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("outcome")
    @classmethod
    def _outcome_known(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in ("admit", "reject"):
            raise ValueError("outcome must be 'admit' or 'reject'")
        return value

    @field_validator("limit")
    @classmethod
    def _limit_in_range(cls, value: int) -> int:
        if value < 1 or value > 1000:
            raise ValueError("limit must be in [1, 1000]")
        return value


def _iso(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def handle_query_policy_list(
    query: QueryPolicyList,
    subsystems: Any,
) -> dict[str, Any]:
    """Read active policy_definitions rows, optionally filtered."""
    pg = subsystems.get_pg_conn()
    where: list[str] = []
    params: list[Any] = []
    if not query.include_retired:
        where.append("effective_to IS NULL")
    if query.target_table is not None:
        where.append(f"target_table = ${len(params) + 1}")
        params.append(query.target_table)
    if query.enforcement_kind is not None:
        where.append(f"enforcement_kind = ${len(params) + 1}")
        params.append(query.enforcement_kind)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = pg.execute(
        f"""
        SELECT
            policy_id,
            decision_key,
            enforcement_kind,
            target_table,
            target_column,
            predicate_sql,
            rationale,
            effective_from,
            effective_to,
            created_at,
            updated_at
          FROM policy_definitions
          {where_sql}
         ORDER BY target_table, enforcement_kind, decision_key
        """,
        *params,
    )

    items = [
        {
            "policy_id": r["policy_id"],
            "decision_key": r["decision_key"],
            "enforcement_kind": r["enforcement_kind"],
            "target_table": r["target_table"],
            "target_column": r["target_column"],
            "predicate_sql": r["predicate_sql"],
            "rationale": r["rationale"],
            "effective_from": _iso(r["effective_from"]),
            "effective_to": _iso(r["effective_to"]),
            "created_at": _iso(r["created_at"]),
            "updated_at": _iso(r["updated_at"]),
        }
        for r in rows
    ]
    return {
        "items": items,
        "count": len(items),
        "filters": {
            "target_table": query.target_table,
            "enforcement_kind": query.enforcement_kind,
            "include_retired": query.include_retired,
        },
    }


def handle_query_compliance_receipts(
    query: QueryComplianceReceipts,
    subsystems: Any,
) -> dict[str, Any]:
    """Read authority_compliance_receipts rows, filtered + capped."""
    pg = subsystems.get_pg_conn()
    where: list[str] = []
    params: list[Any] = []
    if query.policy_id is not None:
        where.append(f"policy_id = ${len(params) + 1}")
        params.append(query.policy_id)
    if query.target_table is not None:
        where.append(f"target_table = ${len(params) + 1}")
        params.append(query.target_table)
    if query.outcome is not None:
        where.append(f"outcome = ${len(params) + 1}")
        params.append(query.outcome)
    if query.correlation_id is not None:
        where.append(f"correlation_id = ${len(params) + 1}::uuid")
        params.append(query.correlation_id)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = pg.execute(
        f"""
        SELECT
            compliance_receipt_id,
            policy_id,
            decision_key,
            target_table,
            operation,
            outcome,
            rejected_reason,
            subject_pk,
            operation_receipt_id,
            correlation_id,
            created_at
          FROM authority_compliance_receipts
          {where_sql}
         ORDER BY created_at DESC
         LIMIT ${len(params) + 1}
        """,
        *params,
        query.limit,
    )

    items = [
        {
            "compliance_receipt_id": str(r["compliance_receipt_id"]),
            "policy_id": r["policy_id"],
            "decision_key": r["decision_key"],
            "target_table": r["target_table"],
            "operation": r["operation"],
            "outcome": r["outcome"],
            "rejected_reason": r["rejected_reason"],
            "subject_pk": r["subject_pk"],
            "operation_receipt_id": str(r["operation_receipt_id"]) if r["operation_receipt_id"] else None,
            "correlation_id": str(r["correlation_id"]) if r["correlation_id"] else None,
            "created_at": _iso(r["created_at"]),
        }
        for r in rows
    ]
    return {
        "items": items,
        "count": len(items),
        "filters": query.model_dump(),
    }


__all__ = [
    "QueryPolicyList",
    "QueryComplianceReceipts",
    "handle_query_policy_list",
    "handle_query_compliance_receipts",
]
