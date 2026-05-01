"""CQRS read operations for agent_principal authority.

Reads are receipt-recorded but never replayed (idempotency_policy=read_only)
because wake counts and standing-order projections move between calls.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic input models
# ─────────────────────────────────────────────────────────────────────────────


class ListAgentPrincipalsQuery(BaseModel):
    """List agent principals filtered by status."""

    status: Literal["active", "paused", "killed", "any"] = "active"
    limit: int = Field(default=50, ge=1, le=500)


class DescribeAgentPrincipalQuery(BaseModel):
    """Describe one principal with recent wakes, delegations, and tool gaps."""

    agent_principal_ref: str = Field(..., min_length=1)
    history_limit: int = Field(default=10, ge=1, le=200)

    @field_validator("agent_principal_ref")
    @classmethod
    def _strip(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("agent_principal_ref must be non-empty")
        return stripped


class ListAgentWakesQuery(BaseModel):
    """List wake-ledger rows."""

    agent_principal_ref: str | None = None
    trigger_kind: Literal["chat", "schedule", "webhook", "delegation", "manual"] | None = None
    status: Literal["pending", "dispatched", "completed", "failed", "skipped"] | None = None
    limit: int = Field(default=50, ge=1, le=500)


class ListAgentToolGapsQuery(BaseModel):
    """List tool-gap rows for roadmap triage."""

    reporter_agent_ref: str | None = None
    severity: Literal["low", "medium", "high", "blocking"] | None = None
    status: Literal["open", "triaged", "planned", "shipped", "declined", "duplicate"] | None = (
        "open"
    )
    missing_capability: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


# ─────────────────────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────────────────────


def _principal_row(row: Any) -> dict[str, Any]:
    return {
        "agent_principal_ref": row["agent_principal_ref"],
        "title": row["title"],
        "status": row["status"],
        "max_in_flight_wakes": row["max_in_flight_wakes"],
        "write_envelope": row.get("write_envelope") or [],
        "capability_refs": row.get("capability_refs") or [],
        "integration_refs": row.get("integration_refs") or [],
        "standing_order_keys": row.get("standing_order_keys") or [],
        "allowed_tools": row.get("allowed_tools") or [],
        "network_policy": row["network_policy"],
        "default_conversation_id": row.get("default_conversation_id"),
        "routing_policy": row.get("routing_policy"),
        "metadata": row.get("metadata") or {},
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def handle_list_agent_principals(
    query: ListAgentPrincipalsQuery,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.status == "any":
        rows = conn.execute(
            """SELECT * FROM agent_registry
               ORDER BY agent_principal_ref
               LIMIT $1""",
            int(query.limit),
        )
    else:
        rows = conn.execute(
            """SELECT * FROM agent_registry
               WHERE status = $1
               ORDER BY agent_principal_ref
               LIMIT $2""",
            query.status,
            int(query.limit),
        )
    principals = [_principal_row(dict(row)) for row in (rows or [])]
    return {
        "ok": True,
        "operation": "agent_principal.list",
        "principals": principals,
        "count": len(principals),
    }


def _wake_row(row: Any) -> dict[str, Any]:
    return {
        "wake_id": str(row["wake_id"]),
        "agent_principal_ref": row["agent_principal_ref"],
        "trigger_kind": row["trigger_kind"],
        "trigger_source_ref": row.get("trigger_source_ref"),
        "status": row["status"],
        "run_id": row.get("run_id"),
        "received_at": row["received_at"].isoformat() if row.get("received_at") else None,
        "dispatched_at": row["dispatched_at"].isoformat() if row.get("dispatched_at") else None,
        "completed_at": row["completed_at"].isoformat() if row.get("completed_at") else None,
        "skip_reason": row.get("skip_reason"),
        "closeout_receipt_id": str(row["closeout_receipt_id"])
        if row.get("closeout_receipt_id")
        else None,
    }


def _delegation_row(row: Any) -> dict[str, Any]:
    return {
        "delegation_id": str(row["delegation_id"]),
        "parent_agent_ref": row["parent_agent_ref"],
        "parent_run_id": row.get("parent_run_id"),
        "child_task": row["child_task"],
        "child_run_id": row.get("child_run_id"),
        "status": row["status"],
        "network_policy": row["network_policy"],
        "admitted_tools": row.get("admitted_tools") or [],
        "gap_count": row["gap_count"],
        "requested_at": row["requested_at"].isoformat() if row.get("requested_at") else None,
        "completed_at": row["completed_at"].isoformat() if row.get("completed_at") else None,
    }


def _gap_row(row: Any) -> dict[str, Any]:
    return {
        "gap_id": str(row["gap_id"]),
        "reporter_agent_ref": row["reporter_agent_ref"],
        "missing_capability": row["missing_capability"],
        "attempted_task": row["attempted_task"],
        "blocked_action": row["blocked_action"],
        "severity": row["severity"],
        "status": row["status"],
        "roadmap_item_ref": row.get("roadmap_item_ref"),
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
    }


def handle_describe_agent_principal(
    query: DescribeAgentPrincipalQuery,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    rows = conn.execute(
        "SELECT * FROM agent_registry WHERE agent_principal_ref = $1",
        query.agent_principal_ref,
    )
    if not rows:
        return {
            "ok": False,
            "operation": "agent_principal.describe",
            "error_code": "agent_principal.not_found",
            "agent_principal_ref": query.agent_principal_ref,
        }
    principal = _principal_row(dict(rows[0]))

    wakes = conn.execute(
        """SELECT * FROM agent_wakes
           WHERE agent_principal_ref = $1
           ORDER BY received_at DESC
           LIMIT $2""",
        query.agent_principal_ref,
        int(query.history_limit),
    )
    delegations = conn.execute(
        """SELECT * FROM agent_delegations
           WHERE parent_agent_ref = $1
           ORDER BY requested_at DESC
           LIMIT $2""",
        query.agent_principal_ref,
        int(query.history_limit),
    )
    gaps = conn.execute(
        """SELECT * FROM agent_tool_gaps
           WHERE reporter_agent_ref = $1
           ORDER BY created_at DESC
           LIMIT $2""",
        query.agent_principal_ref,
        int(query.history_limit),
    )

    return {
        "ok": True,
        "operation": "agent_principal.describe",
        "principal": principal,
        "recent_wakes": [_wake_row(dict(row)) for row in (wakes or [])],
        "recent_delegations": [_delegation_row(dict(row)) for row in (delegations or [])],
        "recent_tool_gaps": [_gap_row(dict(row)) for row in (gaps or [])],
    }


def handle_list_agent_wakes(
    query: ListAgentWakesQuery,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    where_parts: list[str] = []
    args: list[Any] = []

    def _add(filter_sql: str, value: Any) -> None:
        args.append(value)
        where_parts.append(filter_sql.replace("?", f"${len(args)}"))

    if query.agent_principal_ref:
        _add("agent_principal_ref = ?", query.agent_principal_ref)
    if query.trigger_kind:
        _add("trigger_kind = ?", query.trigger_kind)
    if query.status:
        _add("status = ?", query.status)

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    args.append(int(query.limit))
    sql = (
        f"SELECT * FROM agent_wakes{where_sql} "
        f"ORDER BY received_at DESC LIMIT ${len(args)}"
    )
    rows = conn.execute(sql, *args)
    wakes = [_wake_row(dict(row)) for row in (rows or [])]
    return {
        "ok": True,
        "operation": "agent_wake.list",
        "wakes": wakes,
        "count": len(wakes),
    }


def handle_list_agent_tool_gaps(
    query: ListAgentToolGapsQuery,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    where_parts: list[str] = []
    args: list[Any] = []

    def _add(filter_sql: str, value: Any) -> None:
        args.append(value)
        where_parts.append(filter_sql.replace("?", f"${len(args)}"))

    if query.reporter_agent_ref:
        _add("reporter_agent_ref = ?", query.reporter_agent_ref)
    if query.severity:
        _add("severity = ?", query.severity)
    if query.status:
        _add("status = ?", query.status)
    if query.missing_capability:
        _add("missing_capability = ?", query.missing_capability)

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    args.append(int(query.limit))
    sql = (
        f"SELECT * FROM agent_tool_gaps{where_sql} "
        f"ORDER BY created_at DESC LIMIT ${len(args)}"
    )
    rows = conn.execute(sql, *args)
    gaps = [_gap_row(dict(row)) for row in (rows or [])]
    return {
        "ok": True,
        "operation": "agent_tool_gap.list",
        "gaps": gaps,
        "count": len(gaps),
    }


__all__ = [
    "DescribeAgentPrincipalQuery",
    "ListAgentPrincipalsQuery",
    "ListAgentToolGapsQuery",
    "ListAgentWakesQuery",
    "handle_describe_agent_principal",
    "handle_list_agent_principals",
    "handle_list_agent_tool_gaps",
    "handle_list_agent_wakes",
]
