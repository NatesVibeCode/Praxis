"""CQRS query handler for the ``audit.summary`` aggregate lens.

The two ledger tables ``authority_operation_receipts`` and
``authority_compliance_receipts`` already expose row-level lenses via
``search.authority_receipts`` and ``search.compliance_receipts``. This
module ships the *summary* lens — one query that bundles "what
happened in the last N hours" so an operator can ask "are receipts
healthy?" without fanning out across multiple search calls.

Registered through ``praxis_register_operation`` (see
``Skills/praxis-discover`` / ``praxis_operation_forge`` flow).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


_DEFAULT_SINCE_HOURS = 24
_MAX_SINCE_HOURS = 24 * 30  # one month


class AuditSummaryQuery(BaseModel):
    """Inputs for ``audit.summary``."""

    model_config = ConfigDict(extra="forbid")

    since_hours: int = Field(default=_DEFAULT_SINCE_HOURS, ge=1, le=_MAX_SINCE_HOURS)

    @field_validator("since_hours", mode="before")
    @classmethod
    def _validate_since_hours(cls, value: object) -> int:
        if value is None:
            return _DEFAULT_SINCE_HOURS
        try:
            candidate = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("since_hours must be an integer") from exc
        if candidate < 1:
            raise ValueError("since_hours must be >= 1")
        if candidate > _MAX_SINCE_HOURS:
            raise ValueError(f"since_hours must be <= {_MAX_SINCE_HOURS}")
        return candidate


def _bucket_rows(
    rows: list[dict[str, Any]], *, key: str, default: str
) -> list[dict[str, Any]]:
    bucketed: list[dict[str, Any]] = []
    for row in rows:
        bucket_value = row.get(key)
        bucket_label = str(bucket_value) if bucket_value not in (None, "") else default
        bucketed.append(
            {
                key: bucket_label,
                "count": int(row.get("count") or 0),
            }
        )
    bucketed.sort(key=lambda item: -item["count"])
    return bucketed


def _operation_receipts_aggregates(
    conn: Any, *, since: datetime
) -> dict[str, Any]:
    by_transport_rows = conn.execute(
        """
        SELECT transport_kind, COUNT(*) AS count
          FROM authority_operation_receipts
         WHERE created_at >= $1
         GROUP BY transport_kind
        """,
        since,
    )
    by_status_rows = conn.execute(
        """
        SELECT execution_status, COUNT(*) AS count
          FROM authority_operation_receipts
         WHERE created_at >= $1
         GROUP BY execution_status
        """,
        since,
    )
    by_kind_rows = conn.execute(
        """
        SELECT operation_kind, COUNT(*) AS count
          FROM authority_operation_receipts
         WHERE created_at >= $1
         GROUP BY operation_kind
        """,
        since,
    )
    top_operations = conn.execute(
        """
        SELECT operation_name,
               COUNT(*) AS count,
               COUNT(*) FILTER (WHERE execution_status = 'failed') AS failures
          FROM authority_operation_receipts
         WHERE created_at >= $1
         GROUP BY operation_name
         ORDER BY COUNT(*) DESC
         LIMIT 10
        """,
        since,
    )
    totals_row = conn.fetchrow(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE execution_status = 'completed') AS completed,
            COUNT(*) FILTER (WHERE execution_status = 'replayed') AS replayed,
            COUNT(*) FILTER (WHERE execution_status = 'failed') AS failed,
            COUNT(*) FILTER (WHERE transport_kind IS NULL) AS untagged_transport
          FROM authority_operation_receipts
         WHERE created_at >= $1
        """,
        since,
    )
    totals = dict(totals_row) if totals_row else {}
    return {
        "totals": {
            "receipts": int(totals.get("total") or 0),
            "completed": int(totals.get("completed") or 0),
            "replayed": int(totals.get("replayed") or 0),
            "failed": int(totals.get("failed") or 0),
            "untagged_transport": int(totals.get("untagged_transport") or 0),
        },
        "by_transport": _bucket_rows(
            [dict(r) for r in by_transport_rows or ()],
            key="transport_kind",
            default="unknown",
        ),
        "by_execution_status": _bucket_rows(
            [dict(r) for r in by_status_rows or ()],
            key="execution_status",
            default="unknown",
        ),
        "by_operation_kind": _bucket_rows(
            [dict(r) for r in by_kind_rows or ()],
            key="operation_kind",
            default="unknown",
        ),
        "top_operations": [
            {
                "operation_name": str(row.get("operation_name") or ""),
                "count": int(row.get("count") or 0),
                "failures": int(row.get("failures") or 0),
            }
            for row in (top_operations or ())
        ],
    }


def _compliance_aggregates(conn: Any, *, since: datetime) -> dict[str, Any]:
    by_outcome_rows = conn.execute(
        """
        SELECT outcome, COUNT(*) AS count
          FROM authority_compliance_receipts
         WHERE created_at >= $1
         GROUP BY outcome
        """,
        since,
    )
    by_table_rows = conn.execute(
        """
        SELECT target_table,
               COUNT(*) AS count,
               COUNT(*) FILTER (WHERE outcome = 'reject') AS rejects
          FROM authority_compliance_receipts
         WHERE created_at >= $1
         GROUP BY target_table
         ORDER BY COUNT(*) DESC
         LIMIT 10
        """,
        since,
    )
    by_policy_rows = conn.execute(
        """
        SELECT policy_id,
               COUNT(*) AS count,
               COUNT(*) FILTER (WHERE outcome = 'reject') AS rejects
          FROM authority_compliance_receipts
         WHERE created_at >= $1
         GROUP BY policy_id
         ORDER BY COUNT(*) DESC
         LIMIT 10
        """,
        since,
    )
    totals_row = conn.fetchrow(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE outcome = 'admit') AS admits,
            COUNT(*) FILTER (WHERE outcome = 'reject') AS rejects
          FROM authority_compliance_receipts
         WHERE created_at >= $1
        """,
        since,
    )
    totals = dict(totals_row) if totals_row else {}
    return {
        "totals": {
            "compliance_rows": int(totals.get("total") or 0),
            "admits": int(totals.get("admits") or 0),
            "rejects": int(totals.get("rejects") or 0),
        },
        "by_outcome": _bucket_rows(
            [dict(r) for r in by_outcome_rows or ()],
            key="outcome",
            default="unknown",
        ),
        "top_target_tables": [
            {
                "target_table": str(row.get("target_table") or ""),
                "count": int(row.get("count") or 0),
                "rejects": int(row.get("rejects") or 0),
            }
            for row in (by_table_rows or ())
        ],
        "top_policies": [
            {
                "policy_id": str(row.get("policy_id") or ""),
                "count": int(row.get("count") or 0),
                "rejects": int(row.get("rejects") or 0),
            }
            for row in (by_policy_rows or ())
        ],
    }


def handle_audit_summary(query: AuditSummaryQuery, subsystems: Any) -> dict[str, Any]:
    """Return audit aggregates for the trailing window.

    Two source tables get summarised in one call:
      - ``authority_operation_receipts`` (gateway dispatches)
      - ``authority_compliance_receipts`` (policy enforcement)

    Reads from the same Postgres connection used by every other
    gateway-dispatched query handler. Errors are surfaced as a normal
    handler exception so the gateway records a ``failed`` receipt with
    the underlying error_code/error_detail.
    """

    conn = subsystems.get_pg_conn()
    since = datetime.now(timezone.utc) - timedelta(hours=query.since_hours)
    return {
        "ok": True,
        "status": "complete",
        "since_hours": query.since_hours,
        "since": since.isoformat(),
        "operation_receipts": _operation_receipts_aggregates(conn, since=since),
        "compliance_receipts": _compliance_aggregates(conn, since=since),
    }


__all__ = [
    "AuditSummaryQuery",
    "handle_audit_summary",
]
