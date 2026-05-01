"""Allowlist-only DB read source plugin.

Replaces ad-hoc ``psql -c "SELECT ..."`` fallbacks with a structured,
typed query that compiles to a parameterized SQL statement. **Never** a
SQL passthrough — only columns and tables from the allowlist may
appear, and ``where`` predicates are restricted to ``=``, ``ILIKE``,
``IN``, and ``IS NULL`` against allowlisted columns.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from surfaces.mcp.tools._search_envelope import SOURCE_DB, SearchEnvelope


_ALLOWLIST: dict[str, dict[str, Any]] = {
    "workflow_runs": {
        "select": (
            "run_id",
            "workflow_id",
            "current_state",
            "terminal_reason_code",
            "requested_at",
            "admitted_at",
            "started_at",
            "finished_at",
        ),
        "where": ("run_id", "workflow_id", "current_state", "terminal_reason_code"),
    },
    "bugs": {
        "select": (
            "bug_id",
            "bug_key",
            "title",
            "status",
            "severity",
            "category",
            "priority",
            "opened_at",
            "resolved_at",
        ),
        "where": ("bug_id", "bug_key", "status", "severity", "category", "priority"),
    },
    "operator_decisions": {
        "select": (
            "operator_decision_id",
            "decision_key",
            "decision_kind",
            "decision_status",
            "title",
            "decision_scope_kind",
            "decision_scope_ref",
            "decided_at",
        ),
        "where": (
            "decision_kind",
            "decision_status",
            "decision_scope_kind",
            "decision_scope_ref",
        ),
    },
    "module_embeddings": {
        "select": ("module_id", "module_path", "kind", "name", "indexed_at"),
        "where": ("kind", "module_path"),
    },
    "action_fingerprints": {
        "select": (
            "fingerprint_id",
            "source_surface",
            "action_kind",
            "operation_name",
            "normalized_command",
            "path_shape",
            "shape_hash",
            "session_ref",
            "receipt_id",
            "ts",
        ),
        "where": (
            "source_surface",
            "action_kind",
            "operation_name",
            "shape_hash",
            "session_ref",
        ),
    },
    "tool_opportunities_pending": {
        "select": (
            "shape_hash",
            "proposed_decision_key",
            "occurrence_count",
            "distinct_surfaces",
            "distinct_sessions",
            "action_kinds",
            "surfaces",
            "operation_names",
            "sample_commands",
            "sample_path_shapes",
            "first_seen",
            "last_seen",
        ),
        "where": ("shape_hash",),
    },
    "gateway_op_recurrence": {
        "select": (
            "operation_name",
            "occurrence_count",
            "distinct_surfaces",
            "distinct_sessions",
            "surfaces",
            "first_seen",
            "last_seen",
        ),
        "where": ("operation_name",),
    },
}


class DbReadSourceError(RuntimeError):
    """Raised when a db_read query is malformed or hits the allowlist."""


def _normalize_columns(
    requested: Sequence[str] | None, allowed: tuple[str, ...]
) -> tuple[str, ...]:
    if not requested:
        return allowed
    cleaned: list[str] = []
    for column in requested:
        column_str = str(column).strip()
        if column_str not in allowed:
            raise DbReadSourceError(
                f"column '{column_str}' not in allowlist for table"
            )
        cleaned.append(column_str)
    return tuple(cleaned) or allowed


def _build_where(
    where: Mapping[str, Any] | None,
    allowed: tuple[str, ...],
) -> tuple[str, list[Any]]:
    if not where:
        return "", []
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    for column, value in where.items():
        if column not in allowed:
            raise DbReadSourceError(
                f"where column '{column}' not in allowlist"
            )
        if isinstance(value, list):
            placeholders = ", ".join(f"${idx + i}" for i in range(len(value)))
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(value)
            idx += len(value)
        elif value is None:
            clauses.append(f"{column} IS NULL")
        elif isinstance(value, str) and value.startswith("ILIKE:"):
            clauses.append(f"{column} ILIKE ${idx}")
            params.append(value[len("ILIKE:") :])
            idx += 1
        else:
            clauses.append(f"{column} = ${idx}")
            params.append(value)
            idx += 1
    return " WHERE " + " AND ".join(clauses), params


def _row_to_payload(row: Any) -> dict[str, Any]:
    if hasattr(row, "items"):
        return {key: value for key, value in row.items()}
    if isinstance(row, dict):
        return dict(row)
    return {"value": str(row)}


def search_db(
    *,
    envelope: SearchEnvelope,
    subsystems: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run an allowlisted SELECT against Praxis.db."""

    extras = envelope.scope.extras or {}
    table = str(extras.get("table") or "").strip()
    if not table:
        return [], {
            "status": "skipped",
            "reason": "scope.extras.table is required for db source",
        }
    if table not in _ALLOWLIST:
        return [], {
            "status": "error",
            "error": f"table '{table}' not in db read allowlist",
        }
    spec = _ALLOWLIST[table]
    try:
        select_columns = _normalize_columns(extras.get("select"), spec["select"])
        where_sql, params = _build_where(extras.get("where"), spec["where"])
    except DbReadSourceError as exc:
        return [], {"status": "error", "error": str(exc)}

    sql = (
        f"SELECT {', '.join(select_columns)} FROM {table}"
        f"{where_sql}"
        f" LIMIT {int(envelope.limit)}"
    )

    try:
        conn = subsystems.get_pg_conn()
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    try:
        rows = conn.execute(sql, *params)
    except Exception as exc:
        return [], {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    results = [
        {
            "source": SOURCE_DB,
            "table": table,
            "score": 1.0,
            "found_via": "db_read",
            "row": _row_to_payload(row),
        }
        for row in (rows or [])
    ]
    return results, {
        "status": "complete",
        "table": table,
        "rows_considered": len(results),
        "select": list(select_columns),
    }


__all__ = ["DbReadSourceError", "search_db"]
