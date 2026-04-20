"""Sync Postgres repository for data dictionary quality rules + runs.

Rules on (object_kind, field_path, rule_kind) with three layered sources
(auto / inferred / operator) and an append-mostly runs table that records
each evaluation outcome.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from .validators import PostgresWriteError, _require_text

_VALID_SOURCES = frozenset({"auto", "inferred", "operator"})
_VALID_SEVERITIES = frozenset({"info", "warning", "error", "critical"})
_VALID_STATUSES = frozenset({"pass", "fail", "error"})


def _encode_jsonb(value: Any, *, default: str = "{}") -> str:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            json.loads(stripped)
        except (TypeError, ValueError):
            return default
        return stripped
    return default


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


# --- rules ---------------------------------------------------------------


def upsert_rule(
    conn: Any,
    *,
    object_kind: str,
    rule_kind: str,
    source: str,
    field_path: str = "",
    expression: Any = None,
    severity: str = "warning",
    description: str = "",
    enabled: bool = True,
    confidence: float = 1.0,
    origin_ref: Any = None,
    metadata: Any = None,
) -> dict[str, Any]:
    kind = _require_text(object_kind, field_name="object_kind")
    rk = _require_text(rule_kind, field_name="rule_kind")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_quality_rules.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    if severity not in _VALID_SEVERITIES:
        raise PostgresWriteError(
            "data_dictionary_quality_rules.invalid_submission",
            f"severity must be one of {sorted(_VALID_SEVERITIES)}",
            details={"field": "severity", "value": severity},
        )
    conf = max(0.0, min(1.0, float(confidence)))
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_quality_rules (
            object_kind, field_path, rule_kind, source,
            expression, severity, description, enabled,
            confidence, origin_ref, metadata
        ) VALUES (
            $1, $2, $3, $4,
            $5::jsonb, $6, $7, $8,
            $9, $10::jsonb, $11::jsonb
        )
        ON CONFLICT (object_kind, field_path, rule_kind, source) DO UPDATE
           SET expression = EXCLUDED.expression,
               severity = EXCLUDED.severity,
               description = EXCLUDED.description,
               enabled = EXCLUDED.enabled,
               confidence = EXCLUDED.confidence,
               origin_ref = EXCLUDED.origin_ref,
               metadata = EXCLUDED.metadata
        RETURNING *
        """,
        kind, field_path or "", rk, source,
        _encode_jsonb(expression), severity, description or "", bool(enabled),
        conf, _encode_jsonb(origin_ref), _encode_jsonb(metadata),
    )
    return _row(row)


def replace_projected_rules(
    conn: Any,
    *,
    source: str,
    projector_tag: str,
    rules: Iterable[dict[str, Any]],
) -> int:
    """Idempotently replace rules written by a single projector."""
    if source == "operator":
        raise PostgresWriteError(
            "data_dictionary_quality_rules.invalid_submission",
            "replace_projected_rules refuses to bulk-replace operator rows",
            details={"field": "source"},
        )
    tag = _require_text(projector_tag, field_name="projector_tag")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_quality_rules.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    rules_list = list(rules)
    keep_keys = [
        (
            str(r.get("object_kind") or ""),
            str(r.get("field_path") or ""),
            str(r.get("rule_kind") or ""),
        )
        for r in rules_list
    ]
    obj_kinds = [k[0] for k in keep_keys]
    field_paths = [k[1] for k in keep_keys]
    rule_kinds = [k[2] for k in keep_keys]
    if keep_keys:
        conn.execute(
            """
            DELETE FROM data_dictionary_quality_rules
             WHERE source = $1
               AND origin_ref ->> 'projector' = $2
               AND NOT EXISTS (
                   SELECT 1 FROM unnest($3::text[], $4::text[], $5::text[])
                     AS keep(ok, fp, rk)
                   WHERE keep.ok = data_dictionary_quality_rules.object_kind
                     AND keep.fp = data_dictionary_quality_rules.field_path
                     AND keep.rk = data_dictionary_quality_rules.rule_kind
               )
            """,
            source, tag, obj_kinds, field_paths, rule_kinds,
        )
    else:
        conn.execute(
            """
            DELETE FROM data_dictionary_quality_rules
             WHERE source = $1 AND origin_ref ->> 'projector' = $2
            """,
            source, tag,
        )
    written = 0
    for rule in rules_list:
        upsert_rule(
            conn,
            source=source,
            object_kind=str(rule.get("object_kind") or ""),
            field_path=str(rule.get("field_path") or ""),
            rule_kind=str(rule.get("rule_kind") or ""),
            expression=rule.get("expression") or {},
            severity=str(rule.get("severity") or "warning"),
            description=str(rule.get("description") or ""),
            enabled=bool(rule.get("enabled", True)),
            confidence=float(rule.get("confidence", 1.0)),
            origin_ref=rule.get("origin_ref") or {"projector": tag},
            metadata=rule.get("metadata") or {},
        )
        written += 1
    return written


def delete_rule(
    conn: Any,
    *,
    object_kind: str,
    rule_kind: str,
    source: str,
    field_path: str = "",
) -> bool:
    kind = _require_text(object_kind, field_name="object_kind")
    rk = _require_text(rule_kind, field_name="rule_kind")
    if source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_quality_rules.invalid_submission",
            f"source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "source", "value": source},
        )
    row = conn.fetchrow(
        """
        DELETE FROM data_dictionary_quality_rules
         WHERE object_kind = $1 AND field_path = $2
           AND rule_kind = $3 AND source = $4
         RETURNING object_kind
        """,
        kind, field_path or "", rk, source,
    )
    return row is not None


def list_effective_rules(
    conn: Any,
    *,
    object_kind: str | None = None,
    field_path: str | None = None,
) -> list[dict[str, Any]]:
    """Effective rules for dashboards / evaluation."""
    sql = "SELECT * FROM data_dictionary_quality_rules_effective"
    clauses: list[str] = []
    params: list[Any] = []
    if object_kind:
        params.append(object_kind)
        clauses.append(f"object_kind = ${len(params)}")
    if field_path is not None:
        params.append(field_path)
        clauses.append(f"field_path = ${len(params)}")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY object_kind, field_path, rule_kind"
    rows = conn.execute(sql, *params)
    return [dict(r) for r in rows or []]


def list_rule_layers(
    conn: Any,
    *,
    object_kind: str,
    field_path: str | None = None,
) -> list[dict[str, Any]]:
    """Raw per-source rows (auto/inferred/operator)."""
    kind = _require_text(object_kind, field_name="object_kind")
    if field_path is None:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_quality_rules "
            "WHERE object_kind = $1 "
            "ORDER BY field_path, rule_kind, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind,
        )
    else:
        rows = conn.execute(
            "SELECT * FROM data_dictionary_quality_rules "
            "WHERE object_kind = $1 AND field_path = $2 "
            "ORDER BY rule_kind, "
            "CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 "
            "WHEN 'auto' THEN 2 ELSE 3 END",
            kind, field_path,
        )
    return [dict(r) for r in rows or []]


def count_rules_by_source(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT source, COUNT(*) AS n "
        "FROM data_dictionary_quality_rules GROUP BY source"
    )
    return {str(r["source"]): int(r["n"]) for r in rows or []}


# --- runs ----------------------------------------------------------------


def insert_run(
    conn: Any,
    *,
    object_kind: str,
    rule_kind: str,
    effective_source: str,
    status: str,
    field_path: str = "",
    observed: Any = None,
    duration_ms: float = 0.0,
    error_message: str = "",
    finished_at: Any = None,
) -> dict[str, Any]:
    kind = _require_text(object_kind, field_name="object_kind")
    rk = _require_text(rule_kind, field_name="rule_kind")
    if status not in _VALID_STATUSES:
        raise PostgresWriteError(
            "data_dictionary_quality_runs.invalid_submission",
            f"status must be one of {sorted(_VALID_STATUSES)}",
            details={"field": "status", "value": status},
        )
    if effective_source not in _VALID_SOURCES:
        raise PostgresWriteError(
            "data_dictionary_quality_runs.invalid_submission",
            f"effective_source must be one of {sorted(_VALID_SOURCES)}",
            details={"field": "effective_source", "value": effective_source},
        )
    row = conn.fetchrow(
        """
        INSERT INTO data_dictionary_quality_runs (
            object_kind, field_path, rule_kind, effective_source,
            status, observed, duration_ms, error_message, finished_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7, $8,
            COALESCE($9::timestamptz, now())
        )
        RETURNING *
        """,
        kind, field_path or "", rk, effective_source, status,
        _encode_jsonb(observed), float(duration_ms),
        error_message or "", finished_at,
    )
    return _row(row)


def list_latest_runs(
    conn: Any,
    *,
    object_kind: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM data_dictionary_quality_latest_runs"
    clauses: list[str] = []
    params: list[Any] = []
    if object_kind:
        params.append(object_kind)
        clauses.append(f"object_kind = ${len(params)}")
    if status:
        if status not in _VALID_STATUSES:
            raise PostgresWriteError(
                "data_dictionary_quality_runs.invalid_submission",
                f"status must be one of {sorted(_VALID_STATUSES)}",
                details={"field": "status", "value": status},
            )
        params.append(status)
        clauses.append(f"status = ${len(params)}")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    params.append(max(1, min(1000, int(limit))))
    sql += f" ORDER BY started_at DESC LIMIT ${len(params)}"
    rows = conn.execute(sql, *params)
    return [dict(r) for r in rows or []]


def list_run_history(
    conn: Any,
    *,
    object_kind: str,
    field_path: str,
    rule_kind: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    kind = _require_text(object_kind, field_name="object_kind")
    rk = _require_text(rule_kind, field_name="rule_kind")
    rows = conn.execute(
        """
        SELECT * FROM data_dictionary_quality_runs
         WHERE object_kind = $1 AND field_path = $2 AND rule_kind = $3
         ORDER BY started_at DESC
         LIMIT $4
        """,
        kind, field_path or "", rk, max(1, min(1000, int(limit))),
    )
    return [dict(r) for r in rows or []]


def count_runs_by_status(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n "
        "FROM data_dictionary_quality_latest_runs GROUP BY status"
    )
    return {str(r["status"]): int(r["n"]) for r in rows or []}


__all__ = [
    "upsert_rule",
    "replace_projected_rules",
    "delete_rule",
    "list_effective_rules",
    "list_rule_layers",
    "count_rules_by_source",
    "insert_run",
    "list_latest_runs",
    "list_run_history",
    "count_runs_by_status",
]
