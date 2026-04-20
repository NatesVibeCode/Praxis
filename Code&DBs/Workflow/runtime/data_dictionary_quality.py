"""Runtime authority for data dictionary quality rules + runs.

Three layers coexist in `data_dictionary_quality_rules`:
- `auto`     — projector-derived (NOT NULL from pg_attribute, UNIQUE from
               pg_index, FK referential checks).
- `inferred` — sampler-derived (observed enum candidates, range bounds).
- `operator` — hand-authored, highest precedence.

The runtime layer composes the storage repository. The evaluator
(`evaluate_rule`) is a small dispatch over rule_kind that runs a Postgres
query and records a `data_dictionary_quality_runs` row. `evaluate_all` walks
every enabled effective rule and evaluates each in sequence — wired into the
heartbeat so dashboards stay current.

Rule kinds & expressions:
    not_null            — expression: {}                        SELECT count WHERE field IS NULL
    unique              — expression: {}                        SELECT count of duplicate groups
    regex_match         — expression: {"regex": "^\\S+@\\S+$"}   count where NOT ~ regex
    enum                — expression: {"values": ["a","b"]}     count where field NOT IN values
    range               — expression: {"min": 0, "max": 100}    count out-of-range
    row_count_min       — expression: {"min": 1}                fail if count < min
    row_count_max       — expression: {"max": 1e9}              fail if count > max
    referential         — expression: {"references":
                                         {"table":"t","column":"c"}}
                          count where field has no matching parent row
    custom_sql          — expression: {"sql": "SELECT count(*) ..."}
                          row count = failing-row count; pass iff zero
"""

from __future__ import annotations

import re
import time
from typing import Any, Iterable

from storage.postgres.data_dictionary_quality_repository import (
    count_rules_by_source,
    count_runs_by_status,
    delete_rule,
    insert_run,
    list_effective_rules,
    list_latest_runs,
    list_rule_layers,
    list_run_history,
    replace_projected_rules,
    upsert_rule,
)
from storage.postgres.data_dictionary_repository import get_object
from storage.postgres.validators import PostgresWriteError

_ALLOWED_RULE_KINDS = frozenset({
    "not_null", "unique", "regex_match", "enum",
    "range", "row_count_min", "row_count_max",
    "referential", "custom_sql",
    # Rule kinds emitted by projector pipelines — not directly evaluated by
    # the SQL runner, but surfaced in the governance/compliance axis.
    "policy_compliance", "owner_present",
})

_ALLOWED_SEVERITIES = frozenset({"info", "warning", "error", "critical"})


class DataDictionaryQualityError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _raise_storage(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise DataDictionaryQualityError(str(exc), status_code=status_code) from exc


def _ensure_object_known(conn: Any, object_kind: str) -> None:
    row = get_object(conn, object_kind=object_kind)
    if row is None:
        raise DataDictionaryQualityError(
            f"object_kind {object_kind!r} is not registered in the data dictionary",
            status_code=404,
        )


# --- projector-facing API -------------------------------------------------


def apply_projected_rules(
    conn: Any,
    *,
    projector_tag: str,
    rules: Iterable[dict[str, Any]],
    source: str = "auto",
) -> dict[str, Any]:
    tag = _text(projector_tag)
    if not tag:
        raise DataDictionaryQualityError("projector_tag is required")
    if source not in ("auto", "inferred"):
        raise DataDictionaryQualityError(
            "apply_projected_rules only writes auto/inferred layers"
        )

    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(rules or []):
        if not isinstance(raw, dict):
            raise DataDictionaryQualityError(f"rules[{index}] must be an object")
        kind = _text(raw.get("object_kind"))
        rk = _text(raw.get("rule_kind"))
        if not kind or not rk:
            raise DataDictionaryQualityError(
                f"rules[{index}] requires object_kind and rule_kind"
            )
        if rk not in _ALLOWED_RULE_KINDS:
            raise DataDictionaryQualityError(
                f"rules[{index}].rule_kind={rk!r} not in {sorted(_ALLOWED_RULE_KINDS)}"
            )
        severity = _text(raw.get("severity")) or "warning"
        if severity not in _ALLOWED_SEVERITIES:
            raise DataDictionaryQualityError(
                f"rules[{index}].severity={severity!r} not in {sorted(_ALLOWED_SEVERITIES)}"
            )
        origin_ref = dict(raw.get("origin_ref") or {})
        origin_ref.setdefault("projector", tag)
        normalized.append({
            "object_kind": kind,
            "field_path": _text(raw.get("field_path")),
            "rule_kind": rk,
            "expression": raw.get("expression") or {},
            "severity": severity,
            "description": _text(raw.get("description")),
            "enabled": bool(raw.get("enabled", True)),
            "confidence": float(raw.get("confidence", 1.0)),
            "origin_ref": origin_ref,
            "metadata": raw.get("metadata") or {},
        })

    try:
        written = replace_projected_rules(
            conn, source=source, projector_tag=tag, rules=normalized,
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"projector": tag, "source": source, "rules_written": written}


# --- operator-facing API --------------------------------------------------


def set_operator_rule(
    conn: Any,
    *,
    object_kind: str,
    rule_kind: str,
    field_path: str = "",
    expression: Any = None,
    severity: str = "warning",
    description: str = "",
    enabled: bool = True,
    metadata: Any = None,
) -> dict[str, Any]:
    kind = _text(object_kind)
    rk = _text(rule_kind)
    if not kind or not rk:
        raise DataDictionaryQualityError(
            "object_kind and rule_kind are required"
        )
    if rk not in _ALLOWED_RULE_KINDS:
        raise DataDictionaryQualityError(
            f"rule_kind must be one of: {', '.join(sorted(_ALLOWED_RULE_KINDS))}"
        )
    if severity not in _ALLOWED_SEVERITIES:
        raise DataDictionaryQualityError(
            f"severity must be one of: {', '.join(sorted(_ALLOWED_SEVERITIES))}"
        )
    _ensure_object_known(conn, kind)
    try:
        row = upsert_rule(
            conn,
            object_kind=kind,
            field_path=_text(field_path),
            rule_kind=rk,
            source="operator",
            expression=expression or {},
            severity=severity,
            description=_text(description),
            enabled=enabled,
            confidence=1.0,
            origin_ref={"source": "operator"},
            metadata=metadata or {},
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {"rule": dict(row)}


def clear_operator_rule(
    conn: Any,
    *,
    object_kind: str,
    rule_kind: str,
    field_path: str = "",
) -> dict[str, Any]:
    kind = _text(object_kind)
    rk = _text(rule_kind)
    if not kind or not rk:
        raise DataDictionaryQualityError(
            "object_kind and rule_kind are required"
        )
    try:
        removed = delete_rule(
            conn,
            object_kind=kind,
            field_path=_text(field_path),
            rule_kind=rk,
            source="operator",
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return {
        "object_kind": kind, "field_path": _text(field_path),
        "rule_kind": rk, "removed": removed,
    }


# --- read API -------------------------------------------------------------


def describe_rules(
    conn: Any,
    *,
    object_kind: str | None = None,
    field_path: str | None = None,
    include_layers: bool = False,
) -> dict[str, Any]:
    ok = _text(object_kind) or None
    fp = field_path if field_path is None else _text(field_path)
    effective = list_effective_rules(conn, object_kind=ok, field_path=fp)
    response: dict[str, Any] = {
        "object_kind": ok, "field_path": fp,
        "effective": effective,
    }
    if include_layers and ok:
        response["layers"] = list_rule_layers(
            conn, object_kind=ok, field_path=fp,
        )
    return response


def quality_summary(conn: Any) -> dict[str, Any]:
    return {
        "rules_by_source": count_rules_by_source(conn),
        "latest_runs_by_status": count_runs_by_status(conn),
    }


def run_history(
    conn: Any,
    *,
    object_kind: str,
    rule_kind: str,
    field_path: str = "",
    limit: int = 50,
) -> dict[str, Any]:
    kind = _text(object_kind)
    rk = _text(rule_kind)
    if not kind or not rk:
        raise DataDictionaryQualityError(
            "object_kind and rule_kind are required"
        )
    rows = list_run_history(
        conn, object_kind=kind, field_path=_text(field_path),
        rule_kind=rk, limit=limit,
    )
    return {
        "object_kind": kind, "field_path": _text(field_path),
        "rule_kind": rk, "runs": rows,
    }


def latest_runs(
    conn: Any,
    *,
    object_kind: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    rows = list_latest_runs(
        conn,
        object_kind=_text(object_kind) or None,
        status=_text(status) or None,
        limit=limit,
    )
    return {"runs": rows}


# --- evaluator ------------------------------------------------------------


# Only object_kind of category `table:` can be evaluated (for now). The
# rule's object_kind is in the form `table:<name>`. `field_path` maps to a
# SQL column. We use psycopg / asyncpg quoting via the repo's conn.execute,
# but for identifiers we need to quote ourselves — validate against a safe
# pattern to avoid injection.

_SAFE_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _quote_ident(name: str) -> str:
    if not _SAFE_IDENT.match(name or ""):
        raise DataDictionaryQualityError(
            f"identifier {name!r} is not a safe unquoted Postgres identifier",
            status_code=400,
        )
    return f'"{name}"'


def _table_from_object_kind(object_kind: str) -> str:
    if ":" not in object_kind:
        raise DataDictionaryQualityError(
            f"evaluator requires object_kind of form 'table:<name>', got {object_kind!r}",
        )
    category, _, name = object_kind.partition(":")
    if category != "table":
        raise DataDictionaryQualityError(
            f"evaluator only supports category=table, got {category!r}",
        )
    return _quote_ident(name)


def _scalar(conn: Any, sql: str, *args: Any) -> int:
    row = conn.fetchrow(sql, *args)
    if row is None:
        return 0
    # The row is a mapping — pick the first value regardless of its name.
    first = next(iter(dict(row).values()))
    try:
        return int(first or 0)
    except (TypeError, ValueError):
        return 0


def evaluate_rule(conn: Any, rule: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a single effective-rule row and record a run.

    `rule` is a dict shaped like a row from
    `data_dictionary_quality_rules_effective`.
    Returns the inserted run record.
    """
    object_kind = str(rule.get("object_kind") or "")
    field_path = str(rule.get("field_path") or "")
    rule_kind = str(rule.get("rule_kind") or "")
    effective_source = str(rule.get("effective_source") or "auto")
    expression = rule.get("expression") or {}
    if isinstance(expression, str):
        # jsonb comes back as a dict normally, but guard for defensive use.
        import json as _json
        try:
            expression = _json.loads(expression)
        except Exception:
            expression = {}

    t0 = time.monotonic()
    status: str = "pass"
    observed: dict[str, Any] = {}
    error_message = ""

    try:
        table_ident = _table_from_object_kind(object_kind)
        field_ident = _quote_ident(field_path) if field_path else None

        if rule_kind == "not_null":
            if field_ident is None:
                raise DataDictionaryQualityError("not_null requires field_path")
            failing = _scalar(
                conn,
                f"SELECT count(*) AS n FROM {table_ident} WHERE {field_ident} IS NULL",
            )
            observed = {"failing_rows": failing}
            status = "pass" if failing == 0 else "fail"

        elif rule_kind == "unique":
            if field_ident is None:
                raise DataDictionaryQualityError("unique requires field_path")
            failing = _scalar(
                conn,
                f"SELECT coalesce(sum(cnt),0) AS n FROM ("
                f"  SELECT count(*) AS cnt FROM {table_ident} "
                f"   WHERE {field_ident} IS NOT NULL "
                f"   GROUP BY {field_ident} HAVING count(*) > 1"
                f") t",
            )
            observed = {"duplicate_rows": failing}
            status = "pass" if failing == 0 else "fail"

        elif rule_kind == "regex_match":
            if field_ident is None:
                raise DataDictionaryQualityError("regex_match requires field_path")
            pattern = expression.get("regex") if isinstance(expression, dict) else None
            if not isinstance(pattern, str) or not pattern:
                raise DataDictionaryQualityError(
                    "regex_match requires expression.regex (non-empty string)"
                )
            failing = _scalar(
                conn,
                f"SELECT count(*) AS n FROM {table_ident} "
                f" WHERE {field_ident} IS NOT NULL AND {field_ident}::text !~ $1",
                pattern,
            )
            observed = {"failing_rows": failing, "regex": pattern}
            status = "pass" if failing == 0 else "fail"

        elif rule_kind == "enum":
            if field_ident is None:
                raise DataDictionaryQualityError("enum requires field_path")
            values = expression.get("values") if isinstance(expression, dict) else None
            if not isinstance(values, list) or not values:
                raise DataDictionaryQualityError(
                    "enum requires expression.values (non-empty list)"
                )
            string_values = [str(v) for v in values]
            failing = _scalar(
                conn,
                f"SELECT count(*) AS n FROM {table_ident} "
                f" WHERE {field_ident} IS NOT NULL "
                f"   AND NOT ({field_ident}::text = ANY($1::text[]))",
                string_values,
            )
            observed = {"failing_rows": failing, "allowed": string_values}
            status = "pass" if failing == 0 else "fail"

        elif rule_kind == "range":
            if field_ident is None:
                raise DataDictionaryQualityError("range requires field_path")
            mn = expression.get("min") if isinstance(expression, dict) else None
            mx = expression.get("max") if isinstance(expression, dict) else None
            if mn is None and mx is None:
                raise DataDictionaryQualityError(
                    "range requires expression.min and/or expression.max"
                )
            clauses: list[str] = []
            args: list[Any] = []
            if mn is not None:
                args.append(float(mn))
                clauses.append(f"{field_ident}::numeric < ${len(args)}")
            if mx is not None:
                args.append(float(mx))
                clauses.append(f"{field_ident}::numeric > ${len(args)}")
            failing = _scalar(
                conn,
                f"SELECT count(*) AS n FROM {table_ident} "
                f" WHERE {field_ident} IS NOT NULL AND (" + " OR ".join(clauses) + ")",
                *args,
            )
            observed = {"failing_rows": failing, "min": mn, "max": mx}
            status = "pass" if failing == 0 else "fail"

        elif rule_kind == "row_count_min":
            mn = expression.get("min") if isinstance(expression, dict) else None
            if mn is None:
                raise DataDictionaryQualityError(
                    "row_count_min requires expression.min"
                )
            total = _scalar(conn, f"SELECT count(*) AS n FROM {table_ident}")
            observed = {"row_count": total, "min": int(mn)}
            status = "pass" if total >= int(mn) else "fail"

        elif rule_kind == "row_count_max":
            mx = expression.get("max") if isinstance(expression, dict) else None
            if mx is None:
                raise DataDictionaryQualityError(
                    "row_count_max requires expression.max"
                )
            total = _scalar(conn, f"SELECT count(*) AS n FROM {table_ident}")
            observed = {"row_count": total, "max": int(mx)}
            status = "pass" if total <= int(mx) else "fail"

        elif rule_kind == "referential":
            if field_ident is None:
                raise DataDictionaryQualityError("referential requires field_path")
            ref = expression.get("references") if isinstance(expression, dict) else None
            if not isinstance(ref, dict):
                raise DataDictionaryQualityError(
                    "referential requires expression.references {table, column}"
                )
            ref_table = _quote_ident(str(ref.get("table") or ""))
            ref_column = _quote_ident(str(ref.get("column") or ""))
            failing = _scalar(
                conn,
                f"SELECT count(*) AS n FROM {table_ident} t "
                f" WHERE t.{field_ident} IS NOT NULL "
                f"   AND NOT EXISTS (SELECT 1 FROM {ref_table} r "
                f"                    WHERE r.{ref_column} = t.{field_ident})",
            )
            observed = {
                "dangling_rows": failing,
                "references": {"table": str(ref.get("table")), "column": str(ref.get("column"))},
            }
            status = "pass" if failing == 0 else "fail"

        elif rule_kind == "custom_sql":
            sql = expression.get("sql") if isinstance(expression, dict) else None
            if not isinstance(sql, str) or not sql.strip():
                raise DataDictionaryQualityError(
                    "custom_sql requires expression.sql (non-empty string)"
                )
            # Only allow a single SELECT statement that returns a scalar.
            # For safety we require the operator layer to own custom_sql —
            # projectors never emit it.
            if ";" in sql.strip().rstrip(";"):
                raise DataDictionaryQualityError(
                    "custom_sql must be a single statement"
                )
            failing = _scalar(conn, sql)
            observed = {"failing_rows": failing, "sql": sql}
            status = "pass" if failing == 0 else "fail"

        else:
            raise DataDictionaryQualityError(f"unsupported rule_kind: {rule_kind!r}")

    except DataDictionaryQualityError as exc:
        status = "error"
        error_message = str(exc)
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error_message = f"{type(exc).__name__}: {exc}"

    duration_ms = (time.monotonic() - t0) * 1000.0

    try:
        run = insert_run(
            conn,
            object_kind=object_kind,
            field_path=field_path,
            rule_kind=rule_kind,
            effective_source=effective_source,
            status=status,
            observed=observed,
            duration_ms=duration_ms,
            error_message=error_message,
        )
    except PostgresWriteError as exc:
        _raise_storage(exc)
    return dict(run)


def evaluate_all(
    conn: Any,
    *,
    object_kind: str | None = None,
    only_enabled: bool = True,
) -> dict[str, Any]:
    """Walk every effective rule and evaluate it."""
    rules = list_effective_rules(
        conn, object_kind=_text(object_kind) or None,
    )
    results = {"evaluated": 0, "passed": 0, "failed": 0, "errored": 0}
    for rule in rules:
        if only_enabled and not rule.get("enabled", True):
            continue
        outcome = evaluate_rule(conn, rule)
        results["evaluated"] += 1
        status = outcome.get("status") or "error"
        if status == "pass":
            results["passed"] += 1
        elif status == "fail":
            results["failed"] += 1
        else:
            results["errored"] += 1
    return results


__all__ = [
    "DataDictionaryQualityError",
    "apply_projected_rules",
    "clear_operator_rule",
    "describe_rules",
    "evaluate_all",
    "evaluate_rule",
    "latest_runs",
    "quality_summary",
    "run_history",
    "set_operator_rule",
]
