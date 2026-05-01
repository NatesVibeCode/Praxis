"""Postgres reads for registered verifier authority refs."""

from __future__ import annotations

import json
from typing import Any

from .validators import _require_positive_int


def _normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except (TypeError, json.JSONDecodeError):
                return value
    return value


def list_verification_runs(
    conn: Any,
    *,
    verifier_ref: str | None = None,
    target_kind: str | None = None,
    target_ref: str | None = None,
    status: str | None = None,
    since_iso: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List verification_runs rows newest-first, optionally filtered.

    Filters compose with AND. ``since_iso`` is parsed as a Postgres
    timestamptz literal — caller is responsible for ISO-8601 formatting.
    Empty / None filters are skipped (no clause emitted).
    """
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    if verifier_ref:
        clauses.append(f"verifier_ref = ${idx}")
        params.append(str(verifier_ref))
        idx += 1
    if target_kind:
        clauses.append(f"target_kind = ${idx}")
        params.append(str(target_kind))
        idx += 1
    if target_ref:
        clauses.append(f"target_ref = ${idx}")
        params.append(str(target_ref))
        idx += 1
    if status:
        clauses.append(f"status = ${idx}")
        params.append(str(status))
        idx += 1
    if since_iso:
        clauses.append(f"attempted_at >= ${idx}::timestamptz")
        params.append(str(since_iso))
        idx += 1
    params.append(_require_positive_int(limit, field_name="limit"))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        """
        SELECT
            verification_run_id,
            verifier_ref,
            target_kind,
            target_ref,
            status,
            inputs,
            outputs,
            suggested_healer_ref,
            healing_candidate,
            decision_ref,
            attempted_at,
            duration_ms
          FROM verification_runs
        """
        + where
        + f"""
         ORDER BY attempted_at DESC, verification_run_id ASC
         LIMIT ${idx}
        """,
        *params,
    )
    return [
        {key: _normalize_value(value) for key, value in dict(row).items()}
        for row in (rows or ())
    ]


def list_healer_catalog(
    conn: Any,
    *,
    enabled: bool | None = True,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List registered healer authority refs.

    Returns each healer's registry row plus its bound verifier refs (one
    healer can be bound to multiple verifiers). Mirrors list_verifier_catalog.
    """
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    if enabled is not None:
        clauses.append(f"hr.enabled = ${idx}")
        params.append(bool(enabled))
        idx += 1
    params.append(_require_positive_int(limit, field_name="limit"))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        """
        SELECT
            hr.healer_ref,
            hr.display_name,
            hr.description,
            hr.executor_kind,
            hr.action_ref,
            hr.auto_mode,
            hr.safety_mode,
            hr.enabled,
            hr.decision_ref,
            COALESCE(
                (SELECT array_agg(vhb.verifier_ref ORDER BY vhb.verifier_ref)
                   FROM verifier_healer_bindings AS vhb
                  WHERE vhb.healer_ref = hr.healer_ref
                    AND vhb.enabled = TRUE),
                '{}'::text[]
            ) AS bound_verifier_refs
          FROM healer_registry AS hr
        """
        + where
        + f"""
         ORDER BY hr.enabled DESC, hr.display_name ASC, hr.healer_ref ASC
         LIMIT ${idx}
        """,
        *params,
    )
    return [
        {key: _normalize_value(value) for key, value in dict(row).items()}
        for row in (rows or ())
    ]


def list_healing_runs(
    conn: Any,
    *,
    healer_ref: str | None = None,
    verifier_ref: str | None = None,
    target_kind: str | None = None,
    target_ref: str | None = None,
    status: str | None = None,
    since_iso: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List healing_runs rows newest-first, optionally filtered.

    Filters compose with AND. ``since_iso`` is parsed as a Postgres
    timestamptz literal.
    """
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    if healer_ref:
        clauses.append(f"healer_ref = ${idx}")
        params.append(str(healer_ref))
        idx += 1
    if verifier_ref:
        clauses.append(f"verifier_ref = ${idx}")
        params.append(str(verifier_ref))
        idx += 1
    if target_kind:
        clauses.append(f"target_kind = ${idx}")
        params.append(str(target_kind))
        idx += 1
    if target_ref:
        clauses.append(f"target_ref = ${idx}")
        params.append(str(target_ref))
        idx += 1
    if status:
        clauses.append(f"status = ${idx}")
        params.append(str(status))
        idx += 1
    if since_iso:
        clauses.append(f"attempted_at >= ${idx}::timestamptz")
        params.append(str(since_iso))
        idx += 1
    params.append(_require_positive_int(limit, field_name="limit"))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        """
        SELECT
            healing_run_id,
            healer_ref,
            verifier_ref,
            target_kind,
            target_ref,
            status,
            inputs,
            outputs,
            decision_ref,
            attempted_at,
            duration_ms
          FROM healing_runs
        """
        + where
        + f"""
         ORDER BY attempted_at DESC, healing_run_id ASC
         LIMIT ${idx}
        """,
        *params,
    )
    return [
        {key: _normalize_value(value) for key, value in dict(row).items()}
        for row in (rows or ())
    ]


def list_verifier_catalog(
    conn: Any,
    *,
    enabled: bool | None = True,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    if enabled is not None:
        clauses.append(f"vr.enabled = ${idx}")
        params.append(bool(enabled))
        idx += 1
    params.append(_require_positive_int(limit, field_name="limit"))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        """
        SELECT
            vr.verifier_ref,
            vr.display_name,
            vr.description,
            vr.verifier_kind,
            vr.verification_ref,
            vr.builtin_ref,
            vr.default_inputs,
            vr.enabled,
            vr.decision_ref,
            v.executor_kind,
            v.template_inputs,
            v.default_timeout_seconds
          FROM verifier_registry AS vr
          LEFT JOIN verification_registry AS v
            ON v.verification_ref = vr.verification_ref
        """
        + where
        + f"""
         ORDER BY vr.enabled DESC, vr.display_name ASC, vr.verifier_ref ASC
         LIMIT ${idx}
        """,
        *params,
    )
    return [
        {key: _normalize_value(value) for key, value in dict(row).items()}
        for row in (rows or ())
    ]

