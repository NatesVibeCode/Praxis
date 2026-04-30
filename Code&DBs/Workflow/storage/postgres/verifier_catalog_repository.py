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

