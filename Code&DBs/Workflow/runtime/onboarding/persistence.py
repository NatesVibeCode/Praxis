"""Onboarding gate-state persistence: cache reads/writes against Praxis.db.

The ``onboarding_gate_state`` table (migration 221) is a freshness cache over
probe evaluation. ``GateGraph.evaluate`` consults it when a connection is
provided and returns the cached ``GateResult`` if ``evaluated_at + cache_ttl_s``
is still in the future. Apply handlers (Packet 2b) upsert through
``write_gate_state`` with ``applied_by``/``applied_at`` populated.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from .graph import GateProbe, GateResult, GateStatus


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_result(row: Mapping[str, Any]) -> GateResult:
    observed_raw = row.get("observed_state")
    if isinstance(observed_raw, str):
        try:
            observed: dict[str, Any] = json.loads(observed_raw)
        except json.JSONDecodeError:
            observed = {}
    elif isinstance(observed_raw, dict):
        observed = dict(observed_raw)
    else:
        observed = {}

    evaluated_at = row["evaluated_at"]
    if isinstance(evaluated_at, str):
        evaluated_at = datetime.fromisoformat(evaluated_at)
    elif isinstance(evaluated_at, datetime) and evaluated_at.tzinfo is None:
        evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)

    return GateResult(
        gate_ref=row["gate_ref"],
        status=row["status"],
        observed_state=observed,
        remediation_hint=row.get("remediation_hint"),
        remediation_doc_url=row.get("remediation_doc_url"),
        apply_ref=row.get("apply_ref"),
        evaluated_at=evaluated_at,
    )


def read_gate_state(conn: Any, gate_ref: str) -> GateResult | None:
    """Return a cached ``GateResult`` when it is still fresh, else None.

    Freshness is defined by the row's own ``cache_ttl_s``; callers do not
    override TTL at read time. Callers that want to force a refresh should
    skip ``read_gate_state`` and call the probe directly.
    """
    rows = conn.execute(
        """
        SELECT gate_ref, domain, status, observed_state, remediation_hint,
               remediation_doc_url, apply_ref, evaluated_at, cache_ttl_s
          FROM onboarding_gate_state
         WHERE gate_ref = $1
           AND evaluated_at + make_interval(secs => cache_ttl_s) > now()
         LIMIT 1
        """,
        gate_ref,
    )
    if not rows:
        return None
    return _row_to_result(rows[0])


def read_all_gate_states(conn: Any) -> dict[str, GateResult]:
    """Return every fresh cached result keyed by gate_ref."""
    rows = conn.execute(
        """
        SELECT gate_ref, domain, status, observed_state, remediation_hint,
               remediation_doc_url, apply_ref, evaluated_at, cache_ttl_s
          FROM onboarding_gate_state
         WHERE evaluated_at + make_interval(secs => cache_ttl_s) > now()
        """,
    )
    return {row["gate_ref"]: _row_to_result(row) for row in rows}


def write_gate_state(
    conn: Any,
    result: GateResult,
    probe: GateProbe,
    *,
    platform: str | None = None,
    applied_by: str | None = None,
    applied_at: datetime | None = None,
) -> None:
    """Upsert the latest evaluation into the cache.

    Applied handlers pass ``applied_by`` and ``applied_at`` to mark the row as
    the result of a mutation; plain reads leave both NULL.
    """
    conn.execute(
        """
        INSERT INTO onboarding_gate_state (
            gate_ref, domain, status, observed_state, remediation_hint,
            remediation_doc_url, apply_ref, platform, cache_ttl_s,
            evaluated_at, applied_at, applied_by, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, $12, now(), now()
        )
        ON CONFLICT (gate_ref) DO UPDATE SET
            domain = EXCLUDED.domain,
            status = EXCLUDED.status,
            observed_state = EXCLUDED.observed_state,
            remediation_hint = EXCLUDED.remediation_hint,
            remediation_doc_url = EXCLUDED.remediation_doc_url,
            apply_ref = EXCLUDED.apply_ref,
            platform = EXCLUDED.platform,
            cache_ttl_s = EXCLUDED.cache_ttl_s,
            evaluated_at = EXCLUDED.evaluated_at,
            applied_at = COALESCE(EXCLUDED.applied_at, onboarding_gate_state.applied_at),
            applied_by = COALESCE(EXCLUDED.applied_by, onboarding_gate_state.applied_by),
            updated_at = now()
        """,
        result.gate_ref,
        probe.domain,
        result.status,
        json.dumps(dict(result.observed_state), default=str),
        result.remediation_hint,
        result.remediation_doc_url,
        result.apply_ref,
        platform,
        probe.ok_cache_ttl_s,
        result.evaluated_at,
        applied_at,
        applied_by,
    )


def clear_stale_cache(conn: Any) -> int:
    """Delete rows whose cached result has expired. Returns the delete count."""
    rows = conn.execute(
        """
        WITH deleted AS (
            DELETE FROM onboarding_gate_state
             WHERE evaluated_at + make_interval(secs => cache_ttl_s) <= now()
             RETURNING 1
        )
        SELECT count(*) AS deleted_count FROM deleted
        """,
    )
    if rows:
        return int(rows[0].get("deleted_count") or 0)
    return 0
