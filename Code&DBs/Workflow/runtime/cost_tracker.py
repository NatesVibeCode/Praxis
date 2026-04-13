"""Cost tracking for dispatch runs.

The canonical cost ledger is Postgres. This module extracts usage from
WorkflowResult outputs and persists one durable record per run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from storage.postgres import SyncPostgresConnection, ensure_postgres_available

if TYPE_CHECKING:
    from .workflow import WorkflowResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class CostRecord:
    """One cost observation from a completed dispatch."""

    run_id: str
    provider_slug: str
    model_slug: str | None
    cost_usd: float
    input_tokens: int
    output_tokens: int
    recorded_at: datetime


def _safe_int(val: Any) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _extract_cost(outputs: dict[str, Any]) -> tuple[float, int, int]:
    cost_usd = 0.0
    input_tokens = 0
    output_tokens = 0

    raw_json = outputs.get("raw_json")
    if isinstance(raw_json, dict):
        cost_usd = _safe_float(raw_json.get("total_cost_usd"))
        usage = raw_json.get("usage")
        if isinstance(usage, dict):
            input_tokens = _safe_int(usage.get("input_tokens"))
            output_tokens = _safe_int(usage.get("output_tokens"))

    if input_tokens == 0 and output_tokens == 0:
        usage = outputs.get("usage")
        if isinstance(usage, dict):
            input_tokens = _safe_int(usage.get("prompt_tokens") or usage.get("input_tokens"))
            output_tokens = _safe_int(usage.get("completion_tokens") or usage.get("output_tokens"))

    if cost_usd == 0.0:
        cost_usd = _safe_float(outputs.get("total_cost_usd"))

    return cost_usd, input_tokens, output_tokens


_DEFAULT_COST_TRACKER_BUFFER = int(os.environ.get("PRAXIS_COST_TRACKER_BUFFER", "500"))


class CostTracker:
    """Postgres-backed ledger of recent cost records."""

    __slots__ = ("_conn", "_conn_factory", "_max_size")

    def __init__(
        self,
        *,
        max_size: int = _DEFAULT_COST_TRACKER_BUFFER,
        conn: SyncPostgresConnection | None = None,
        conn_factory: Callable[[], SyncPostgresConnection] = ensure_postgres_available,
    ) -> None:
        self._max_size = max_size
        self._conn = conn
        self._conn_factory = conn_factory

    def _get_conn(self) -> SyncPostgresConnection | None:
        if self._conn is not None:
            return self._conn
        try:
            self._conn = self._conn_factory()
        except Exception:
            return None
        return self._conn

    def _row_to_record(self, row: Any) -> CostRecord:
        return CostRecord(
            run_id=str(row.get("run_id") or ""),
            provider_slug=str(row.get("provider_slug") or ""),
            model_slug=row.get("model_slug"),
            cost_usd=_safe_float(row.get("cost_usd")),
            input_tokens=_safe_int(row.get("input_tokens")),
            output_tokens=_safe_int(row.get("output_tokens")),
            recorded_at=row.get("recorded_at") or _utc_now(),
        )

    def record_cost(self, result: WorkflowResult) -> CostRecord | None:
        outputs = dict(result.outputs) if result.outputs else {}
        cost_usd, input_tokens, output_tokens = _extract_cost(outputs)
        if cost_usd == 0.0 and input_tokens == 0 and output_tokens == 0:
            return None

        conn = self._get_conn()
        if conn is None:
            return None

        rows = conn.execute(
            """
            INSERT INTO workflow_cost_ledger (
                run_id, provider_slug, model_slug, cost_usd,
                input_tokens, output_tokens, recorded_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (run_id) DO UPDATE SET
                provider_slug = EXCLUDED.provider_slug,
                model_slug = EXCLUDED.model_slug,
                cost_usd = EXCLUDED.cost_usd,
                input_tokens = EXCLUDED.input_tokens,
                output_tokens = EXCLUDED.output_tokens,
                recorded_at = EXCLUDED.recorded_at
            RETURNING run_id, provider_slug, model_slug, cost_usd,
                      input_tokens, output_tokens, recorded_at
            """,
            result.run_id,
            result.provider_slug,
            result.model_slug,
            cost_usd,
            input_tokens,
            output_tokens,
            _utc_now(),
        )
        row = rows[0] if rows else None
        return self._row_to_record(row) if row is not None else None

    def recent_records(self, limit: int = 500) -> tuple[CostRecord, ...]:
        conn = self._get_conn()
        if conn is None:
            return ()
        bounded_limit = max(0, min(limit, self._max_size))
        rows = conn.execute(
            """
            SELECT run_id, provider_slug, model_slug, cost_usd,
                   input_tokens, output_tokens, recorded_at
              FROM workflow_cost_ledger
             ORDER BY recorded_at DESC
             LIMIT $1
            """,
            bounded_limit,
        )
        return tuple(self._row_to_record(row) for row in rows)

    def summary(self) -> dict[str, Any]:
        records = self.recent_records(limit=self._max_size)
        total_cost = 0.0
        total_input = 0
        total_output = 0
        cost_by_agent: dict[str, float] = {}

        for record in records:
            total_cost += record.cost_usd
            total_input += record.input_tokens
            total_output += record.output_tokens
            slug = f"{record.provider_slug}/{record.model_slug or 'unknown'}"
            cost_by_agent[slug] = cost_by_agent.get(slug, 0.0) + record.cost_usd

        return {
            "total_cost_usd": round(total_cost, 6),
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "cost_by_agent": {
                key: round(value, 6)
                for key, value in sorted(cost_by_agent.items(), key=lambda item: -item[1])
            },
            "record_count": len(records),
        }


_COST_TRACKER: CostTracker | None = None


def get_cost_tracker() -> CostTracker:
    global _COST_TRACKER
    if _COST_TRACKER is None:
        _COST_TRACKER = CostTracker()
    return _COST_TRACKER
