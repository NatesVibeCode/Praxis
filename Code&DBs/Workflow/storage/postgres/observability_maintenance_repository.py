"""Explicit Postgres repository for destructive observability maintenance."""

from __future__ import annotations

from typing import Any

from .validators import _optional_text


class PostgresObservabilityMaintenanceRepository:
    """Owns explicit observability maintenance mutations."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def delete_quality_rollups_before(self, *, before_date: str) -> None:
        normalized_before_date = _optional_text(
            before_date,
            field_name="before_date",
        )
        if normalized_before_date is None:
            return
        self._conn.execute(
            "DELETE FROM quality_rollups WHERE window_start < $1",
            normalized_before_date,
        )

    def delete_agent_profiles_before(self, *, before_date: str) -> None:
        normalized_before_date = _optional_text(
            before_date,
            field_name="before_date",
        )
        if normalized_before_date is None:
            return
        self._conn.execute(
            "DELETE FROM agent_profiles WHERE window_start < $1",
            normalized_before_date,
        )

    def clear_failure_catalog(self) -> None:
        self._conn.execute("DELETE FROM failure_catalog")

    def truncate_quality_rollups(self) -> None:
        self._conn.execute("TRUNCATE quality_rollups")

    def truncate_agent_profiles(self) -> None:
        self._conn.execute("TRUNCATE agent_profiles")

    def truncate_failure_catalog(self) -> None:
        self._conn.execute("TRUNCATE failure_catalog")

    def zero_task_type_routing_counters(self) -> None:
        self._conn.execute(
            "UPDATE task_type_routing SET recent_successes = 0, recent_failures = 0"
        )

    def reset_observability_metrics(
        self,
        *,
        before_date: str | None = None,
    ) -> dict[str, Any]:
        normalized_before_date = _optional_text(
            before_date,
            field_name="before_date",
        )
        results: dict[str, Any] = {}
        if normalized_before_date:
            self.delete_quality_rollups_before(before_date=normalized_before_date)
            results["quality_rollups"] = f"deleted rows before {normalized_before_date}"
            self.delete_agent_profiles_before(before_date=normalized_before_date)
            results["agent_profiles"] = f"deleted rows before {normalized_before_date}"
            self.clear_failure_catalog()
            results["failure_catalog"] = "cleared"
        else:
            self.truncate_quality_rollups()
            results["quality_rollups"] = "truncated"
            self.truncate_agent_profiles()
            results["agent_profiles"] = "truncated"
            self.truncate_failure_catalog()
            results["failure_catalog"] = "truncated"

        self.zero_task_type_routing_counters()
        results["task_type_routing_counters"] = "zeroed"
        results["note"] = (
            "Canonical receipts are preserved. Next rollup cycle will regenerate clean aggregations."
        )
        return results


__all__ = ["PostgresObservabilityMaintenanceRepository"]
