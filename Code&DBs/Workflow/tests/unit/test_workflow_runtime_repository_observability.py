from __future__ import annotations

from storage.postgres import workflow_runtime_repository as repo
from storage.postgres.observability_maintenance_repository import (
    PostgresObservabilityMaintenanceRepository,
)


class _Conn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args: object) -> list[dict[str, object]]:
        self.calls.append((query, args))
        return []


def test_reset_observability_metrics_delegates_to_the_dedicated_repository(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeRepository:
        def __init__(self, conn: object) -> None:
            captured["conn"] = conn

        def reset_observability_metrics(
            self,
            *,
            before_date: str | None = None,
        ) -> dict[str, object]:
            captured["before_date"] = before_date
            return {"delegated": True, "before_date": before_date}

    monkeypatch.setattr(repo, "PostgresObservabilityMaintenanceRepository", _FakeRepository)

    conn = object()
    result = repo.reset_observability_metrics(conn, before_date="2026-04-01")

    assert result == {"delegated": True, "before_date": "2026-04-01"}
    assert captured == {"conn": conn, "before_date": "2026-04-01"}


def test_postgres_observability_maintenance_repository_resets_with_before_date() -> None:
    conn = _Conn()
    maintenance = PostgresObservabilityMaintenanceRepository(conn)

    result = maintenance.reset_observability_metrics(before_date="2026-04-01")

    assert result == {
        "quality_rollups": "deleted rows before 2026-04-01",
        "agent_profiles": "deleted rows before 2026-04-01",
        "failure_catalog": "cleared",
        "task_type_routing_counters": "zeroed",
        "note": "Canonical receipts are preserved. Next rollup cycle will regenerate clean aggregations.",
    }
    assert conn.calls == [
        ("DELETE FROM quality_rollups WHERE window_start < $1", ("2026-04-01",)),
        ("DELETE FROM agent_profiles WHERE window_start < $1", ("2026-04-01",)),
        ("DELETE FROM failure_catalog", ()),
        ("UPDATE task_type_routing SET recent_successes = 0, recent_failures = 0", ()),
    ]


def test_postgres_observability_maintenance_repository_truncates_when_no_before_date() -> None:
    conn = _Conn()
    maintenance = PostgresObservabilityMaintenanceRepository(conn)

    result = maintenance.reset_observability_metrics()

    assert result == {
        "quality_rollups": "truncated",
        "agent_profiles": "truncated",
        "failure_catalog": "truncated",
        "task_type_routing_counters": "zeroed",
        "note": "Canonical receipts are preserved. Next rollup cycle will regenerate clean aggregations.",
    }
    assert conn.calls == [
        ("TRUNCATE quality_rollups", ()),
        ("TRUNCATE agent_profiles", ()),
        ("TRUNCATE failure_catalog", ()),
        ("UPDATE task_type_routing SET recent_successes = 0, recent_failures = 0", ()),
    ]
