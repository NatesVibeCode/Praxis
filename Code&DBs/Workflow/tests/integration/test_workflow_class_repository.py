from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from policy.workflow_classes import PostgresWorkflowClassRepository
from authority.workflow_schedule import NativeWorkflowScheduleCatalog, ScheduleRepositoryError
from storage.postgres.workflow_schedule_repository import PostgresWorkflowScheduleRepository


def test_workflow_class_repository_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_workflow_class_repository_is_deterministic_and_fail_closed())


async def _exercise_workflow_class_repository_is_deterministic_and_fail_closed() -> None:
    as_of = datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)
    valid_conn = _FakeConnection(
        workflow_class_rows=(
            {
                "workflow_class_id": "workflow_class.smoke",
                "class_name": "smoke",
                "class_kind": "smoke",
                "workflow_lane_id": "workflow_lane.smoke",
                "status": "active",
                "queue_shape": {
                    "max_parallel": 2,
                    "batching": "fast",
                },
                "throttle_policy": {
                    "max_attempts": 3,
                    "backoff": "fast",
                },
                "review_required": False,
                "effective_from": as_of - timedelta(hours=1),
                "effective_to": None,
                "decision_ref": "decision:workflow-class:smoke",
                "created_at": as_of - timedelta(minutes=30),
            },
            {
                "workflow_class_id": "workflow_class.review",
                "class_name": "review",
                "class_kind": "review",
                "workflow_lane_id": "workflow_lane.review",
                "status": "active",
                "queue_shape": {
                    "max_parallel": 1,
                    "batching": "manual",
                },
                "throttle_policy": {
                    "max_attempts": 1,
                    "backoff": "none",
                },
                "review_required": True,
                "effective_from": as_of - timedelta(hours=1),
                "effective_to": None,
                "decision_ref": "decision:workflow-class:review",
                "created_at": as_of - timedelta(minutes=20),
            },
        ),
        schedule_definition_rows=(
            {
                "schedule_definition_id": "schedule_definition.repo-archive.review",
                "workflow_class_id": "workflow_class.review",
                "schedule_name": "archive-review",
                "schedule_kind": "review",
                "status": "active",
                "cadence_policy": {
                    "cadence": "daily",
                },
                "throttle_policy": {
                    "max_runs_per_window": 1,
                },
                "target_ref": "repo:archive",
                "effective_from": as_of - timedelta(hours=1),
                "effective_to": None,
                "decision_ref": "decision:schedule:archive-review",
                "created_at": as_of - timedelta(minutes=15),
            },
            {
                "schedule_definition_id": "schedule_definition.repo-canonical.hourly",
                "workflow_class_id": "workflow_class.smoke",
                "schedule_name": "canonical-hourly",
                "schedule_kind": "hourly",
                "status": "active",
                "cadence_policy": {
                    "cadence": "hourly",
                },
                "throttle_policy": {
                    "max_runs_per_window": 2,
                },
                "target_ref": "repo:canonical",
                "effective_from": as_of - timedelta(hours=1),
                "effective_to": None,
                "decision_ref": "decision:schedule:canonical-hourly",
                "created_at": as_of - timedelta(minutes=10),
            },
        ),
        recurring_run_window_rows=(
            {
                "recurring_run_window_id": "recurring_run_window.repo-canonical.hourly",
                "schedule_definition_id": "schedule_definition.repo-canonical.hourly",
                "window_started_at": as_of - timedelta(minutes=5),
                "window_ended_at": as_of + timedelta(minutes=55),
                "window_status": "active",
                "capacity_limit": 4,
                "capacity_used": 1,
                "last_workflow_at": as_of - timedelta(minutes=1),
                "created_at": as_of - timedelta(minutes=5),
            },
            {
                "recurring_run_window_id": "recurring_run_window.repo-archive.review",
                "schedule_definition_id": "schedule_definition.repo-archive.review",
                "window_started_at": as_of - timedelta(minutes=10),
                "window_ended_at": as_of + timedelta(minutes=50),
                "window_status": "active",
                "capacity_limit": 1,
                "capacity_used": 0,
                "last_workflow_at": None,
                "created_at": as_of - timedelta(minutes=8),
            },
        ),
    )

    class_repo = PostgresWorkflowClassRepository(valid_conn)
    schedule_repo = PostgresWorkflowScheduleRepository(valid_conn)

    class_catalog = await class_repo.load_catalog(as_of=as_of)
    class_catalog_again = await class_repo.load_catalog(as_of=as_of)

    assert class_catalog == class_catalog_again
    assert class_catalog.as_of == as_of
    assert class_catalog.class_names == ("review", "smoke")
    assert class_catalog.class_keys == (
        ("review", "review"),
        ("smoke", "smoke"),
    )

    smoke_class = class_catalog.resolve(class_name="smoke")
    assert smoke_class.workflow_class_id == "workflow_class.smoke"
    assert smoke_class.workflow_lane_id == "workflow_lane.smoke"
    assert smoke_class.queue_shape == {
        "max_parallel": 2,
        "batching": "fast",
    }
    assert smoke_class.throttle_policy == {
        "max_attempts": 3,
        "backoff": "fast",
    }
    assert smoke_class.review_required is False
    assert smoke_class.decision_ref == "decision:workflow-class:smoke"

    schedule_catalog = await schedule_repo.load_catalog(as_of=as_of)
    schedule_catalog_again = await schedule_repo.load_catalog(as_of=as_of)

    assert schedule_catalog == schedule_catalog_again
    assert isinstance(schedule_catalog, NativeWorkflowScheduleCatalog)
    assert schedule_catalog.as_of == as_of
    assert schedule_catalog.schedule_keys == (
        ("repo:archive", "review"),
        ("repo:canonical", "hourly"),
    )
    assert schedule_catalog.schedule_names == (
        "archive-review",
        "canonical-hourly",
    )

    hourly_resolution = schedule_catalog.resolve(
        target_ref="repo:canonical",
        schedule_kind="hourly",
    )

    assert hourly_resolution.schedule_definition_id == "schedule_definition.repo-canonical.hourly"
    assert hourly_resolution.schedule_name == "canonical-hourly"
    assert hourly_resolution.class_name == "smoke"
    assert hourly_resolution.workflow_class_id == "workflow_class.smoke"
    assert hourly_resolution.workflow_lane_id == "workflow_lane.smoke"
    assert hourly_resolution.queue_shape == {
        "max_parallel": 2,
        "batching": "fast",
    }
    assert hourly_resolution.window_status == "active"
    assert hourly_resolution.capacity_limit == 4
    assert hourly_resolution.capacity_used == 1
    assert hourly_resolution.decision_ref == "decision:schedule:canonical-hourly"

    bad_conn = _FakeConnection(
        workflow_class_rows=valid_conn.workflow_class_rows,
        schedule_definition_rows=(
            {
                "schedule_definition_id": "schedule_definition.repo-canonical.hourly",
                "workflow_class_id": "workflow_class.missing",
                "schedule_name": "canonical-hourly",
                "schedule_kind": "hourly",
                "status": "active",
                "cadence_policy": {
                    "cadence": "hourly",
                },
                "throttle_policy": {
                    "max_runs_per_window": 2,
                },
                "target_ref": "repo:canonical",
                "effective_from": as_of - timedelta(hours=1),
                "effective_to": None,
                "decision_ref": "decision:schedule:canonical-hourly",
                "created_at": as_of - timedelta(minutes=10),
            },
        ),
        recurring_run_window_rows=(
            {
                "recurring_run_window_id": "recurring_run_window.repo-canonical.hourly",
                "schedule_definition_id": "schedule_definition.repo-canonical.hourly",
                "window_started_at": as_of - timedelta(minutes=5),
                "window_ended_at": as_of + timedelta(minutes=55),
                "window_status": "active",
                "capacity_limit": 4,
                "capacity_used": 1,
                "last_workflow_at": as_of - timedelta(minutes=1),
                "created_at": as_of - timedelta(minutes=5),
            },
        ),
    )
    bad_schedule_repo = PostgresWorkflowScheduleRepository(bad_conn)

    with pytest.raises(ScheduleRepositoryError) as exc_info:
        await bad_schedule_repo.load_catalog(as_of=as_of)

    assert exc_info.value.reason_code == "schedule.workflow_class_missing"
    assert exc_info.value.details == {
        "as_of": as_of.isoformat(),
        "workflow_class_ids": "workflow_class.missing",
    }


@dataclass
class _FakeTransaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


@dataclass
class _FakeConnection:
    workflow_class_rows: tuple[dict[str, object], ...]
    schedule_definition_rows: tuple[dict[str, object], ...]
    recurring_run_window_rows: tuple[dict[str, object], ...]
    fetch_calls: list[tuple[str, tuple[object, ...]]] | None = None

    def __post_init__(self) -> None:
        if self.fetch_calls is None:
            self.fetch_calls = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        assert len(args) == 1
        assert isinstance(args[0], datetime)
        self.fetch_calls.append((query, args))
        if "FROM workflow_classes" in query:
            return list(self.workflow_class_rows)
        if "FROM schedule_definitions" in query:
            return list(self.schedule_definition_rows)
        if "FROM recurring_run_windows" in query:
            return list(self.recurring_run_window_rows)
        raise AssertionError(f"unexpected query: {query}")
