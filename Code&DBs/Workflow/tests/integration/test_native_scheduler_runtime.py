from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from runtime.instance import NativeWorkflowInstance
from runtime.native_scheduler import NativeSchedulerError
from surfaces.api import native_scheduler

import pathlib

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


@dataclass
class _FakeConnection:
    schedule_rows: tuple[dict[str, object], ...]
    dispatch_rows: tuple[dict[str, object], ...]
    run_window_rows: tuple[dict[str, object], ...]
    seen: dict[str, object]

    async def fetch(self, query: str, *args: object):
        self.seen["queries"].append((query, args))
        if "FROM schedule_definitions" in query:
            return self.schedule_rows
        if "FROM workflow_classes" in query:
            return self.dispatch_rows
        if "FROM recurring_run_windows" in query:
            return self.run_window_rows
        raise AssertionError(f"unexpected query: {query}")

    async def close(self) -> None:
        self.seen["closed_connections"] += 1


def _native_instance() -> NativeWorkflowInstance:
    return NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )


def _schedule_row() -> dict[str, object]:
    as_of = datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)
    return {
        "schedule_definition_id": "schedule_definition.hourly.alpha",
        "workflow_class_id": "workflow_class.hourly.alpha",
        "schedule_name": "hourly-alpha",
        "schedule_kind": "hourly",
        "status": "active",
        "cadence_policy": {
            "cadence": "P1H",
            "bounded": True,
        },
        "throttle_policy": {
            "capacity_limit": 1,
        },
        "target_ref": "workspace.alpha",
        "effective_from": as_of - timedelta(hours=1),
        "effective_to": None,
        "decision_ref": "decision:schedule:hourly-alpha",
        "created_at": as_of,
    }


def _dispatch_row() -> dict[str, object]:
    as_of = datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)
    return {
        "workflow_class_id": "workflow_class.hourly.alpha",
        "class_name": "hourly",
        "class_kind": "hourly",
        "workflow_lane_id": "workflow_lane.hourly",
        "status": "active",
        "queue_shape": {
            "shape": "single-run",
        },
        "throttle_policy": {
            "dispatch_limit": 1,
        },
        "review_required": False,
        "effective_from": as_of - timedelta(hours=1),
        "effective_to": None,
        "decision_ref": "decision:workflow-class:hourly-alpha",
        "created_at": as_of,
    }


def _run_window_row(as_of: datetime) -> dict[str, object]:
    return {
        "recurring_run_window_id": "recurring_run_window.hourly.alpha",
        "schedule_definition_id": "schedule_definition.hourly.alpha",
        "window_started_at": as_of - timedelta(hours=1),
        "window_ended_at": as_of + timedelta(hours=1),
        "window_status": "active",
        "capacity_limit": 1,
        "capacity_used": 0,
        "last_workflow_at": None,
        "created_at": as_of,
    }


def test_native_scheduler_runtime_is_deterministic_and_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    as_of = datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)
    env = {
        "WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow",
    }
    seen: dict[str, object] = {
        "queries": [],
        "closed_connections": 0,
        "resolved_envs": [],
    }

    def _resolve_instance(*, env=None):
        seen["resolved_envs"].append(dict(env or {}))
        return _native_instance()

    async def _connect_database(env=None):
        return _FakeConnection(
            schedule_rows=(_schedule_row(),),
            dispatch_rows=(_dispatch_row(),),
            run_window_rows=(_run_window_row(as_of),),
            seen=seen,
        )

    monkeypatch.setattr(native_scheduler, "resolve_native_instance", _resolve_instance)

    api = native_scheduler.NativeSchedulerFrontdoor(connect_database=_connect_database)
    first_payload = api.inspect_schedule(
        target_ref="workspace.alpha",
        schedule_kind="hourly",
        env=env,
        as_of=as_of,
    )
    second_payload = api.inspect_schedule(
        target_ref="workspace.alpha",
        schedule_kind="hourly",
        env=env,
        as_of=as_of,
    )

    assert first_payload == second_payload
    assert first_payload["native_instance"]["praxis_instance_name"] == "praxis"
    assert first_payload["schedule"]["as_of"] == as_of.isoformat()
    assert first_payload["schedule"]["schedule_definition"]["schedule_definition_id"] == (
        "schedule_definition.hourly.alpha"
    )
    assert first_payload["schedule"]["workflow_class"]["class_name"] == "hourly"
    assert seen["resolved_envs"] == [env, env]
    assert seen["closed_connections"] == 2
    assert [
        "FROM workflow_classes" in query for query, _ in seen["queries"][:3]
    ] == [True, False, False]
    assert [
        "FROM schedule_definitions" in query for query, _ in seen["queries"][:3]
    ] == [False, True, False]
    assert [
        "FROM recurring_run_windows" in query for query, _ in seen["queries"][:3]
    ] == [False, False, True]
    assert len(seen["queries"]) >= 6

    async def _connect_ambiguous_database(env=None):
        return _FakeConnection(
            schedule_rows=(
                _schedule_row(),
                {
                    **_schedule_row(),
                    "schedule_definition_id": "schedule_definition.hourly.beta",
                    "created_at": as_of,
                },
            ),
            dispatch_rows=(_dispatch_row(),),
            run_window_rows=(_run_window_row(as_of),),
            seen=seen,
        )

    ambiguous_api = native_scheduler.NativeSchedulerFrontdoor(
        connect_database=_connect_ambiguous_database,
    )
    with pytest.raises(NativeSchedulerError) as exc_info:
        ambiguous_api.inspect_schedule(
            target_ref="workspace.alpha",
            schedule_kind="hourly",
            env=env,
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_scheduler.schedule_ambiguous"
