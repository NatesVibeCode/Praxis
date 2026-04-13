from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from runtime.instance import NativeDagInstance
from policy.workflow_classes import WorkflowClassCatalogError
from surfaces.api import operator_write

import pathlib

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


@dataclass
class _FakeTransaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


@dataclass
class _FakeConnection:
    workflow_class_rows: tuple[dict[str, object], ...]
    seen: dict[str, object]

    async def fetch(self, query: str, *args: object):
        self.seen["queries"].append((query, args))
        if "FROM workflow_classes" in query:
            return self.workflow_class_rows
        raise AssertionError(f"unexpected query: {query}")

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def close(self) -> None:
        self.seen["closed_connections"] += 1


def _native_instance() -> NativeDagInstance:
    return NativeDagInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )


def _workflow_class_row(
    *,
    class_name: str,
    workflow_class_id: str,
    workflow_lane_id: str,
    review_required: bool,
    as_of: datetime,
) -> dict[str, object]:
    return {
        "workflow_class_id": workflow_class_id,
        "class_name": class_name,
        "class_kind": class_name,
        "workflow_lane_id": workflow_lane_id,
        "status": "active",
        "queue_shape": {
            "class_name": class_name,
            "mode": "native",
        },
        "throttle_policy": {
            "max_attempts": 1 if class_name == "review" else 2,
            "backoff": "none" if class_name != "repair" else "linear",
        },
        "review_required": review_required,
        "effective_from": as_of - timedelta(hours=1),
        "effective_to": None,
        "decision_ref": f"decision:workflow-class:{class_name}",
        "created_at": as_of,
    }


def test_native_review_repair_fanout_flow_is_deterministic_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
            workflow_class_rows=(
                _workflow_class_row(
                    class_name="review",
                    workflow_class_id="workflow_class.review",
                    workflow_lane_id="workflow_lane.review",
                    review_required=True,
                    as_of=as_of,
                ),
                _workflow_class_row(
                    class_name="repair",
                    workflow_class_id="workflow_class.repair",
                    workflow_lane_id="workflow_lane.repair",
                    review_required=True,
                    as_of=as_of,
                ),
                _workflow_class_row(
                    class_name="fanout",
                    workflow_class_id="workflow_class.fanout",
                    workflow_lane_id="workflow_lane.fanout",
                    review_required=False,
                    as_of=as_of,
                ),
            ),
            seen=seen,
        )

    monkeypatch.setattr(operator_write, "resolve_native_instance", _resolve_instance)

    frontdoor = operator_write.NativeWorkflowFlowFrontdoor(
        connect_database=_connect_database,
    )
    first_payload = frontdoor.inspect_workflow_flows(
        env=env,
        as_of=as_of,
    )
    second_payload = frontdoor.inspect_workflow_flows(
        env=env,
        as_of=as_of,
    )

    assert first_payload == second_payload
    assert first_payload["native_instance"]["praxis_instance_name"] == "praxis"
    assert first_payload["workflow_class_authority"] == "policy.workflow_classes"
    assert first_payload["as_of"] == as_of.isoformat()
    assert first_payload["flow_names"] == ["review", "repair", "fanout"]
    assert [flow["flow_name"] for flow in first_payload["flows"]] == [
        "review",
        "repair",
        "fanout",
    ]
    assert [
        flow["workflow_class"]["workflow_class_id"] for flow in first_payload["flows"]
    ] == [
        "workflow_class.review",
        "workflow_class.repair",
        "workflow_class.fanout",
    ]
    assert [
        flow["workflow_class"]["workflow_lane_id"] for flow in first_payload["flows"]
    ] == [
        "workflow_lane.review",
        "workflow_lane.repair",
        "workflow_lane.fanout",
    ]
    assert seen["resolved_envs"] == [env, env]
    assert seen["closed_connections"] == 2
    assert all("FROM workflow_classes" in query for query, _ in seen["queries"])

    async def _connect_missing_database(env=None):
        return _FakeConnection(
            workflow_class_rows=(
                _workflow_class_row(
                    class_name="review",
                    workflow_class_id="workflow_class.review",
                    workflow_lane_id="workflow_lane.review",
                    review_required=True,
                    as_of=as_of,
                ),
                _workflow_class_row(
                    class_name="fanout",
                    workflow_class_id="workflow_class.fanout",
                    workflow_lane_id="workflow_lane.fanout",
                    review_required=False,
                    as_of=as_of,
                ),
            ),
            seen=seen,
        )

    missing_frontdoor = operator_write.NativeWorkflowFlowFrontdoor(
        connect_database=_connect_missing_database,
    )

    with pytest.raises(WorkflowClassCatalogError) as exc_info:
        missing_frontdoor.inspect_workflow_flows(
            env=env,
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "workflow_class.class_missing"
    assert exc_info.value.details == {
        "class_name": "repair",
    }
