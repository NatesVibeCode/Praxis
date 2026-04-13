from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from policy.workflow_classes import PostgresWorkflowClassRepository
from runtime.instance import NativeWorkflowInstance
from surfaces.api import native_scheduler

_QUEUE_FILENAME = "PRAXIS_NATIVE_DEFAULT_PARALLEL_PROOF.queue.json"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _queue_path() -> Path:
    return _repo_root() / "artifacts" / "workflow" / _QUEUE_FILENAME


def _load_queue() -> dict[str, object]:
    return json.loads(_queue_path().read_text(encoding="utf-8"))


def _native_instance() -> NativeWorkflowInstance:
    root = str(_repo_root())
    return NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=root,
        workdir=root,
        receipts_dir=f"{root}/artifacts/runtime_receipts",
        topology_dir=f"{root}/artifacts/runtime_topology",
        runtime_profiles_config=f"{root}/config/runtime_profiles.json",
    )


def _schedule_row(as_of: datetime) -> dict[str, object]:
    return {
        "schedule_definition_id": "schedule_definition.fanout.parallel_proof",
        "workflow_class_id": "workflow_class.fanout.parallel_proof",
        "schedule_name": "fanout-parallel-proof",
        "schedule_kind": "fanout",
        "status": "active",
        "cadence_policy": {
            "cadence": "manual",
            "bounded": True,
        },
        "throttle_policy": {
            "capacity_limit": 2,
        },
        "target_ref": "workspace.alpha",
        "effective_from": as_of - timedelta(minutes=5),
        "effective_to": None,
        "decision_ref": "decision:schedule:fanout-parallel-proof",
        "created_at": as_of,
    }


def _dispatch_rows(as_of: datetime) -> tuple[dict[str, object], ...]:
    return (
        {
            "workflow_class_id": "workflow_class.fanout.parallel_proof",
            "class_name": "fanout",
            "class_kind": "fanout",
            "workflow_lane_id": "workflow_lane.fanout",
            "status": "active",
            "queue_shape": {
                "max_parallel": 2,
                "wave_kind": "parallel_default",
            },
            "throttle_policy": {
                "dispatch_limit": 2,
            },
            "review_required": False,
            "effective_from": as_of - timedelta(minutes=5),
            "effective_to": None,
            "decision_ref": "decision:workflow-class:fanout-parallel-proof",
            "created_at": as_of,
        },
        {
            "workflow_class_id": "workflow_class.smoke.parallel_proof",
            "class_name": "smoke",
            "class_kind": "smoke",
            "workflow_lane_id": "workflow_lane.smoke",
            "status": "active",
            "queue_shape": {
                "max_parallel": 1,
                "wave_kind": "single_default",
            },
            "throttle_policy": {
                "dispatch_limit": 1,
            },
            "review_required": False,
            "effective_from": as_of - timedelta(minutes=5),
            "effective_to": None,
            "decision_ref": "decision:workflow-class:smoke-parallel-proof",
            "created_at": as_of,
        },
    )


@dataclass
class _FakeTransaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


@dataclass
class _FakeConnection:
    schedule_rows: tuple[dict[str, object], ...]
    dispatch_rows: tuple[dict[str, object], ...]
    seen: dict[str, object]

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, query: str, *args: object):
        self.seen["queries"].append((query, args))
        if "FROM schedule_definitions" in query:
            return self.schedule_rows
        if "FROM workflow_classes" in query:
            if "workflow_class_id = $1" in query:
                workflow_class_id = args[0]
                return tuple(
                    row
                    for row in self.dispatch_rows
                    if row["workflow_class_id"] == workflow_class_id
                )
            return self.dispatch_rows
        raise AssertionError(f"unexpected query: {query}")

    async def close(self) -> None:
        self.seen["closed_connections"] += 1


def test_native_default_parallel_proof_uses_stored_class_authority_and_native_truth_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    proof_packet = queue_payload["proof_packet"]
    assert isinstance(proof_packet, dict)

    assert queue_payload["phase"] == "DAGW19A"
    assert queue_payload["workflow_id"] == "dag_native_default_parallel_proof"
    assert queue_payload["packet_id"] == "native_default_parallel_proof"
    assert queue_payload["anti_requirements"] == [
        "no broad legacy cutover",
        "no hosted deployment work",
        "no worker-type expansion",
    ]
    assert len(queue_payload["jobs"]) == 1
    assert queue_payload["jobs"][0]["label"] == "DAGW19A.1_native_default_parallel_proof_packet"
    assert queue_payload["jobs"][0]["prompt"].startswith("OBJECTIVE:\nProve native default workflow classes")
    assert proof_packet["authority_path"] == {
        "workflow_classes": "policy.workflow_classes.PostgresWorkflowClassRepository",
        "native_scheduler": "surfaces.api.native_scheduler.NativeSchedulerFrontdoor",
        "native_truth": "artifacts/workflow/PRAXIS_NATIVE_STATUS_AND_RECEIPT_TRUTH.md",
    }
    assert proof_packet["parallel_wave"] == {
        "target_ref": "workspace.alpha",
        "schedule_kind": "fanout",
        "schedule_definition_id": "schedule_definition.fanout.parallel_proof",
        "workflow_class_id": "workflow_class.fanout.parallel_proof",
        "workflow_class_name": "fanout",
        "workflow_lane_id": "workflow_lane.fanout",
        "parallel_task_refs": ["node_0", "node_1"],
        "max_parallel": 2,
    }
    assert proof_packet["native_truth_surfaces"] == [
        "python3 -m surfaces.cli.main native-operator status <run_id>",
        "python3 -m surfaces.cli.main native-operator inspect <run_id>",
        "canonical evidence and receipts in repo-local Postgres",
    ]
    _root = str(_repo_root())
    assert queue_payload["verify"] == [
        {
            "command": (
                f"PYTHONPATH='{_root}/Code&DBs/Workflow' "
                "python3 -m pytest -q "
                f"'{_root}/Code&DBs/Workflow/tests/integration/"
                "test_native_default_parallel_proof.py'"
            )
        },
        {
            "command": (
                f"cd '{_root}' && ./scripts/validate-queue.sh"
                f"'{_root}/artifacts/workflow/"
                "PRAXIS_NATIVE_DEFAULT_PARALLEL_PROOF.queue.json'"
            )
        },
    ]

    as_of = datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)
    env = {"WORKFLOW_DATABASE_URL": "postgresql://workflow@127.0.0.1:5432/workflow"}
    seen: dict[str, object] = {
        "queries": [],
        "closed_connections": 0,
        "resolved_envs": [],
    }
    conn = _FakeConnection(
        schedule_rows=(_schedule_row(as_of),),
        dispatch_rows=_dispatch_rows(as_of),
        seen=seen,
    )

    def _resolve_instance(*, env=None):
        seen["resolved_envs"].append(dict(env or {}))
        return _native_instance()

    async def _connect_database(env=None):
        return conn

    monkeypatch.setattr(native_scheduler, "resolve_native_instance", _resolve_instance)
    frontdoor = native_scheduler.NativeSchedulerFrontdoor(connect_database=_connect_database)

    payload = frontdoor.inspect_schedule(
        target_ref="workspace.alpha",
        schedule_kind="fanout",
        env=env,
        as_of=as_of,
    )

    catalog = asyncio.run(PostgresWorkflowClassRepository(conn).load_catalog(as_of=as_of))
    fanout_class = catalog.resolve(class_name="fanout")
    parallel_wave = proof_packet["parallel_wave"]

    assert payload["native_instance"] == {
        "praxis_instance_name": "praxis",
        "praxis_receipts_dir": f"{_root}/artifacts/runtime_receipts",
        "praxis_runtime_profile": "praxis",
        "praxis_topology_dir": f"{_root}/artifacts/runtime_topology",
        "repo_root": _root,
        "runtime_profiles_config": f"{_root}/config/runtime_profiles.json",
        "workdir": _root,
    }
    assert payload["schedule"]["schedule_authority"] == "runtime.schedule_definitions"
    assert payload["schedule"]["workflow_class_authority"] == "policy.workflow_classes"
    assert payload["schedule"]["schedule_definition"]["schedule_definition_id"] == parallel_wave[
        "schedule_definition_id"
    ]
    assert payload["schedule"]["schedule_definition"]["workflow_class_id"] == parallel_wave[
        "workflow_class_id"
    ]
    assert payload["schedule"]["workflow_class"]["workflow_class_id"] == parallel_wave[
        "workflow_class_id"
    ]
    assert payload["schedule"]["workflow_class"]["class_name"] == "fanout"
    assert payload["schedule"]["workflow_class"]["workflow_lane_id"] == "workflow_lane.fanout"
    assert payload["schedule"]["workflow_class"]["queue_shape"]["max_parallel"] == 2
    assert payload["schedule"]["workflow_class"]["review_required"] is False

    assert catalog.class_names == ("fanout", "smoke")
    assert fanout_class.workflow_class_id == parallel_wave["workflow_class_id"]
    assert fanout_class.queue_shape == {
        "max_parallel": 2,
        "wave_kind": "parallel_default",
    }
    assert fanout_class.review_required is False

    assert len(parallel_wave["parallel_task_refs"]) == parallel_wave["max_parallel"] == 2
    assert proof_packet["native_truth_surfaces"] == [
        "python3 -m surfaces.cli.main native-operator status <run_id>",
        "python3 -m surfaces.cli.main native-operator inspect <run_id>",
        "canonical evidence and receipts in repo-local Postgres",
    ]
    assert seen["resolved_envs"] == [env]
    assert seen["closed_connections"] == 1
    assert [("FROM schedule_definitions" in query) for query, _ in seen["queries"][:2]] == [
        True,
        False,
    ]
    assert [("FROM workflow_classes" in query) for query, _ in seen["queries"][:2]] == [
        False,
        True,
    ]
