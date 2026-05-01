from __future__ import annotations

from typing import Any

from runtime import runtime_truth
from runtime.operations.commands.runtime_remediation import (
    RuntimeRemediationApplyCommand,
    handle_runtime_remediation_apply,
)


class _FakeConn:
    def __init__(
        self,
        *,
        queue: dict[str, int] | None = None,
        db_pressure: dict[str, Any] | None = None,
        workers: list[dict[str, Any]] | None = None,
        providers: list[dict[str, Any]] | None = None,
        control_plane: list[dict[str, Any]] | None = None,
        leases: list[dict[str, Any]] | None = None,
        manifest_records: list[dict[str, Any]] | None = None,
        failures: list[dict[str, Any]] | None = None,
        raise_all: bool = False,
    ) -> None:
        self.queue = queue or {"pending": 0, "ready": 0, "claimed": 0, "running": 0}
        self.db_pressure = db_pressure or {
            "max_connections": 100,
            "cluster_connections": 12,
            "cluster_active_connections": 2,
            "cluster_idle_connections": 10,
            "database_connections": 12,
            "database_active_connections": 2,
            "database_idle_connections": 10,
        }
        self.workers = workers or []
        self.providers = providers or []
        self.control_plane = control_plane or []
        self.leases = leases or []
        self.manifest_records = manifest_records or []
        self.failures = failures or []
        self.raise_all = raise_all
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        if self.raise_all:
            raise RuntimeError("database unavailable")
        normalized = " ".join(query.split())
        if normalized == "SELECT 1 AS ok":
            return [{"ok": 1}]
        if "FROM registry_native_runtime_defaults" in normalized:
            return [{"runtime_profile_ref": "praxis"}]
        if "FROM registry_native_runtime_profile_authority" in normalized:
            return [
                {
                    "runtime_profile_ref": "praxis",
                    "workspace_ref": "workspace.praxis",
                    "instance_name": "Praxis",
                    "provider_name": "openai",
                    "provider_names": "[\"openai\"]",
                    "allowed_models": "[\"gpt-5.4\"]",
                    "receipts_dir": "artifacts/receipts",
                    "topology_dir": "artifacts/topology",
                    "repo_root": "/Users/nate/Praxis",
                    "workdir": "/Users/nate/Praxis",
                    "base_path_ref": "workspace.base.default",
                    "repo_root_path": "/Users/nate/Praxis",
                    "workdir_path": "/Users/nate/Praxis",
                    "base_path": "/Users/nate/Praxis",
                    "model_profile_id": "model_profile.default",
                    "provider_policy_id": "provider_policy.default",
                    "sandbox_profile_ref": "sandbox.default",
                }
            ]
        if "FROM pg_stat_activity" in normalized:
            return [self.db_pressure]
        if "FROM workflow_jobs" in normalized and "GROUP BY status" in normalized:
            return self.workers
        if "FROM workflow_jobs" in normalized:
            return [self.queue]
        if "FROM private_model_access_control_matrix" in normalized:
            return self.control_plane
        if "FROM provider_concurrency" in normalized:
            return self.providers
        if "FROM execution_leases" in normalized:
            return self.leases
        if "outputs ? 'workspace_manifest_audit'" in normalized:
            return self.manifest_records
        if "FROM receipts" in normalized and "status = 'failed'" in normalized:
            return self.failures
        raise AssertionError(normalized)


class _Subsystems:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    def get_pg_conn(self):
        return self._conn


class _RemediationConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE provider_concurrency"):
            return [{"provider_slug": "openai", "active_slots": 0.0, "max_concurrent": 4}]
        if normalized.startswith("DELETE FROM execution_leases"):
            return [{"lease_id": "lease_1", "resource_key": "host_resource:slot:1"}]
        raise AssertionError(normalized)


def _docker_ok() -> dict[str, Any]:
    return {"status": "ok", "available": True, "error": None}


def test_firecheck_is_ready_when_no_runtime_blockers(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)

    result = runtime_truth.build_firecheck(_FakeConn())

    assert result["can_fire"] is True
    assert result["fire_state"] == "ready"
    assert result["next_actions"][0]["plan"]["action"] == "launch_one_proof_then_scale"


def test_manifest_hydration_gap_blocks_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        manifest_records=[
            {
                "receipt_id": "receipt_1",
                "run_id": "run_1",
                "node_id": "job_a",
                "status": "failed",
                "failure_code": "sandbox_error",
                "workspace_manifest_audit": {
                    "intended_manifest_paths": ["Code&DBs/Workflow/runtime/foo.py"],
                    "hydrated_manifest_paths": [],
                    "missing_intended_paths": ["Code&DBs/Workflow/runtime/foo.py"],
                    "observed_file_read_refs": [],
                },
            }
        ]
    )

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is False
    assert result["fire_state"] == "blocked"
    assert {item["code"] for item in result["blockers"]} == {"context_not_hydrated"}
    assert result["next_actions"][0]["plan"]["retry_delta_required"] == (
        "repair read-scope hydration or regenerate the execution manifest"
    )


def test_succeeded_write_only_manifest_gap_does_not_block_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        manifest_records=[
            {
                "receipt_id": "receipt_2",
                "run_id": "run_2",
                "node_id": "job_b",
                "status": "succeeded",
                "failure_code": "",
                "workspace_manifest_audit": {
                    "intended_manifest_paths": ["artifacts/workflow/packet/CLOSEOUT.md"],
                    "hydrated_manifest_paths": [
                        "artifacts/workflow/packet/PLAN.md",
                        "artifacts/workflow/packet/EXECUTION.md",
                    ],
                    "missing_intended_paths": ["artifacts/workflow/packet/CLOSEOUT.md"],
                    "observed_file_read_refs": ["artifacts/workflow/packet/CLOSEOUT.md"],
                    "observed_file_read_mode": "provider_output_path_mentions",
                },
            }
        ]
    )

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is True
    assert result["fire_state"] == "ready"
    assert result["blockers"] == []


def test_succeeded_manifest_gap_with_authoritative_read_proof_blocks_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        manifest_records=[
            {
                "receipt_id": "receipt_3",
                "run_id": "run_3",
                "node_id": "job_c",
                "status": "succeeded",
                "failure_code": "",
                "workspace_manifest_audit": {
                    "intended_manifest_paths": ["runtime/context.py"],
                    "hydrated_manifest_paths": [],
                    "missing_intended_paths": ["runtime/context.py"],
                    "observed_file_read_refs": ["runtime/context.py"],
                    "observed_file_read_mode": "sandbox_trace",
                },
            }
        ]
    )

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is False
    assert result["fire_state"] == "blocked"
    assert {item["code"] for item in result["blockers"]} == {"context_not_hydrated"}


def test_superseded_failed_manifest_gap_does_not_block_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        manifest_records=[
            {
                "receipt_id": "receipt_success",
                "run_id": "run_4",
                "node_id": "job_d",
                "status": "succeeded",
                "failure_code": "",
                "finished_at": "2026-05-01T20:01:31+00:00",
                "workspace_manifest_audit": {
                    "intended_manifest_paths": ["runtime/context.py"],
                    "hydrated_manifest_paths": [".gemini/settings.json"],
                    "missing_intended_paths": ["runtime/context.py"],
                    "observed_file_read_refs": ["runtime/context.py"],
                    "observed_file_read_mode": "provider_output_path_mentions",
                },
            },
            {
                "receipt_id": "receipt_failed",
                "run_id": "run_4",
                "node_id": "job_d",
                "status": "failed",
                "failure_code": "workflow.timeout",
                "finished_at": "2026-05-01T19:55:33+00:00",
                "workspace_manifest_audit": {
                    "intended_manifest_paths": ["runtime/context.py"],
                    "hydrated_manifest_paths": [".gemini/settings.json"],
                    "missing_intended_paths": ["runtime/context.py"],
                    "observed_file_read_refs": [],
                    "observed_file_read_mode": "provider_output_path_mentions",
                },
            },
        ]
    )

    result = runtime_truth.build_firecheck(conn)
    records = result["snapshot"]["manifest_audit"]["records"]

    assert result["can_fire"] is True
    assert result["fire_state"] == "ready"
    assert result["blockers"] == []
    assert records[1]["actionable_missing_intended_paths"] == []
    assert records[1]["superseded_by_success"] is True
    assert records[1]["superseded_by_receipt_id"] == "receipt_success"


def test_queued_work_without_fresh_worker_heartbeat_blocks_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(queue={"pending": 2, "ready": 0, "claimed": 0, "running": 0})

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is False
    assert [item["code"] for item in result["blockers"]] == [
        "queued_without_fresh_worker_heartbeat"
    ]


def test_disabled_provider_capacity_is_not_reported_as_firecheck_blocker(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        providers=[
            {
                "provider_slug": "anthropic",
                "max_concurrent": 4,
                "active_slots": 4.0,
                "cost_weight_default": 1.0,
                "updated_at": "2026-04-28T23:47:10+00:00",
                "age_seconds": 30.0,
            }
        ],
        control_plane=[
            {
                "provider_slug": "anthropic",
                "any_control_on": False,
                "any_control_off": True,
            }
        ],
    )

    result = runtime_truth.build_firecheck(conn)
    provider_slots = result["snapshot"]["provider_slots"]

    assert result["can_fire"] is True
    assert "provider_capacity" not in {item["code"] for item in result["blockers"]}
    assert provider_slots["saturated_providers"] == []
    assert provider_slots["providers"][0]["provider_disabled"] is True


def test_db_authority_unavailable_blocks_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)

    result = runtime_truth.build_firecheck(_FakeConn(raise_all=True))

    assert result["can_fire"] is False
    assert result["blockers"][0]["code"] == "db_authority_unavailable"
    assert result["summary"]["db"] == "unavailable"


def test_db_connection_pressure_warns_before_connection_exhaustion(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        db_pressure={
            "max_connections": 100,
            "cluster_connections": 88,
            "cluster_active_connections": 8,
            "cluster_idle_connections": 80,
            "database_connections": 86,
            "database_active_connections": 7,
            "database_idle_connections": 79,
        }
    )

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is True
    assert result["fire_state"] == "degraded"
    assert "db_pool_pressure" in {item["code"] for item in result["blockers"]}
    assert result["summary"]["db_connection_pressure"] == "warning"
    assert result["summary"]["db_free_connection_slots"] == 12


def test_db_connection_pressure_blocks_when_slots_are_exhausted(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(
        db_pressure={
            "max_connections": 100,
            "cluster_connections": 100,
            "cluster_active_connections": 9,
            "cluster_idle_connections": 91,
            "database_connections": 98,
            "database_active_connections": 8,
            "database_idle_connections": 90,
        }
    )

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is False
    assert result["fire_state"] == "blocked"
    assert any(
        item["code"] == "db_pool_pressure" and item["severity"] == "critical"
        for item in result["blockers"]
    )


def test_remediation_plan_declares_retry_delta() -> None:
    result = runtime_truth.build_remediation_plan(
        None,
        failure_code="host_resource_capacity",
    )

    assert result["failure_type"] == "host_resource_capacity"
    assert result["plan"]["tier"] == "controlled_auto"
    assert result["plan"]["retry_delta_required"] == "capacity window changed; no spec mutation"


def test_runtime_remediation_apply_dry_run_does_not_mutate() -> None:
    conn = _RemediationConn()

    result = handle_runtime_remediation_apply(
        RuntimeRemediationApplyCommand(failure_type="provider.capacity"),
        _Subsystems(conn),
    )

    assert result["status"] == "planned"
    assert result["applied"] is False
    assert result["actions"][0]["action"] == "reap_stale_provider_slots"
    assert conn.calls == []


def test_runtime_remediation_apply_cleans_stale_provider_slots() -> None:
    conn = _RemediationConn()

    result = handle_runtime_remediation_apply(
        RuntimeRemediationApplyCommand(
            failure_type="provider.capacity",
            dry_run=False,
            confirm=True,
        ),
        _Subsystems(conn),
    )

    assert result["status"] == "applied"
    assert result["applied"] is True
    assert result["actions"][0]["row_count"] == 1
    assert "UPDATE provider_concurrency" in conn.calls[0][0]
    assert result["retry_delta_required"] == "provider slot or route changed"


def test_runtime_remediation_apply_refuses_human_gated_failures() -> None:
    conn = _RemediationConn()

    result = handle_runtime_remediation_apply(
        RuntimeRemediationApplyCommand(
            failure_type="credential_error",
            dry_run=False,
            confirm=True,
        ),
        _Subsystems(conn),
    )

    assert result["status"] == "blocked"
    assert result["actions"][0]["status"] == "blocked"
    assert conn.calls == []
