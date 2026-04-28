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
        workers: list[dict[str, Any]] | None = None,
        providers: list[dict[str, Any]] | None = None,
        leases: list[dict[str, Any]] | None = None,
        manifest_records: list[dict[str, Any]] | None = None,
        failures: list[dict[str, Any]] | None = None,
        raise_all: bool = False,
    ) -> None:
        self.queue = queue or {"pending": 0, "ready": 0, "claimed": 0, "running": 0}
        self.workers = workers or []
        self.providers = providers or []
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
        if "FROM workflow_jobs" in normalized and "GROUP BY status" in normalized:
            return self.workers
        if "FROM workflow_jobs" in normalized:
            return [self.queue]
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


def test_queued_work_without_fresh_worker_heartbeat_blocks_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)
    conn = _FakeConn(queue={"pending": 2, "ready": 0, "claimed": 0, "running": 0})

    result = runtime_truth.build_firecheck(conn)

    assert result["can_fire"] is False
    assert [item["code"] for item in result["blockers"]] == [
        "queued_without_fresh_worker_heartbeat"
    ]


def test_db_authority_unavailable_blocks_firecheck(monkeypatch) -> None:
    monkeypatch.setattr(runtime_truth, "_docker_snapshot", _docker_ok)

    result = runtime_truth.build_firecheck(_FakeConn(raise_all=True))

    assert result["can_fire"] is False
    assert result["blockers"][0]["code"] == "db_authority_unavailable"
    assert result["summary"]["db"] == "unavailable"


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
