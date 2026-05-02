from __future__ import annotations

import concurrent.futures
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import inspect
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from runtime import task_type_router
import runtime._workflow_database as runtime_db
import runtime.materialize_index as compile_index
from runtime.idempotency import canonical_hash
import runtime.retry_orchestrator as retry_orchestrator
from runtime.domain import RouteIdentity
from receipts import EvidenceRow, ReceiptV1
from runtime.shadow_execution_packet import inspect_shadow_execution_packets
from runtime.workflow import unified as unified_dispatch
from runtime.workflow import _admission as _admission_mod
from registry.runtime_profile_admission import RuntimeProfileAdmissionError
from runtime.workflow import _context_building as _ctx_mod
from runtime.workflow import _execution_core as _exec_mod
from runtime.workflow.submission_gate import SubmissionGateResult as _SubmissionGateResult
from runtime.workflow import _worker_loop as _wloop_mod
from runtime.workflow import _claiming as _claiming_mod
from runtime.workflow import _shared as _shared_mod
from runtime.workflow.worker import WorkflowWorker
import storage.postgres as storage_postgres
from storage.postgres import connection as pg_connection
from surfaces.cli.workflow_runner import WorkflowSpec

unified_workflow = unified_dispatch


@pytest.fixture(autouse=True)
def _patch_task_profile_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    from adapters import task_profiles

    profiles = {
        "general": task_profiles.TaskProfile(
            task_type="general",
            allowed_tools=("rg", "pytest"),
            default_tier="mid",
            file_attach=True,
            system_prompt_hint="",
        ),
        "build": task_profiles.TaskProfile(
            task_type="build",
            allowed_tools=("rg", "pytest"),
            default_tier="mid",
            file_attach=True,
            system_prompt_hint="",
        ),
        "code_generation": task_profiles.TaskProfile(
            task_type="code_generation",
            allowed_tools=("rg", "pytest"),
            default_tier="mid",
            file_attach=True,
            system_prompt_hint="",
        ),
        "creative": task_profiles.TaskProfile(
            task_type="creative",
            allowed_tools=(),
            default_tier="mid",
            file_attach=False,
            system_prompt_hint="",
        ),
    }
    keywords = [
        (("implement", "fix", "debug", "test", "code"), "code_generation", (), ()),
        (("write", "draft", "story"), "creative", (), ()),
    ]
    monkeypatch.setattr(task_profiles, "_PROFILES_DB_LOADED", True)
    monkeypatch.setattr(task_profiles, "_DB_TASK_PROFILES", profiles)
    monkeypatch.setattr(task_profiles, "_DB_TASK_TYPE_KEYWORDS", keywords)


@pytest.fixture(autouse=True)
def _patch_repo_policy_contract_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _ctx_mod,
        "get_repo_policy_contract",
        lambda _conn, *, repo_root: None,
    )


class _FakeConn:
    def __init__(
        self,
        *,
        existing_idempotency: dict[str, dict] | None = None,
        existing_jobs: dict[tuple[str, str], dict] | None = None,
        existing_run_states: dict[str, str] | None = None,
        workflow_run_rows: list[dict[str, object]] | None = None,
        compile_artifact_rows: list[dict[str, object]] | None = None,
        execution_packet_rows: list[dict[str, object]] | None = None,
        queue_depth: int = 0,
    ) -> None:
        self.next_job_id = 0
        self.edge_inserts: list[tuple[int, int]] = []
        self.existing_idempotency = existing_idempotency or {}
        self.existing_jobs = existing_jobs or {}
        self.existing_run_states = existing_run_states or {}
        self.workflow_run_rows = workflow_run_rows or []
        self.compile_artifact_rows = compile_artifact_rows or []
        self.execution_packet_rows = execution_packet_rows or []
        self.queue_depth = queue_depth
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, query: str, *args):
        self.queries.append((query, args))
        if "FROM compile_artifacts" in query or "FROM materialize_artifacts" in query:
            artifact_kind = args[0]
            input_fingerprint = args[1]
            return [
                {
                    **row,
                    "materialize_artifact_id": row.get("materialize_artifact_id")
                    or row.get("compile_artifact_id"),
                }
                for row in self.compile_artifact_rows
                if row["artifact_kind"] == artifact_kind and row["input_fingerprint"] == input_fingerprint
            ]
        if "FROM execution_packets" in query:
            run_id = args[0]
            return [row for row in self.execution_packet_rows if row["run_id"] == run_id]
        if "FROM workflow_runs" in query and "request_envelope->>'name'" in query:
            spec_name = args[0]
            limit = int(args[1]) if len(args) > 1 else 50
            rows = [
                row
                for row in self.workflow_run_rows
                if row.get("spec_name") == spec_name
                and row.get("started_at") is not None
                and row.get("finished_at") is not None
            ]
            return rows[:limit]
        if "FROM route_policy_registry" in query:
            return [{
                "task_rank_weight": 0.35,
                "route_health_weight": 0.40,
                "cost_weight": 0.10,
                "benchmark_weight": 0.15,
                "prefer_cost_task_rank_weight": 0.25,
                "prefer_cost_route_health_weight": 0.35,
                "prefer_cost_cost_weight": 0.30,
                "prefer_cost_benchmark_weight": 0.10,
                "claim_route_health_weight": 0.55,
                "claim_rank_weight": 0.30,
                "claim_load_weight": 0.15,
                "claim_internal_failure_penalty_step": 0.08,
                "claim_priority_penalty_step": 0.01,
                "neutral_benchmark_score": 0.50,
                "mixed_benchmark_score": 0.55,
                "neutral_route_health": 0.65,
                "min_route_health": 0.05,
                "max_route_health": 1.0,
                "success_health_bump": 0.04,
                "review_success_bump": 0.02,
                "consecutive_failure_penalty_step": 0.08,
                "consecutive_failure_penalty_cap": 0.20,
                "internal_failure_penalties": {"verification_failed": 0.25, "unknown": 0.10},
                "review_severity_penalties": {"high": 0.15, "medium": 0.08, "low": 0.03},
            }]
        if "FROM failure_category_zones" in query:
            return [{"category": "verification_failed", "zone": "internal"}]
        if "FROM task_type_route_profiles" in query:
            return []
        if "FROM market_benchmark_metric_registry" in query:
            return []
        if "FROM idempotency_ledger" in query:
            row = self.existing_idempotency.get((args[0], args[1]))
            return [row] if row else []
        if "SELECT COUNT(*) FROM workflow_jobs WHERE status IN ('pending', 'ready')" in query:
            return [{"count": self.queue_depth}]
        if "SELECT current_state" in query and "FROM workflow_runs" in query:
            state = self.existing_run_states.get(args[0])
            return [{"current_state": state}] if state else []
        if "SELECT * FROM workflow_runs WHERE run_id = $1" in query:
            return [
                row for row in self.workflow_run_rows
                if str(row.get("run_id") or "") == str(args[0])
            ]
        if "SELECT id, label, attempt, status" in query and "FROM workflow_jobs" in query:
            row = self.existing_jobs.get((args[0], args[1]))
            if row and row.get("status") in {"failed", "dead_letter", "cancelled"}:
                return [{
                    "id": row.get("id"),
                    "label": row.get("label", ""),
                    "attempt": row.get("attempt", 0),
                    "status": row.get("status", ""),
                }]
            return []
        if "SELECT id, prompt, agent_slug, resolved_agent, status FROM workflow_jobs" in query:
            row = self.existing_jobs.get((args[0], args[1]))
            return [row] if row else []
        if "INSERT INTO workflow_jobs" in query:
            self.next_job_id += 1
            return [{"id": self.next_job_id}]
        if "INSERT INTO idempotency_ledger" in query:
            return []
        if "INSERT INTO workflow_job_edges" in query:
            self.edge_inserts.append((args[0], args[1]))
            return []
        if "SELECT parent_id, child_id" in query and "FROM workflow_job_edges" in query:
            child_ids = set(args[0] or [])
            return [
                {"parent_id": parent_id, "child_id": child_id}
                for parent_id, child_id in self.edge_inserts
                if child_id in child_ids
            ]
        if "UPDATE workflow_jobs" in query and "SET status = 'ready'" in query and len(args) == 2:
            row = self.existing_jobs.get((args[0], args[1]))
            if row:
                return [{"id": row["id"], "label": row.get("label", ""), "attempt": row.get("attempt", 0) + 1}]
            return []
        if "UPDATE workflow_jobs" in query and "SET status = 'ready'" in query and len(args) == 1:
            return []
        if "UPDATE workflow_runs" in query and "SET current_state = 'claim_accepted'" in query:
            for row in self.workflow_run_rows:
                if str(row.get("run_id") or "") == str(args[0]) and row.get("current_state") in {
                    "failed",
                    "dead_letter",
                    "cancelled",
                }:
                    row["current_state"] = "claim_accepted"
                    row["terminal_reason_code"] = None
                    row["finished_at"] = None
                    return [{"run_id": args[0]}]
            return []
        if "SELECT pg_notify" in query:
            return []
        return []

    def fetchrow(self, query: str, *args):
        self.queries.append((query, args))
        if "FROM operator_repo_policy_contracts" in query:
            return None
        raise AssertionError(f"Unexpected fetchrow: {' '.join(query.split())}")


def test_definition_version_for_hash_is_positive_int32_and_deterministic():
    definition_hash = "90000000" + ("abcdef12" * 7)

    version = unified_dispatch._definition_version_for_hash(definition_hash)

    assert version == unified_dispatch._definition_version_for_hash(definition_hash)
    assert 0 < version <= 2_147_483_647


def test_get_run_status_includes_job_timestamps_and_classification_fields():
    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [{
                    "run_id": "dispatch_abc",
                    "spec_name": "status-spec",
                    "status": "running",
                    "total_jobs": 2,
                    "created_at": datetime.now(timezone.utc) - timedelta(minutes=4),
                    "finished_at": None,
                }]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return [
                    {
                        "id": 1,
                        "label": "job-a",
                        "agent_slug": "agent-a",
                        "resolved_agent": "agent-a",
                        "status": "running",
                        "attempt": 1,
                        "max_attempts": 3,
                        "last_error_code": "rate_limited",
                        "failure_category": "external_quota",
                        "failure_zone": "rate_limits",
                        "is_transient": True,
                        "duration_ms": 1500,
                        "cost_usd": 0.01,
                        "token_input": 4,
                        "token_output": 2,
                        "stdout_preview": "x",
                        "created_at": datetime.now(timezone.utc) - timedelta(seconds=90),
                        "ready_at": datetime.now(timezone.utc) - timedelta(seconds=80),
                        "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=70),
                        "started_at": datetime.now(timezone.utc) - timedelta(seconds=60),
                        "finished_at": None,
                        "heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=5),
                        "next_retry_at": None,
                        "claimed_by": "worker-1",
                    },
                    {
                        "id": 2,
                        "label": "job-b",
                        "agent_slug": "agent-b",
                        "resolved_agent": "agent-b",
                        "status": "succeeded",
                        "attempt": 1,
                        "max_attempts": 3,
                        "last_error_code": "",
                        "failure_category": "",
                        "failure_zone": "",
                        "is_transient": False,
                        "duration_ms": 1200,
                        "cost_usd": 0.0,
                        "token_input": 3,
                        "token_output": 3,
                        "stdout_preview": "",
                        "created_at": datetime.now(timezone.utc) - timedelta(seconds=70),
                        "ready_at": datetime.now(timezone.utc) - timedelta(seconds=65),
                        "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=55),
                        "started_at": datetime.now(timezone.utc) - timedelta(seconds=45),
                        "finished_at": datetime.now(timezone.utc) - timedelta(seconds=30),
                        "heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=31),
                        "next_retry_at": None,
                        "claimed_by": "worker-1",
                    },
                ]
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.01,
                    "total_tokens_in": 7,
                    "total_tokens_out": 5,
                    "total_duration_ms": 2700,
                }]
            raise AssertionError(f"Unexpected query: {normalized}")

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_abc")
    assert status is not None
    assert status["run_id"] == "dispatch_abc"
    assert status["jobs"][0]["ready_at"] is not None
    assert status["jobs"][1]["finished_at"] is not None
    assert status["total_cost_usd"] == 0.01


def test_get_run_status_does_not_backfill_missing_failure_fields():
    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [{
                    "run_id": "dispatch_missing_failure_fields",
                    "spec_name": "status-spec",
                    "status": "failed",
                    "total_jobs": 1,
                    "created_at": datetime.now(timezone.utc) - timedelta(minutes=4),
                    "finished_at": datetime.now(timezone.utc) - timedelta(minutes=1),
                }]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return [{
                    "id": 1,
                    "label": "job-a",
                    "agent_slug": "agent-a",
                    "resolved_agent": "agent-a",
                    "status": "failed",
                    "attempt": 1,
                    "max_attempts": 3,
                    "last_error_code": "rate_limited",
                    "failure_category": "",
                    "failure_zone": "",
                    "is_transient": False,
                    "duration_ms": 1500,
                    "cost_usd": 0.01,
                    "token_input": 4,
                    "token_output": 2,
                    "stdout_preview": "HTTP 429 rate limit exceeded",
                    "created_at": datetime.now(timezone.utc) - timedelta(seconds=90),
                    "ready_at": datetime.now(timezone.utc) - timedelta(seconds=80),
                    "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=70),
                    "started_at": datetime.now(timezone.utc) - timedelta(seconds=60),
                    "finished_at": datetime.now(timezone.utc) - timedelta(seconds=30),
                    "heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=31),
                    "next_retry_at": None,
                    "claimed_by": "worker-1",
                }]
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.01,
                    "total_tokens_in": 4,
                    "total_tokens_out": 2,
                    "total_duration_ms": 1500,
                }]
            raise AssertionError(f"Unexpected query: {normalized}")

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_missing_failure_fields")
    health = unified_dispatch.summarize_run_health(status, datetime.now(timezone.utc))

    assert status is not None
    assert status["jobs"][0]["last_error_code"] == "rate_limited"
    assert status["jobs"][0]["failure_category"] == ""
    assert status["jobs"][0]["failure_zone"] == ""
    assert status["jobs"][0]["is_transient"] is False
    assert health["non_retryable_failed_jobs"] == []


def test_get_run_status_attaches_latest_submission_summary(monkeypatch) -> None:
    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [{
                    "run_id": "dispatch_submission",
                    "spec_name": "status-spec",
                    "status": "running",
                    "total_jobs": 1,
                    "created_at": datetime.now(timezone.utc) - timedelta(minutes=4),
                    "finished_at": None,
                }]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return [{
                    "id": 1,
                    "label": "job-a",
                    "agent_slug": "agent-a",
                    "resolved_agent": "agent-a",
                    "status": "running",
                    "attempt": 1,
                    "max_attempts": 3,
                    "last_error_code": "",
                    "failure_category": "",
                    "failure_zone": "",
                    "is_transient": False,
                    "duration_ms": 0,
                    "cost_usd": 0.0,
                    "token_input": 0,
                    "token_output": 0,
                    "stdout_preview": "",
                    "created_at": datetime.now(timezone.utc) - timedelta(seconds=90),
                    "ready_at": datetime.now(timezone.utc) - timedelta(seconds=80),
                    "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=70),
                    "started_at": datetime.now(timezone.utc) - timedelta(seconds=60),
                    "finished_at": None,
                    "heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=5),
                    "next_retry_at": None,
                    "claimed_by": "worker-1",
                }]
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.0,
                    "total_tokens_in": 0,
                    "total_tokens_out": 0,
                    "total_duration_ms": 0,
                }]
            raise AssertionError(f"Unexpected query: {normalized}")

    monkeypatch.setattr(
        _claiming_mod,
        "_submission_list_latest_submission_summaries_for_run",
        lambda _conn, *, run_id: {
            "job-a": {
                "submission_id": "submission-1",
                "job_label": "job-a",
                "comparison_status": "matched",
                "measured_summary": {"update": 1, "total": 1},
                "latest_review": {"decision": "approve"},
                "review_timeline": [{"review_id": "review-1", "decision": "approve"}],
            }
        },
    )

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_submission")

    assert status is not None
    assert status["jobs"][0]["submission_id"] == "submission-1"
    assert status["jobs"][0]["submission"]["latest_review"]["decision"] == "approve"


def test_summarize_run_health_projects_canonical_failure_category_over_legacy_error_code():
    now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    run_data = {
        "run_id": "dispatch_abc",
        "status": "failed",
        "created_at": now - timedelta(minutes=5),
        "total_jobs": 1,
        "jobs": [
            {
                "label": "job-a",
                "status": "failed",
                "last_error_code": "rate_limited",
                "failure_category": "credential_error",
                "failure_zone": "config",
                "is_transient": False,
                "created_at": now - timedelta(minutes=4),
                "ready_at": now - timedelta(minutes=4),
                "claimed_at": now - timedelta(minutes=3),
                "started_at": now - timedelta(minutes=2),
                "finished_at": now - timedelta(minutes=1),
                "heartbeat_at": now - timedelta(minutes=1),
            }
        ],
    }

    health = unified_dispatch.summarize_run_health(run_data, now)

    assert health["non_retryable_failed_jobs"] == ["job-a"]


def test_summarize_run_recovery_surfaces_retry_for_orchestration_envelope_failure():
    now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    run_data = {
        "run_id": "dispatch_orchestration_retry",
        "status": "failed",
        "created_at": now - timedelta(minutes=5),
        "total_jobs": 2,
        "jobs": [
            {
                "label": "phase_001_workspace_boundary_contract",
                "status": "failed",
                "last_error_code": "execution_exception",
                "failure_category": "",
                "failure_zone": "",
                "is_transient": False,
                "stdout_preview": "TypeError: failure_code must be a non-empty string",
                "created_at": now - timedelta(minutes=4),
                "ready_at": now - timedelta(minutes=4),
                "claimed_at": now - timedelta(minutes=3),
                "started_at": now - timedelta(minutes=2),
                "finished_at": now - timedelta(minutes=1),
                "heartbeat_at": now - timedelta(minutes=1),
            },
            {
                "label": "phase_001_010_synthesis",
                "status": "cancelled",
                "last_error_code": "dependency_blocked",
                "failure_category": "",
                "failure_zone": "",
                "is_transient": False,
                "stdout_preview": "",
                "created_at": now - timedelta(minutes=4),
                "ready_at": None,
                "claimed_at": None,
                "started_at": None,
                "finished_at": now - timedelta(minutes=1),
                "heartbeat_at": None,
            },
        ],
    }

    health = unified_dispatch.summarize_run_health(run_data, now)
    recovery = unified_dispatch.summarize_run_recovery(run_data, health, now)

    assert recovery["mode"] == "retry_failed_job"
    assert recovery["heal_action"] == "fix_and_retry"
    assert recovery["resolved_failure_code"] == "orchestration.failure_code_missing"
    assert recovery["recommended_tool"]["arguments"] == {
        "action": "retry",
        "run_id": "dispatch_orchestration_retry",
        "label": "phase_001_workspace_boundary_contract",
    }


def test_summarize_run_recovery_surfaces_live_stream_for_active_runs():
    now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    run_data = {
        "run_id": "dispatch_live_stream",
        "status": "running",
        "created_at": now - timedelta(minutes=1),
        "total_jobs": 1,
        "jobs": [
            {
                "label": "build",
                "status": "running",
                "created_at": now - timedelta(minutes=1),
                "claimed_at": now - timedelta(seconds=30),
                "started_at": now - timedelta(seconds=20),
                "heartbeat_at": now - timedelta(seconds=5),
            }
        ],
    }

    health = unified_dispatch.summarize_run_health(run_data, now)
    recovery = unified_dispatch.summarize_run_recovery(run_data, health, now)

    assert recovery["mode"] == "monitor"
    assert recovery["live_stream"] == {
        "cli_command": "praxis workflow stream dispatch_live_stream",
        "stream_url": "/api/workflow-runs/dispatch_live_stream/stream",
        "status_command": "praxis workflow run-status dispatch_live_stream --summary",
    }


def test_summarize_run_health_treats_recent_running_job_without_heartbeat_as_active():
    now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    run_data = {
        "run_id": "dispatch_recent_running_without_heartbeat",
        "status": "running",
        "created_at": now - timedelta(minutes=4),
        "total_jobs": 1,
        "jobs": [
            {
                "label": "run",
                "status": "running",
                "created_at": now - timedelta(minutes=4),
                "claimed_at": now - timedelta(seconds=110),
                "started_at": now - timedelta(seconds=90),
                "heartbeat_at": None,
            }
        ],
    }

    health = unified_dispatch.summarize_run_health(run_data, now)

    assert health["state"] == "healthy"
    assert health["likely_failed"] is False
    assert health["running_or_claimed"] == 1
    assert not any(signal["type"] == "stale_claimed_jobs" for signal in health["signals"])


def test_summarize_run_health_flags_running_job_without_activity_as_stale():
    now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    run_data = {
        "run_id": "dispatch_stale_running_without_heartbeat",
        "status": "running",
        "created_at": now - timedelta(minutes=8),
        "total_jobs": 1,
        "jobs": [
            {
                "label": "run",
                "status": "running",
                "created_at": now - timedelta(minutes=8),
                "claimed_at": now - timedelta(minutes=5),
                "started_at": now - timedelta(minutes=4),
                "heartbeat_at": None,
            }
        ],
    }

    health = unified_dispatch.summarize_run_health(run_data, now)

    assert health["state"] == "degraded"
    assert health["likely_failed"] is True
    assert health["stalled_jobs"]["claimed"] == ["run"]
    assert any(signal["type"] == "stale_claimed_jobs" for signal in health["signals"])


def test_get_run_status_includes_shadow_packet_inspection_and_drift():
    run_row = {
        "run_id": "dispatch_shadow_abc",
        "workflow_id": "workflow.shadow",
        "request_id": "request.shadow",
        "workflow_definition_id": "workflow_definition.shadow.v1",
        "current_state": "claim_accepted",
        "request_envelope": {
            "name": "shadow-spec",
            "spec_snapshot": {
                "definition_revision": "definition.rev.exec",
                "plan_revision": "plan.rev.exec",
                "verify_refs": ["verify.exec"],
                "packet_provenance": {
                    "source_kind": "shadow_execution",
                    "workspace_ref": "workspace.alpha",
                    "runtime_profile_ref": "runtime.alpha",
                },
            },
        },
        "requested_at": datetime.now(timezone.utc) - timedelta(minutes=4),
        "admitted_at": datetime.now(timezone.utc) - timedelta(minutes=3, seconds=30),
        "started_at": datetime.now(timezone.utc) - timedelta(minutes=2),
        "finished_at": None,
    }
    packet_payload = {
        "definition_revision": "definition.rev.exec",
        "plan_revision": "plan.rev.packet",
        "packet_version": 1,
        "workflow_id": "workflow.shadow",
        "run_id": "dispatch_shadow_abc",
        "spec_name": "shadow-spec",
        "source_kind": "shadow_execution",
        "authority_refs": ["definition.rev.exec", "plan.rev.packet"],
        "model_messages": [
            {
                "job_label": "job-shadow",
                "adapter_type": "chat",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "system seed"},
                    {"role": "user", "content": "inspect me"},
                ],
            }
        ],
        "reference_bindings": [
            {
                "binding_kind": "model_input",
                "ref": "binding:model.shadow",
            }
        ],
        "capability_bindings": [
            {
                "binding_kind": "filesystem",
                "ref": "capability:fs.read",
            }
        ],
        "verify_refs": ["verify.packet"],
        "packet_provenance": {
            "source_kind": "shadow_execution",
            "workspace_ref": "workspace.alpha",
            "runtime_profile_ref": "runtime.alpha",
        },
        "authority_inputs": {
            "packet_provenance": {
                "source_kind": "shadow_execution",
                "workspace_ref": "workspace.alpha",
                "runtime_profile_ref": "runtime.alpha",
            },
        },
        "file_inputs": {
            "workdir": "/tmp/work",
            "scope_read": ["input.py"],
            "scope_write": ["output.py"],
        },
        "packet_hash": "packet.hash.shadow",
        "packet_revision": "packet.rev.shadow.1",
        "decision_ref": "decision.packet.shadow.1",
    }

    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [dict(run_row)]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return []
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.0,
                    "total_tokens_in": 0,
                    "total_tokens_out": 0,
                    "total_duration_ms": 0,
                }]
            if "FROM execution_packets" in normalized:
                return [{"packets": [dict(packet_payload)]}]
            raise AssertionError(f"Unexpected query: {normalized}")

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_shadow_abc")
    assert status is not None
    expected_inspection = inspect_shadow_execution_packets([packet_payload], run_row=run_row)
    assert status["packet_inspection"] == expected_inspection
    assert status["packet_inspection"]["packet_revision"] == "packet.rev.shadow.1"
    assert status["packet_inspection"]["current_packet"]["model_messages"][0]["messages"][1]["content"] == "inspect me"
    assert status["packet_inspection"]["current_packet"]["reference_bindings"][0]["ref"] == "binding:model.shadow"
    assert status["packet_inspection"]["drift"]["status"] == "drifted"


def test_get_run_status_uses_jsonb_aggregation_for_execution_packets():
    run_row = {
        "run_id": "dispatch_shadow_jsonb",
        "workflow_id": "workflow.shadow",
        "request_id": "request.shadow",
        "workflow_definition_id": "workflow_definition.shadow.v1",
        "current_state": "claim_accepted",
        "request_envelope": {
            "name": "shadow-spec",
            "spec_snapshot": {
                "definition_revision": "definition.rev.exec",
                "plan_revision": "plan.rev.exec",
            },
        },
        "requested_at": datetime.now(timezone.utc) - timedelta(minutes=2),
    }
    packet_payload = {
        "definition_revision": "definition.rev.exec",
        "plan_revision": "plan.rev.exec",
        "packet_version": 1,
        "workflow_id": "workflow.shadow",
        "run_id": "dispatch_shadow_jsonb",
        "spec_name": "shadow-spec",
        "source_kind": "shadow_execution",
    }

    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [dict(run_row)]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return []
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.0,
                    "total_tokens_in": 0,
                    "total_tokens_out": 0,
                    "total_duration_ms": 0,
                }]
            if "FROM execution_packets" in normalized:
                assert "jsonb_agg(payload ORDER BY created_at, execution_packet_id)" in normalized
                assert "'[]'::jsonb" in normalized
                return [{"packets": [dict(packet_payload)]}]
            raise AssertionError(f"Unexpected query: {normalized}")

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_shadow_jsonb")

    assert status is not None
    assert status["packet_inspection"]["packet_revision"] is None


def test_get_run_status_ignores_legacy_verify_snapshot_bindings():
    run_row = {
        "run_id": "dispatch_shadow_legacy",
        "workflow_id": "workflow.shadow",
        "request_id": "request.shadow",
        "workflow_definition_id": "workflow_definition.shadow.v1",
        "current_state": "claim_accepted",
        "request_envelope": {
            "name": "shadow-spec",
            "spec_snapshot": {
                "definition_revision": "definition.rev.exec",
                "plan_revision": "plan.rev.exec",
                "verify": [
                    {
                        "verification_ref": "verification.python.py_compile",
                        "inputs": {"path": "legacy.py"},
                    }
                ],
                "packet_provenance": {
                    "source_kind": "shadow_execution",
                    "workspace_ref": "workspace.alpha",
                    "runtime_profile_ref": "runtime.alpha",
                },
            },
        },
        "requested_at": datetime.now(timezone.utc) - timedelta(minutes=4),
        "admitted_at": datetime.now(timezone.utc) - timedelta(minutes=3, seconds=30),
        "started_at": datetime.now(timezone.utc) - timedelta(minutes=2),
        "finished_at": None,
    }
    packet_payload = {
        "definition_revision": "definition.rev.exec",
        "plan_revision": "plan.rev.exec",
        "packet_version": 1,
        "workflow_id": "workflow.shadow",
        "run_id": "dispatch_shadow_legacy",
        "spec_name": "shadow-spec",
        "source_kind": "shadow_execution",
        "authority_refs": ["definition.rev.exec", "plan.rev.exec"],
        "model_messages": [
            {
                "job_label": "job-shadow",
                "adapter_type": "chat",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "system seed"},
                    {"role": "user", "content": "inspect me"},
                ],
            }
        ],
        "reference_bindings": [
            {
                "binding_kind": "model_input",
                "ref": "binding:model.shadow",
            }
        ],
        "capability_bindings": [
            {
                "binding_kind": "filesystem",
                "ref": "capability:fs.read",
            }
        ],
        "verify_refs": [],
        "packet_provenance": {
            "source_kind": "shadow_execution",
            "workspace_ref": "workspace.alpha",
            "runtime_profile_ref": "runtime.alpha",
        },
        "authority_inputs": {
            "packet_provenance": {
                "source_kind": "shadow_execution",
                "workspace_ref": "workspace.alpha",
                "runtime_profile_ref": "runtime.alpha",
            },
        },
        "file_inputs": {
            "workdir": "/tmp/work",
            "scope_read": ["input.py"],
            "scope_write": ["output.py"],
        },
        "packet_hash": "packet.hash.shadow",
        "packet_revision": "packet.rev.shadow.1",
        "decision_ref": "decision.packet.shadow.1",
    }

    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [dict(run_row)]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return []
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.0,
                    "total_tokens_in": 0,
                    "total_tokens_out": 0,
                    "total_duration_ms": 0,
                }]
            if "FROM execution_packets" in normalized:
                return [{"packets": [dict(packet_payload)]}]
            raise AssertionError(f"Unexpected query: {normalized}")

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_shadow_legacy")
    assert status is not None
    expected_inspection = inspect_shadow_execution_packets([packet_payload], run_row=run_row)
    assert status["packet_inspection"] == expected_inspection
    assert status["packet_inspection"]["drift"]["status"] == "aligned"
    assert all(diff["field"] != "verify_refs" for diff in status["packet_inspection"]["drift"]["differences"])


def test_get_run_status_graph_receipts_use_runtime_database_authority(monkeypatch) -> None:
    run_row = {
        "run_id": "dispatch_graph_receipts",
        "workflow_id": "workflow.graph",
        "request_id": "request.graph",
        "current_state": "succeeded",
        "request_envelope": {
            "name": "graph-spec",
            "nodes": [
                {
                    "node_id": "run",
                    "adapter_type": "cli_llm",
                    "position_index": 0,
                    "display_name": "run",
                }
            ],
        },
        "requested_at": datetime.now(timezone.utc) - timedelta(seconds=10),
        "started_at": datetime.now(timezone.utc) - timedelta(seconds=9),
        "finished_at": datetime.now(timezone.utc) - timedelta(seconds=1),
    }
    route_identity = RouteIdentity(
        workflow_id="workflow.graph",
        run_id="dispatch_graph_receipts",
        request_id="request.graph",
        authority_context_ref="authority.graph",
        authority_context_digest="digest.graph",
        claim_id="claim.graph",
        attempt_no=1,
        transition_seq=1,
    )
    started_at = datetime.now(timezone.utc) - timedelta(seconds=8)
    finished_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    receipt = ReceiptV1(
        receipt_id="receipt.graph.1",
        receipt_type="node_execution_receipt",
        schema_version=1,
        workflow_id="workflow.graph",
        run_id="dispatch_graph_receipts",
        request_id="request.graph",
        route_identity=route_identity,
        transition_seq=1,
        evidence_seq=2,
        started_at=started_at,
        finished_at=finished_at,
        executor_type="cli_llm",
        status="succeeded",
        inputs={},
        outputs={"result": "CURSOR_WORKFLOW_OK"},
        node_id="run",
        attempt_no=1,
    )
    evidence_row = EvidenceRow(
        kind="receipt",
        evidence_seq=2,
        row_id="receipt.graph.1",
        route_identity=route_identity,
        transition_seq=1,
        record=receipt,
    )
    captured: dict[str, object] = {}

    class _FakeReader:
        def __init__(self, *, database_url=None, env=None):
            captured["database_url"] = database_url
            captured["env"] = env

        def evidence_timeline(self, run_id: str):
            captured["run_id"] = run_id
            return (evidence_row,)

    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [dict(run_row)]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return []
            if "COALESCE(SUM(cost_usd)" in normalized:
                return [{
                    "total_cost": 0.0,
                    "total_tokens_in": 0,
                    "total_tokens_out": 0,
                    "total_duration_ms": 0,
                }]
            if "FROM execution_packets" in normalized:
                return [{"packets": []}]
            raise AssertionError(f"Unexpected query: {normalized}")

    monkeypatch.setattr(
        runtime_db,
        "resolve_runtime_database_url",
        lambda *, required=True, **_: "postgresql://postgres@repo.test/workflow",
    )
    monkeypatch.setattr(storage_postgres, "PostgresEvidenceReader", _FakeReader)

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_graph_receipts")

    assert status is not None
    assert captured == {
        "database_url": "postgresql://postgres@repo.test/workflow",
        "env": None,
        "run_id": "dispatch_graph_receipts",
    }
    assert status["completed_jobs"] == 1
    assert status["jobs"][0]["status"] == "succeeded"
    assert status["jobs"][0]["attempt"] == 1
    assert "CURSOR_WORKFLOW_OK" in status["jobs"][0]["stdout_preview"]


def test_get_run_status_prefers_graph_receipts_over_stale_legacy_jobs(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    run_row = {
        "run_id": "dispatch_graph_stale_jobs",
        "workflow_id": "workflow.graph",
        "request_id": "request.graph",
        "current_state": "succeeded",
        "request_envelope": {
            "name": "graph-spec",
            "nodes": [
                {
                    "node_id": "http_echo",
                    "adapter_type": "api_task",
                    "position_index": 0,
                    "display_name": "http_echo",
                }
            ],
        },
        "requested_at": now - timedelta(seconds=10),
        "started_at": now - timedelta(seconds=9),
        "finished_at": now - timedelta(seconds=1),
    }
    route_identity = RouteIdentity(
        workflow_id="workflow.graph",
        run_id="dispatch_graph_stale_jobs",
        request_id="request.graph",
        authority_context_ref="authority.graph",
        authority_context_digest="digest.graph",
        claim_id="claim.graph",
        attempt_no=1,
        transition_seq=1,
    )
    receipt = ReceiptV1(
        receipt_id="receipt.graph.stale.1",
        receipt_type="node_execution_receipt",
        schema_version=1,
        workflow_id="workflow.graph",
        run_id="dispatch_graph_stale_jobs",
        request_id="request.graph",
        route_identity=route_identity,
        transition_seq=1,
        evidence_seq=2,
        started_at=now - timedelta(seconds=8),
        finished_at=now - timedelta(seconds=2),
        executor_type="api_task",
        status="succeeded",
        inputs={},
        outputs={"result": "API_TASK_OK"},
        node_id="http_echo",
        attempt_no=1,
    )
    evidence_row = EvidenceRow(
        kind="receipt",
        evidence_seq=2,
        row_id="receipt.graph.stale.1",
        route_identity=route_identity,
        transition_seq=1,
        record=receipt,
    )

    class _FakeReader:
        def __init__(self, *, database_url=None, env=None):
            pass

        def evidence_timeline(self, run_id: str):
            assert run_id == "dispatch_graph_stale_jobs"
            return (evidence_row,)

    class _StatusConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT * FROM workflow_runs"):
                return [dict(run_row)]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return [
                    {
                        "id": 1,
                        "label": "http_echo",
                        "agent_slug": "api_task",
                        "resolved_agent": "api_task",
                        "status": "pending",
                        "attempt": 0,
                        "max_attempts": 1,
                        "last_error_code": "",
                        "failure_category": "",
                        "failure_zone": "",
                        "is_transient": False,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "token_input": 0,
                        "token_output": 0,
                        "stdout_preview": "",
                        "created_at": run_row["requested_at"],
                        "ready_at": None,
                        "claimed_at": None,
                        "started_at": None,
                        "finished_at": None,
                        "heartbeat_at": None,
                        "next_retry_at": None,
                        "claimed_by": None,
                    }
                ]
            if "FROM execution_packets" in normalized:
                return [{"packets": []}]
            raise AssertionError(f"Unexpected query: {normalized}")

    monkeypatch.setattr(
        runtime_db,
        "resolve_runtime_database_url",
        lambda *, required=True, **_: "postgresql://postgres@repo.test/workflow",
    )
    monkeypatch.setattr(storage_postgres, "PostgresEvidenceReader", _FakeReader)

    status = unified_dispatch.get_run_status(_StatusConn(), "dispatch_graph_stale_jobs")

    assert status is not None
    assert status["completed_jobs"] == 1
    assert status["jobs"][0]["status"] == "succeeded"
    assert "API_TASK_OK" in status["jobs"][0]["stdout_preview"]


def test_execute_job_always_uses_job_prompt(monkeypatch) -> None:
    """Job prompt is truth — execution_packets are ignored, runtime context loaded from DB."""
    _unused_packet_row = {
        "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
        "definition_revision": "def_alpha",
        "plan_revision": "plan_alpha",
        "packet_revision": "packet_exec.alpha:1",
        "parent_artifact_ref": "packet_lineage.alpha",
        "packet_version": 1,
        "packet_hash": "packet_hash_alpha",
        "workflow_id": "workflow.alpha",
        "run_id": "run.alpha",
        "spec_name": "alpha",
        "source_kind": "workflow_runtime",
        "authority_refs": ["def_alpha", "plan_alpha"],
        "model_messages": [
            {
                "job_label": "job-alpha",
                "messages": [{"role": "user", "content": "hello from packet"}],
            }
        ],
        "reference_bindings": [],
        "capability_bindings": [],
        "verify_refs": [],
        "authority_inputs": {},
        "file_inputs": {},
        "payload": {
            "packet_revision": "packet_exec.alpha:1",
            "packet_hash": "packet_hash_alpha",
        },
        "decision_ref": "decision.compile.packet.alpha",
    }

    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "definition_revision": "def_alpha",
                            "plan_revision": "plan_alpha",
                        }
                    },
                }]
            if "FROM workflow_job_runtime_context" in normalized:
                return []  # No persisted runtime context — will be built fresh
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}
    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace()),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    monkeypatch.setattr(
        task_type_router,
        "TaskTypeRouter",
        lambda _conn: SimpleNamespace(resolve_explicit_eligibility=lambda *args, **kwargs: None),
    )
    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=True,
            reason=None,
            checked_at=datetime.now(timezone.utc),
        ),
    )
    def _capture_cli(_config, prompt, _repo_root, **_kwargs):
        captured["prompt"] = prompt
        return {
            "status": "succeeded",
            "stdout": "done",
            "exit_code": 0,
            "token_input": 0,
            "token_output": 0,
            "cost_usd": 0.0,
        }

    monkeypatch.setattr(_exec_mod, "_execute_cli", _capture_cli)
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(_exec_mod, "_get_verify_bindings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "_run_post_execution_verification",
        lambda *_args, **_kwargs: {
            "result": {"status": "succeeded", "stdout": "done"},
            "final_status": "succeeded",
            "final_error_code": "",
            "verification_summary": None,
            "verification_bindings": None,
            "verification_error": None,
        },
    )
    monkeypatch.setattr(
        _ctx_mod,
        "_submission_capture_baseline_for_job",
        lambda *_args, **_kwargs: {"status": "captured"},
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_submission",
        lambda _conn, **kwargs: _SubmissionGateResult(
            submission_state={"submission_id": "submission.alpha"},
            final_status=kwargs["final_status"],
            final_error_code=kwargs["final_error_code"],
            result=kwargs["result"],
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")

    unified_dispatch.execute_job(
        _Conn(),
        {
            "id": 11,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "the actual job prompt",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    # Job prompt is always used — execution_packets are not consulted
    assert "the actual job prompt" in captured["prompt"]
    assert completed["status"] == "succeeded"


def test_execute_job_non_packet_runtime_injects_execution_bundle(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "verify_refs": ["verify.spec.global"],
                            "jobs": [
                                {
                                    "label": "job-alpha",
                                    "prompt": "implement the feature",
                                    "task_type": "build",
                                    "submission_required": True,
                                    "write_scope": ["runtime/example.py"],
                                }
                            ],
                        }
                    },
                }]
            if "FROM workflow_job_runtime_context" in normalized:
                return []  # No persisted context — built fresh
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}
    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=True,
            reason=None,
            checked_at=datetime.now(timezone.utc),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: ("implement the feature", None, False, None, None),
    )
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=["runtime/support.py"],
            test_scope=["tests/test_example.py"],
            blast_radius=["runtime/downstream.py"],
            context_sections=[],
        ),
    )

    def _capture_cli(_config, prompt, _repo_root, **kwargs):
        captured["prompt"] = prompt
        captured["execution_bundle"] = kwargs.get("execution_bundle")
        return {
            "status": "succeeded",
            "stdout": "done",
            "exit_code": 0,
            "token_input": 0,
            "token_output": 0,
            "cost_usd": 0.0,
        }

    monkeypatch.setattr(_exec_mod, "_execute_cli", _capture_cli)
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(_exec_mod, "_get_verify_bindings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "_submission_capture_baseline_for_job",
        lambda *_args, **_kwargs: {"status": "captured"},
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_submission",
        lambda _conn, **kwargs: _SubmissionGateResult(
            submission_state={"submission_id": "submission.alpha"},
            final_status=kwargs["final_status"],
            final_error_code=kwargs["final_error_code"],
            result=kwargs["result"],
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 15,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "implement the feature",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    execution_bundle = captured["execution_bundle"]
    assert completed["status"] == "succeeded"
    assert "--- EXECUTION CONTEXT SHARD ---" in captured["prompt"]
    assert "--- EXECUTION CONTROL BUNDLE ---" in captured["prompt"]
    assert "praxis_query" in captured["prompt"]
    assert "praxis_context_shard" in execution_bundle["mcp_tool_names"]
    assert execution_bundle["tool_bucket"] == "build"
    assert execution_bundle["completion_contract"]["submission_required"] is True
    assert "praxis_submit_code_change_candidate" in execution_bundle["mcp_tool_names"]
    assert "praxis_get_submission" in execution_bundle["mcp_tool_names"]
    assert execution_bundle["access_policy"]["blast_radius"] == ["runtime/downstream.py"]
    assert execution_bundle["access_policy"]["verify_refs"] == ["verify.spec.global"]


def test_execute_job_non_packet_runtime_uses_execution_manifest_authority(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "workflow_id": "wf_alpha",
                        "spec_snapshot": {
                            "definition_revision": "def_alpha",
                            "execution_manifest": {
                                "execution_manifest_ref": "execution_manifest:wf_alpha:def_alpha:manifest_alpha",
                                "approved_bundle_refs": ["capability_bundle:email_triage"],
                                "tool_allowlist": {
                                    "mcp_tools": ["praxis_integration", "praxis_status_snapshot"],
                                    "adapter_tools": ["repo_fs"],
                                },
                                "verify_refs": ["verify.approved"],
                            },
                            "jobs": [
                                {
                                    "label": "job-alpha",
                                    "prompt": "Research otters and invent a totally different tool story.",
                                    "write_scope": ["runtime/example.py"],
                                }
                            ],
                        }
                    },
                }]
            if "FROM workflow_job_runtime_context" in normalized:
                return []
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}
    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=True,
            reason=None,
            checked_at=datetime.now(timezone.utc),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: ("Research otters and invent a tool story.", None, False, None, None),
    )
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=[],
            test_scope=[],
            blast_radius=[],
            context_sections=[],
        ),
    )

    def _capture_cli(_config, prompt, _repo_root, **kwargs):
        captured["execution_bundle"] = kwargs.get("execution_bundle")
        return {
            "status": "succeeded",
            "stdout": "done",
            "exit_code": 0,
            "token_input": 0,
            "token_output": 0,
            "cost_usd": 0.0,
        }

    monkeypatch.setattr(_exec_mod, "_execute_cli", _capture_cli)
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(_exec_mod, "_get_verify_bindings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_exec_mod, "_resolve_submission", lambda _conn, **kwargs: _SubmissionGateResult(
        submission_state={},
        final_status=kwargs["final_status"],
        final_error_code=kwargs["final_error_code"],
        result=kwargs["result"],
    ))
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 15,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "Research otters and invent a tool story.",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    execution_bundle = captured["execution_bundle"]
    assert completed["status"] == "succeeded"
    assert execution_bundle["execution_manifest_ref"] == "execution_manifest:wf_alpha:def_alpha:manifest_alpha"
    assert execution_bundle["allowed_tools"] == ["repo_fs"]
    assert "praxis_integration" in execution_bundle["mcp_tool_names"]
    assert "praxis_status_snapshot" in execution_bundle["mcp_tool_names"]
    assert "praxis_query" not in execution_bundle["mcp_tool_names"]
    assert execution_bundle["access_policy"]["verify_refs"] == ["verify.approved"]


def test_execute_job_allows_mcp_transport_through_cli_sandbox(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "verify_refs": ["verify.spec.global"],
                            "jobs": [
                                {
                                    "label": "job-alpha",
                                    "prompt": "implement the feature",
                                    "task_type": "build",
                                    "submission_required": True,
                                    "write_scope": ["runtime/example.py"],
                                }
                            ],
                        }
                    },
                }]
            if "FROM workflow_job_runtime_context" in normalized:
                return []
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}
    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="mcp"),
    )
    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=True,
            reason=None,
            checked_at=datetime.now(timezone.utc),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: ("implement the feature", None, False, None, None),
    )
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=["runtime/support.py"],
            test_scope=["tests/test_example.py"],
            blast_radius=["runtime/downstream.py"],
            context_sections=[],
        ),
    )

    def _capture_cli(_config, prompt, _repo_root, **kwargs):
        captured["prompt"] = prompt
        captured["execution_bundle"] = kwargs.get("execution_bundle")
        return {
            "status": "succeeded",
            "stdout": "done",
            "exit_code": 0,
            "token_input": 0,
            "token_output": 0,
            "cost_usd": 0.0,
        }

    monkeypatch.setattr(_exec_mod, "_execute_cli", _capture_cli)
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(_exec_mod, "_get_verify_bindings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "_submission_capture_baseline_for_job",
        lambda *_args, **_kwargs: {"status": "captured"},
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_submission",
        lambda _conn, **kwargs: _SubmissionGateResult(
            submission_state={"submission_id": "submission.alpha"},
            final_status=kwargs["final_status"],
            final_error_code=kwargs["final_error_code"],
            result=kwargs["result"],
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 17,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "implement the feature",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    execution_bundle = captured["execution_bundle"]
    assert completed["status"] == "succeeded"
    assert "implement the feature" in captured["prompt"]
    assert execution_bundle["mcp_tool_names"]
    assert "praxis_submit_code_change_candidate" in execution_bundle["mcp_tool_names"]
    assert "praxis_get_submission" in execution_bundle["mcp_tool_names"]


def test_execute_job_pauses_for_approval_checkpoint(monkeypatch) -> None:
    class _Conn:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            self.queries.append((normalized, args))
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [
                    {
                        "run_id": "run.alpha",
                        "current_state": "queued",
                        "request_envelope": {
                            "spec_snapshot": {},
                        },
                    }
                ]
            if normalized.startswith("UPDATE workflow_jobs SET status = 'approval_required'"):
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    conn = _Conn()
    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai", model="gpt-5.4")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_exec_mod, "_resolve_job_prompt_authority", lambda *_args, **_kwargs: (
        "approve me",
        None,
        False,
        {"approval_required": True, "approval_question": "Approve deployment?"},
        None,
    ))
    monkeypatch.setattr(
        _exec_mod,
        "_approval_checkpoint_for_job",
        lambda *_args, **_kwargs: {"checkpoint_id": "checkpoint.alpha", "status": "pending"},
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )

    _exec_mod.execute_job(
        conn,
        {
            "id": 21,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "approve me",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    assert completed == {}
    assert conn.queries[-1][0].startswith("UPDATE workflow_jobs SET status = 'approval_required'")


def test_execute_job_continues_after_approved_approval_checkpoint(monkeypatch) -> None:
    class _Conn:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            self.queries.append((normalized, args))
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [
                    {
                        "run_id": "run.alpha",
                        "current_state": "queued",
                        "request_envelope": {
                            "spec_snapshot": {},
                        },
                    }
                ]
            raise AssertionError(f"Unexpected query: {normalized}")

    conn = _Conn()
    captured: dict[str, object] = {}
    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai", model="gpt-5.4")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_exec_mod, "resolve_execution_transport", lambda _config: SimpleNamespace(transport_kind="cli"))
    monkeypatch.setattr(_exec_mod, "_resolve_job_prompt_authority", lambda *_args, **_kwargs: (
        "approve me",
        None,
        False,
        {"approval_required": True, "approval_question": "Approve deployment?"},
        None,
    ))
    monkeypatch.setattr(
        _exec_mod,
        "_approval_checkpoint_for_job",
        lambda *_args, **_kwargs: {"checkpoint_id": "checkpoint.alpha", "status": "approved"},
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")
    monkeypatch.setattr(_exec_mod, "_persist_runtime_context_for_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_exec_mod, "_capture_submission_baseline_if_required", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(_exec_mod, "_run_post_execution_verification", lambda *_args, **_kwargs: {
        "result": {"status": "succeeded", "stdout": "done", "exit_code": 0, "token_input": 0, "token_output": 0, "cost_usd": 0.0},
        "final_status": "succeeded",
        "final_error_code": "",
        "verification_summary": {},
        "verification_bindings": {},
        "verification_error": None,
    })
    monkeypatch.setattr(_exec_mod, "_resolve_submission", lambda _conn, **kwargs: _SubmissionGateResult(
        submission_state=None,
        final_status=kwargs["final_status"],
        final_error_code=kwargs["final_error_code"],
        result=kwargs["result"],
    ))
    monkeypatch.setattr(_exec_mod, "_execute_cli", lambda _config, prompt, _repo_root, **_kwargs: captured.update({"prompt": prompt}) or {
        "status": "succeeded",
        "stdout": "done",
        "exit_code": 0,
        "token_input": 0,
        "token_output": 0,
        "cost_usd": 0.0,
    })
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )

    _exec_mod.execute_job(
        conn,
        {
            "id": 22,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "approve me",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    assert "approve me" in str(captured["prompt"])
    assert completed["status"] == "succeeded"
    assert any(
        query == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1"
        for query, _args in conn.queries
    )
    assert not any("status = 'approval_required'" in query for query, _args in conn.queries)


def test_execute_job_fails_closed_when_submission_required_job_has_no_submission(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "verify_refs": ["verify.spec.global"],
                            "jobs": [
                                {
                                    "label": "job-alpha",
                                    "prompt": "implement the feature",
                                    "task_type": "build",
                                    "submission_required": True,
                                    "write_scope": ["runtime/example.py"],
                                }
                            ],
                        }
                    },
                }]
            if "FROM workflow_job_runtime_context" in normalized:
                return []
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=True,
            reason=None,
            checked_at=datetime.now(timezone.utc),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: ("implement the feature", None, False, None, None),
    )
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=["runtime/support.py"],
            test_scope=["tests/test_example.py"],
            blast_radius=["runtime/downstream.py"],
            context_sections=[],
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_execute_cli",
        lambda *_args, **_kwargs: {
            "status": "succeeded",
            "stdout": "done",
            "exit_code": 0,
            "token_input": 0,
            "token_output": 0,
            "cost_usd": 0.0,
        },
    )
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(_exec_mod, "_get_verify_bindings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "_submission_capture_baseline_for_job",
        lambda *_args, **_kwargs: {"status": "captured"},
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_submission",
        lambda _conn, **kwargs: _SubmissionGateResult(
            submission_state=None,
            final_status="failed",
            final_error_code="workflow_submission.required_missing",
            result=kwargs["result"],
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 16,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "implement the feature",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    assert completed["status"] == "failed"
    assert completed["error_code"] == "workflow_submission.required_missing"


def _REMOVED_test_execute_job_fails_closed_when_migrated_run_compile_index_is_stale():
    """Removed: execution_packets no longer gate job execution (refactor: job.prompt is truth)."""
    pass


def _KEEP_test_execute_job_fails_closed_when_migrated_run_compile_index_is_stale(monkeypatch) -> None:
    _unused_packet_row = {
        "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
        "definition_revision": "def_alpha",
        "plan_revision": "plan_alpha",
        "packet_revision": "packet_exec.alpha:1",
        "parent_artifact_ref": "packet_lineage.alpha",
        "packet_version": 1,
        "packet_hash": "packet_hash_alpha",
        "workflow_id": "workflow.alpha",
        "run_id": "run.alpha",
        "spec_name": "alpha",
        "source_kind": "workflow_runtime",
        "authority_refs": ["def_alpha", "plan_alpha"],
        "model_messages": [
            {
                "job_label": "job-alpha",
                "messages": [{"role": "user", "content": "hello from packet"}],
            }
        ],
        "reference_bindings": [],
        "capability_bindings": [],
        "verify_refs": [],
        "authority_inputs": {
            "workflow_definition": {
                "type": "operating_model",
                "definition_revision": "def_alpha",
                "materialize_provenance": {
                    "compile_index_ref": "compile_index.alpha",
                    "compile_surface_revision": "surface.alpha",
                },
            }
        },
        "file_inputs": {},
        "payload": {
            "packet_revision": "packet_exec.alpha:1",
            "packet_hash": "packet_hash_alpha",
        },
        "decision_ref": "decision.compile.packet.alpha",
    }

    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "definition_revision": "def_alpha",
                            "plan_revision": "plan_alpha",
                        }
                    },
                }]
            if "FROM execution_packets" in normalized:
                return [packet_row]
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    completed: dict[str, object] = {}

    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace()),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    monkeypatch.setattr(
        compile_index,
        "load_compile_index_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            compile_index.MaterializeIndexAuthorityError(
                "compile_index.snapshot_stale",
                "compile index snapshot is stale",
            )
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_execute_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("stale packet should fail before execution")),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 13,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "raw prompt must not execute",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    assert completed["status"] == "failed"
    assert completed["error_code"] == "execution_packet.compile_index_stale"
    assert "stale" in str(completed["stdout_preview"]).lower()


def test_execute_job_blocks_before_cli_spawn_when_provider_not_ready(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "definition_revision": "def_alpha",
                            "plan_revision": "plan_alpha",
                        }
                    },
                }]
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}

    monkeypatch.setattr(_shared_mod, "_CIRCUIT_BREAKERS", None)
    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(
            get=lambda _slug: SimpleNamespace(provider="anthropic", model="claude-sonnet-4"),
        ),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: ("use cli", None, False, None, None),
    )

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=False,
            reason="Missing credential: EXAMPLE_API_KEY",
            checked_at=datetime.now(timezone.utc),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_execute_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("CLI subprocess should not run")),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: captured.update(kwargs),
    )

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 14,
            "label": "job-alpha",
            "agent_slug": "example/example-model",
            "prompt": "use cli",
            "run_id": "run.alpha",
        },
        repo_root="/tmp/workspace.alpha",
    )

    assert captured["status"] == "failed"
    assert captured["error_code"] == "credential.env_var_missing"
    assert "EXAMPLE_API_KEY" in str(captured["stdout_preview"])


def test_execute_job_blocks_before_prompt_and_cli_when_provider_is_route_disabled(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [{
                    "run_id": "run.alpha",
                    "current_state": "queued",
                    "request_envelope": {
                        "spec_snapshot": {
                            "definition_revision": "def_alpha",
                            "plan_revision": "plan_alpha",
                        }
                    },
                }]
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}

    class _RejectingRouter:
        def __init__(self, conn) -> None:
            self.conn = conn

        def resolve_explicit_eligibility(
            self,
            agent_slug: str,
            *,
            task_type: str | None = None,
            as_of=None,
        ):
            assert agent_slug == "anthropic/claude-sonnet-4"
            assert task_type == "build"
            return SimpleNamespace(
                eligibility_status="rejected",
                reason_code="provider_disabled",
                rationale="Anthropic off until Friday morning",
                decision_ref="decision:anthropic-off",
            )

    monkeypatch.setattr(_shared_mod, "_CIRCUIT_BREAKERS", None)
    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_type_router, "TaskTypeRouter", _RejectingRouter)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(
            get=lambda _slug: SimpleNamespace(provider="anthropic", model="claude-sonnet-4"),
        ),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("prompt authority should not resolve after route disable"),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_execute_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("CLI subprocess should not run when provider is disabled"),
        ),
    )
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: captured.update(kwargs),
    )

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 15,
            "label": "job-alpha",
            "agent_slug": "anthropic/claude-sonnet-4",
            "prompt": "use cli",
            "run_id": "run.alpha",
            "route_task_type": "build",
        },
        repo_root="/tmp/workspace.alpha",
    )

    assert captured["status"] == "failed"
    assert captured["error_code"] == "provider_disabled"
    assert "Anthropic off until Friday morning" in str(captured["stdout_preview"])
    assert "decision:anthropic-off" in str(captured["stdout_preview"])


class _NoopRouter:
    def __init__(self, conn) -> None:
        self.conn = conn

    def resolve_spec_jobs(
        self,
        jobs: list[dict],
        *,
        runtime_profile_ref: str | None = None,
    ) -> list[dict]:
        return jobs


class _ScopedRoutingRouter:
    def __init__(self, conn) -> None:
        self.conn = conn

    def validate_routes(self) -> list[str]:
        return []

    def resolve_spec_jobs(
        self,
        jobs: list[dict],
        *,
        runtime_profile_ref: str | None = None,
    ) -> list[dict]:
        if runtime_profile_ref:
            raise RuntimeError("no admitted candidates for scoped auto route")
        for job in jobs:
            if not str(job.get("agent", "")).startswith("auto/"):
                continue
            job["agent"] = "openai/gpt-5.4"
            job["route_candidates"] = ["openai/gpt-5.4"]
            job["_route_plan"] = SimpleNamespace(
                primary="openai/gpt-5.4",
                chain=("openai/gpt-5.4",),
                task_type="build",
                original_slug="auto/build",
                failover_eligible_codes=frozenset(),
                transient_retry_codes=frozenset(),
                max_same_model_retries=1,
                backoff_seconds=(5,),
            )
        return jobs


class _ScopedRouteConn:
    def __init__(self, runtime_profile_ref: str = "runtime.profile.scoped") -> None:
        self.runtime_profile_ref = runtime_profile_ref
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, query: str, *args):
        self.queries.append((query, args))
        normalized = " ".join(query.split())
        if "SELECT request_envelope->>'runtime_profile_ref'" in normalized:
            return [{"runtime_profile_ref": self.runtime_profile_ref}]
        if "FROM provider_model_candidates" in normalized:
            return [{
                "candidate_ref": "cand-openai",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
            }]
        if "FROM registry_runtime_profile_authority" in normalized:
            return [{
                "model_profile_id": "model-profile-1",
                "provider_policy_id": "provider-policy-1",
            }]
        if "FROM route_eligibility_states" in normalized:
            return []
        if "INSERT INTO workflow_jobs" in normalized:
            return [{"id": 1}]
        if "INSERT INTO workflow_runs" in normalized:
            return []
        if "UPDATE workflow_jobs" in normalized and "SET status = 'ready'" in normalized:
            return []
        if "FROM failure_category_zones" in normalized:
            return []
        if "FROM route_policy_registry" in normalized:
            return []
        if "FROM task_type_route_profiles" in normalized:
            return []
        if "FROM market_benchmark_metric_registry" in normalized:
            return []
        if "GROUP BY 1" in normalized:
            return []
        return []


def _make_spec(jobs: list[dict], *, runtime_profile_ref: str | None = None) -> SimpleNamespace:
    raw = {"jobs": [dict(job) for job in jobs]}
    if runtime_profile_ref is not None:
        raw["runtime_profile_ref"] = runtime_profile_ref
    return SimpleNamespace(
        name="auto-deps",
        phase="test",
        jobs=jobs,
        runtime_profile_ref=runtime_profile_ref,
        _raw=raw,
    )


@pytest.fixture(autouse=True)
def _patch_routing(monkeypatch):
    monkeypatch.setattr(task_type_router, "TaskTypeRouter", _NoopRouter)


@pytest.fixture(autouse=True)
def _patch_runtime_profile_sandbox_payload(monkeypatch):
    monkeypatch.setattr(_ctx_mod, "_runtime_profile_sandbox_payload", lambda *args, **kwargs: None)


def test_submit_workflow_auto_infers_dependencies_from_scopes(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "openai/gpt-5.4", "write_scope": ["a.py"]},
        {
            "label": "build_b",
            "prompt": "create b",
            "agent": "openai/gpt-5.4",
            "write_scope": ["b.py"],
            "read_scope": ["a.py"],
        },
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    conn = _FakeConn()

    unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")

    assert jobs[1]["depends_on"] == ["build_a"]
    assert conn.edge_inserts == [(1, 2)]


def test_submit_workflow_fails_closed_when_write_scope_authority_unavailable(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "openai/gpt-5.4", "write_scope": ["a.py"]},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    class _BrokenStepCompiler:
        def compile(self, _spec):
            raise RuntimeError("step compiler is unavailable")

    monkeypatch.setattr(_admission_mod, "StepCompiler", _BrokenStepCompiler)

    conn = _FakeConn()

    with pytest.raises(RuntimeError, match="failed closed while resolving write-scope authority"):
        unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")

    assert not any("INSERT INTO workflow_runs" in query for query, _ in conn.queries)


def test_submit_workflow_scoped_routes_fail_closed_without_admitted_candidates(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs, runtime_profile_ref="runtime.profile.scoped")
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))
    monkeypatch.setattr(task_type_router, "TaskTypeRouter", _ScopedRoutingRouter)

    conn = _ScopedRouteConn()

    with pytest.raises(RuntimeError, match="failed closed"):
        unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")

    assert not any("INSERT INTO workflow_runs" in query for query, _ in conn.queries)


def test_submit_workflow_keeps_explicit_dependencies(monkeypatch):
    jobs = [
        {"label": "manual_parent", "prompt": "manual", "agent": "openai/gpt-5.4"},
        {"label": "build_a", "prompt": "create a", "agent": "openai/gpt-5.4", "write_scope": ["a.py"]},
        {
            "label": "build_b",
            "prompt": "create b",
            "agent": "openai/gpt-5.4",
            "write_scope": ["b.py"],
            "read_scope": ["a.py"],
            "depends_on": ["manual_parent"],
        },
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    conn = _FakeConn()

    unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")

    assert jobs[2]["depends_on"] == ["manual_parent"]
    assert (1, 3) in conn.edge_inserts
    assert (2, 3) not in conn.edge_inserts


def test_submit_workflow_uses_transaction_when_available(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    events: list[str] = []

    class _TxConn(_FakeConn):
        @contextmanager
        def transaction(self):
            events.append("begin")
            try:
                yield self
            except Exception:
                events.append("rollback")
                raise
            else:
                events.append("commit")

    conn = _TxConn()

    unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")

    assert events == ["begin", "commit"]


def test_submit_workflow_fails_closed_when_dependency_edges_not_persisted(monkeypatch):
    jobs = [
        {"label": "seed", "prompt": "create a", "agent": "openai/gpt-5.4"},
        {"label": "synth", "prompt": "create b", "agent": "openai/gpt-5.4", "depends_on": ["seed"]},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    class _BrokenEdgeConn(_FakeConn):
        @contextmanager
        def transaction(self):
            yield self

        def execute(self, query: str, *args):
            if "INSERT INTO workflow_job_edges" in query:
                return []
            if "SELECT parent_id, child_id" in query and "FROM workflow_job_edges" in query:
                return []
            return super().execute(query, *args)

    conn = _BrokenEdgeConn()

    with pytest.raises(RuntimeError, match="dependency edges were not persisted"):
        unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")


def test_submit_workflow_records_idempotency_for_new_jobs(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    conn = _FakeConn()

    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo")

    assert result["replayed_jobs"] == []
    assert conn.next_job_id == 1
    assert any("INSERT INTO idempotency_ledger" in query for query, _ in conn.queries)


def test_submit_workflow_rejects_when_queue_is_at_critical_threshold(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    conn = _FakeConn(queue_depth=1000)

    with pytest.raises(RuntimeError, match="queue admission rejected"):
        unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo")

    assert not any("INSERT INTO workflow_jobs" in query for query, _ in conn.queries)


def test_submit_workflow_uses_shared_queue_admission_gate(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    captured: dict[str, object] = {}

    class _FakeGate:
        def __init__(self, *, critical_threshold: int = 1000, **_kwargs) -> None:
            captured["critical_threshold"] = critical_threshold

        def check_connection(self, conn, *, job_count: int = 1):
            captured["conn"] = conn
            captured["job_count"] = job_count
            return SimpleNamespace(admitted=True, queue_depth=0, reason="ok")

    monkeypatch.setattr(_admission_mod, "QueueAdmissionGate", _FakeGate)

    conn = _FakeConn()
    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo")

    assert result["status"] == "queued"
    assert captured["conn"] is conn
    assert captured["job_count"] == 1


def test_submit_workflow_routes_graph_specs_through_graph_runtime_submit(monkeypatch):
    jobs = [
        {"label": "seed", "agent": "openai/gpt-5.4-mini", "adapter_type": "deterministic_task", "expected_outputs": {"go": True}},
        {
            "label": "gate",
            "agent": "openai/gpt-5.4-mini",
            "adapter_type": "control_operator",
            "depends_on": ["seed"],
            "operator": {"kind": "if", "predicate": {"field": "go", "op": "equals", "value": True}},
            "branches": {
                "then": [{"label": "then_path", "expected_outputs": {"selected": "then"}}],
                "else": [{"label": "else_path", "expected_outputs": {"selected": "else"}}],
            },
        },
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    captured: dict[str, object] = {}

    def _fake_graph_submit(conn, spec_dict, *, run_id, packet_provenance=None):
        captured["conn"] = conn
        captured["spec_dict"] = spec_dict
        captured["run_id"] = run_id
        captured["packet_provenance"] = packet_provenance
        return {"run_id": run_id, "status": "succeeded", "execution_mode": "graph_runtime"}

    monkeypatch.setattr(_admission_mod, "_submit_graph_workflow_inline", _fake_graph_submit)
    monkeypatch.setattr(
        _admission_mod,
        "_do_submit_workflow",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("flat submission path should not run")),
    )

    conn = _FakeConn()
    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="graph_dispatch")

    assert result["execution_mode"] == "graph_runtime"
    assert captured["conn"] is conn
    assert captured["run_id"] == "graph_dispatch"
    assert captured["spec_dict"] == spec._raw
    assert captured["packet_provenance"]["source_kind"] == "file_submit"


def test_submit_workflow_routes_deterministic_specs_through_graph_runtime_submit(monkeypatch):
    jobs = [
        {
            "label": "prepare",
            "agent": "openai/gpt-5.4-mini",
            "adapter_type": "deterministic_task",
            "expected_outputs": {"result": "prepared"},
        },
        {
            "label": "admit",
            "agent": "openai/gpt-5.4-mini",
            "adapter_type": "deterministic_task",
            "depends_on": ["prepare"],
            "expected_outputs": {"result": "admitted"},
        },
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    captured: dict[str, object] = {}

    def _fake_graph_submit(conn, spec_dict, *, run_id, packet_provenance=None):
        captured["conn"] = conn
        captured["spec_dict"] = spec_dict
        captured["run_id"] = run_id
        captured["packet_provenance"] = packet_provenance
        return {"run_id": run_id, "status": "succeeded", "execution_mode": "graph_runtime"}

    monkeypatch.setattr(_admission_mod, "_submit_graph_workflow_inline", _fake_graph_submit)
    monkeypatch.setattr(
        _admission_mod,
        "_do_submit_workflow",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("flat submission path should not run")),
    )

    conn = _FakeConn()
    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="deterministic_dispatch")

    assert result["execution_mode"] == "graph_runtime"
    assert captured["conn"] is conn
    assert captured["run_id"] == "deterministic_dispatch"
    assert captured["spec_dict"] == spec._raw
    assert captured["packet_provenance"]["source_kind"] == "file_submit"


def test_submit_workflow_inline_routes_single_prompt_specs_through_graph_runtime_submit(monkeypatch):
    spec_dict = {
        "name": "prompt inline",
        "phase": "execute",
        "jobs": [
            {
                "label": "run",
                "adapter_type": "cli_llm",
                "agent": "openai/gpt-5.4-mini",
                "prompt": "Reply with exactly: GRAPH_ONLY",
                "write_scope": ["runtime/example.py"],
                "workdir": "/repo",
            }
        ],
    }

    captured: dict[str, object] = {}

    def _fake_graph_submit(conn, inline_spec, *, run_id, packet_provenance=None):
        captured["conn"] = conn
        captured["spec_dict"] = inline_spec
        captured["run_id"] = run_id
        captured["packet_provenance"] = packet_provenance
        return {"run_id": run_id, "status": "succeeded", "execution_mode": "graph_runtime"}

    monkeypatch.setattr(_admission_mod, "_submit_graph_workflow_inline", _fake_graph_submit)
    monkeypatch.setattr(
        _admission_mod,
        "_do_submit_workflow",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("flat submission path should not run")),
    )

    conn = _FakeConn()
    result = _admission_mod.submit_workflow_inline(conn, spec_dict, run_id="prompt_inline_dispatch")

    assert result["execution_mode"] == "graph_runtime"
    assert captured["conn"] is conn
    assert captured["run_id"] == "prompt_inline_dispatch"
    assert captured["spec_dict"] == spec_dict
    assert captured["packet_provenance"] is None


def test_submit_workflow_inline_fails_closed_when_graph_only_single_job_cannot_compile(monkeypatch):
    spec_dict = {
        "name": "prompt inline",
        "phase": "execute",
        "graph_runtime_submit": True,
        "jobs": [
            {
                "label": "run",
                "prompt": "Reply with exactly: SHOULD_NOT_FALL_BACK",
                "agent": "auto/build",
            }
        ],
    }

    monkeypatch.setattr(
        _admission_mod,
        "_do_submit_workflow",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("flat submission path should not run")),
    )

    conn = _FakeConn()

    with pytest.raises(RuntimeError, match="graph-capable workflow submit failed closed"):
        _admission_mod.submit_workflow_inline(conn, spec_dict, run_id="prompt_inline_dispatch")


def test_submit_graph_workflow_inline_reports_current_state_for_success(monkeypatch):
    fake_request = SimpleNamespace(workflow_id="deterministic_smoke")
    fake_outcome = SimpleNamespace(
        validation_result=SimpleNamespace(is_valid=True),
        current_state=SimpleNamespace(value="claim_accepted"),
        run_id="run:graph:inline",
        admission_decision=SimpleNamespace(reason_code="claim.validated"),
    )

    class _FakePlanner:
        def __init__(self, *, registry):
            self.registry = registry

        def plan(self, *, request):
            return fake_outcome

    class _FakeWriter:
        def __init__(self, *, database_url):
            self.database_url = database_url

        def close_blocking(self):
            return None

    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/praxis")
    monkeypatch.setattr(_admission_mod, "compile_graph_workflow_request", lambda *_args, **_kwargs: fake_request)
    monkeypatch.setattr(_admission_mod, "_graph_registry_for_request", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(_admission_mod, "WorkflowIntakePlanner", _FakePlanner)
    monkeypatch.setattr(_admission_mod, "_persist_graph_authority", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_admission_mod, "PostgresEvidenceWriter", _FakeWriter)
    captured: dict[str, object] = {}

    def _fake_persist_graph_submission_evidence(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        _admission_mod,
        "_persist_graph_submission_evidence",
        _fake_persist_graph_submission_evidence,
    )
    monkeypatch.setattr(
        _admission_mod,
        "_build_graph_execution_packet",
        lambda *_args, **_kwargs: {
            "materialize_provenance": {
                "input_fingerprint": "packet_input.alpha",
                "reuse": {
                    "decision": "compiled",
                    "reason_code": "packet.compile.miss",
                },
            }
        },
    )

    result = _admission_mod._submit_graph_workflow_inline(
        _FakeConn(
            workflow_run_rows=[
                {
                    "spec_name": "deterministic smoke",
                    "started_at": datetime(2026, 4, 9, 11, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 9, 11, 1, 40, tzinfo=timezone.utc),
                },
            ],
        ),
        {"name": "deterministic smoke", "jobs": [{}, {}]},
        run_id="run:graph:inline",
    )

    assert result["run_id"] == "run:graph:inline"
    assert result["status"] == "claim_accepted"
    assert result["reason_code"] == "claim.validated"
    assert result["execution_mode"] == "graph_runtime"
    assert result["packet_reuse_provenance"] == {
        "decision": "compiled",
        "reason_code": "packet.compile.miss",
        "input_fingerprint": "packet_input.alpha",
    }
    assert captured["request"] is fake_request


def test_persist_graph_submission_evidence_advances_route_identity_from_intake(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeWriter:
        def commit_submission(self, **kwargs):
            captured["submission_route_identity"] = kwargs["route_identity"]
            return SimpleNamespace(evidence_seq=2)

        def append_transition_proof(self, proof):
            captured["admission_proof"] = proof

    intake_route_identity = RouteIdentity(
        workflow_id="workflow.graph",
        run_id="run:graph:inline",
        request_id="request.graph",
        authority_context_ref="authority.graph",
        authority_context_digest="digest.graph",
        claim_id="claim.graph",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=0,
    )
    intake_outcome = SimpleNamespace(
        route_identity=intake_route_identity,
        admitted_definition_ref="workflow_definition.graph:v1",
        admitted_definition_hash="sha256:graph",
        current_state=SimpleNamespace(value="claim_accepted"),
        run_id="run:graph:inline",
        validation_result=SimpleNamespace(validation_result_ref="validation.graph"),
        request_digest="sha256:req",
        admission_decision=SimpleNamespace(
            reason_code="claim.validated",
            decided_at=datetime(2026, 4, 16, 23, 5, tzinfo=timezone.utc),
            authority_context_ref="authority.graph",
            admission_decision_id="admission:graph",
        ),
    )

    monkeypatch.setattr(_admission_mod, "_workflow_request_payload", lambda _request: {})

    _admission_mod._persist_graph_submission_evidence(
        evidence_writer=_FakeWriter(),
        intake_outcome=intake_outcome,
        request=SimpleNamespace(workflow_definition_id="workflow_definition.graph:v1", definition_hash="sha256:graph"),
    )

    assert captured["submission_route_identity"] == replace(intake_route_identity, transition_seq=1)
    assert captured["admission_proof"].route_identity == replace(intake_route_identity, transition_seq=2)
    assert captured["admission_proof"].transition_seq == 2


def test_graph_registry_from_authority_uses_logical_workspace_identity_for_bundle_hash(tmp_path) -> None:
    repo_root = str(tmp_path / "workspace-root")

    class _RegistryConn:
        def execute(self, query: str, *args):
            if "FROM registry_workspace_authority" in query:
                assert args == ("praxis",)
                return [
                    {
                        "workspace_ref": "praxis",
                        "repo_root": repo_root,
                        "workdir": "/workspace",
                    }
                ]
            if "FROM registry_runtime_profile_authority" in query:
                assert args == ("praxis",)
                return [
                    {
                        "runtime_profile_ref": "praxis",
                        "model_profile_id": "model_profile.codex",
                        "provider_policy_id": "provider_policy.default",
                        "sandbox_profile_ref": "sandbox_profile.praxis.default",
                    }
                ]
            raise AssertionError(f"unexpected query: {query}")

    registry = _admission_mod._graph_registry_from_authority(
        _RegistryConn(),
        SimpleNamespace(workspace_ref="praxis", runtime_profile_ref="praxis"),
    )

    workspace = registry.resolve_workspace(workspace_ref="praxis")
    runtime_profile = registry.resolve_runtime_profile(runtime_profile_ref="praxis")
    bundle = registry.resolve_context_bundle(
        workflow_id="workflow.graph",
        run_id="run:workflow.graph:alpha",
        workspace=workspace,
        runtime_profile=runtime_profile,
        bundle_version=1,
    )

    assert workspace.repo_root == "praxis"
    assert workspace.workdir == "praxis"
    assert bundle.bundle_payload["workspace"] == {
        "repo_root": "praxis",
        "workdir": "praxis",
        "workspace_ref": "praxis",
    }


def test_graph_adapter_registry_prefers_docker_for_cli_llm() -> None:
    registry = _admission_mod._graph_adapter_registry(
        SimpleNamespace(nodes=[SimpleNamespace(adapter_type="cli_llm")])
    )
    cli_adapter = registry._registry["cli_llm"]

    assert getattr(cli_adapter, "_prefer_docker", None) is True


def test_runtime_setup_and_graph_admission_share_adapter_registry_authority() -> None:
    from runtime.workflow import runtime_setup as _runtime_setup_mod

    setup_registry = _runtime_setup_mod._build_adapter_registry(
        SimpleNamespace(
            adapter_type="cli_llm",
            scope_write=["README.md"],
            workdir="/tmp/workflow",
            verify_refs=["verifier.job.python.pytest_file"],
            packet_provenance=None,
            definition_revision=None,
            plan_revision=None,
            allowed_tools=(),
            capabilities=(),
            label=None,
            task_type="implement",
        )
    )
    graph_registry = _admission_mod._graph_adapter_registry(
        SimpleNamespace(
            nodes=[
                SimpleNamespace(adapter_type="context_compiler"),
                SimpleNamespace(adapter_type="cli_llm"),
                SimpleNamespace(adapter_type="output_parser"),
                SimpleNamespace(adapter_type="file_writer"),
                SimpleNamespace(adapter_type="verifier"),
            ]
        )
    )

    assert set(setup_registry._registry) == set(graph_registry._registry)


def test_graph_runtime_timeout_seconds_uses_finished_run_history(monkeypatch):
    from runtime.workflow._admission import _graph_runtime_timeout_seconds

    started = datetime(2026, 4, 9, 11, 0, tzinfo=timezone.utc)
    conn = _FakeConn(
        workflow_run_rows=[
            {
                "spec_name": "deterministic smoke",
                "started_at": started,
                "finished_at": started + timedelta(seconds=100),
            },
            {
                "spec_name": "deterministic smoke",
                "started_at": started,
                "finished_at": started + timedelta(seconds=119),
            },
        ],
    )

    timeout = _graph_runtime_timeout_seconds(
        conn,
        spec_dict={
            "name": "deterministic smoke",
            "jobs": [{"complexity": "low"}],
        },
    )

    assert timeout == 178


def test_graph_runtime_timeout_seconds_respects_explicit_floor(monkeypatch):
    from runtime.workflow._admission import _graph_runtime_timeout_seconds

    conn = _FakeConn()
    monkeypatch.setattr(_admission_mod, "_graph_runtime_history_p95_seconds", lambda *_args, **_kwargs: 10.0)
    monkeypatch.setattr(_admission_mod, "calculate_timeout_seconds", lambda *_args, **_kwargs: 120)

    timeout = _graph_runtime_timeout_seconds(
        conn,
        spec_dict={
            "name": "deterministic smoke",
            "timeout": 300,
            "jobs": [{"complexity": "high"}],
        },
    )

    assert timeout == 300


def test_submit_workflow_insert_keeps_integration_action_and_args(monkeypatch):
    jobs = [
        {
            "label": "build_a",
            "prompt": "create a",
            "agent": "auto/build",
            "integration_id": "integration.example",
            "integration_action": "run",
            "integration_args": {"mode": "fast"},
        },
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    conn = _FakeConn()

    unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_test")

    insert_query, insert_args = next(
        (query, args)
        for query, args in conn.queries
        if "INSERT INTO workflow_jobs" in query
    )
    assert "$18::jsonb, $19::jsonb" in insert_query
    assert len(insert_args) == 22
    assert insert_args[15] == "integration.example"
    assert insert_args[16] == "run"
    assert insert_args[17] == '{"mode": "fast"}'
    assert insert_args[20] == "moderate"


def test_submit_workflow_persists_execution_bundle_and_control_prompt(monkeypatch):
    jobs = [
        {
            "label": "build_a",
            "prompt": "Implement the feature safely.",
            "agent": "auto/build",
            "write_scope": ["app.py"],
            "read_scope": ["tests/test_app.py"],
            "task_type": "build",
            "submission_required": True,
            "verify_refs": ["verify.job.local"],
        },
    ]
    raw = {
        "name": "bundle-spec",
        "workflow_id": "workflow.bundle_spec",
        "phase": "build",
        "jobs": [dict(job) for job in jobs],
        "definition_revision": "definition.bundle",
        "plan_revision": "plan.bundle",
        "verify_refs": ["verify.spec.global"],
    }
    spec = SimpleNamespace(
        name="bundle-spec",
        workflow_id="workflow.bundle_spec",
        phase="build",
        jobs=jobs,
        verify_refs=["verify.spec.global"],
        outcome_goal="",
        anti_requirements=[],
        workspace_ref=None,
        runtime_profile_ref=None,
        _raw=raw,
    )
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=["runtime/helpers.py"],
            test_scope=["tests/test_app.py"],
            blast_radius=["runtime/downstream.py"],
            context_sections=[{"name": "FILE: runtime/helpers.py", "content": "def helper():\n    return 1\n"}],
        ),
    )
    monkeypatch.setattr(
        _ctx_mod,
        "proof_metrics",
        lambda conn: {
            "receipts": {
                "total": 7,
                "verification_coverage": 0.75,
                "fully_proved_verification_coverage": 0.5,
                "write_manifest_coverage": 1.0,
            },
            "compile_authority": {
                "execution_packets_ready": True,
                "verify_refs_ready": True,
                "verification_registry_ready": True,
                "repo_snapshots_ready": True,
            },
        },
    )
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_job_decision_pack",
        lambda *args, **kwargs: {
            "pack_version": 1,
            "authority_domains": ["sandbox_execution"],
            "decision_keys": ["architecture-policy::sandbox-execution::docker-only-authority"],
            "decisions": [
                {
                    "decision_key": "architecture-policy::sandbox-execution::docker-only-authority",
                    "title": "Workflow sandbox execution is Docker-only",
                    "rationale": "Do not add host-local execution lanes.",
                    "decision_scope_ref": "sandbox_execution",
                }
            ],
        },
    )
    monkeypatch.setattr(_admission_mod, "_runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._workspace_ref_from_spec", lambda _spec, conn=None: None)

    conn = _FakeConn()

    unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_bundle")

    _, insert_args = next(
        (query, args)
        for query, args in conn.queries
        if "INSERT INTO execution_packets" in query
    )
    model_messages = json.loads(insert_args[12])
    capability_bindings = json.loads(insert_args[14])
    file_inputs = json.loads(insert_args[17])

    user_prompt = model_messages[0]["messages"][0]["content"]
    bundle = file_inputs["execution_bundles"]["build_a"]

    assert "--- EXECUTION CONTEXT SHARD ---" in user_prompt
    assert "--- EXECUTION CONTROL BUNDLE ---" in user_prompt
    assert "** APPLICABLE DECISIONS **" in user_prompt
    assert bundle["tool_bucket"] == "build"
    assert bundle["decision_pack"]["authority_domains"] == ["sandbox_execution"]
    assert "praxis_context_shard" in bundle["mcp_tool_names"]
    assert "praxis_query" in bundle["mcp_tool_names"]
    assert "praxis_workflow_validate" in bundle["mcp_tool_names"]
    assert "praxis_submit_code_change_candidate" in bundle["mcp_tool_names"]
    assert "praxis_get_submission" in bundle["mcp_tool_names"]
    assert "workflow" in bundle["skill_refs"]
    assert bundle["completion_contract"]["submission_required"] is True
    assert bundle["completion_contract"]["result_kind"] == "code_change_candidate"
    assert bundle["completion_contract"]["submit_tool_names"] == [
        "praxis_submit_code_change_candidate",
        "praxis_get_submission",
    ]


def test_preview_workflow_execution_returns_worker_facing_payload(monkeypatch):
    inline_spec = {
        "name": "preview-spec",
        "workflow_id": "workflow.preview_spec",
        "phase": "build",
        "workdir": "/repo",
        "jobs": [
            {
                "label": "build_a",
                "prompt": "Implement the preview execution lane.",
                "agent": "auto/build",
                "task_type": "code_generation",
                "write_scope": ["runtime/workflow/preview.py"],
                "verify_refs": ["verify.preview"],
            }
        ],
    }
    monkeypatch.setattr(_ctx_mod, "_runtime_profile_sandbox_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_job_decision_pack",
        lambda *args, **kwargs: {
            "pack_version": 1,
            "authority_domains": ["workspace_boundary"],
            "decision_keys": [],
            "decisions": [],
        },
    )

    preview = _admission_mod.preview_workflow_execution(
        _FakeConn(),
        inline_spec=inline_spec,
        repo_root="/repo",
    )

    assert preview["action"] == "preview"
    assert preview["preview_mode"] == "execution"
    assert preview["workspace"]["repo_root"] == "/repo"
    assert preview["jobs"][0]["label"] == "build_a"
    assert preview["jobs"][0]["route_status"] == "unresolved"
    assert preview["jobs"][0]["resolved_agent"] is None
    assert any("task_type_router" in warning for warning in preview["warnings"])
    assert "--- EXECUTION CONTEXT SHARD ---" in preview["jobs"][0]["rendered_user_prompt"]
    assert "--- EXECUTION CONTROL BUNDLE ---" in preview["jobs"][0]["rendered_user_prompt"]
    assert "praxis_query" in preview["jobs"][0]["mcp_tool_names"]
    assert preview["jobs"][0]["workspace"]["workdir"] == "/repo"


def test_preview_workflow_execution_requires_explicit_workdir(monkeypatch):
    inline_spec = {
        "name": "preview-spec-no-workdir",
        "workflow_id": "workflow.preview_spec_no_workdir",
        "phase": "build",
        "jobs": [
            {
                "label": "build_a",
                "prompt": "Implement the preview execution lane.",
                "agent": "auto/build",
            }
        ],
    }
    monkeypatch.setattr(_ctx_mod, "_runtime_profile_sandbox_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_job_decision_pack",
        lambda *args, **kwargs: {
            "pack_version": 1,
            "authority_domains": ["workspace_boundary"],
            "decision_keys": [],
            "decisions": [],
        },
    )

    with pytest.raises(ValueError, match="requires an explicit job.workdir or top-level workdir"):
        _admission_mod.preview_workflow_execution(
            _FakeConn(),
            inline_spec=inline_spec,
            repo_root="/repo",
        )


def test_preview_workflow_execution_marks_non_agent_jobs_not_applicable(monkeypatch):
    inline_spec = {
        "name": "preview-spec-non-agent",
        "workflow_id": "workflow.preview_spec_non_agent",
        "phase": "build",
        "workdir": "/repo",
        "jobs": [
            {
                "label": "seed",
                "prompt": "Seed deterministic state.",
                "adapter_type": "deterministic_task",
                "expected_outputs": {"go": True},
            },
            {
                "label": "query_db",
                "prompt": "Query the current workflow state.",
                "adapter_type": "mcp_task",
                "mcp_tools": ["praxis_query"],
            },
        ],
    }
    monkeypatch.setattr(_ctx_mod, "_runtime_profile_sandbox_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_job_decision_pack",
        lambda *args, **kwargs: {
            "pack_version": 1,
            "authority_domains": ["workspace_boundary"],
            "decision_keys": [],
            "decisions": [],
        },
    )

    preview = _admission_mod.preview_workflow_execution(
        _FakeConn(),
        inline_spec=inline_spec,
        repo_root="/repo",
    )

    assert preview["warnings"] == []
    assert preview["jobs"][0]["route_status"] == "not_applicable"
    assert preview["jobs"][0]["requested_agent"] is None
    assert preview["jobs"][0]["resolved_agent"] is None
    assert preview["jobs"][1]["route_status"] == "not_applicable"
    assert preview["jobs"][1]["requested_agent"] is None
    assert preview["jobs"][1]["resolved_agent"] is None


def test_preview_and_submit_share_execution_assembly_until_submit_boundary(monkeypatch):
    inline_spec = {
        "name": "preview-submit-shared",
        "workflow_id": "workflow.preview_submit_shared",
        "phase": "build",
        "definition_revision": "definition.preview_submit_shared",
        "plan_revision": "plan.preview_submit_shared",
        "workdir": "/repo",
        "jobs": [
            {
                "label": "build_a",
                "prompt": "Implement the shared preview lane contract.",
                "agent": "openai/gpt-5.4-mini",
                "adapter_type": "cli_llm",
                "task_type": "build",
                "write_scope": ["runtime/workflow/preview.py"],
                "read_scope": ["runtime/workflow/_admission.py"],
                "verify_refs": ["verify.preview_shared"],
            }
        ],
    }
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=["runtime/workflow/_admission.py"],
            test_scope=["tests/unit/test_unified_workflow.py"],
            blast_radius=["runtime/workflow/_admission.py"],
            context_sections=[
                {
                    "name": "Existing Contract",
                    "content": "Preview and submit must share execution assembly.",
                }
            ],
        ),
    )
    monkeypatch.setattr(
        _ctx_mod,
        "proof_metrics",
        lambda conn: {
            "receipts": {
                "total": 10,
                "verification_coverage": 0.5,
                "fully_proved_verification_coverage": 0.25,
                "write_manifest_coverage": 0.75,
            },
            "compile_authority": {
                "execution_packets_ready": True,
                "verify_refs_ready": True,
                "verification_registry_ready": True,
                "repo_snapshots_ready": True,
            },
        },
    )
    monkeypatch.setattr(_ctx_mod, "_runtime_profile_sandbox_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_job_decision_pack",
        lambda *args, **kwargs: {
            "pack_version": 1,
            "authority_domains": ["workspace_boundary"],
            "decision_keys": ["authority.workspace_boundary"],
            "decisions": [{"decision_key": "authority.workspace_boundary"}],
        },
    )
    monkeypatch.setattr(_admission_mod, "_runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._workspace_ref_from_spec", lambda _spec, conn=None: None)

    preview_conn = _FakeConn()
    preview = _admission_mod.preview_workflow_execution(
        preview_conn,
        inline_spec=inline_spec,
        repo_root="/repo",
    )

    submit_conn = _FakeConn()
    _admission_mod.submit_workflow_inline(
        submit_conn,
        inline_spec,
        run_id="dispatch_preview_submit_shared",
        packet_provenance={
            "source_kind": "inline_submit",
            "repo_root": "/repo",
            "file_inputs": inline_spec,
        },
    )

    _, insert_args = next(
        (query, args)
        for query, args in submit_conn.queries
        if "INSERT INTO workflow_job_runtime_context" in query
    )
    persisted_shard = (
        json.loads(insert_args[3]) if isinstance(insert_args[3], str) else insert_args[3]
    )
    persisted_bundle = (
        json.loads(insert_args[4]) if isinstance(insert_args[4], str) else insert_args[4]
    )
    submit_messages = _ctx_mod._execution_model_messages(
        {
            **inline_spec["jobs"][0],
            "_execution_context": persisted_shard,
            "_execution_bundle": persisted_bundle,
        }
    )
    preview_bundle = dict(preview["execution_bundles"]["build_a"])
    submit_bundle = dict(persisted_bundle)
    preview_bundle.pop("run_id", None)
    submit_bundle.pop("run_id", None)

    assert preview["execution_context_shards"]["build_a"] == persisted_shard
    assert preview_bundle == submit_bundle
    assert preview["jobs"][0]["messages"] == submit_messages
    assert (
        preview["jobs"][0]["rendered_execution_context_shard"]
        in preview["jobs"][0]["rendered_user_prompt"]
    )
    assert (
        preview["jobs"][0]["rendered_execution_bundle"]
        in preview["jobs"][0]["rendered_user_prompt"]
    )
    assert any("INSERT INTO workflow_job_runtime_context" in query for query, _ in submit_conn.queries)


def test_submit_workflow_prefers_execution_manifest_authority_over_prompt_bucket(monkeypatch):
    jobs = [
        {
            "label": "build_a",
            "prompt": "Write an architecture essay about otters.",
            "agent": "auto/build",
            "write_scope": ["app.py"],
        },
    ]
    raw = {
        "name": "bundle-spec",
        "workflow_id": "workflow.bundle_spec",
        "phase": "build",
        "jobs": [dict(job) for job in jobs],
        "definition_revision": "definition.bundle",
        "plan_revision": "plan.bundle",
        "execution_manifest": {
            "execution_manifest_ref": "execution_manifest:wf:definition.bundle:1",
            "approved_bundle_refs": ["capability_bundle:email_triage"],
            "tool_allowlist": {
                "mcp_tools": ["praxis_integration", "praxis_status_snapshot"],
                "adapter_tools": ["repo_fs"],
            },
            "verify_refs": ["verify.approved"],
        },
    }
    spec = SimpleNamespace(
        name="bundle-spec",
        workflow_id="workflow.bundle_spec",
        phase="build",
        jobs=jobs,
        verify_refs=[],
        outcome_goal="",
        anti_requirements=[],
        workspace_ref=None,
        runtime_profile_ref=None,
        _raw=raw,
    )
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=[],
            test_scope=[],
            blast_radius=[],
            context_sections=[],
        ),
    )
    monkeypatch.setattr(_ctx_mod, "proof_metrics", lambda conn: {})
    monkeypatch.setattr(_admission_mod, "_runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._workspace_ref_from_spec", lambda _spec, conn=None: None)

    conn = _FakeConn()

    unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_bundle_manifest")

    _, insert_args = next(
        (query, args)
        for query, args in conn.queries
        if "INSERT INTO execution_packets" in query
    )
    file_inputs = json.loads(insert_args[17])
    bundle = file_inputs["execution_bundles"]["build_a"]

    assert bundle["execution_manifest_ref"] == "execution_manifest:wf:definition.bundle:1"
    assert bundle["approved_bundle_refs"] == ["capability_bundle:email_triage"]
    assert bundle["allowed_tools"] == ["repo_fs"]
    assert "praxis_integration" in bundle["mcp_tool_names"]
    assert "praxis_status_snapshot" in bundle["mcp_tool_names"]
    assert "praxis_query" not in bundle["mcp_tool_names"]
    assert bundle["access_policy"]["verify_refs"] == ["verify.approved"]
    assert bundle["access_policy"]["write_scope"] == ["app.py"]
    assert bundle["access_policy"]["resolved_read_scope"] == []
    assert bundle["access_policy"]["blast_radius"] == []
    assert any("INSERT INTO workflow_job_runtime_context" in query for query, _ in conn.queries)


def test_submit_workflow_inline_fails_closed_for_builder_path_without_execution_manifest(monkeypatch):
    monkeypatch.setattr(_admission_mod, "_runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._runtime_profile_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr("runtime.workflow._routing._workspace_ref_from_spec", lambda _spec, conn=None: None)
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_scope",
        lambda write_scope, root_dir: SimpleNamespace(
            computed_read_scope=[],
            test_scope=[],
            blast_radius=[],
            context_sections=[],
        ),
    )
    monkeypatch.setattr(_ctx_mod, "proof_metrics", lambda conn: {})

    conn = _FakeConn()

    with pytest.raises(RuntimeError) as exc_info:
        unified_workflow.submit_workflow_inline(
            conn,
            {
                "name": "builder-inline",
                "workflow_id": "wf.builder.inline",
                "phase": "build",
                "definition_revision": "definition.builder.inline",
                "plan_revision": "plan.builder.inline",
                "jobs": [
                    {
                        "label": "build_a",
                        "prompt": "Write an architecture essay about otters.",
                        "agent": "auto/build",
                        "write_scope": ["app.py"],
                    }
                ],
                "packet_provenance": {
                    "source_kind": "workflow_trigger",
                },
            },
            run_id="dispatch_builder_missing_manifest",
        )

    assert "ExecutionManifest authority" in str(exc_info.value)


def test_submit_workflow_replays_existing_idempotency(monkeypatch, caplog):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    prompt_hash = hashlib.sha256("create a".encode()).hexdigest()[:16]
    idem_key = f"{spec.name}:build_a:{prompt_hash}"
    payload_hash = canonical_hash(
        {"spec_name": spec.name, "label": "build_a", "prompt_hash": prompt_hash}
    )
    conn = _FakeConn(
        existing_idempotency={
            ("workflow.run", idem_key): {
                "payload_hash": payload_hash,
                "run_id": "dispatch_existing",
                "created_at": None,
            },
        },
        existing_run_states={"dispatch_existing": "succeeded"},
    )

    caplog.set_level(logging.INFO)

    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo")

    assert "Idempotent replay: returning existing run_id=dispatch_existing" in caplog.text
    assert conn.next_job_id == 0
    assert any("DELETE FROM workflow_runs" in query for query, _ in conn.queries)
    assert not any("FROM compile_artifacts" in query for query, _ in conn.queries)
    assert not any("INSERT INTO execution_packets" in query for query, _ in conn.queries)
    assert result["run_id"] == "dispatch_existing"
    assert result["status"] == "replayed"
    assert result["replayed_jobs"] == [{"label": "build_a", "existing_run_id": "dispatch_existing"}]


def test_submit_workflow_honors_explicit_run_id_over_existing_replay(monkeypatch, caplog):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    prompt_hash = hashlib.sha256("create a".encode()).hexdigest()[:16]
    idem_key = f"{spec.name}:build_a:{prompt_hash}"
    payload_hash = canonical_hash(
        {"spec_name": spec.name, "label": "build_a", "prompt_hash": prompt_hash}
    )
    conn = _FakeConn(
        existing_idempotency={
            ("workflow.run", idem_key): {
                "payload_hash": payload_hash,
                "run_id": "dispatch_existing",
                "created_at": None,
            },
        },
        existing_run_states={"dispatch_existing": "succeeded"},
    )

    caplog.set_level(logging.INFO)

    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_forced")

    assert "Idempotent replay: returning existing run_id=dispatch_existing" not in caplog.text
    assert result["run_id"] == "dispatch_forced"
    assert result["status"] == "queued"
    assert result["replayed_jobs"] == []
    assert conn.next_job_id == 1
    assert not any("DELETE FROM workflow_runs" in query for query, _ in conn.queries)


def test_submit_workflow_force_fresh_run_keeps_system_owned_run_id(monkeypatch, caplog):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))
    monkeypatch.setattr(
        _admission_mod.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="freshgenerated1234567890"),
    )

    prompt_hash = hashlib.sha256("create a".encode()).hexdigest()[:16]
    idem_key = f"{spec.name}:build_a:{prompt_hash}"
    payload_hash = canonical_hash(
        {"spec_name": spec.name, "label": "build_a", "prompt_hash": prompt_hash}
    )
    conn = _FakeConn(
        existing_idempotency={
            ("workflow.run", idem_key): {
                "payload_hash": payload_hash,
                "run_id": "dispatch_existing",
                "created_at": None,
            },
        },
        existing_run_states={"dispatch_existing": "succeeded"},
    )

    caplog.set_level(logging.INFO)

    result = unified_workflow.submit_workflow(
        conn,
        "spec.queue.json",
        "/repo",
        force_fresh_run=True,
    )

    assert "Idempotent replay: returning existing run_id=dispatch_existing" not in caplog.text
    assert result["run_id"] == "workflow_freshgenerat"
    assert result["status"] == "queued"
    assert result["replayed_jobs"] == []
    assert conn.next_job_id == 1
    assert not any("DELETE FROM workflow_runs" in query for query, _ in conn.queries)


def test_submit_workflow_creates_fresh_run_after_failed_idempotent_attempt(monkeypatch):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    prompt_hash = hashlib.sha256("create a".encode()).hexdigest()[:16]
    idem_key = f"{spec.name}:build_a:{prompt_hash}"
    payload_hash = canonical_hash(
        {"spec_name": spec.name, "label": "build_a", "prompt_hash": prompt_hash}
    )
    conn = _FakeConn(
        existing_idempotency={
            ("workflow.run", idem_key): {
                "payload_hash": payload_hash,
                "run_id": "dispatch_failed",
                "created_at": None,
            },
        },
        existing_run_states={"dispatch_failed": "failed"},
    )

    result = unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo", run_id="dispatch_retry")

    assert result["run_id"] == "dispatch_retry"
    assert result["status"] == "queued"
    assert conn.next_job_id == 1
    assert not any("DELETE FROM workflow_runs" in query for query, _ in conn.queries)
    workflow_job_insert = next(
        args for query, args in conn.queries if "INSERT INTO workflow_jobs" in query
    )
    assert workflow_job_insert[12].startswith("dispatch_retry:")


def test_submit_workflow_raises_on_idempotency_conflict(monkeypatch, caplog):
    jobs = [
        {"label": "build_a", "prompt": "create a", "agent": "auto/build"},
    ]
    spec = _make_spec(jobs)
    monkeypatch.setattr(WorkflowSpec, "load", classmethod(lambda cls, path: spec))

    prompt_hash = hashlib.sha256("create a".encode()).hexdigest()[:16]
    idem_key = f"{spec.name}:build_a:{prompt_hash}"
    conn = _FakeConn(
        existing_idempotency={
            ("workflow.run", idem_key): {
                "payload_hash": "different-payload",
                "run_id": "dispatch_existing",
                "created_at": None,
            },
        }
    )

    caplog.set_level(logging.WARNING)

    with pytest.raises(unified_dispatch.IdempotencyConflict) as excinfo:
        unified_workflow.submit_workflow(conn, "spec.queue.json", "/repo")

    assert "Idempotency conflict: key=" in caplog.text
    assert excinfo.value.idempotency_key == idem_key
    assert excinfo.value.existing_run_id == "dispatch_existing"


def test_retry_job_records_dispatch_retry_idempotency(monkeypatch, caplog):
    conn = _FakeConn(
        existing_jobs={
            ("dispatch_test", "build_a"): {
                "id": 7,
                "label": "build_a",
                "prompt": "create a",
                "agent_slug": "openai/gpt-5.4",
                "resolved_agent": "openai/gpt-5.4",
                "status": "failed",
                "attempt": 2,
            },
        }
    )

    caplog.set_level(logging.INFO)

    result = unified_workflow.retry_job(conn, "dispatch_test", "build_a")

    assert result["status"] == "requeued"
    assert result["attempt"] == 3
    assert any(
        "INSERT INTO idempotency_ledger" in query and args[0] == "workflow.retry"
        for query, args in conn.queries
    )
    retry_ledger = [
        args for query, args in conn.queries
        if "INSERT INTO idempotency_ledger" in query and args[0] == "workflow.retry"
    ]
    assert retry_ledger[0][1] == "dispatch_test:build_a:retry:failed:2"
    retry_updates = [
        query for query, args in conn.queries
        if "UPDATE workflow_jobs" in query and "SET status = 'ready'" in query and args == ("dispatch_test", "build_a")
    ]
    assert retry_updates
    assert "next_retry_at = NULL" in retry_updates[0]
    assert "failure_category = ''" in retry_updates[0]
    assert "failure_zone = ''" in retry_updates[0]
    assert "is_transient = false" in retry_updates[0]


def test_retry_job_reopens_graph_runtime_run_from_evidence(monkeypatch):
    from runtime.workflow import _status as status_mod

    conn = _FakeConn(
        workflow_run_rows=[
            {
                "run_id": "graph_retry_run",
                "workflow_id": "graph_retry",
                "current_state": "failed",
                "terminal_reason_code": "sandbox_error",
                "request_envelope": {"nodes": [], "edges": []},
                "requested_at": datetime.now(timezone.utc),
                "finished_at": datetime.now(timezone.utc),
            }
        ]
    )
    monkeypatch.setattr(
        status_mod,
        "_graph_job_rows_from_evidence",
        lambda *, run_row, run_id: [
            {
                "id": 2,
                "label": "graph_build",
                "status": "failed",
                "attempt": 1,
                "last_error_code": "sandbox_error",
                "stdout_preview": "receipt:graph_retry_run:16",
            }
        ],
    )

    result = unified_workflow.retry_job(conn, "graph_retry_run", "graph_build")

    assert result["status"] == "requeued"
    assert result["execution_mode"] == "graph_runtime"
    assert conn.workflow_run_rows[0]["current_state"] == "claim_accepted"
    assert any(
        "UPDATE workflow_runs" in query and "SET current_state = 'claim_accepted'" in query
        for query, _ in conn.queries
    )
    assert any("SELECT pg_notify('system_event'" in query for query, _ in conn.queries)


def test_retry_job_rejects_when_queue_is_at_critical_threshold():
    conn = _FakeConn(
        queue_depth=1000,
        existing_jobs={
            ("dispatch_test", "build_a"): {
                "id": 7,
                "label": "build_a",
                "prompt": "create a",
                "agent_slug": "openai/gpt-5.4",
                "resolved_agent": "openai/gpt-5.4",
                "status": "failed",
                "attempt": 2,
            },
        },
    )

    with pytest.raises(RuntimeError, match="queue admission rejected"):
        unified_workflow.retry_job(conn, "dispatch_test", "build_a")

    assert not any(
        "UPDATE workflow_jobs" in query and "SET status = 'ready'" in query
        for query, _ in conn.queries
    )


def test_retry_job_uses_shared_queue_admission_gate(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeGate:
        def __init__(self, *, critical_threshold: int = 1000, **_kwargs) -> None:
            captured["critical_threshold"] = critical_threshold

        def check_connection(self, conn, *, job_count: int = 1):
            captured["conn"] = conn
            captured["job_count"] = job_count
            return SimpleNamespace(admitted=True, queue_depth=0, reason="ok")

    monkeypatch.setattr(_admission_mod, "QueueAdmissionGate", _FakeGate)

    conn = _FakeConn(
        existing_jobs={
            ("dispatch_test", "build_a"): {
                "id": 7,
                "label": "build_a",
                "prompt": "create a",
                "agent_slug": "openai/gpt-5.4",
                "resolved_agent": "openai/gpt-5.4",
                "status": "failed",
                "attempt": 2,
            },
        }
    )

    result = unified_workflow.retry_job(conn, "dispatch_test", "build_a")

    assert result["status"] == "requeued"
    assert captured["conn"] is conn
    assert captured["job_count"] == 1


def test_retry_job_reports_validated_packet_reuse_provenance() -> None:
    packet_lineage_hash = "packet_lineage_hash.alpha"
    materialize_provenance = {
        "input_fingerprint": "packet-input.alpha",
        "packet_lineage_revision": "packet_lineage.alpha",
        "packet_lineage_hash": packet_lineage_hash,
        "reuse": {
            "decision": "compiled",
            "reason_code": "packet.compile.miss",
        },
    }
    packet_lineage_payload = {
        "definition_revision": "def_alpha",
        "plan_revision": "plan_alpha",
        "packet_version": 1,
        "workflow_id": "workflow.alpha",
        "spec_name": "alpha workflow",
        "source_kind": "workflow_submit",
        "authority_refs": ["def_alpha", "plan_alpha"],
        "model_messages": [],
        "reference_bindings": [],
        "capability_bindings": [],
        "verify_refs": [],
        "authority_inputs": {},
        "file_inputs": {},
        "materialize_provenance": dict(materialize_provenance),
        "packet_hash": packet_lineage_hash,
        "packet_revision": materialize_provenance["packet_lineage_revision"],
        "decision_ref": "decision.compile.packet.lineage.alpha",
        "parent_artifact_ref": "plan_alpha",
    }
    artifact_content_hash = hashlib.sha256(
        json.dumps(packet_lineage_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    conn = _FakeConn(
        existing_jobs={
            ("dispatch_test", "build_a"): {
                "id": 7,
                "label": "build_a",
                "prompt": "create a",
                "agent_slug": "openai/gpt-5.4",
                "resolved_agent": "openai/gpt-5.4",
                "status": "failed",
                "attempt": 2,
            },
        },
        compile_artifact_rows=[
            {
                "compile_artifact_id": "compile_artifact.packet_lineage.alpha",
                "artifact_kind": "packet_lineage",
                "artifact_ref": "packet_lineage.alpha",
                "revision_ref": "packet_lineage.alpha",
                "parent_artifact_ref": "plan_alpha",
                "input_fingerprint": "packet-input.alpha",
                "content_hash": artifact_content_hash,
                "authority_refs": ["def_alpha", "plan_alpha"],
                "payload": packet_lineage_payload,
                "decision_ref": "decision.compile.packet.lineage.alpha",
            },
        ],
        execution_packet_rows=[
            {
                "execution_packet_id": "execution_packet.dispatch_test.packet_alpha",
                "definition_revision": "def_alpha",
                "plan_revision": "plan_alpha",
                "packet_revision": "packet_execution.alpha:1",
                "parent_artifact_ref": "packet_lineage.alpha",
                "packet_version": 1,
                "packet_hash": "packet_execution_hash.alpha",
                "workflow_id": "workflow.alpha",
                "run_id": "dispatch_test",
                "spec_name": "alpha workflow",
                "source_kind": "workflow_submit",
                "authority_refs": ["def_alpha", "plan_alpha"],
                "model_messages": [],
                "reference_bindings": [],
                "capability_bindings": [],
                "verify_refs": [],
                "authority_inputs": {},
                "file_inputs": {},
                "payload": {
                    "materialize_provenance": materialize_provenance,
                },
                "decision_ref": "decision.compile.packet.execution.alpha",
            },
        ],
    )

    result = unified_workflow.retry_job(conn, "dispatch_test", "build_a")

    assert result["status"] == "requeued"
    assert result["packet_reuse_provenance"] == {
        "artifact_kind": "packet_lineage",
        "decision": "reused",
        "reason_code": "packet.retry.existing_execution_packet",
        "input_fingerprint": "packet-input.alpha",
        "artifact_ref": "packet_lineage.alpha",
        "revision_ref": "packet_lineage.alpha",
        "content_hash": artifact_content_hash,
        "packet_lineage_hash": packet_lineage_hash,
        "decision_ref": "decision.compile.packet.lineage.alpha",
        "execution_packet_ref": "packet_execution.alpha:1",
        "recorded_submission_reuse": {
            "decision": "compiled",
            "reason_code": "packet.compile.miss",
        },
    }


def test_retry_job_rejects_stale_packet_lineage_artifact() -> None:
    packet_lineage_hash = "packet_lineage_hash.alpha"
    packet_lineage_payload = {
        "definition_revision": "def_alpha",
        "plan_revision": "plan_alpha",
        "packet_version": 1,
        "workflow_id": "workflow.alpha",
        "spec_name": "alpha workflow",
        "source_kind": "workflow_submit",
        "authority_refs": ["def_alpha", "plan_alpha"],
        "model_messages": [],
        "reference_bindings": [],
        "capability_bindings": [],
        "verify_refs": [],
        "authority_inputs": {},
        "file_inputs": {},
        "materialize_provenance": {
            "input_fingerprint": "packet-input.alpha",
        },
        "packet_hash": packet_lineage_hash,
        "packet_revision": "packet_lineage.alpha",
        "decision_ref": "decision.compile.packet.lineage.alpha",
        "parent_artifact_ref": "plan_alpha",
    }
    conn = _FakeConn(
        existing_jobs={
            ("dispatch_test", "build_a"): {
                "id": 7,
                "label": "build_a",
                "prompt": "create a",
                "agent_slug": "openai/gpt-5.4",
                "resolved_agent": "openai/gpt-5.4",
                "status": "failed",
                "attempt": 2,
            },
        },
        compile_artifact_rows=[
            {
                "compile_artifact_id": "compile_artifact.packet_lineage.alpha",
                "artifact_kind": "packet_lineage",
                "artifact_ref": "packet_lineage.alpha",
                "revision_ref": "packet_lineage.alpha",
                "parent_artifact_ref": "plan_alpha",
                "input_fingerprint": "packet-input.alpha",
                "content_hash": "corrupt",
                "authority_refs": ["def_alpha", "plan_alpha"],
                "payload": packet_lineage_payload,
                "decision_ref": "decision.compile.packet.lineage.alpha",
            },
        ],
        execution_packet_rows=[
            {
                "execution_packet_id": "execution_packet.dispatch_test.packet_alpha",
                "definition_revision": "def_alpha",
                "plan_revision": "plan_alpha",
                "packet_revision": "packet_execution.alpha:1",
                "parent_artifact_ref": "packet_lineage.alpha",
                "packet_version": 1,
                "packet_hash": "packet_execution_hash.alpha",
                "workflow_id": "workflow.alpha",
                "run_id": "dispatch_test",
                "spec_name": "alpha workflow",
                "source_kind": "workflow_submit",
                "authority_refs": ["def_alpha", "plan_alpha"],
                "model_messages": [],
                "reference_bindings": [],
                "capability_bindings": [],
                "verify_refs": [],
                "authority_inputs": {},
                "file_inputs": {},
                "payload": {
                    "materialize_provenance": {
                        "input_fingerprint": "packet-input.alpha",
                    },
                },
                "decision_ref": "decision.compile.packet.execution.alpha",
            },
        ],
    )

    with pytest.raises(RuntimeError, match="retry compile reuse failed closed"):
        unified_workflow.retry_job(conn, "dispatch_test", "build_a")


def test_claim_and_stale_queries_use_heartbeat_state():
    assert "heartbeat_at = now()" in inspect.getsource(unified_dispatch.claim_one)
    assert "COALESCE(heartbeat_at, claimed_at) < now() - interval '5 minutes'" in unified_dispatch.STALE_REAPER_QUERY


def test_write_output_uses_static_run_id_basename(tmp_path):
    output_path = unified_dispatch._write_output(
        str(tmp_path),
        "workflow_run_abc123",
        42,
        "Build A",
        {"stdout": "hello"},
    )

    assert output_path.endswith("workflow_output_workflow_run_abc123_job_42_build.a.md")
    assert Path(output_path).read_text() == "hello"


def test_write_output_extracts_transcript_stdout(tmp_path):
    # Transcript JSONL is now extracted — agent_message text is written as the artifact,
    # not the raw event stream.
    transcript = "\n".join([
        json.dumps({"type": "thread.started", "thread_id": "thread_123"}),
        json.dumps({"type": "turn.started"}),
        json.dumps({
            "type": "item.completed",
            "item": {
                "id": "item_1",
                "type": "agent_message",
                "text": "working",
            },
        }),
        json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 1}}),
    ])

    output_path = unified_dispatch._write_output(
        str(tmp_path),
        "workflow_run_transcript",
        6,
        "Review",
        {"stdout": transcript},
    )

    assert output_path != ""
    written = Path(output_path).read_text()
    assert "working" in written
    assert "thread.started" not in written  # raw JSONL not written


def test_write_output_truncates_recursive_workflow_output_capture(tmp_path):
    recursive_line = json.dumps({
        "type": "item.completed",
        "item": {
            "id": "item_8",
            "type": "command_execution",
            "command": "rg -n artifacts",
            "aggregated_output": "artifacts/workflow_outputs/workflow_output_workflow_old_job_1_build.md\n" + ("x" * 50_000),
            "exit_code": 0,
            "status": "completed",
        },
    })

    output_path = unified_dispatch._write_output(
        str(tmp_path),
        "workflow_run_recursive",
        7,
        "Audit",
        {"stdout": recursive_line + "\n"},
    )

    written = Path(output_path).read_text()
    payload = json.loads(written.strip())
    aggregated_output = payload["item"]["aggregated_output"]
    assert "recursive workflow_output capture" in aggregated_output
    assert len(aggregated_output) < 10_000


def test_write_output_caps_oversized_plain_stdout(tmp_path):
    output_path = unified_dispatch._write_output(
        str(tmp_path),
        "workflow_run_big",
        9,
        "Compile",
        {"stdout": "a" * 300_000},
    )

    written = Path(output_path).read_text()
    assert "workflow output artifact limit" in written
    assert len(written) < 251_000


def test_write_job_receipt_writes_authority_receipt_and_notification():
    class _ReceiptConn:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple]] = []

        def execute(self, query: str, *args):
            self.queries.append((query, args))
            normalized = " ".join(query.split())
            if "FROM workflow_jobs j JOIN workflow_runs wr ON wr.run_id = j.run_id" in normalized:
                return [{
                    "workflow_id": "workflow.db.only.phase.wave0",
                    "request_id": "req_workflow_test",
                    "request_envelope": {
                        "workspace_ref": "workspace://praxis",
                        "runtime_profile_ref": "runtime://praxis",
                        "spec_snapshot": {
                            "workdir": "/repo",
                            "write_scope": ["runtime/example.py"],
                        },
                    },
                    "attempt": 2,
                    "started_at": datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 8, 18, 0, 5, tzinfo=timezone.utc),
                    "touch_keys": [
                        {"key": "file:runtime/example.py", "mode": "write"},
                    ],
                }]
            if "WITH lock_token AS (" in normalized and "INSERT INTO receipts (" in normalized:
                return [{"evidence_seq": 1702}]
            return []

    conn = _ReceiptConn()

    receipt_id = unified_dispatch._write_job_receipt(
        conn,
        "workflow_test",
        17,
        "build_a",
        "openai/gpt-5.4",
        {
            "status": "failed",
            "error_code": "boom",
            "token_input": 1,
            "token_output": 2,
            "cost_usd": 0.5,
            "workspace_snapshot_ref": "workspace_snapshot:abc123",
            "workspace_snapshot_cache_hit": True,
            "workspace_manifest_audit": {
                "intended_manifest_paths": ["runtime/example.py", "runtime/context.py"],
                "hydrated_manifest_paths": ["runtime/example.py"],
                "missing_intended_paths": ["runtime/context.py"],
                "observed_file_read_refs": ["runtime/context.py"],
                "observed_file_read_mode": "provider_output_path_mentions",
            },
        },
        2500,
        repo_root="/repo",
        output_path="/repo/artifacts/workflow_outputs/workflow_output_workflow_test_job_17_build.a.md",
        final_status="failed",
        final_error_code="verification.failed",
        verification_summary={
            "total": 2,
            "passed": 1,
            "failed": 1,
            "all_passed": False,
            "results": [
                {"label": "py_compile", "passed": False, "exit_code": 1},
            ],
        },
        verification_bindings=[
            {
                "verification_ref": "verification.python.py_compile",
                "inputs": {"path": "runtime/example.py"},
            },
        ],
    )

    assert receipt_id == "receipt:workflow_test:17:2"
    query_texts = [query for query, _ in conn.queries]
    receipts_idx = next(i for i, query in enumerate(query_texts) if "INSERT INTO receipts" in query)
    receipt_insert_query = query_texts[receipts_idx]
    receipt_insert_args = conn.queries[receipts_idx][1]
    receipt_inputs = json.loads(receipt_insert_args[15])
    receipt_outputs = json.loads(receipt_insert_args[16])
    assert "'{transition_seq}'" in receipt_insert_query
    assert not any("INSERT INTO workflow_notifications" in query for query in query_texts)
    assert any("SELECT pg_notify('job_completed'" in query for query in query_texts)
    assert receipt_inputs["workspace_root"] == "/repo"
    assert receipt_inputs["workspace_ref"] == "workspace://praxis"
    assert receipt_inputs["runtime_profile_ref"] == "runtime://praxis"
    assert receipt_inputs["write_scope"] == ["runtime/example.py"]
    assert receipt_inputs["touch_keys"] == [{"key": "file:runtime/example.py", "mode": "write"}]
    assert receipt_outputs["status"] == "failed"
    assert receipt_outputs["failure_code"] == "verification.failed"
    assert receipt_outputs["failure_classification"]["category"] == "verification_failed"
    assert receipt_outputs["failure_classification"]["is_retryable"] is False
    assert receipt_outputs["stderr_preview"] == ""
    assert receipt_outputs["verification_status"] == "failed"
    assert receipt_outputs["verified_paths"] == ["runtime/example.py"]
    assert receipt_outputs["verification"]["failed"] == 1
    assert receipt_outputs["workspace_provenance"]["workspace_root"] == "/repo"
    assert receipt_outputs["workspace_provenance"]["workspace_snapshot_ref"] == "workspace_snapshot:abc123"
    assert receipt_outputs["workspace_snapshot_cache_hit"] is True
    assert receipt_outputs["workspace_manifest_audit"]["missing_intended_paths"] == [
        "runtime/context.py"
    ]
    assert receipt_outputs["workspace_manifest_audit"]["observed_file_read_refs"] == [
        "runtime/context.py"
    ]
    assert receipt_outputs["git_provenance"]["available"] is False
    assert receipt_outputs["write_manifest"]["total_files"] == 1
    assert receipt_outputs["mutation_provenance"]["write_paths"] == ["runtime/example.py"]
    assert receipt_insert_args[11] == datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc)
    assert receipt_insert_args[12] == datetime(2026, 4, 8, 18, 0, 5, tzinfo=timezone.utc)
    notify_idx = next(i for i, query in enumerate(query_texts) if "SELECT pg_notify('job_completed'" in query)
    assert notify_idx > receipts_idx


def test_complete_job_requeues_rate_limit_failures_to_next_agent(monkeypatch):
    class _BreakerStub:
        def record_outcome(self, provider: str, *, succeeded: bool, failure_code: str | None = None) -> None:
            self.last_call = (provider, succeeded, failure_code)

    class _RetryConn:
        def __init__(self) -> None:
            self.requeue_args: tuple | None = None
            self.terminal_update = False

        def execute(self, query: str, *args):
            if "UPDATE workflow_jobs" in query and "SET status = 'ready'" in query:
                self.requeue_query = query
                self.requeue_args = args
                return []
            if "UPDATE workflow_jobs" in query and "SET status = $2" in query:
                self.terminal_query = query
                self.terminal_args = args
            if "SELECT status, run_id, route_task_type" in query:
                return [{
                    "status": "running",
                    "run_id": "run-1",
                    "route_task_type": "debate",
                    "effective_agent": "google/gemini-3.1-pro-preview",
                }]
            if "SELECT resolved_agent, agent_slug FROM workflow_jobs WHERE id = $1" in query:
                return [{"resolved_agent": "google/gemini-3.1-pro-preview", "agent_slug": "auto/debate"}]
            if "SELECT attempt, max_attempts, failover_chain, resolved_agent FROM workflow_jobs WHERE id = $1" in query:
                return [{
                    "attempt": 1,
                    "max_attempts": 3,
                    "failover_chain": [
                        "google/gemini-3.1-pro-preview",
                        "anthropic/claude-opus-4-6",
                        "openai/gpt-5.4",
                    ],
                    "resolved_agent": "google/gemini-3.1-pro-preview",
                }]
            if "FROM failure_category_zones" in query:
                return [{"category": "rate_limit", "zone": "external"}]
            if "UPDATE workflow_jobs" in query and "SET status = 'ready'" in query:
                self.requeue_args = args
                return []
            if "UPDATE workflow_jobs" in query and "SET status = $2" in query:
                self.terminal_update = True
                return []
            return []

    class _Decision:
        should_requeue = True
        next_agent = "anthropic/claude-opus-4-6"
        backoff_seconds = 5
        action = "requeue"
        reason = "rate limit retry"

    monkeypatch.setattr(retry_orchestrator, "decide", lambda **kwargs: _Decision())

    conn = _RetryConn()
    monkeypatch.setattr(_shared_mod, "_CIRCUIT_BREAKERS", _BreakerStub())

    _claiming_mod.complete_job(
        conn,
        7,
        status="failed",
        error_code="rate_limit",
        stdout_preview="429 Too Many Requests",
    )

    assert conn.requeue_args is not None
    assert "failure_category" in conn.requeue_query
    assert "failure_zone" in conn.requeue_query
    assert "is_transient" in conn.requeue_query
    assert conn.requeue_args[0] == 7
    assert conn.requeue_args[1] == "rate_limit"
    assert conn.requeue_args[2] == "rate_limit"
    assert conn.requeue_args[3] == "external"
    assert conn.requeue_args[4] is True
    assert conn.requeue_args[5] == "anthropic/claude-opus-4-6"
    assert conn.requeue_args[6] == "5"
    assert conn.terminal_update is False


def test_complete_job_terminal_failure_update_writes_failure_columns(monkeypatch):
    class _BreakerStub:
        def record_outcome(self, provider: str, *, succeeded: bool, failure_code: str | None = None) -> None:
            self.last_call = (provider, succeeded, failure_code)

    class _TerminalConn:
        def __init__(self) -> None:
            self.terminal_query: str | None = None
            self.terminal_args: tuple | None = None

        def execute(self, query: str, *args):
            if "UPDATE workflow_jobs" in query and "SET status = $2" in query:
                self.terminal_query = query
                self.terminal_args = args
                return []
            if "SELECT status, run_id, route_task_type" in query:
                return [{
                    "status": "running",
                    "run_id": "run-1",
                    "route_task_type": "debate",
                    "effective_agent": "google/gemini-3.1-pro-preview",
                }]
            if "SELECT resolved_agent, agent_slug FROM workflow_jobs WHERE id = $1" in query:
                return [{"resolved_agent": "google/gemini-3.1-pro-preview", "agent_slug": "auto/debate"}]
            if "SELECT attempt, max_attempts, failover_chain, resolved_agent FROM workflow_jobs WHERE id = $1" in query:
                return [{
                    "attempt": 1,
                    "max_attempts": 1,
                    "failover_chain": [
                        "google/gemini-3.1-pro-preview",
                    ],
                    "resolved_agent": "google/gemini-3.1-pro-preview",
                }]
            if "FROM failure_category_zones" in query:
                return [{"category": "verification_failed", "zone": "internal"}]
            return []

    class _Decision:
        should_requeue = False
        next_agent = None
        backoff_seconds = 0
        action = "dead_letter"
        reason = "terminal failure"

    monkeypatch.setattr(retry_orchestrator, "decide", lambda **kwargs: _Decision())

    conn = _TerminalConn()
    monkeypatch.setattr(_shared_mod, "_CIRCUIT_BREAKERS", _BreakerStub())

    _claiming_mod.complete_job(
        conn,
        7,
        status="failed",
        error_code="verification.failed",
        stdout_preview="verification failed",
    )

    assert conn.terminal_query is not None
    assert "failure_category" in conn.terminal_query
    assert "failure_zone" in conn.terminal_query
    assert "is_transient" in conn.terminal_query
    assert conn.terminal_args[11] == "verification_failed"
    assert conn.terminal_args[12] == "internal"
    assert conn.terminal_args[13] is False


def test_complete_job_non_retryable_failure_does_not_call_retry_orchestrator(monkeypatch):
    class _BreakerStub:
        def record_outcome(self, provider: str, *, succeeded: bool, failure_code: str | None = None) -> None:
            self.last_call = (provider, succeeded, failure_code)

    class _TerminalConn:
        def __init__(self) -> None:
            self.terminal_query: str | None = None
            self.terminal_args: tuple | None = None

        def execute(self, query: str, *args):
            if "UPDATE workflow_jobs" in query and "SET status = $2" in query:
                self.terminal_query = query
                self.terminal_args = args
                return []
            if "SELECT status, run_id, route_task_type" in query:
                return [{
                    "status": "running",
                    "run_id": "run-1",
                    "route_task_type": "build",
                    "effective_agent": "openai/gpt-5.4",
                }]
            if "SELECT resolved_agent, agent_slug FROM workflow_jobs WHERE id = $1" in query:
                return [{"resolved_agent": "openai/gpt-5.4", "agent_slug": "auto/build"}]
            if "SELECT attempt, max_attempts, failover_chain, resolved_agent FROM workflow_jobs WHERE id = $1" in query:
                return [{
                    "attempt": 1,
                    "max_attempts": 3,
                    "failover_chain": ["openai/gpt-5.4", "anthropic/claude-opus-4-6"],
                    "resolved_agent": "openai/gpt-5.4",
                }]
            if "FROM failure_category_zones" in query:
                return [{"category": "credential_error", "zone": "configuration"}]
            return []

    def _should_not_run(**kwargs):
        raise AssertionError("retry_orchestrator.decide should not run for non-retryable failures")

    monkeypatch.setattr(retry_orchestrator, "decide", _should_not_run)

    conn = _TerminalConn()
    monkeypatch.setattr(_shared_mod, "_CIRCUIT_BREAKERS", _BreakerStub())

    _claiming_mod.complete_job(
        conn,
        9,
        status="failed",
        error_code="credential_error",
        stdout_preview="missing provider credential",
    )

    assert conn.terminal_query is not None
    assert conn.terminal_args is not None
    assert conn.terminal_args[1] == "failed"
    assert conn.terminal_args[10] == "credential_error"
    assert conn.terminal_args[11] == "credential_error"
    assert conn.terminal_args[12] == "configuration"
    assert conn.terminal_args[13] is False


def test_complete_job_skips_success_write_when_job_was_cancelled_midflight(monkeypatch):
    downstream_calls: list[str] = []

    class _CancelledMidflightConn:
        def execute(self, query: str, *args):
            if "SELECT status, run_id, route_task_type" in query:
                return [{
                    "status": "running",
                    "run_id": "run-cancelled",
                    "route_task_type": "build",
                    "effective_agent": "openai/gpt-5.4",
                }]
            if "SELECT resolved_agent, agent_slug FROM workflow_jobs" in query:
                return [{
                    "resolved_agent": "openai/gpt-5.4",
                    "agent_slug": "openai/gpt-5.4",
                }]
            if "UPDATE workflow_jobs" in query and "SET status = 'succeeded'" in query:
                return []
            raise AssertionError(query)

    monkeypatch.setattr(
        _claiming_mod,
        "_record_task_route_outcome",
        lambda *args, **kwargs: downstream_calls.append("route"),
    )
    monkeypatch.setattr(
        _claiming_mod,
        "_release_ready_children",
        lambda *args, **kwargs: downstream_calls.append("release"),
    )
    monkeypatch.setattr(
        _claiming_mod,
        "_recompute_workflow_run_state",
        lambda *args, **kwargs: downstream_calls.append("recompute"),
    )

    _claiming_mod.complete_job(_CancelledMidflightConn(), 42, status="succeeded")

    assert downstream_calls == []


def test_select_claim_route_prefers_healthy_task_type_candidate() -> None:
    class _ClaimConn:
        def execute(self, query: str, *args):
            if "FROM route_policy_registry" in query:
                return [{
                    "task_rank_weight": 0.35,
                    "route_health_weight": 0.40,
                    "cost_weight": 0.10,
                    "benchmark_weight": 0.15,
                    "prefer_cost_task_rank_weight": 0.25,
                    "prefer_cost_route_health_weight": 0.35,
                    "prefer_cost_cost_weight": 0.30,
                    "prefer_cost_benchmark_weight": 0.10,
                    "claim_route_health_weight": 0.55,
                    "claim_rank_weight": 0.30,
                    "claim_load_weight": 0.15,
                    "claim_internal_failure_penalty_step": 0.08,
                    "claim_priority_penalty_step": 0.01,
                    "neutral_benchmark_score": 0.50,
                    "mixed_benchmark_score": 0.55,
                    "neutral_route_health": 0.65,
                    "min_route_health": 0.05,
                    "max_route_health": 1.0,
                    "success_health_bump": 0.04,
                    "review_success_bump": 0.02,
                    "consecutive_failure_penalty_step": 0.08,
                    "consecutive_failure_penalty_cap": 0.20,
                    "internal_failure_penalties": {"verification_failed": 0.25, "unknown": 0.10},
                    "review_severity_penalties": {"high": 0.15, "medium": 0.08, "low": 0.03},
                }]
            if "FROM failure_category_zones" in query:
                return [{"category": "verification_failed", "zone": "internal"}]
            if "FROM task_type_route_profiles" in query:
                return [{
                    "task_type": "build",
                    "affinity_labels": {
                        "primary": ["build", "coding"],
                        "secondary": ["review", "analysis", "wiring"],
                        "specialized": [],
                        "fallback": ["chat"],
                        "avoid": ["tts", "voice-agent", "audio", "image", "image-generation", "image-editing", "live-audio"],
                    },
                    "affinity_weights": {"primary": 1.0, "secondary": 0.7, "specialized": 0.4, "fallback": 0.2, "unclassified": 0.1, "avoid": 0.0},
                    "task_rank_weights": {"affinity": 0.6, "route_tier": 0.25, "latency": 0.15},
                    "benchmark_metric_weights": {},
                    "route_tier_preferences": ["high", "medium", "low"],
                    "latency_class_preferences": ["reasoning", "instant"],
                    "allow_unclassified_candidates": True,
                    "rationale": "build profile",
                }]
            if "FROM market_benchmark_metric_registry" in query:
                return []
            if "FROM provider_model_candidates" in query:
                return [
                    {
                        "candidate_ref": "cand-openai",
                        "provider_slug": "openai",
                        "model_slug": "gpt-5.4",
                        "priority": 1,
                        "route_tier": "high",
                        "route_tier_rank": 1,
                        "latency_class": "reasoning",
                        "latency_rank": 1,
                        "capability_tags": ["build", "coding"],
                        "task_affinities": {"primary": ["build"], "secondary": ["review"], "specialized": [], "avoid": []},
                        "benchmark_profile": {},
                    },
                    {
                        "candidate_ref": "cand-anthropic",
                        "provider_slug": "anthropic",
                        "model_slug": "claude-sonnet-4-6",
                        "priority": 1,
                        "route_tier": "medium",
                        "route_tier_rank": 1,
                        "latency_class": "instant",
                        "latency_rank": 1,
                        "capability_tags": ["build", "analysis"],
                        "task_affinities": {"primary": ["build"], "secondary": ["analysis"], "specialized": [], "avoid": []},
                        "benchmark_profile": {},
                    },
                ]
            if "FROM workflow_runs" in query:
                return [{"runtime_profile_ref": "nate-private"}]
            if "FROM effective_private_provider_job_catalog" in query:
                return [
                    {
                        "runtime_profile_ref": "nate-private",
                        "job_type": "build",
                        "transport_type": "CLI",
                        "adapter_type": "cli_llm",
                        "provider_slug": "openai",
                        "model_slug": "gpt-5.4",
                        "model_version": "gpt-5.4",
                        "cost_structure": "subscription_included",
                        "cost_metadata": {},
                        "reason_code": "catalog.available",
                        "candidate_ref": "cand-openai",
                        "provider_ref": "provider.openai",
                        "source_refs": [],
                        "projected_at": None,
                        "projection_ref": "projection.private_provider_control_plane_snapshot",
                    },
                    {
                        "runtime_profile_ref": "nate-private",
                        "job_type": "build",
                        "transport_type": "CLI",
                        "adapter_type": "cli_llm",
                        "provider_slug": "anthropic",
                        "model_slug": "claude-sonnet-4-6",
                        "model_version": "claude-sonnet-4-6",
                        "cost_structure": "subscription_included",
                        "cost_metadata": {},
                        "reason_code": "catalog.available",
                        "candidate_ref": "cand-anthropic",
                        "provider_ref": "provider.anthropic",
                        "source_refs": [],
                        "projected_at": None,
                        "projection_ref": "projection.private_provider_control_plane_snapshot",
                    },
                ]
            if "FROM registry_runtime_profile_authority" in query:
                return []
            if "GROUP BY 1" in query:
                return [
                    {"provider_slug": "openai", "active_count": 0},
                    {"provider_slug": "anthropic", "active_count": 0},
                ]
            if "FROM task_type_routing" in query:
                return [
                    {
                        "provider_slug": "openai",
                        "model_slug": "gpt-5.4",
                        "rank": 1,
                        "benchmark_score": 95.0,
                        "benchmark_name": "claim-route",
                        "cost_per_m_tokens": 8.0,
                        "route_health_score": 0.22,
                        "consecutive_internal_failures": 2,
                    },
                    {
                        "provider_slug": "anthropic",
                        "model_slug": "claude-sonnet-4-6",
                        "rank": 2,
                        "benchmark_score": 88.0,
                        "benchmark_name": "claim-route",
                        "cost_per_m_tokens": 6.0,
                        "route_health_score": 0.94,
                        "consecutive_internal_failures": 0,
                    },
                ]
            raise AssertionError(query)

    job = {
        "run_id": "run-1",
        "agent_slug": "openai/gpt-5.4",
        "failover_chain": ["openai/gpt-5.4", "anthropic/claude-sonnet-4-6"],
        "route_task_type": "build",
    }

    selected = unified_dispatch._select_claim_route(_ClaimConn(), job)

    assert selected == "anthropic/claude-sonnet-4-6"


def test_runtime_profile_admitted_route_candidates_fail_closed_when_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        "registry.runtime_profile_admission.load_admitted_runtime_profile_candidates",
        lambda _conn, runtime_profile_ref: [],
    )

    candidates = [
        "openai/gpt-5.4",
        "anthropic/claude-sonnet-4-6",
    ]

    with pytest.raises(RuntimeProfileAdmissionError) as excinfo:
        unified_dispatch._runtime_profile_admitted_route_candidates(
            object(),
            runtime_profile_ref="praxis",
            candidates=candidates,
        )

    assert excinfo.value.reason_code == "routing.no_admitted_candidate_overlap"
    assert excinfo.value.details["workflow_candidate_slugs"] == candidates
    assert excinfo.value.details["admitted_candidate_slugs"] == []


def test_claim_one_quarantines_ready_job_with_missing_runtime_profile_authority(monkeypatch) -> None:
    blocked: list[tuple[int, str]] = []
    recomputed: list[str] = []

    class _ClaimConn:
        def execute(self, query: str, *args):
            if "FROM workflow_jobs j" in query and "r.requested_at DESC" in query:
                return [
                    {
                        "id": 7,
                        "run_id": "run-dead-profile",
                        "label": "phase_ghost",
                        "status": "ready",
                        "agent_slug": "auto/architecture",
                        "route_task_type": "architecture",
                    }
                ]
            if "SET status = 'failed'" in query and "WHERE id = $1" in query:
                return [
                    {
                        "id": 7,
                        "run_id": "run-dead-profile",
                        "route_task_type": "architecture",
                        "effective_agent": "auto/architecture",
                    }
                ]
            raise AssertionError(query)

    monkeypatch.setattr(_claiming_mod, "_job_has_touch_conflict", lambda _conn, _job: False)
    monkeypatch.setattr(
        _claiming_mod,
        "_select_claim_route",
        lambda _conn, _job: (_ for _ in ()).throw(
            RuntimeProfileAdmissionError(
                "routing.profile_unknown",
                "runtime profile 'dag-project' is missing authority",
            )
        ),
    )
    monkeypatch.setattr(_claiming_mod, "_block_descendants", lambda _conn, job_id, code: blocked.append((job_id, code)))
    monkeypatch.setattr(_claiming_mod, "_recompute_workflow_run_state", lambda _conn, run_id: recomputed.append(run_id))

    claimed = _claiming_mod.claim_one(_ClaimConn(), "worker-1")

    assert claimed is None
    assert blocked == [(7, "routing.profile_unknown")]
    assert recomputed == ["run-dead-profile"]


def test_cancel_run_default_does_not_cancel_running_jobs():
    class _CancelConn:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.rows = [{"id": 5, "label": "build_a"}, {"id": 6, "label": "build_b"}]

        def execute(self, query: str, *args):
            self.executed.append(query)
            if query.startswith("UPDATE workflow_jobs"):
                return self.rows
            if query.startswith("UPDATE workflow_runs"):
                return []
            return []

    conn = _CancelConn()
    result = unified_workflow.cancel_run(conn, "run-7")

    assert result["cancelled_jobs"] == 2
    assert result["labels"] == ["build_a", "build_b"]
    assert any("status IN ('pending', 'ready', 'claimed')" in q for q in conn.executed if q.startswith("UPDATE workflow_jobs"))
    assert not any("status IN ('pending', 'ready', 'claimed', 'running')" in q for q in conn.executed if q.startswith("UPDATE workflow_jobs"))


def test_cancel_run_include_running_jobs():
    class _CancelConn:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.rows = [{"id": 5, "label": "build_a"}]

        def execute(self, query: str, *args):
            self.executed.append(query)
            if query.startswith("UPDATE workflow_jobs"):
                return self.rows
            if query.startswith("UPDATE workflow_runs"):
                return []
            return []

    conn = _CancelConn()
    result = unified_workflow.cancel_run(conn, "run-7", include_running=True)

    assert result["cancelled_jobs"] == 1
    assert any("status IN ('pending', 'ready', 'claimed', 'running')" in q for q in conn.executed if q.startswith("UPDATE workflow_jobs"))


def test_reap_stale_claims_recomputes_touched_run_state():
    class _ReaperConn:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple]] = []
            self.recompute_updates: list[tuple[str, tuple]] = []

        def execute(self, query: str, *args):
            self.queries.append((query, args))
            normalized = " ".join(query.split())
            if normalized.startswith("UPDATE workflow_jobs SET status = 'ready', claimed_by = NULL, claimed_at = NULL"):
                return [
                    {"id": 5, "label": "build_a", "run_id": "run-stale"},
                    {"id": 6, "label": "build_b", "run_id": "run-stale"},
                ]
            if normalized.startswith("SELECT status, COUNT(*) AS count FROM workflow_jobs WHERE run_id = $1 GROUP BY status"):
                return [{"status": "ready", "count": 2}]
            if normalized.startswith("SELECT current_state, workflow_id, request_envelope, started_at, admitted_at, requested_at FROM workflow_runs"):
                return [{
                    "current_state": "running",
                    "workflow_id": "workflow.recover",
                    "request_envelope": {"name": "recover", "phase": "build"},
                    "started_at": datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc),
                    "admitted_at": datetime(2026, 4, 8, 17, 55, tzinfo=timezone.utc),
                    "requested_at": datetime(2026, 4, 8, 17, 50, tzinfo=timezone.utc),
                }]
            if normalized.startswith("UPDATE workflow_runs"):
                self.recompute_updates.append((query, args))
                return []
            if normalized.startswith("INSERT INTO system_events"):
                return []
            if normalized.startswith("SELECT pg_notify('run_complete', $1)"):
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    conn = _ReaperConn()

    reaped = unified_workflow.reap_stale_claims(conn)

    assert reaped == 2
    assert len(conn.recompute_updates) == 1
    update_query, update_args = conn.recompute_updates[0]
    assert "current_state = $2" in update_query
    assert update_args == ("run-stale", "queued", None)


def test_recompute_workflow_run_state_backfills_start_time_for_terminal_runs():
    class _RecomputeConn:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple]] = []

        def execute(self, query: str, *args):
            self.queries.append((query, args))
            normalized = " ".join(query.split())
            if "FROM workflow_jobs" in normalized and "GROUP BY status" in normalized:
                return [{"status": "failed", "count": 1}]
            if normalized.startswith("SELECT current_state, workflow_id, request_envelope, started_at, admitted_at, requested_at FROM workflow_runs"):
                return [{
                    "current_state": "queued",
                    "workflow_id": "workflow.test",
                    "request_envelope": {"parent_run_id": "run-parent", "trigger_depth": 2},
                    "started_at": None,
                    "admitted_at": datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc),
                    "requested_at": datetime(2026, 4, 8, 17, 55, tzinfo=timezone.utc),
                }]
            if normalized.startswith("UPDATE workflow_runs"):
                return []
            if normalized.startswith("INSERT INTO system_events"):
                return []
            if normalized.startswith("SELECT pg_notify('run_complete', $1)"):
                return []
            raise AssertionError(query)

    conn = _RecomputeConn()

    state = unified_workflow._recompute_workflow_run_state(conn, "run-1")

    assert state == "failed"
    update_query, update_args = next((query, args) for query, args in conn.queries if query.startswith("UPDATE workflow_runs"))
    assert "COALESCE(admitted_at, requested_at, now())" in update_query
    assert "GREATEST(COALESCE(started_at, admitted_at, requested_at, now()), now())" in update_query
    assert update_args == ("run-1", "failed", "job_failed")
    event_inserts = [
        args[0] for query, args in conn.queries
        if query.startswith("INSERT INTO system_events")
    ]
    assert event_inserts == ["workflow.failed", "run.failed"]
    assert any(query.startswith("SELECT pg_notify('run_complete', $1)") for query, _ in conn.queries)


def test_recompute_workflow_run_state_can_reopen_terminal_run_for_retry():
    class _RecomputeRetryConn:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple]] = []

        def execute(self, query: str, *args):
            self.queries.append((query, args))
            normalized = " ".join(query.split())
            if "FROM workflow_jobs" in normalized and "GROUP BY status" in normalized:
                return [{"status": "succeeded", "count": 1}, {"status": "ready", "count": 1}]
            if normalized.startswith("SELECT current_state, workflow_id, request_envelope, started_at, admitted_at, requested_at FROM workflow_runs"):
                return [{
                    "current_state": "failed",
                    "workflow_id": "workflow.retry",
                    "request_envelope": {},
                    "started_at": datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc),
                    "admitted_at": datetime(2026, 4, 8, 17, 59, tzinfo=timezone.utc),
                    "requested_at": datetime(2026, 4, 8, 17, 58, tzinfo=timezone.utc),
                }]
            if normalized.startswith("UPDATE workflow_runs"):
                return []
            raise AssertionError(query)

    blocked_conn = _RecomputeRetryConn()
    assert unified_workflow._recompute_workflow_run_state(blocked_conn, "run-1") == "failed"
    assert not any(query.startswith("UPDATE workflow_runs") for query, _ in blocked_conn.queries)

    reopen_conn = _RecomputeRetryConn()
    assert unified_workflow._recompute_workflow_run_state(
        reopen_conn,
        "run-1",
        allow_terminal_reopen=True,
    ) == "queued"
    update_query, update_args = next(
        (query, args) for query, args in reopen_conn.queries if query.startswith("UPDATE workflow_runs")
    )
    assert update_args == ("run-1", "queued", None)
    assert "finished_at = CASE" in update_query


def test_run_worker_loop_starts_job_heartbeat(monkeypatch):
    heartbeat_queries: list[tuple[str, tuple]] = []
    executed_jobs: list[int] = []
    listener_actions: list[str] = []
    listener_channels: list[tuple[str, ...]] = []

    class _WorkerConn:
        def execute(self, query: str, *args):
            heartbeat_queries.append((query, args))
            return []

    class _FakeEvent:
        def __init__(self) -> None:
            self._calls = 0
            self._set = False

        def wait(self, timeout: float) -> bool:
            self._calls += 1
            return self._set or self._calls > 1

        def set(self) -> None:
            self._set = True

        def is_set(self) -> bool:
            return self._set

    created_threads: list[bool] = []

    class _FakeThread:
        def __init__(self, target=None, daemon=None):
            self._target = target
            created_threads.append(bool(daemon))

        def start(self) -> None:
            self._target()

    class _ImmediateFuture:
        def done(self) -> bool:
            return True

        def result(self):
            return None

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_called = False

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
            return _ImmediateFuture()

        def shutdown(self, wait: bool = True) -> None:
            self.shutdown_called = True

    class _FakeListener:
        def __init__(self, database_url: str, channels: tuple[str, ...], wakeup_event, reconnect_delay: float = 5.0) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay
            listener_channels.append(channels)

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    def _claim_sequence():
        yielded = False
        while True:
            if not yielded:
                yielded = True
                yield {"id": 123, "label": "build_a", "agent_slug": "openai/gpt-5.4", "run_id": "dispatch_test"}
            raise KeyboardInterrupt

    claims = _claim_sequence()

    monkeypatch.setattr(_wloop_mod, "claim_one", lambda conn, worker_id: next(claims))
    monkeypatch.setattr(
        _wloop_mod,
        "_get_cached_registry",
        lambda conn: {
            "openai/gpt-5.4": SimpleNamespace(
                execution_backend=SimpleNamespace(value="cli")
            )
        },
    )
    monkeypatch.setattr(_wloop_mod, "execute_job", lambda conn, job, repo_root: executed_jobs.append(job["id"]))
    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    monkeypatch.setattr(_wloop_mod.threading, "Event", _FakeEvent)
    monkeypatch.setattr(_wloop_mod.threading, "Thread", _FakeThread)
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda: object())
    monkeypatch.setattr(pg_connection, "SyncPostgresConnection", lambda pool: _WorkerConn())

    _wloop_mod.run_worker_loop(_FakeConn(), "/repo", poll_interval=0.01, max_local_concurrent=1)

    assert executed_jobs == [123]
    assert created_threads == [True]
    assert listener_actions == ["start", "stop"]
    assert ("job_ready", "run_complete", "system_event") in listener_channels
    assert heartbeat_queries == [("UPDATE workflow_jobs SET heartbeat_at = now() WHERE id = $1", (123,))]


def test_run_worker_loop_dispatches_claim_accepted_graph_runs(monkeypatch):
    executed_runs: list[str] = []
    listener_actions: list[str] = []
    listener_channels: list[tuple[str, ...]] = []

    class _Conn:
        def __init__(self) -> None:
            self._graph_queries = 0

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM workflow_runs" in normalized and "current_state = 'claim_accepted'" in normalized:
                self._graph_queries += 1
                if self._graph_queries == 1:
                    return [{
                        "run_id": "run.graph",
                        "workflow_id": "workflow.graph",
                        "requested_at": datetime.now(timezone.utc),
                    }]
                return []
            return []

    class _ThreadConn:
        @contextmanager
        def transaction(self):
            yield self

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT pg_try_advisory_xact_lock"):
                return [{"locked": True}]
            return []

    class _FakeListener:
        def __init__(self, database_url: str, channels: tuple[str, ...], wakeup_event, reconnect_delay: float = 5.0) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay
            listener_channels.append(channels)

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    class _FakeEvent:
        def __init__(self) -> None:
            self._calls = 0
            self._set = False

        def wait(self, timeout: float) -> bool:
            self._calls += 1
            return self._set or self._calls > 1

        def set(self) -> None:
            self._set = True

        def is_set(self) -> bool:
            return self._set

    class _ImmediateFuture:
        def done(self) -> bool:
            return True

        def result(self):
            return None

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_called = False

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
            return _ImmediateFuture()

        def shutdown(self, wait: bool = True) -> None:
            self.shutdown_called = True

    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(_wloop_mod, "_execute_admitted_graph_run", lambda _conn, run_id: executed_runs.append(run_id))
    monkeypatch.setattr(_wloop_mod, "claim_one", lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))
    monkeypatch.setattr(_wloop_mod, "reap_stale_claims", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "reap_stale_runs", lambda _conn: 0)
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    monkeypatch.setattr(_wloop_mod.threading, "Event", _FakeEvent)
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda: object())
    monkeypatch.setattr(pg_connection, "SyncPostgresConnection", lambda pool: _ThreadConn())

    _wloop_mod.run_worker_loop(_Conn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert executed_runs == ["run.graph"]
    assert listener_actions == ["start", "stop"]
    assert ("job_ready", "run_complete", "system_event") in listener_channels


def test_graph_run_lock_key_is_signed_bigint_safe() -> None:
    lower = -(2**63)
    upper = 2**63 - 1

    for run_id in (
        "run.graph",
        "run:deterministic_smoke:004c3b09c0bf189b",
        "run:mcp_workflow_live_proof_20260413:3e0c93573e6b1290",
    ):
        key = _admission_mod._graph_run_lock_key(run_id)
        assert lower <= key <= upper


def test_run_worker_loop_fails_failing_graph_runs_closed_without_retry(monkeypatch):
    executed_runs: list[str] = []
    listener_actions: list[str] = []
    state = {"run_state": "claim_accepted"}

    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if (
                "FROM workflow_runs" in normalized
                and "current_state = 'claim_accepted'" in normalized
                and state["run_state"] == "claim_accepted"
            ):
                return [{
                    "run_id": "run.graph",
                    "workflow_id": "workflow.graph",
                    "requested_at": datetime.now(timezone.utc),
                }]
            return []

    class _ThreadConn:
        @contextmanager
        def transaction(self):
            yield self

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT pg_try_advisory_xact_lock"):
                return [{"locked": True}]
            if normalized.startswith("UPDATE workflow_runs SET current_state = 'failed'"):
                state["run_state"] = "failed"
                return [{"workflow_id": "workflow.graph", "request_id": "request.graph"}]
            if normalized.startswith("SELECT pg_notify('run_complete',"):
                return []
            return []

    class _FakeListener:
        def __init__(self, database_url: str, channels: tuple[str, ...], wakeup_event, reconnect_delay: float = 5.0) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    class _FakeEvent:
        def __init__(self) -> None:
            self._calls = 0
            self._set = False

        def wait(self, timeout: float) -> bool:
            self._calls += 1
            return self._set or self._calls > 1

        def set(self) -> None:
            self._set = True

        def is_set(self) -> bool:
            return self._set

    class _ImmediateFuture:
        def __init__(self, exc: Exception | None = None) -> None:
            self._exc = exc

        def done(self) -> bool:
            return True

        def result(self):
            if self._exc is not None:
                raise self._exc
            return None

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_called = False

        def submit(self, fn, *args, **kwargs):
            try:
                fn(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - exercised via future.result()
                return _ImmediateFuture(exc)
            return _ImmediateFuture()

        def shutdown(self, wait: bool = True) -> None:
            self.shutdown_called = True

    claim_calls = {"count": 0}

    def _claim_one(*_args, **_kwargs):
        claim_calls["count"] += 1
        if claim_calls["count"] >= 2:
            raise KeyboardInterrupt()
        return None

    monotonic_values = iter([0.0, 0.0, 0.0, 0.0, 0.1, 0.1, 0.1, 0.1])
    monkeypatch.setattr(_wloop_mod.time, "monotonic", lambda: next(monotonic_values, 0.1))
    def _fail_graph_run(_conn, run_id):
        executed_runs.append(run_id)
        raise RuntimeError("boom")

    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(_wloop_mod, "_execute_admitted_graph_run", _fail_graph_run)
    monkeypatch.setattr(_wloop_mod, "claim_one", _claim_one)
    monkeypatch.setattr(_wloop_mod, "reap_stale_claims", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "reap_stale_runs", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "_start_embedding_prewarm_for_worker", lambda: None)
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    monkeypatch.setattr(_wloop_mod.threading, "Event", _FakeEvent)
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda: object())
    monkeypatch.setattr(pg_connection, "SyncPostgresConnection", lambda pool: _ThreadConn())

    _wloop_mod.run_worker_loop(_Conn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert executed_runs == ["run.graph"]
    assert state["run_state"] == "failed"
    assert listener_actions == ["start", "stop"]


def test_run_worker_loop_does_not_starve_runnable_graph_runs_behind_backoffed_run(
    monkeypatch,
):
    executed_runs: list[str] = []
    listener_actions: list[str] = []
    state = {"run_old_state": "claim_accepted"}

    class _Conn:
        def __init__(self) -> None:
            self._graph_queries = 0

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM workflow_runs" in normalized and "current_state = 'claim_accepted'" in normalized:
                self._graph_queries += 1
                rows = [
                    {
                        "run_id": "run.new",
                        "workflow_id": "workflow.new",
                        "requested_at": datetime.now(timezone.utc),
                    }
                ]
                if state["run_old_state"] == "claim_accepted":
                    rows.insert(
                        0,
                        {
                            "run_id": "run.old",
                            "workflow_id": "workflow.old",
                            "requested_at": datetime.now(timezone.utc),
                        },
                    )
                if self._graph_queries <= 2:
                    return rows
                return []
            return []

    class _ThreadConn:
        @contextmanager
        def transaction(self):
            yield self

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT pg_try_advisory_xact_lock"):
                return [{"locked": True}]
            if normalized.startswith("UPDATE workflow_runs SET current_state = 'failed'"):
                state["run_old_state"] = "failed"
                return [{"workflow_id": "workflow.old", "request_id": "request.old"}]
            if normalized.startswith("SELECT pg_notify('run_complete',"):
                return []
            return []

    class _FakeListener:
        def __init__(
            self,
            database_url: str,
            channels: tuple[str, ...],
            wakeup_event,
            reconnect_delay: float = 5.0,
        ) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    class _FakeEvent:
        def __init__(self) -> None:
            self._calls = 0
            self._set = False

        def wait(self, timeout: float) -> bool:
            self._calls += 1
            return self._set or self._calls > 2

        def set(self) -> None:
            self._set = True

        def is_set(self) -> bool:
            return self._set

    class _ImmediateFuture:
        def __init__(self, exc: Exception | None = None) -> None:
            self._exc = exc

        def done(self) -> bool:
            return True

        def result(self):
            if self._exc is not None:
                raise self._exc
            return None

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_called = False

        def submit(self, fn, *args, **kwargs):
            try:
                fn(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - exercised via future.result()
                return _ImmediateFuture(exc)
            return _ImmediateFuture()

        def shutdown(self, wait: bool = True) -> None:
            self.shutdown_called = True

    claim_calls = {"count": 0}

    def _claim_one(*_args, **_kwargs):
        claim_calls["count"] += 1
        if claim_calls["count"] >= 3:
            raise KeyboardInterrupt()
        return None

    monotonic_values = iter([
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.1,
        0.1,
        0.1,
        0.2,
        0.2,
        0.2,
        0.2,
    ])

    def _run_graph(_conn, run_id):
        executed_runs.append(run_id)
        if run_id == "run.old":
            raise RuntimeError("boom")

    monkeypatch.setattr(_wloop_mod.time, "monotonic", lambda: next(monotonic_values, 0.2))
    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(_wloop_mod, "_execute_admitted_graph_run", _run_graph)
    monkeypatch.setattr(_wloop_mod, "claim_one", _claim_one)
    monkeypatch.setattr(_wloop_mod, "reap_stale_claims", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "reap_stale_runs", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "_start_embedding_prewarm_for_worker", lambda: None)
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    monkeypatch.setattr(_wloop_mod.threading, "Event", _FakeEvent)
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda: object())
    monkeypatch.setattr(pg_connection, "SyncPostgresConnection", lambda pool: _ThreadConn())

    _wloop_mod.run_worker_loop(_Conn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert executed_runs == ["run.old", "run.new"]
    assert listener_actions == ["start", "stop"]


def test_run_worker_loop_fails_poisoned_graph_run_closed(monkeypatch):
    listener_actions: list[str] = []
    emitted_events: list[tuple[str, str, str, dict[str, object]]] = []
    state = {
        "current_state": "claim_accepted",
        "terminal_reason_code": None,
        "notified_run_complete": False,
    }

    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if (
                "FROM workflow_runs" in normalized
                and "current_state = 'claim_accepted'" in normalized
                and state["current_state"] == "claim_accepted"
            ):
                return [
                    {
                        "run_id": "run.graph",
                        "workflow_id": "workflow.graph",
                        "requested_at": datetime.now(timezone.utc),
                    }
                ]
            return []

    class _ThreadConn:
        @contextmanager
        def transaction(self):
            yield self

        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT pg_try_advisory_xact_lock"):
                return [{"locked": True}]
            if normalized.startswith("UPDATE workflow_runs SET current_state = 'failed'"):
                state["current_state"] = "failed"
                state["terminal_reason_code"] = args[1]
                return [{"workflow_id": "workflow.graph", "request_id": "request.graph"}]
            if normalized.startswith("SELECT pg_notify('run_complete',"):
                state["notified_run_complete"] = True
                return []
            return []

    class _FakeListener:
        def __init__(
            self,
            database_url: str,
            channels: tuple[str, ...],
            wakeup_event,
            reconnect_delay: float = 5.0,
        ) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    class _FakeEvent:
        def __init__(self) -> None:
            self._calls = 0
            self._set = False

        def wait(self, timeout: float) -> bool:
            self._calls += 1
            return self._set or self._calls > 2

        def set(self) -> None:
            self._set = True

        def is_set(self) -> bool:
            return self._set

    class _ImmediateFuture:
        def done(self) -> bool:
            return True

        def result(self):
            return None

    class _ImmediateExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_called = False

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
            return _ImmediateFuture()

        def shutdown(self, wait: bool = True) -> None:
            self.shutdown_called = True

    claim_calls = {"count": 0}

    def _claim_one(*_args, **_kwargs):
        claim_calls["count"] += 1
        if claim_calls["count"] >= 3:
            raise KeyboardInterrupt()
        return None

    class _PoisonedGraphError(RuntimeError):
        reason_code = "registry.runtime_profile_missing"

    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(
        _wloop_mod,
        "_execute_admitted_graph_run",
        lambda _conn, run_id: (_ for _ in ()).throw(_PoisonedGraphError("boom")),
    )
    monkeypatch.setattr(_wloop_mod, "claim_one", _claim_one)
    monkeypatch.setattr(_wloop_mod, "reap_stale_claims", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "reap_stale_runs", lambda _conn: 0)
    monkeypatch.setattr(
        _wloop_mod,
        "emit_system_event",
        lambda conn, *, event_type, source_id, source_type, payload: emitted_events.append(
            (event_type, source_id, source_type, payload)
        ),
    )
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    monkeypatch.setattr(_wloop_mod.threading, "Event", _FakeEvent)
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _ImmediateExecutor)
    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda: object())
    monkeypatch.setattr(pg_connection, "SyncPostgresConnection", lambda pool: _ThreadConn())

    _wloop_mod.run_worker_loop(_Conn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert state["current_state"] == "failed"
    assert state["terminal_reason_code"] == "registry.runtime_profile_missing"
    assert state["notified_run_complete"] is True
    assert emitted_events == [
        (
            "workflow.failed",
            "run.graph",
            "workflow_run",
            {
                "run_id": "run.graph",
                "workflow_id": "workflow.graph",
                "status": "failed",
                "reason_code": "registry.runtime_profile_missing",
                "total_jobs": 0,
                "succeeded": 0,
                "failed": 1,
                "blocked": 0,
                "cancelled": 0,
                "parent_run_id": None,
                "trigger_depth": 0,
            },
        ),
        (
            "run.failed",
            "run.graph",
            "workflow_run",
            {
                "run_id": "run.graph",
                "workflow_id": "workflow.graph",
                "status": "failed",
                "reason_code": "registry.runtime_profile_missing",
                "total_jobs": 0,
                "succeeded": 0,
                "failed": 1,
                "blocked": 0,
                "cancelled": 0,
                "parent_run_id": None,
                "trigger_depth": 0,
            },
        ),
    ]
    assert listener_actions == ["start", "stop"]


def test_run_worker_loop_logs_trigger_failures_without_stopping_scheduler(
    monkeypatch,
    caplog,
):
    listener_actions: list[str] = []
    listener_channels: list[tuple[str, ...]] = []
    claim_count = 0
    tick = {"value": 0}

    class _FakeListener:
        def __init__(self, database_url: str, channels: tuple[str, ...], wakeup_event, reconnect_delay: float = 5.0) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay
            listener_channels.append(channels)

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    def _fake_monotonic() -> float:
        tick["value"] += 10
        return float(tick["value"])

    def _claim_one(_conn, _worker_id):
        nonlocal claim_count
        claim_count += 1
        if claim_count == 1:
            return None
        raise KeyboardInterrupt

    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(_wloop_mod.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(_wloop_mod, "claim_one", _claim_one)
    monkeypatch.setattr(_wloop_mod, "reap_stale_claims", lambda _conn: 0)
    monkeypatch.setattr(
        _wloop_mod,
        "_evaluate_workflow_triggers",
        lambda _conn: (_ for _ in ()).throw(RuntimeError("trigger boom")),
    )
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    caplog.set_level(logging.WARNING)

    _wloop_mod.run_worker_loop(_FakeConn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert "Trigger evaluation failed: trigger boom" in caplog.text
    assert listener_actions == ["start", "stop"]
    assert ("job_ready", "run_complete", "system_event") in listener_channels


def test_run_worker_loop_schedules_embedding_prewarm_once(monkeypatch):
    listener_actions: list[str] = []
    prewarm_actions: list[str] = []
    claim_count = 0

    class _FakeListener:
        def __init__(self, database_url: str, channels: tuple[str, ...], wakeup_event, reconnect_delay: float = 5.0) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    def _claim_one(_conn, _worker_id):
        nonlocal claim_count
        claim_count += 1
        if claim_count == 1:
            return None
        raise KeyboardInterrupt

    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(_wloop_mod, "_start_embedding_prewarm_for_worker", lambda: prewarm_actions.append("scheduled"))
    monkeypatch.setattr(_wloop_mod, "claim_one", _claim_one)
    monkeypatch.setattr(_wloop_mod, "reap_stale_claims", lambda _conn: 0)
    monkeypatch.setattr(_wloop_mod, "_evaluate_workflow_triggers", lambda _conn: 0)
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")

    _wloop_mod.run_worker_loop(_FakeConn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert prewarm_actions == ["scheduled"]
    assert listener_actions == ["start", "stop"]


def test_run_worker_loop_logs_future_failures_without_stopping_scheduler(
    monkeypatch,
    caplog,
):
    listener_actions: list[str] = []
    claim_count = 0
    completed: list[dict[str, object]] = []

    class _ExplodingFuture:
        def done(self) -> bool:
            return True

        def result(self):
            raise RuntimeError("future boom")

    class _FakeExecutor:
        def __init__(self, *args, **kwargs) -> None:
            self.shutdown_called = False

        def submit(self, fn, *args, **kwargs):
            del fn, args, kwargs
            return _ExplodingFuture()

        def shutdown(self, wait: bool = True) -> None:
            self.shutdown_called = True

    class _FakeListener:
        def __init__(self, database_url: str, channels: tuple[str, ...], wakeup_event, reconnect_delay: float = 5.0) -> None:
            self.database_url = database_url
            self.channels = channels
            self.wakeup_event = wakeup_event
            self.reconnect_delay = reconnect_delay

        def start(self) -> None:
            listener_actions.append("start")

        def stop(self) -> None:
            listener_actions.append("stop")

    def _claim_one(_conn, _worker_id):
        nonlocal claim_count
        claim_count += 1
        if claim_count == 1:
            return {"id": 123, "label": "build_a", "agent_slug": "openai/gpt-5.4", "run_id": "dispatch_test"}
        if claim_count == 2:
            return None
        raise KeyboardInterrupt

    monkeypatch.setattr(_wloop_mod, "_WorkerNotificationListener", _FakeListener)
    monkeypatch.setattr(_wloop_mod, "claim_one", _claim_one)
    monkeypatch.setattr(
        _wloop_mod,
        "_get_cached_registry",
        lambda _conn: {"openai/gpt-5.4": SimpleNamespace()},
    )
    monkeypatch.setattr(
        _wloop_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(execution_lane="local"),
    )
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(
        _wloop_mod,
        "complete_job",
        lambda _conn, job_id, **kwargs: completed.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://example.test/workflow")
    caplog.set_level(logging.ERROR)

    _wloop_mod.run_worker_loop(_FakeConn(), "/repo", poll_interval=0.0, max_local_concurrent=1)

    assert "Future failed for build_a: future boom" in caplog.text
    assert completed == [
        {
            "job_id": 123,
            "status": "failed",
            "error_code": "worker_future_exception",
            "duration_ms": 0,
            "stdout_preview": "future boom",
        }
    ]
    assert listener_actions == ["start", "stop"]


def test_worker_notification_listener_wakes_on_notify():
    wakeup_event = unified_dispatch.threading.Event()
    listener = unified_dispatch._WorkerNotificationListener(
        "postgresql://example.test/workflow",
        ("job_ready", "run_complete"),
        wakeup_event,
    )

    listener._on_notify(None, 123, "job_ready", "run-1:job-42")

    assert wakeup_event.is_set()


def test_workflow_worker_polls_card_nodes_only():
    class _CardOnlyConn:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute(self, query: str, *args):
            self.queries.append(" ".join(query.split()))
            if "FROM run_nodes" in query:
                return []
            raise AssertionError(f"Unexpected query: {query}")

    worker = WorkflowWorker(_CardOnlyConn(), "/repo")
    worker._poll_once()

    assert worker.is_running is False
    assert any("FROM run_nodes" in query for query in worker._conn.queries)
    assert not any("FROM workflow_runs" in query for query in worker._conn.queries)
