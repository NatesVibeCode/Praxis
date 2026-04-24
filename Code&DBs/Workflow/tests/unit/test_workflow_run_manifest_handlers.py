from __future__ import annotations

import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from runtime.wave_orchestrator import WaveOrchestrator
from surfaces.api.handlers import workflow_run


class _RequestStub:
    def __init__(self, body: dict[str, Any], *, subsystems: Any) -> None:
        raw = json.dumps(body).encode("utf-8")
        self.headers = {"Content-Length": str(len(raw))}
        self.rfile = io.BytesIO(raw)
        self.subsystems = subsystems
        self.sent: tuple[int, dict[str, Any]] | None = None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.sent = (status, payload)


class _FakePg:
    def __init__(self) -> None:
        self.manifest_rows: dict[str, dict[str, Any]] = {}
        self.saved: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.saved.append((query, args))
        return []

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "FROM app_manifests WHERE id = $1" in query:
            return self.manifest_rows.get(str(args[0]))
        return None

    def fetchval(self, query: str, *args: Any) -> Any:
        if "SELECT 1 FROM app_manifests WHERE id = $1" in query:
            return 1 if str(args[0]) in self.manifest_rows else None
        if "SELECT EXTRACT(EPOCH FROM updated_at)::bigint FROM app_manifests WHERE id = $1" in query:
            return 1234567890
        return None


class _MatcherStub:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def match(self, intent: str):
        self.calls.append(intent)
        return SimpleNamespace(coverage_score=0.0)


class _SubsystemsStub:
    def __init__(self) -> None:
        self.pg = _FakePg()
        self.matcher = _MatcherStub()
        self.generator = object()

    def get_pg_conn(self):
        return self.pg

    def get_intent_matcher(self):
        return self.matcher

    def get_manifest_generator(self):
        return self.generator


class _StreamRequestStub:
    def __init__(self, subsystems: Any) -> None:
        self.subsystems = subsystems
        self.status: int | None = None
        self.headers: dict[str, str] = {}
        self.ended = False
        self.wfile = io.BytesIO()

    def send_response(self, status: int) -> None:
        self.status = status

    def send_header(self, key: str, value: str) -> None:
        self.headers[key] = value

    def end_headers(self) -> None:
        self.ended = True


def test_manifest_generate_uses_subsystem_matcher() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub({"intent": "Build a support dashboard"}, subsystems=subsystems)

    with patch.object(
        workflow_run,
        "generate_manifest",
        return_value=SimpleNamespace(
            manifest_id="manifest_123",
            manifest={
                "kind": "helm_surface_bundle",
                "tabs": [{"id": "main"}],
                "surfaces": {
                    "main": {
                        "manifest": {
                            "title": "Build a support dashboard",
                        }
                    }
                },
            },
            version=4,
            confidence=0.75,
            explanation="stubbed",
        ),
    ) as generate_mock:
        workflow_run._handle_manifest_generate_post(request, "/api/manifest/generate")

    assert generate_mock.call_count == 1
    assert generate_mock.call_args.kwargs["matcher"] is subsystems.matcher
    assert generate_mock.call_args.kwargs["generator"] is subsystems.get_manifest_generator()
    assert generate_mock.call_args.kwargs["intent"] == "Build a support dashboard"
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["manifest_id"] == "manifest_123"
    assert payload["manifest"]["kind"] == "helm_surface_bundle"
    assert payload["manifest"]["tabs"][0]["id"] == "main"
    assert payload["manifest"]["surfaces"]["main"]["manifest"]["title"] == "Build a support dashboard"


def test_manifest_generate_quick_uses_subsystem_matcher() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub({"intent": "Build a support dashboard"}, subsystems=subsystems)

    with patch.object(
        workflow_run,
        "generate_manifest_quick",
        return_value={
            "manifest_id": "manifest_123",
            "manifest": {"version": 4},
            "method": "generate",
        },
    ) as quick_mock:
        workflow_run._handle_manifest_generate_quick_post(
            request,
            "/api/manifest/generate/quick",
        )

    assert quick_mock.call_count == 1
    assert quick_mock.call_args.kwargs["matcher"] is subsystems.matcher
    assert quick_mock.call_args.kwargs["generator"] is subsystems.get_manifest_generator()
    assert quick_mock.call_args.kwargs["intent"] == "Build a support dashboard"
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["method"] == "generate"
    assert payload["manifest_id"] == "manifest_123"
    assert payload["manifest"]["version"] == 4


def test_workflow_status_get_returns_serialize_payload() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub({}, subsystems=subsystems)

    workflow_unified = ModuleType("runtime.workflow.unified")
    workflow_unified.get_run_status = lambda *_args, **_kwargs: {
        "run_id": "dispatch_abc",
        "status": "running",
        "jobs": [],
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }

    with patch.dict(sys.modules, {"runtime.workflow.unified": workflow_unified}):
        workflow_run._handle_workflow_status(request, "/api/workflow-runs/dispatch_abc/status")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["run_id"] == "dispatch_abc"
    assert payload["created_at"] == "2026-01-01T00:00:00+00:00"


def test_workflow_status_get_not_found_when_missing_run() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub({}, subsystems=subsystems)

    workflow_unified = ModuleType("runtime.workflow.unified")
    workflow_unified.get_run_status = lambda *_args, **_kwargs: None

    with patch.dict(sys.modules, {"runtime.workflow.unified": workflow_unified}):
        workflow_run._handle_workflow_status(request, "/api/workflow-runs/missing/status")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 404
    assert "not found" in payload["error"]


def test_workflows_run_post_uses_command_bus_helper(tmp_path, monkeypatch) -> None:
    temp_dir = tmp_path / "artifacts" / "workflow"
    temp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(workflow_run, "REPO_ROOT", tmp_path)

    request = _RequestStub(
        {
            "steps": [
                {
                    "prompt": "Build the support report",
                    "model": "auto/build",
                }
            ]
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
    )

    fake_result = {
        "run_id": "dispatch_001",
        "status": "queued",
        "spec_name": "workflow-wf-test",
        "total_jobs": 1,
    }

    with patch.object(workflow_run, "_submit_workflow_via_service_bus", return_value=fake_result) as bus_mock:
        workflow_run._handle_workflows_run_post(request, "/api/workflows/run")

    assert bus_mock.call_count == 1
    assert bus_mock.call_args.args[0] is request.subsystems
    assert bus_mock.call_args.kwargs["requested_by_kind"] == "http"
    assert bus_mock.call_args.kwargs["requested_by_ref"] == "workflow_run"
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["workflow_run_id"] == "dispatch_001"
    assert payload["stream_url"] == "/api/workflow-runs/dispatch_001/stream"
    assert payload["status_url"] == "/api/workflow-runs/dispatch_001/status"


def test_workflow_job_post_uses_command_bus_helper(tmp_path, monkeypatch) -> None:
    temp_dir = tmp_path / "artifacts" / "workflow"
    temp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(workflow_run, "REPO_ROOT", tmp_path)

    request = _RequestStub(
        {
            "prompt": "Draft the support report",
            "model": "auto/build",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
    )

    fake_result = {
        "run_id": "dispatch_002",
        "status": "queued",
        "spec_name": "ui-workflow-test",
        "total_jobs": 1,
    }

    with patch.object(workflow_run, "_submit_workflow_via_service_bus", return_value=fake_result) as bus_mock:
        workflow_run._handle_workflow_job_post(request, "/api/workflow-job")

    assert bus_mock.call_count == 1
    assert bus_mock.call_args.args[0] is request.subsystems
    assert bus_mock.call_args.kwargs["requested_by_kind"] == "http"
    assert bus_mock.call_args.kwargs["requested_by_ref"] == "workflow_job"
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["run_id"] == "dispatch_002"
    assert payload["stream_url"] == "/api/workflow-runs/dispatch_002/stream"
    assert payload["status_url"] == "/api/workflow-runs/dispatch_002/status"


def test_submit_workflow_via_service_bus_uses_control_command_request(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    pg = object()

    command = SimpleNamespace(
        command_id="control.command.submit.77",
        command_status="succeeded",
        result_ref="workflow_run:dispatch_077",
        error_detail=None,
        to_json=lambda: {
            "command_id": "control.command.submit.77",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_077",
        },
    )

    control_commands = ModuleType("runtime.control_commands")

    def _fake_request_workflow_submit_command(_pg, **kwargs):
        captured["kwargs"] = kwargs
        return command

    def _fake_render_workflow_submit_response(command_obj, *, spec_name: str, total_jobs: int):
        return {
            "run_id": "dispatch_077",
            "status": "queued",
            "spec_name": spec_name,
            "total_jobs": total_jobs,
            "command_id": command_obj.command_id,
            "command_status": command_obj.command_status,
            "approval_required": False,
            "stream_url": "/api/workflow-runs/dispatch_077/stream",
            "status_url": "/api/workflow-runs/dispatch_077/status",
            "result_ref": command_obj.result_ref,
        }

    control_commands.request_workflow_submit_command = _fake_request_workflow_submit_command
    control_commands.render_workflow_submit_response = _fake_render_workflow_submit_response

    with patch.dict(sys.modules, {"runtime.control_commands": control_commands}):
        result = workflow_run._submit_workflow_via_service_bus(
            SimpleNamespace(get_pg_conn=lambda: pg),
            spec_path="artifacts/workflow/sample.queue.json",
            spec_name="sample",
            total_jobs=3,
            requested_by_kind="http",
            requested_by_ref="workflow_run",
        )

    assert captured["kwargs"] == {
        "requested_by_kind": "http",
        "requested_by_ref": "workflow_run",
        "spec_path": "artifacts/workflow/sample.queue.json",
        "repo_root": str(workflow_run.REPO_ROOT),
    }
    assert result == {
        "run_id": "dispatch_077",
        "status": "queued",
        "spec_name": "sample",
        "total_jobs": 3,
        "command_id": "control.command.submit.77",
        "command_status": "succeeded",
        "approval_required": False,
        "stream_url": "/api/workflow-runs/dispatch_077/stream",
        "status_url": "/api/workflow-runs/dispatch_077/status",
        "result_ref": "workflow_run:dispatch_077",
    }


def test_standard_workflow_run_returns_approval_gate_without_run_id() -> None:
    fake_spec = SimpleNamespace(name="approval gated", jobs=[{"label": "gate"}])
    fake_result = {
        "status": "approval_required",
        "command_status": "requested",
        "command_id": "control.command.needs_approval",
        "approval_required": True,
        "spec_name": "approval gated",
        "total_jobs": 1,
    }

    with patch.object(workflow_run, "_workflow_spec_mod", return_value=SimpleNamespace(WorkflowSpec=SimpleNamespace(load=lambda _path: fake_spec))):
        with patch.object(workflow_run, "_submit_workflow_via_service_bus", return_value=fake_result):
            payload = workflow_run._handle_workflow(
                SimpleNamespace(get_pg_conn=lambda: object()),
                {"spec_path": "artifacts/workflow/gated.queue.json"},
            )

    assert payload["status"] == "approval_required"
    assert payload["approval_required"] is True
    assert payload["command_id"] == "control.command.needs_approval"
    assert "run_id" not in payload
    assert "stream_url" not in payload
    assert "status_url" not in payload


def test_workflow_spawn_post_uses_spawn_command_bus_helper() -> None:
    request = _RequestStub(
        {
            "spec_path": "config/cascade/specs/W_phase_program_review_child_20260414.queue.json",
            "parent_run_id": "workflow_parent_001",
            "parent_job_label": "phase_50_review_spawn",
            "dispatch_reason": "phase.review",
            "lineage_depth": 1,
            "run_id": "workflow_review_child_001",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
    )

    fake_spec = SimpleNamespace(
        name="Phase Program Review Child",
        jobs=[{"label": "phase_50_review"}],
    )
    fake_result = {
        "run_id": "dispatch_spawn_002",
        "status": "queued",
        "spec_name": "Phase Program Review Child",
        "total_jobs": 1,
        "command_id": "control.command.spawn.2",
    }

    with patch.object(workflow_run, "_workflow_spec_mod", return_value=SimpleNamespace(WorkflowSpec=SimpleNamespace(load=lambda _path: fake_spec))):
        with patch.object(workflow_run, "_spawn_workflow_via_service_bus", return_value=fake_result) as bus_mock:
            workflow_run._handle_workflow_spawn_post(request, "/api/workflow-runs/spawn")

    assert bus_mock.call_count == 1
    assert bus_mock.call_args.args[0] is request.subsystems
    assert bus_mock.call_args.kwargs["requested_by_kind"] == "http"
    assert bus_mock.call_args.kwargs["requested_by_ref"] == "workflow_spawn"
    assert bus_mock.call_args.kwargs["parent_run_id"] == "workflow_parent_001"
    assert bus_mock.call_args.kwargs["parent_job_label"] == "phase_50_review_spawn"
    assert bus_mock.call_args.kwargs["dispatch_reason"] == "phase.review"
    assert bus_mock.call_args.kwargs["lineage_depth"] == 1
    assert bus_mock.call_args.kwargs["run_id"] == "workflow_review_child_001"
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["run_id"] == "dispatch_spawn_002"


def test_workflow_wave_next_requires_explicit_wave_id() -> None:
    orch = WaveOrchestrator("orch-api")
    orch.add_wave("wave-live", [{"label": "build"}])
    orch.start_wave("wave-live")
    subs = SimpleNamespace(get_wave_orchestrator=lambda: orch)

    with pytest.raises(workflow_run._ClientError, match="wave_id is required"):
        workflow_run._handle_wave(subs, {"action": "next"})


def test_workflow_wave_rejects_demo_placeholder() -> None:
    orch = WaveOrchestrator("orch-api")
    orch.add_wave("wave-live", [{"label": "build"}])
    orch.start_wave("wave-live")
    subs = SimpleNamespace(get_wave_orchestrator=lambda: orch)

    with pytest.raises(workflow_run._ClientError, match="example placeholder"):
        workflow_run._handle_wave(subs, {"action": "next", "wave_id": "wave_abc123"})


def test_workflow_wave_next_accepts_explicit_wave_id() -> None:
    orch = WaveOrchestrator("orch-api")
    orch.add_wave("wave-live", [{"label": "build"}])
    orch.start_wave("wave-live")
    subs = SimpleNamespace(get_wave_orchestrator=lambda: orch)

    result = workflow_run._handle_wave(subs, {"action": "next", "wave_id": "wave-live"})

    assert result == {"wave_id": "wave-live", "runnable_jobs": ["build"]}


def test_spawn_workflow_via_service_bus_uses_control_command_request(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    pg = object()

    command = SimpleNamespace(
        command_id="control.command.spawn.77",
        command_status="succeeded",
        result_ref="workflow_run:dispatch_spawn_077",
        error_detail=None,
        to_json=lambda: {
            "command_id": "control.command.spawn.77",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_spawn_077",
        },
    )

    control_commands = ModuleType("runtime.control_commands")

    def _fake_request_workflow_spawn_command(_pg, **kwargs):
        captured["kwargs"] = kwargs
        return command

    def _fake_render_workflow_spawn_response(command_obj, *, spec_name: str, total_jobs: int):
        return {
            "run_id": "dispatch_spawn_077",
            "status": "queued",
            "spec_name": spec_name,
            "total_jobs": total_jobs,
            "command_id": command_obj.command_id,
            "command_status": command_obj.command_status,
            "approval_required": False,
            "stream_url": "/api/workflow-runs/dispatch_spawn_077/stream",
            "status_url": "/api/workflow-runs/dispatch_spawn_077/status",
            "result_ref": command_obj.result_ref,
        }

    control_commands.request_workflow_spawn_command = _fake_request_workflow_spawn_command
    control_commands.render_workflow_spawn_response = _fake_render_workflow_spawn_response

    with patch.dict(sys.modules, {"runtime.control_commands": control_commands}):
        result = workflow_run._spawn_workflow_via_service_bus(
            SimpleNamespace(get_pg_conn=lambda: pg),
            spec_path="config/cascade/specs/W_phase_program_review_child_20260414.queue.json",
            spec_name="Phase Program Review Child",
            total_jobs=2,
            requested_by_kind="http",
            requested_by_ref="workflow_spawn",
            parent_run_id="workflow_parent_001",
            parent_job_label="phase_50_review_spawn",
            dispatch_reason="phase.review",
            lineage_depth=1,
            run_id="workflow_review_child_001",
        )

    assert captured["kwargs"] == {
        "requested_by_kind": "http",
        "requested_by_ref": "workflow_spawn",
        "spec_path": "config/cascade/specs/W_phase_program_review_child_20260414.queue.json",
        "repo_root": str(workflow_run.REPO_ROOT),
        "parent_run_id": "workflow_parent_001",
        "parent_job_label": "phase_50_review_spawn",
        "dispatch_reason": "phase.review",
        "lineage_depth": 1,
        "run_id": "workflow_review_child_001",
    }
    assert result["run_id"] == "dispatch_spawn_077"


def test_manifest_save_normalizes_v4_bundle() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub(
        {
            "id": "manifest_123",
            "name": "Support Workspace",
            "manifest": {
                "version": 2,
                "grid": "4x4",
                "title": "Support Workspace",
                "quadrants": {
                    "A1": {
                        "module": "metric",
                        "config": {"label": "Inbox", "value": "12"},
                    }
                },
            },
        },
        subsystems=subsystems,
    )

    with patch.object(
        workflow_run,
        "save_manifest",
        return_value={
            "id": "manifest_123",
            "name": "Support Workspace",
            "description": "",
            "version": 1234567890,
            "manifest": {
                "kind": "helm_surface_bundle",
                "surfaces": {
                    "main": {
                        "manifest": {
                            "quadrants": {
                                "A1": {"module": "metric"}
                            }
                        }
                    }
                },
            },
        },
    ) as save_mock:
        workflow_run._handle_manifest_save_post(request, "/api/manifests/save")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["saved"] is True
    assert payload["manifest"]["kind"] == "helm_surface_bundle"
    assert payload["manifest"]["surfaces"]["main"]["manifest"]["quadrants"]["A1"]["module"] == "metric"
    assert save_mock.call_count == 1
    assert save_mock.call_args.kwargs["manifest"]["kind"] == "helm_surface_bundle"


def test_manifest_save_as_delegates_to_runtime_owner() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub(
        {
            "name": "Support Workspace",
            "description": "Workspace",
            "manifest": {
                "version": 2,
                "grid": "4x4",
                "title": "Support Workspace",
                "quadrants": {"A1": {"module": "metric"}},
            },
        },
        subsystems=subsystems,
    )

    with patch.object(
        workflow_run,
        "save_manifest_as",
        return_value={
            "id": "support-workspace-123abc",
            "name": "Support Workspace",
            "description": "Workspace",
            "manifest": {"kind": "helm_surface_bundle"},
        },
    ) as save_mock:
        workflow_run._handle_manifest_save_as_post(request, "/api/manifests/save-as")

    assert save_mock.call_count == 1
    assert save_mock.call_args.kwargs["name"] == "Support Workspace"
    assert request.sent == (
        200,
        {
            "saved": True,
            "id": "support-workspace-123abc",
            "name": "Support Workspace",
            "description": "Workspace",
            "manifest": {"kind": "helm_surface_bundle"},
        },
    )


def test_checkpoint_create_delegates_to_runtime_owner() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub(
        {
            "card_id": "card_123",
            "model_id": "model_123",
            "authority_level": "high",
            "question": "Ship it?",
        },
        subsystems=subsystems,
    )

    with patch.object(
        workflow_run,
        "request_authority_checkpoint",
        return_value={"checkpoint_id": "checkpoint_123", "status": "pending"},
    ) as create_mock:
        workflow_run._handle_checkpoints_post(request, "/api/checkpoints")

    assert create_mock.call_count == 1
    assert create_mock.call_args.kwargs["card_id"] == "card_123"
    assert request.sent == (200, {"checkpoint_id": "checkpoint_123", "status": "pending"})


def test_checkpoint_decision_delegates_to_runtime_owner() -> None:
    subsystems = _SubsystemsStub()
    request = _RequestStub(
        {
            "decision": "approved",
            "notes": "looks good",
            "decided_by": "operator",
        },
        subsystems=subsystems,
    )

    with patch.object(
        workflow_run,
        "resolve_authority_checkpoint",
        return_value={"checkpoint_id": "checkpoint_123", "status": "approved"},
    ) as resolve_mock:
        workflow_run._handle_checkpoints_post(request, "/api/checkpoints/checkpoint_123/approve")

    assert resolve_mock.call_count == 1
    assert resolve_mock.call_args.kwargs["checkpoint_id"] == "checkpoint_123"
    assert resolve_mock.call_args.kwargs["decision"] == "approved"
    assert request.sent == (200, {"checkpoint_id": "checkpoint_123", "status": "approved"})


def test_checkpoint_approval_requeues_workflow_job() -> None:
    class _ApprovalPg:
        def __init__(self) -> None:
            self.saved: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
            self.saved.append((query, args))
            return []

    pg = _ApprovalPg()
    subsystems = SimpleNamespace(get_pg_conn=lambda: pg)
    request = _RequestStub(
        {
            "decision": "approved",
            "notes": "looks good",
            "decided_by": "operator",
        },
        subsystems=subsystems,
    )

    with patch.object(
        workflow_run,
        "resolve_authority_checkpoint",
        return_value={
            "checkpoint_id": "checkpoint_123",
            "status": "approved",
            "card_id": "workflow_job:17",
            "model_id": "workflow.alpha",
        },
    ):
        workflow_run._handle_checkpoints_post(request, "/api/checkpoints/checkpoint_123/approve")

    assert request.sent == (200, {"checkpoint_id": "checkpoint_123", "status": "approved", "card_id": "workflow_job:17", "model_id": "workflow.alpha"})
    assert any("UPDATE workflow_jobs" in query for query, _args in pg.saved)
    assert any("pg_notify('job_ready'" in query for query, _args in pg.saved)


def test_manifest_get_api_returns_normalized_bundle() -> None:
    subsystems = _SubsystemsStub()
    subsystems.pg.manifest_rows["manifest_123"] = {
        "id": "manifest_123",
        "name": "Support Workspace",
        "description": "Description",
        "manifest": {
            "version": 2,
            "grid": "4x4",
            "title": "Support Workspace",
            "quadrants": {
                "A1": {
                    "module": "metric",
                    "config": {"label": "Inbox", "value": "12"},
                }
            },
        },
    }
    request = _RequestStub({}, subsystems=subsystems)

    workflow_run._handle_manifest_get_api(request, "/api/manifests/manifest_123")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["id"] == "manifest_123"
    assert payload["name"] == "Support Workspace"
    assert payload["kind"] == "helm_surface_bundle"
    assert payload["surfaces"]["main"]["manifest"]["quadrants"]["A1"]["module"] == "metric"


def test_manifest_get_api_returns_raw_control_plane_manifest() -> None:
    subsystems = _SubsystemsStub()
    subsystems.pg.manifest_rows["plan_123"] = {
        "id": "plan_123",
        "name": "Data Cleanup Plan",
        "description": "Plan description",
        "status": "draft",
        "version": 3,
        "parent_manifest_id": None,
        "updated_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
        "manifest": {
            "kind": "praxis_control_manifest",
            "manifest_family": "control_plane",
            "manifest_type": "data_plan",
            "schema_version": 1,
            "plan": {"job": {"operation": "reconcile"}},
            "plan_digest": "digest-123",
        },
    }
    request = _RequestStub({}, subsystems=subsystems)

    workflow_run._handle_manifest_get_api(request, "/api/manifests/plan_123")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["id"] == "plan_123"
    assert payload["name"] == "Data Cleanup Plan"
    assert payload["kind"] == "praxis_control_manifest"
    assert payload["manifest_family"] == "control_plane"
    assert payload["manifest_type"] == "data_plan"
    assert payload["plan_digest"] == "digest-123"
    assert payload["status"] == "draft"
    assert payload["version"] == 3


def test_workflow_run_handler_does_not_import_manifest_or_checkpoint_storage_writes_directly() -> None:
    source = Path(workflow_run.__file__).read_text(encoding="utf-8")

    forbidden_imports = (
        "create_app_manifest",
        "upsert_app_manifest",
        "create_authority_checkpoint",
        "decide_authority_checkpoint",
        "persist_manifest_object_types",
    )
    leaked = [snippet for snippet in forbidden_imports if snippet in source]
    assert leaked == [], f"workflow_run.py still imports storage writes directly: {leaked}"


def test_workflow_run_handler_does_not_own_manifest_or_checkpoint_write_sql() -> None:
    source = Path(workflow_run.__file__).read_text(encoding="utf-8")

    forbidden_sql_snippets = (
        "INSERT INTO app_manifests",
        "UPDATE app_manifests",
        "INSERT INTO app_manifest_history",
        "INSERT INTO object_types",
        "UPDATE object_types",
        "INSERT INTO authority_checkpoints",
        "UPDATE authority_checkpoints",
    )
    leaked = [snippet for snippet in forbidden_sql_snippets if snippet in source]
    assert leaked == [], f"workflow_run.py still owns canonical write SQL: {leaked}"


def test_workflow_stream_uses_shared_run_wakeup_listener(monkeypatch) -> None:
    subsystems = _SubsystemsStub()
    request = _StreamRequestStub(subsystems)

    workflow_unified = __import__("runtime.workflow.unified", fromlist=["*"])
    statuses = [
        {
            "run_id": "run-123",
            "status": "running",
            "spec_name": "stream-spec",
            "total_jobs": 1,
            "jobs": [],
        },
        {
            "run_id": "run-123",
            "status": "succeeded",
            "spec_name": "stream-spec",
            "total_jobs": 1,
            "jobs": [{"status": "succeeded"}],
        },
    ]
    monkeypatch.setattr(workflow_unified, "get_run_status", lambda *_args, **_kwargs: statuses.pop(0))
    monkeypatch.setattr(
        workflow_run,
        "workflow_database_url_for_repo",
        lambda _repo: "postgresql://example",
    )

    listener_calls: list[dict[str, Any]] = []

    class _FakeListener:
        def __init__(self) -> None:
            self.stop_calls = 0

        def stop(self) -> None:
            self.stop_calls += 1

    fake_listener = _FakeListener()

    def _start_run_wakeup_listener(
        *,
        run_id: str,
        wakeup_event,
        database_url: str | None = None,
        channels: tuple[str, ...] = ("job_completed", "run_complete"),
    ):
        listener_calls.append(
            {
                "run_id": run_id,
                "database_url": database_url,
                "channels": channels,
                "wakeup_event": wakeup_event,
            }
        )
        return fake_listener

    workflow_notifications = __import__("runtime.workflow_notifications", fromlist=["*"])

    class _FakeNotificationConsumer:
        def __init__(self, conn) -> None:
            self.conn = conn

        def iter_run(
            self,
            run_id: str,
            total_jobs: int,
            timeout_seconds=None,
            poll_interval: float = 2.0,
            wakeup_event=None,
        ):
            del run_id, total_jobs, timeout_seconds, poll_interval
            assert wakeup_event is not None
            yield SimpleNamespace(
                job_label="build_a",
                status="succeeded",
                agent_slug="openai/gpt-5.4-mini",
                duration_seconds=1.2,
                failure_code="",
                cpu_percent=None,
                mem_bytes=None,
            )

    monkeypatch.setattr(workflow_run, "start_run_wakeup_listener", _start_run_wakeup_listener)
    monkeypatch.setattr(workflow_notifications, "WorkflowNotificationConsumer", _FakeNotificationConsumer)

    workflow_run._handle_workflow_stream(request, "/api/workflow-runs/run-123/stream")

    assert request.status == 200
    assert request.ended is True
    assert len(listener_calls) == 1
    assert listener_calls[0]["run_id"] == "run-123"
    assert listener_calls[0]["database_url"] == "postgresql://example"
    assert listener_calls[0]["channels"] == ("job_completed", "run_complete")
    assert fake_listener.stop_calls == 1

    body = request.wfile.getvalue().decode()
    assert "event: start" in body
    assert "event: job" in body
    assert "event: progress" in body
    assert "event: done" in body
