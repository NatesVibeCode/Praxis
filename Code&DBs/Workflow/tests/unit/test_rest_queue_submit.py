from __future__ import annotations

import json
import sys
from types import ModuleType
from types import SimpleNamespace
from unittest.mock import patch

from surfaces.api import rest


def test_submit_queue_job_uses_command_bus_helper(tmp_path, monkeypatch) -> None:
    temp_dir = tmp_path / "artifacts" / "workflow"
    temp_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(rest, "REPO_ROOT", tmp_path)

    fake_result = {
        "run_id": "dispatch_321",
        "status": "queued",
        "spec_name": "queue-report",
        "total_jobs": 1,
        "command_id": "control.command.123",
    }

    request = rest.QueueSubmitRequest(
        spec=rest.WorkflowRunRequest(
            prompt="Draft the support report",
            label="Queue Report",
            task_type="build",
        ),
        priority=42,
        max_attempts=3,
    )

    with patch.object(rest, "_shared_pg_conn", return_value=SimpleNamespace()) as conn_mock, patch.object(
        rest,
        "_submit_workflow_via_service_bus",
        return_value=fake_result,
    ) as bus_mock:
        result = rest.submit_queue_job(request)

    conn_mock.assert_called_once()
    assert bus_mock.call_count == 1
    assert bus_mock.call_args.kwargs["requested_by_kind"] == "http"
    assert bus_mock.call_args.kwargs["requested_by_ref"] == "queue_submit"
    assert result == {
        "run_id": "dispatch_321",
        "status": "queued",
        "command_id": "control.command.123",
        "priority": 42,
        "note": "priority is accepted for compatibility but scheduling is workflow-runtime driven",
    }


def test_spec_from_request_preserves_shadow_packet_fields() -> None:
    req = rest.WorkflowRunRequest(
        prompt="Draft the support report",
        label="Queue Report",
        task_type="build",
        prefer_cost=True,
        verify_refs=["verify_ref.python.py_compile.test"],
        definition_revision="def_alpha",
        plan_revision="plan_alpha",
        packet_provenance={
            "source_kind": "workflow_runtime",
            "compiled_spec_row": {
                "definition_revision": "def_alpha",
                "plan_revision": "plan_alpha",
            },
        },
    )

    module = ModuleType("runtime.workflow")

    class _WorkflowSpecStub:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    module.WorkflowSpec = _WorkflowSpecStub
    with patch.dict(sys.modules, {"runtime.workflow": module}):
        spec = rest._spec_from_request(req)

    assert spec.verify_refs == ["verify_ref.python.py_compile.test"]
    assert spec.prefer_cost is True
    assert spec.definition_revision == "def_alpha"
    assert spec.plan_revision == "plan_alpha"
    assert spec.packet_provenance == {
        "source_kind": "workflow_runtime",
        "compiled_spec_row": {
            "definition_revision": "def_alpha",
            "plan_revision": "plan_alpha",
        },
    }


def test_cancel_queue_job_routes_through_control_command_bus(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *params):
            if "SELECT run_id" in query:
                assert params == ("42",)
                assert "status IN" not in query
                return [{"run_id": "dispatch_042"}]
            if "UPDATE workflow_jobs" in query:
                raise AssertionError("direct workflow job mutation bypassed the command bus")
            raise AssertionError(f"unexpected query: {query}")

    fake_command = SimpleNamespace(
        command_id="control.command.cancel.42",
        command_status="succeeded",
        error_detail=None,
        to_json=lambda: {
            "command_id": "control.command.cancel.42",
            "command_status": "succeeded",
            "error_detail": None,
        },
    )
    cancel_proof = {
        "cancelled_jobs": 1,
        "labels": ["build_a"],
        "run_status": "cancelled",
        "terminal_reason": "workflow_cancelled",
    }

    with patch.object(rest, "_shared_pg_conn", return_value=_Conn()), patch(
        "runtime.control_commands.bootstrap_control_commands_schema",
        lambda _conn: None,
    ), patch(
        "runtime.control_commands.execute_control_intent",
        return_value=fake_command,
    ) as execute_mock, patch(
        "runtime.control_commands.workflow_cancel_proof",
        lambda _conn, _run_id: dict(cancel_proof),
    ):
        response = rest.cancel_queue_job("42")

    assert execute_mock.call_count == 1
    intent = execute_mock.call_args.args[1]
    assert intent.command_type == "workflow.cancel"
    assert intent.requested_by_kind == "http"
    assert intent.requested_by_ref == "queue_cancel"
    assert intent.payload == {"run_id": "dispatch_042", "include_running": True}
    assert execute_mock.call_args.kwargs["approved_by"] == "http.queue_cancel"
    assert response.status_code == 200
    result = json.loads(response.body)
    assert result == {
        "job_id": "42",
        "run_id": "dispatch_042",
        "status": "cancelled",
        "command_id": "control.command.cancel.42",
        "command_status": "succeeded",
        "approval_required": False,
        "stream_url": "/api/workflow-runs/dispatch_042/stream",
        "status_url": "/api/workflow-runs/dispatch_042/status",
        "cancelled_jobs": 1,
        "labels": ["build_a"],
        "run_status": "cancelled",
        "terminal_reason": "workflow_cancelled",
    }


def test_cancel_queue_job_fails_closed_when_durable_cancel_proof_disagrees() -> None:
    class _Conn:
        def execute(self, query: str, *params):
            if "SELECT run_id" in query:
                assert params == ("42",)
                return [{"run_id": "dispatch_042"}]
            if "UPDATE workflow_jobs" in query:
                raise AssertionError("direct workflow job mutation bypassed the command bus")
            raise AssertionError(f"unexpected query: {query}")

    fake_command = SimpleNamespace(
        command_id="control.command.cancel.42",
        command_status="succeeded",
        result_ref="workflow_run:dispatch_042",
        error_detail=None,
        to_json=lambda: {
            "command_id": "control.command.cancel.42",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_042",
            "error_detail": None,
        },
    )
    disputed_proof = {
        "cancelled_jobs": 0,
        "labels": [],
        "run_status": "running",
        "terminal_reason": None,
    }

    with patch.object(rest, "_shared_pg_conn", return_value=_Conn()), patch(
        "runtime.control_commands.bootstrap_control_commands_schema",
        lambda _conn: None,
    ), patch(
        "runtime.control_commands.execute_control_intent",
        return_value=fake_command,
    ), patch(
        "runtime.control_commands.workflow_cancel_proof",
        lambda _conn, _run_id: dict(disputed_proof),
    ):
        response = rest.cancel_queue_job("42")

    assert response.status_code == 409
    result = json.loads(response.body)
    assert result == {
        "job_id": "42",
        "run_id": "dispatch_042",
        "status": "failed",
        "command_id": "control.command.cancel.42",
        "command_status": "succeeded",
        "approval_required": False,
        "result_ref": "workflow_run:dispatch_042",
        "stream_url": "/api/workflow-runs/dispatch_042/stream",
        "status_url": "/api/workflow-runs/dispatch_042/status",
        "error": "workflow cancel did not reach the cancelled state",
        "error_code": "control.command.workflow_cancel_incomplete",
        "error_detail": "workflow cancel did not reach the cancelled state",
        "command": {
            "command_id": "control.command.cancel.42",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_042",
            "error_detail": None,
        },
        "cancelled_jobs": 0,
        "labels": [],
        "run_status": "running",
        "terminal_reason": None,
        "proof": disputed_proof,
    }


def test_cancel_queue_job_returns_not_found_for_terminal_or_missing_job() -> None:
    class _Conn:
        def execute(self, query: str, *params):
            if "SELECT run_id" in query:
                return []
            raise AssertionError(f"unexpected query: {query}")

    with patch.object(rest, "_shared_pg_conn", return_value=_Conn()):
        response = rest.cancel_queue_job("404")

    assert response.status_code == 404
    result = json.loads(response.body)
    assert result == {
        "status": "failed",
        "command_status": "failed",
        "approval_required": False,
        "error": "Job '404' not found or already in a terminal state",
        "error_code": "control.command.workflow_cancel_target_not_found",
        "error_detail": "Job '404' not found or already in a terminal state",
        "job_id": "404",
    }
