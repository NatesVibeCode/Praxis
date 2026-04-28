from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pathlib

import runtime.chat_tools as chat_tools
from runtime.workflow import unified as unified_dispatch

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


class _PlatformDataConn:
    def __init__(self, *, report: str) -> None:
        self.report = report
        self.last_query: str | None = None

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        self.last_query = normalized
        if "FROM workflow_jobs" not in normalized:
            raise AssertionError(f"Unexpected SQL: {query}")
        return [
            {
                "run_id": "dispatch_001",
                "label": "job-a",
                "status": "failed",
                "error_code": "credential_error",
                "failure_category": "credential_error",
                "failure_zone": "config",
                "is_transient": False,
                "agent": "openai/gpt-5.4",
                "duration_ms": 3000,
                "created_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
            }
        ]


class _RetryStateConn:
    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT id, run_id, label, status, attempt FROM workflow_jobs"):
            return [
                {
                    "id": 77,
                    "run_id": args[0],
                    "label": args[1],
                    "status": "failed",
                    "attempt": 2,
                }
            ]
        raise AssertionError(f"Unexpected SQL: {query}")


def _fake_command(
    *,
    command_id: str,
    command_type: str,
    command_status: str,
    idempotency_key: str,
    payload: dict[str, object],
    result_ref: str | None = None,
    error_code: str | None = None,
    error_detail: str | None = None,
):
    snapshot = {
        "command_id": command_id,
        "command_type": command_type,
        "command_status": command_status,
        "requested_by_kind": "chat",
        "requested_by_ref": "chat.workspace",
        "requested_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
        "approved_at": None,
        "approved_by": None,
        "idempotency_key": idempotency_key,
        "risk_level": "low" if command_type == "workflow.submit" else "medium",
        "payload": payload,
        "result_ref": result_ref,
        "error_code": error_code,
        "error_detail": error_detail,
        "created_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
        "updated_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
    }
    return SimpleNamespace(
        **snapshot,
        to_json=lambda snapshot=snapshot: dict(snapshot),
    )


def test_run_workflow_routes_spec_path_through_command_bus(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fail_direct_submit(*_args, **_kwargs):
        raise AssertionError("chat should not call submit_workflow_inline directly")

    def _fake_request(_conn, **kwargs):
        captured["kwargs"] = kwargs
        return _fake_command(
            command_id="control.command.submit.1",
            command_type="workflow.submit",
            command_status="succeeded",
            idempotency_key="workflow.submit.chat.fake",
            payload={"spec_path": "artifacts/workflow/sample.queue.json", "repo_root": _REPO_ROOT},
            result_ref="workflow_run:run-123",
        )

    monkeypatch.setattr("runtime.control_commands.request_workflow_submit_command", _fake_request)
    monkeypatch.setattr(unified_dispatch, "submit_workflow_inline", _fail_direct_submit)

    result = chat_tools.execute_tool(
        "run_workflow",
        {"spec_path": "artifacts/workflow/sample.queue.json"},
        object(),
        _REPO_ROOT,
    )

    assert captured["kwargs"] == {
        "requested_by_kind": "chat",
        "requested_by_ref": "chat.workspace",
        "spec_path": "artifacts/workflow/sample.queue.json",
        "repo_root": _REPO_ROOT,
    }
    assert result["type"] == "status"
    assert result["data"]["status"] == "queued"
    assert result["data"]["approval_required"] is False
    assert result["data"]["command_id"] == "control.command.submit.1"
    assert result["data"]["run_id"] == "run-123"
    assert result["data"]["spec_name"] == "sample"


def test_run_workflow_compiles_inline_jobs_into_control_command(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fail_direct_submit(*_args, **_kwargs):
        raise AssertionError("chat should not call submit_workflow_inline directly")

    def _fake_request(_conn, intent, **_kwargs):
        captured["intent"] = intent
        return _fake_command(
            command_id="control.command.submit.2",
            command_type="workflow.submit",
            command_status="succeeded",
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:run-456",
        )

    monkeypatch.setattr(chat_tools, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(chat_tools, "request_control_command", _fake_request)
    monkeypatch.setattr(unified_dispatch, "submit_workflow_inline", _fail_direct_submit)

    result = chat_tools.execute_tool(
        "run_workflow",
        {
            "name": "Ad hoc chat workflow",
            "objective": "Do the thing",
            "jobs": [
                {
                    "label": "job-a",
                    "agent": "openai/gpt-5.4-mini",
                    "prompt": "Do the thing.",
                }
            ],
        },
        object(),
        _REPO_ROOT,
    )

    assert captured["intent"].command_type == chat_tools.ControlCommandType.WORKFLOW_SUBMIT
    assert captured["intent"].payload["spec"]["name"] == "Ad hoc chat workflow"
    assert captured["intent"].payload["spec"]["objective"] == "Do the thing"
    assert captured["intent"].payload["spec"]["outcome_goal"] == "Do the thing"
    assert captured["intent"].payload["spec"]["jobs"][0]["label"] == "job-a"
    assert result["type"] == "status"
    assert result["data"]["status"] == "queued"
    assert result["data"]["run_id"] == "run-456"
    assert result["data"]["spec_name"] == "Ad hoc chat workflow"


def test_retry_and_cancel_route_to_control_commands(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    def _fail_direct_retry(*_args, **_kwargs):
        raise AssertionError("chat should not call retry_job directly")

    def _fail_direct_cancel(*_args, **_kwargs):
        raise AssertionError("chat should not call cancel_run directly")

    def _fake_request(_conn, intent, **_kwargs):
        command_type = intent.command_type.value if hasattr(intent.command_type, "value") else str(intent.command_type)
        captured.append(
            {
                "command_type": intent.command_type,
                "payload": dict(intent.payload),
                "idempotency_key": intent.idempotency_key,
            }
        )
        return _fake_command(
            command_id=f"control.command.{len(captured)}",
            command_type=command_type,
            command_status="requested",
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
        )

    monkeypatch.setattr(chat_tools, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(chat_tools, "request_control_command", _fake_request)
    monkeypatch.setattr(unified_dispatch, "retry_job", _fail_direct_retry)
    monkeypatch.setattr(unified_dispatch, "cancel_run", _fail_direct_cancel)

    retry_result = chat_tools.execute_tool(
        "retry_job",
        {
            "run_id": "run-7",
            "label": "build_a",
            "previous_failure": "run-7/build_a failed with provider.capacity",
            "retry_delta": "retry on openai/gpt-5.4",
            "model_override": "openai/gpt-5.4",
        },
        _RetryStateConn(),
        _REPO_ROOT,
    )
    cancel_result = chat_tools.execute_tool(
        "cancel_workflow",
        {"run_id": "run-8"},
        object(),
        _REPO_ROOT,
    )

    assert captured[0]["command_type"] == chat_tools.ControlCommandType.WORKFLOW_RETRY
    assert captured[0]["payload"]["run_id"] == "run-7"
    assert captured[0]["payload"]["label"] == "build_a"
    assert captured[0]["payload"]["previous_failure"] == "run-7/build_a failed with provider.capacity"
    assert captured[0]["payload"]["retry_delta"] == "retry on openai/gpt-5.4"
    assert captured[0]["payload"]["model_override"] == "openai/gpt-5.4"
    assert retry_result["type"] == "status"
    assert retry_result["data"]["status"] == "approval_required"
    assert retry_result["data"]["approval_required"] is True
    assert retry_result["data"]["command_status"] == "requested"
    assert retry_result["data"]["command_id"] == "control.command.1"

    assert captured[1]["command_type"] == chat_tools.ControlCommandType.WORKFLOW_CANCEL
    assert captured[1]["payload"] == {"run_id": "run-8", "include_running": True}
    assert cancel_result["type"] == "status"
    assert cancel_result["data"]["status"] == "approval_required"
    assert cancel_result["data"]["approval_required"] is True
    assert cancel_result["data"]["command_status"] == "requested"
    assert cancel_result["data"]["command_id"] == "control.command.2"


def test_query_platform_data_projects_failure_error_code_from_canonical_category():
    conn = _PlatformDataConn(report="recent_failures")

    result = chat_tools._query_platform_data(
        {"report": "recent_failures", "time_range": "24h", "limit": 5, "filter": ""},
        conn,
    )

    assert conn.last_query is not None
    assert "COALESCE(NULLIF(failure_category, ''), last_error_code) AS error_code" in conn.last_query
    assert result["data"]["rows"][0]["error_code"] == "credential_error"


def test_query_platform_data_projects_job_history_error_code_from_canonical_category():
    conn = _PlatformDataConn(report="job_history")

    result = chat_tools._query_platform_data(
        {"report": "job_history", "time_range": "24h", "limit": 5, "filter": ""},
        conn,
    )

    assert conn.last_query is not None
    assert "COALESCE(NULLIF(failure_category, ''), last_error_code) AS error_code" in conn.last_query
    assert result["data"]["rows"][0]["error_code"] == "credential_error"
