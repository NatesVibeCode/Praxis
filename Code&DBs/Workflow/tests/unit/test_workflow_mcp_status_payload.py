from __future__ import annotations

from datetime import datetime, timezone

from surfaces.mcp.tools import workflow as workflow_tools


def test_run_status_payload_includes_submission_summary(monkeypatch) -> None:
    import runtime.workflow.unified as unified

    now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        unified,
        "get_run_status",
        lambda _pg, _run_id: {
            "run_id": "run-1",
            "status": "running",
            "spec_name": "submission-spec",
            "total_jobs": 1,
            "completed_jobs": 0,
            "total_cost_usd": 0.0,
            "total_tokens_in": 0,
            "total_tokens_out": 0,
            "total_duration_ms": 0,
            "created_at": now,
            "jobs": [
                {
                    "label": "build.codegen",
                    "status": "running",
                    "agent_slug": "agent",
                    "attempt": 1,
                    "duration_ms": 125,
                    "created_at": now,
                    "submission": {
                        "submission_id": "sub-1",
                        "result_kind": "code_change",
                        "summary": "sealed result",
                        "measured_summary": {"create": 0, "update": 1, "delete": 0, "rename": 0, "total": 1},
                        "comparison_status": "matched",
                        "latest_review": {"decision": "approve"},
                    },
                }
            ],
        },
    )
    monkeypatch.setattr(unified, "summarize_run_health", lambda *_args, **_kwargs: {"state": "healthy"})
    monkeypatch.setattr(unified, "summarize_run_recovery", lambda *_args, **_kwargs: {"mode": "monitor"})
    monkeypatch.setattr(workflow_tools, "_render_dashboard_panel", lambda *_args, **_kwargs: "status-dashboard")

    payload = workflow_tools._run_status_payload(object(), "run-1")

    assert payload["jobs"][0]["submission"] == {
        "submission_id": "sub-1",
        "result_kind": "code_change",
        "summary": "sealed result",
        "measured_summary": {"create": 0, "update": 1, "delete": 0, "rename": 0, "total": 1},
        "comparison_status": "matched",
        "integrity_status": "matched",
        "latest_review_decision": "approve",
    }
    assert payload["dashboard"] == "status-dashboard"


def test_tool_praxis_workflow_status_returns_structured_runtime_error(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"
        details = {"environment_variable": "WORKFLOW_DATABASE_URL"}

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "status", "run_id": "run-1"})

    assert payload == {
        "error": "db blocked",
        "error_code": "postgres.authority_unavailable",
        "details": {"environment_variable": "WORKFLOW_DATABASE_URL"},
    }


def test_tool_praxis_workflow_list_returns_structured_runtime_error(monkeypatch) -> None:
    class _BrokenConn:
        def execute(self, query: str, *args):
            raise RuntimeError("workflow_runs unavailable")

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: _BrokenConn())

    payload = workflow_tools.tool_praxis_workflow({"action": "list"})

    assert payload == {
        "error": "workflow_runs unavailable",
        "error_code": "workflow.list.failed",
    }


def test_tool_praxis_workflow_run_returns_structured_runtime_error_when_pg_unavailable(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[]}',
        encoding="utf-8",
    )

    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"
        details = {"environment_variable": "WORKFLOW_DATABASE_URL"}

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"spec_path": str(spec_path), "wait": False})

    assert payload == {
        "error": "db blocked",
        "error_code": "postgres.authority_unavailable",
        "details": {"environment_variable": "WORKFLOW_DATABASE_URL"},
    }


def test_tool_praxis_workflow_retry_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("retry db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "retry", "run_id": "run-1", "label": "build"})

    assert payload == {
        "error": "retry db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_cancel_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("cancel db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "cancel", "run_id": "run-1"})

    assert payload == {
        "error": "cancel db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_inspect_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("inspect db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "inspect", "run_id": "run-1"})

    assert payload == {
        "error": "inspect db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_run_returns_async_payload_without_progress_emitter(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[{"label":"job-a"}]}',
        encoding="utf-8",
    )

    class _Spec:
        def __init__(self) -> None:
            self.name = "workflow"
            self.jobs = [{"label": "job-a"}]

        @classmethod
        def load(cls, _path: str):
            return cls()

    monkeypatch.setattr(workflow_tools, "_workflow_spec_mod", lambda: type("_SpecMod", (), {"WorkflowSpec": _Spec}))
    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(workflow_tools, "_render_dashboard_panel", lambda *_args, **_kwargs: "submit-dashboard")
    monkeypatch.setattr(
        workflow_tools,
        "_submit_workflow_via_service_bus",
        lambda *_args, **_kwargs: {
            "run_id": "dispatch_001",
            "status": "queued",
            "spec_name": "workflow",
            "total_jobs": 1,
            "command_id": "control.command.submit.1",
        },
    )

    payload = workflow_tools.tool_praxis_workflow({"spec_path": str(spec_path)})

    assert payload == {
        "run_id": "dispatch_001",
        "status": "queued",
        "spec_name": "workflow",
        "total_jobs": 1,
        "command_id": "control.command.submit.1",
        "command_status": "succeeded",
        "stream_url": "/api/workflow-runs/dispatch_001/stream",
        "status_url": "/api/workflow-runs/dispatch_001/status",
        "dashboard": "submit-dashboard",
        "delivery": {
            "dashboard_in_payload": True,
            "live_channel": "none",
            "message_notifications": False,
            "progress_notifications": False,
            "wait_requested": True,
            "inline_polling": False,
        },
    }


def test_tool_praxis_workflow_run_with_message_only_emitter_returns_async_payload(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[{"label":"job-a"}]}',
        encoding="utf-8",
    )

    class _Spec:
        def __init__(self) -> None:
            self.name = "workflow"
            self.jobs = [{"label": "job-a"}]

        @classmethod
        def load(cls, _path: str):
            return cls()

    class _MessageOnlyEmitter:
        enabled = False
        progress_token = None

        def log(self, *_args, **_kwargs) -> None:
            raise AssertionError("message-only emitter should not trigger inline polling")

        def emit(self, *_args, **_kwargs) -> None:
            raise AssertionError("message-only emitter should not trigger inline polling")

    monkeypatch.setattr(workflow_tools, "_workflow_spec_mod", lambda: type("_SpecMod", (), {"WorkflowSpec": _Spec}))
    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(workflow_tools, "_render_dashboard_panel", lambda *_args, **_kwargs: "submit-dashboard")
    monkeypatch.setattr(
        workflow_tools,
        "_submit_workflow_via_service_bus",
        lambda *_args, **_kwargs: {
            "run_id": "dispatch_002",
            "status": "queued",
            "spec_name": "workflow",
            "total_jobs": 1,
            "command_id": "control.command.submit.2",
        },
    )
    monkeypatch.setattr(
        workflow_tools,
        "_poll_run_to_completion",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inline polling should not run without progress token")),
    )

    payload = workflow_tools.tool_praxis_workflow(
        {"spec_path": str(spec_path)},
        _progress_emitter=_MessageOnlyEmitter(),
    )

    assert payload["run_id"] == "dispatch_002"
    assert payload["dashboard"] == "submit-dashboard"
    assert payload["delivery"] == {
        "dashboard_in_payload": True,
        "live_channel": "notifications.message",
        "message_notifications": True,
        "progress_notifications": False,
        "wait_requested": True,
        "inline_polling": False,
    }
