from __future__ import annotations

import ast
import json
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import runtime.chat_tools as chat_tools
import runtime.control_commands as control_commands
from runtime.workflow import unified as unified_dispatch
from surfaces.cli.commands import workflow as workflow_commands
from surfaces.cli import workflow_cli
from surfaces.api import rest
from surfaces.api.handlers import workflow_run
from surfaces.mcp.tools import workflow as mcp_workflow


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_MUTATION_SEAMS = {
    "runtime/chat_tools.py": {
        "required_calls": {"request_control_command", "request_workflow_submit_command"},
    },
    "surfaces/cli/commands/workflow.py": {
        "required_calls": {"cmd_run"},
    },
    "surfaces/cli/workflow_cli.py": {
        "required_calls": {"run_cli_tool"},
    },
    "surfaces/mcp/tools/workflow.py": {
        "required_calls": {
            "request_control_command",
            "request_workflow_submit_command",
            "execute_control_intent",
        },
    },
    "surfaces/api/rest.py": {
        "required_calls": {"_submit_workflow_via_service_bus", "execute_control_intent"},
    },
    "surfaces/api/handlers/workflow_run.py": {
        "required_calls": {"request_workflow_submit_command"},
    },
}
_FORBIDDEN_MUTATION_LEAF_NAMES = {
    "submit_workflow",
    "submit_workflow_inline",
    "retry_job",
    "cancel_run",
    "run_workflow",
    "run_workflow_from_spec_file",
    "run_workflow_batch_from_file",
}


def _fail_direct_mutation(message: str):
    def _raiser(*_args, **_kwargs):
        raise AssertionError(message)

    return _raiser


def _fake_command(
    *,
    command_id: str,
    command_type: str,
    command_status: str,
    requested_by_kind: str,
    requested_by_ref: str,
    idempotency_key: str,
    payload: dict[str, Any],
    result_ref: str | None = None,
    error_code: str | None = None,
    error_detail: str | None = None,
):
    snapshot = {
        "command_id": command_id,
        "command_type": command_type,
        "command_status": command_status,
        "requested_by_kind": requested_by_kind,
        "requested_by_ref": requested_by_ref,
        "requested_at": "2026-04-08T12:00:00+00:00",
        "approved_at": None,
        "approved_by": None,
        "idempotency_key": idempotency_key,
        "risk_level": "low" if command_type == "workflow.submit" else "medium",
        "payload": payload,
        "result_ref": result_ref,
        "error_code": error_code,
        "error_detail": error_detail,
        "created_at": "2026-04-08T12:00:00+00:00",
        "updated_at": "2026-04-08T12:00:00+00:00",
    }
    return SimpleNamespace(
        **snapshot,
        to_json=lambda snapshot=snapshot: dict(snapshot),
    )


class _AuthorityRecorder:
    def __init__(self) -> None:
        self.request_calls: list[dict[str, Any]] = []
        self.execute_calls: list[dict[str, Any]] = []

    @staticmethod
    def _command_type_value(command_type: Any) -> str:
        return command_type.value if hasattr(command_type, "value") else str(command_type)

    def request(self, _conn: Any, intent: Any, **_kwargs: Any):
        command_type = self._command_type_value(intent.command_type)
        call = {
            "command_type": command_type,
            "requested_by_kind": intent.requested_by_kind,
            "requested_by_ref": intent.requested_by_ref,
            "idempotency_key": intent.idempotency_key,
            "payload": dict(intent.payload),
        }
        self.request_calls.append(call)
        index = len(self.request_calls)
        succeeded = command_type == "workflow.submit"
        result_ref = f"workflow_run:dispatch_request_{index:03d}" if succeeded else None
        return _fake_command(
            command_id=f"control.command.request.{index}",
            command_type=command_type,
            command_status="succeeded" if succeeded else "requested",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref=result_ref,
        )

    def execute(self, _conn: Any, intent: Any, *, approved_by: str, **_kwargs: Any):
        command_type = self._command_type_value(intent.command_type)
        call = {
            "command_type": command_type,
            "requested_by_kind": intent.requested_by_kind,
            "requested_by_ref": intent.requested_by_ref,
            "approved_by": approved_by,
            "idempotency_key": intent.idempotency_key,
            "payload": dict(intent.payload),
        }
        self.execute_calls.append(call)
        index = len(self.execute_calls)
        run_id = str(intent.payload.get("run_id") or f"dispatch_execute_{index:03d}")
        return _fake_command(
            command_id=f"control.command.execute.{index}",
            command_type=command_type,
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref=f"workflow_run:{run_id}",
        )


class _WorkflowSpecModuleStub:
    class WorkflowSpec:
        def __init__(self, path: str) -> None:
            self.name = Path(path).name.removesuffix(".queue.json") or "workflow"
            self.jobs = [
                {
                    "label": "job-a",
                    "agent": "auto/build",
                    "prompt": "Build the thing.",
                }
            ]

        @classmethod
        def load(cls, path: str):
            return cls(path)


class _CancelConn:
    def execute(self, query: str, *params: Any):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT run_id FROM workflow_jobs"):
            assert params == ("42",)
            assert "status IN" not in normalized
            return [{"run_id": "dispatch_http_cancel"}]
        if "UPDATE workflow_jobs" in normalized:
            raise AssertionError("API cancel should not mutate workflow_jobs directly")
        raise AssertionError(f"unexpected query: {query}")


def _write_queue_spec(tmp_path: Path, *, name: str = "cli run smoke") -> str:
    path = tmp_path / "cli-run.queue.json"
    path.write_text(
        json.dumps(
            {
                "name": name,
                "workflow_id": name.replace(" ", "_"),
                "phase": "test",
                "jobs": [
                    {
                        "label": "job-a",
                        "agent": "openai/gpt-5.4-mini",
                        "prompt": "Run the queued workflow.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return str(path)


def _called_function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()

    def _dotted_name(node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = _dotted_name(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        return None

    for child in ast.walk(tree):
        if isinstance(child, ast.Call):
            name = _dotted_name(child.func)
            if name:
                names.add(name)
    return names


def test_mutating_chat_mcp_and_http_surfaces_are_bus_only_by_construction() -> None:
    for relative_path, contract in _MUTATION_SEAMS.items():
        call_names = _called_function_names(_WORKFLOW_ROOT / relative_path)
        leaf_names = {name.rsplit(".", 1)[-1] for name in call_names}

        forbidden = sorted(_FORBIDDEN_MUTATION_LEAF_NAMES & leaf_names)
        assert forbidden == [], f"{relative_path} bypasses the workflow command bus via {forbidden}"

        missing = sorted(contract["required_calls"] - leaf_names)
        assert missing == [], f"{relative_path} lost its workflow command-bus hook(s): {missing}"


def test_submit_surfaces_converge_on_request_control_command_authority(tmp_path, monkeypatch) -> None:
    recorder = _AuthorityRecorder()
    spec_path = "artifacts/workflow/sample.queue.json"
    cli_spec_path = _write_queue_spec(tmp_path)
    cli_result_path = tmp_path / "cli-result.json"

    monkeypatch.setattr(chat_tools, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(chat_tools, "request_control_command", recorder.request)
    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", recorder.request)
    monkeypatch.setattr(
        unified_dispatch,
        "submit_workflow",
        _fail_direct_mutation("workflow submit should route through control_commands.request_control_command"),
    )
    monkeypatch.setattr(
        unified_dispatch,
        "submit_workflow_inline",
        _fail_direct_mutation("workflow submit should not fall back to submit_workflow_inline"),
    )
    monkeypatch.setattr(mcp_workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(workflow_run, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(rest, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: object())
    monkeypatch.setattr(
        "storage.postgres.connection.SyncPostgresConnection",
        lambda: SimpleNamespace(),
    )
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(workflow_cli, "_repo_root", lambda: str(tmp_path))
    monkeypatch.setattr(workflow_run, "_workflow_spec_mod", lambda: _WorkflowSpecModuleStub)
    monkeypatch.setattr(mcp_workflow, "_workflow_spec_mod", lambda: _WorkflowSpecModuleStub)

    subsystems = SimpleNamespace(
        get_pg_conn=lambda: object(),
    )
    monkeypatch.setattr(mcp_workflow, "_subs", subsystems)

    chat_result = chat_tools.execute_tool(
        "run_workflow",
        {"spec_path": spec_path},
        object(),
        str(tmp_path),
    )
    mcp_result = mcp_workflow.tool_praxis_workflow({"spec_path": spec_path, "dry_run": False})
    api_run_result = workflow_run._handle_workflow(
        subsystems,
        {"spec_path": spec_path, "dry_run": False},
    )
    queue_result = rest.submit_queue_job(
        rest.QueueSubmitRequest(
            spec=rest.WorkflowRunRequest(
                prompt="Draft the support report",
                label="Queue Report",
                task_type="build",
            ),
            priority=7,
            max_attempts=2,
        )
    )
    cli_exit = workflow_cli.cmd_run(
        SimpleNamespace(
            spec=cli_spec_path,
            dry_run=False,
            job_id="cli-job",
            run_id="dispatch_cli_submit",
            result_file=str(cli_result_path),
        )
    )
    cli_result = json.loads(cli_result_path.read_text(encoding="utf-8"))

    assert chat_result["data"]["status"] == "queued"
    assert mcp_result["status"] == "queued"
    assert api_run_result["status"] == "queued"
    assert queue_result["status"] == "queued"
    assert cli_exit == 0
    assert cli_result["status"] == "queued"

    assert [call["command_type"] for call in recorder.request_calls] == [
        "workflow.submit",
        "workflow.submit",
        "workflow.submit",
        "workflow.submit",
        "workflow.submit",
    ]
    assert recorder.request_calls[0]["requested_by_kind"] == "chat"
    assert recorder.request_calls[0]["requested_by_ref"] == "chat.workspace"
    assert recorder.request_calls[0]["payload"] == {
        "spec_path": spec_path,
        "repo_root": str(tmp_path),
    }

    assert recorder.request_calls[1]["requested_by_kind"] == "mcp"
    assert recorder.request_calls[1]["requested_by_ref"] == "praxis_workflow.run"
    assert recorder.request_calls[1]["payload"] == {
        "spec_path": spec_path,
        "repo_root": str(tmp_path),
    }

    assert recorder.request_calls[2]["requested_by_kind"] == "http"
    assert recorder.request_calls[2]["requested_by_ref"] == "workflow_run"
    assert recorder.request_calls[2]["payload"] == {
        "spec_path": spec_path,
        "repo_root": str(tmp_path),
    }

    assert recorder.request_calls[3]["requested_by_kind"] == "http"
    assert recorder.request_calls[3]["requested_by_ref"] == "queue_submit"
    assert recorder.request_calls[3]["payload"]["repo_root"] == str(tmp_path)
    assert recorder.request_calls[3]["payload"]["inline_spec"] == {
        "name": "Queue Report",
        "workflow_id": "workflow.api.queue.report",
        "phase": "build",
        "workspace_ref": rest._DEFAULT_WORKSPACE_REF,
        "runtime_profile_ref": rest._DEFAULT_RUNTIME_PROFILE_REF,
        "jobs": [
            {
                "label": "Queue Report",
                "agent": "auto/build",
                "prompt": "Draft the support report",
                "read_scope": [],
                "write_scope": [],
                "max_attempts": 2,
            }
        ],
    }
    assert recorder.request_calls[4]["requested_by_kind"] == "mcp"
    assert recorder.request_calls[4]["requested_by_ref"] == "praxis_workflow.run"
    assert recorder.request_calls[4]["payload"] == {
        "spec_path": cli_spec_path,
        "repo_root": str(tmp_path),
    }
    assert cli_result["run_id"] == "dispatch_request_005"
    assert cli_result["status"] == "queued"
    assert cli_result["command_id"] == "control.command.request.5"
    assert cli_result["command_status"] == "succeeded"
    assert cli_result["approval_required"] is False
    assert cli_result["result_ref"] == "workflow_run:dispatch_request_005"
    assert cli_result["job_id"] == "cli-job"
    assert cli_result["workflow_id"] == "cli_run_smoke"


def test_legacy_queue_submit_frontdoor_routes_through_request_control_command_authority(tmp_path, monkeypatch) -> None:
    recorder = _AuthorityRecorder()
    spec_path = _write_queue_spec(tmp_path, name="legacy queue")
    stdout = StringIO()

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", recorder.request)
    monkeypatch.setattr(workflow_commands, "_workflow_runtime_conn", lambda: object())

    exit_code = workflow_commands._queue_command(
        ["submit", spec_path, "--priority", "7", "--max-attempts", "2"],
        stdout=stdout,
    )
    payload = json.loads(stdout.getvalue())

    assert exit_code == 0
    assert recorder.request_calls == [
        {
            "command_type": "workflow.submit",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow.queue.submit",
            "idempotency_key": recorder.request_calls[0]["idempotency_key"],
            "payload": {
                "inline_spec": {
                    "name": "legacy queue",
                    "workflow_id": "legacy_queue",
                    "phase": "test",
                    "jobs": [
                        {
                            "label": "job-a",
                            "agent": "openai/gpt-5.4-mini",
                            "prompt": "Run the queued workflow.",
                            "max_attempts": 2,
                        }
                    ],
                }
            },
        }
    ]
    assert payload == {
        "run_id": "dispatch_request_001",
        "status": "queued",
        "total_jobs": 1,
        "command_id": "control.command.request.1",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_request_001",
        "priority": 7,
        "max_attempts": 2,
        "note": "priority is accepted for compatibility but scheduling is workflow-runtime driven",
    }


def test_run_submit_surfaces_share_the_same_queued_envelope(tmp_path, monkeypatch) -> None:
    cli_spec_path = _write_queue_spec(tmp_path, name="sample")
    cli_result_path = tmp_path / "cli-result.json"
    shared_command = _fake_command(
        command_id="control.command.submit.shared",
        command_type="workflow.submit",
        command_status="succeeded",
        requested_by_kind="shared",
        requested_by_ref="shared",
        idempotency_key="workflow.submit.shared",
        payload={"spec_path": cli_spec_path, "repo_root": str(tmp_path)},
        result_ref="workflow_run:dispatch_shared",
    )

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(workflow_cli, "_repo_root", lambda: str(tmp_path))
    monkeypatch.setattr(
        "runtime.control_commands.bootstrap_control_commands_schema",
        lambda _conn: None,
    )
    monkeypatch.setattr(
        "runtime.control_commands.request_control_command",
        lambda *_args, **_kwargs: shared_command,
    )

    api_result = workflow_run._submit_workflow_via_service_bus(
        SimpleNamespace(get_pg_conn=lambda: object()),
        spec_path=cli_spec_path,
        spec_name="sample",
        total_jobs=1,
        requested_by_kind="http",
        requested_by_ref="workflow_run",
    )
    mcp_result = mcp_workflow._submit_workflow_via_service_bus(
        object(),
        spec_path=cli_spec_path,
        spec_name="sample",
        total_jobs=1,
    )
    cli_exit = workflow_cli.cmd_run(
        SimpleNamespace(
            spec=cli_spec_path,
            dry_run=False,
            job_id="cli",
            run_id=None,
            result_file=str(cli_result_path),
        )
    )
    cli_result = json.loads(cli_result_path.read_text(encoding="utf-8"))

    expected = {
        "run_id": "dispatch_shared",
        "status": "queued",
        "spec_name": "sample",
        "total_jobs": 1,
        "command_id": "control.command.submit.shared",
        "command_status": "succeeded",
        "approval_required": False,
        "stream_url": "/api/workflow-runs/dispatch_shared/stream",
        "status_url": "/api/workflow-runs/dispatch_shared/status",
        "result_ref": "workflow_run:dispatch_shared",
    }

    assert api_result == expected
    assert mcp_result == expected
    assert cli_exit == 0
    assert {key: cli_result[key] for key in expected} == expected


def test_retry_cancel_and_repair_surfaces_converge_on_command_bus_authority(monkeypatch) -> None:
    recorder = _AuthorityRecorder()
    cancel_proof = {
        "cancelled_jobs": 1,
        "labels": ["build_a"],
        "run_status": "cancelled",
        "terminal_reason": "workflow_cancelled",
    }

    monkeypatch.setattr(chat_tools, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(chat_tools, "request_control_command", recorder.request)
    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "execute_control_intent", recorder.execute)
    monkeypatch.setattr(control_commands, "workflow_cancel_proof", lambda _conn, _run_id: dict(cancel_proof))
    import runtime.command_handlers as command_handlers
    monkeypatch.setattr(command_handlers, "workflow_cancel_proof", lambda _conn, _run_id: dict(cancel_proof))
    monkeypatch.setattr(
        unified_dispatch,
        "retry_job",
        _fail_direct_mutation("workflow retry should route through the command bus"),
    )
    monkeypatch.setattr(
        unified_dispatch,
        "cancel_run",
        _fail_direct_mutation("workflow cancel should route through the command bus"),
    )
    monkeypatch.setattr(
        "storage.postgres.connection.SyncPostgresConnection",
        lambda: _CancelConn(),
    )
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: _CancelConn())
    monkeypatch.setattr(
        mcp_workflow,
        "_subs",
        SimpleNamespace(get_pg_conn=lambda: object()),
    )

    chat_retry = chat_tools.execute_tool(
        "retry_job",
        {"run_id": "dispatch_chat_retry", "label": "build_a"},
        object(),
        "/repo",
    )
    chat_cancel = chat_tools.execute_tool(
        "cancel_workflow",
        {"run_id": "dispatch_chat_cancel"},
        object(),
        "/repo",
    )
    mcp_retry = mcp_workflow.tool_praxis_workflow(
        {"action": "retry", "run_id": "dispatch_mcp_retry", "label": "build_b"},
    )
    mcp_cancel = mcp_workflow.tool_praxis_workflow(
        {"action": "cancel", "run_id": "dispatch_mcp_cancel"},
    )
    mcp_repair = mcp_workflow.tool_praxis_workflow(
        {"action": "repair", "run_id": "dispatch_mcp_repair"},
    )
    api_cancel_response = rest.cancel_queue_job("42")
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    cli_retry_stdout = StringIO()
    with redirect_stdout(cli_retry_stdout):
        cli_retry_exit = workflow_cli.cmd_retry(
            SimpleNamespace(run_id="dispatch_cli_retry", label="build_c")
        )
    cli_retry = json.loads(cli_retry_stdout.getvalue())
    cli_stdout = StringIO()
    with redirect_stdout(cli_stdout):
        cli_exit = workflow_cli.cmd_cancel(SimpleNamespace(run_id="dispatch_cli_cancel"))
    cli_cancel = json.loads(cli_stdout.getvalue())
    cli_repair_stdout = StringIO()
    with redirect_stdout(cli_repair_stdout):
        cli_repair_exit = workflow_cli.cmd_repair(SimpleNamespace(run_id="dispatch_cli_repair"))
    cli_repair = json.loads(cli_repair_stdout.getvalue())
    legacy_retry_stdout = StringIO()
    legacy_retry_exit = workflow_commands._retry_command(
        ["dispatch_legacy_retry", "build_d"],
        stdout=legacy_retry_stdout,
    )
    legacy_retry = json.loads(legacy_retry_stdout.getvalue())
    legacy_stdout = StringIO()
    legacy_exit = workflow_commands._cancel_command(["dispatch_legacy_cancel"], stdout=legacy_stdout)
    legacy_cancel = json.loads(legacy_stdout.getvalue())
    legacy_repair_stdout = StringIO()
    legacy_repair_exit = workflow_commands._repair_command(["dispatch_legacy_repair"], stdout=legacy_repair_stdout)
    legacy_repair = json.loads(legacy_repair_stdout.getvalue())

    assert chat_retry["data"]["status"] == "approval_required"
    assert chat_cancel["data"]["status"] == "approval_required"
    assert mcp_retry["status"] == "requeued"
    assert mcp_retry["label"] == "build_b"
    assert mcp_retry["approval_required"] is False
    assert mcp_retry["command_id"] == "control.command.execute.1"
    assert mcp_retry["command_status"] == "succeeded"
    assert mcp_retry["run_id"] == "dispatch_mcp_retry"
    assert mcp_retry["result_ref"] == "workflow_run:dispatch_mcp_retry"
    assert mcp_cancel["status"] == "cancelled"
    assert mcp_cancel["approval_required"] is False
    assert mcp_cancel["command_id"] == "control.command.execute.2"
    assert mcp_cancel["run_id"] == "dispatch_mcp_cancel"
    assert mcp_cancel["result_ref"] == "workflow_run:dispatch_mcp_cancel"
    assert mcp_cancel["cancelled_jobs"] == 1
    assert mcp_cancel["labels"] == ["build_a"]
    assert mcp_cancel["run_status"] == "cancelled"
    assert mcp_cancel["terminal_reason"] == "workflow_cancelled"
    assert mcp_repair["status"] == "repaired"
    assert mcp_repair["approval_required"] is False
    assert mcp_repair["command_id"] == "control.command.execute.3"
    assert mcp_repair["run_id"] == "dispatch_mcp_repair"
    assert mcp_repair["result_ref"] == "workflow_run:dispatch_mcp_repair"
    assert api_cancel_response.status_code == 200
    api_cancel = json.loads(api_cancel_response.body)
    assert cli_retry_exit == 0
    assert cli_retry == {
        "run_id": "dispatch_cli_retry",
        "label": "build_c",
        "status": "requeued",
        "approval_required": False,
        "command_id": "control.command.execute.5",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_cli_retry",
        "stream_url": "/api/workflow-runs/dispatch_cli_retry/stream",
        "status_url": "/api/workflow-runs/dispatch_cli_retry/status",
    }
    assert api_cancel == {
        "job_id": "42",
        "run_id": "dispatch_http_cancel",
        "status": "cancelled",
        "approval_required": False,
        "command_id": "control.command.execute.4",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_http_cancel",
        "stream_url": "/api/workflow-runs/dispatch_http_cancel/stream",
        "status_url": "/api/workflow-runs/dispatch_http_cancel/status",
        "cancelled_jobs": 1,
        "labels": ["build_a"],
        "run_status": "cancelled",
        "terminal_reason": "workflow_cancelled",
    }
    assert cli_exit == 0
    assert cli_cancel == {
        "run_id": "dispatch_cli_cancel",
        "status": "cancelled",
        "approval_required": False,
        "command_id": "control.command.execute.6",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_cli_cancel",
        "stream_url": "/api/workflow-runs/dispatch_cli_cancel/stream",
        "status_url": "/api/workflow-runs/dispatch_cli_cancel/status",
        "cancelled_jobs": 1,
        "labels": ["build_a"],
        "run_status": "cancelled",
        "terminal_reason": "workflow_cancelled",
    }
    assert cli_repair_exit == 0
    assert cli_repair == {
        "run_id": "dispatch_cli_repair",
        "status": "repaired",
        "approval_required": False,
        "command_id": "control.command.execute.7",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_cli_repair",
        "stream_url": "/api/workflow-runs/dispatch_cli_repair/stream",
        "status_url": "/api/workflow-runs/dispatch_cli_repair/status",
    }
    assert legacy_retry_exit == 0
    assert legacy_retry == {
        "run_id": "dispatch_legacy_retry",
        "label": "build_d",
        "status": "requeued",
        "approval_required": False,
        "command_id": "control.command.execute.8",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_legacy_retry",
        "stream_url": "/api/workflow-runs/dispatch_legacy_retry/stream",
        "status_url": "/api/workflow-runs/dispatch_legacy_retry/status",
    }
    assert legacy_exit == 0
    assert legacy_cancel == {
        "run_id": "dispatch_legacy_cancel",
        "status": "cancelled",
        "approval_required": False,
        "command_id": "control.command.execute.9",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_legacy_cancel",
        "stream_url": "/api/workflow-runs/dispatch_legacy_cancel/stream",
        "status_url": "/api/workflow-runs/dispatch_legacy_cancel/status",
        "cancelled_jobs": 1,
        "labels": ["build_a"],
        "run_status": "cancelled",
        "terminal_reason": "workflow_cancelled",
    }
    assert legacy_repair_exit == 0
    assert legacy_repair == {
        "run_id": "dispatch_legacy_repair",
        "status": "repaired",
        "approval_required": False,
        "command_id": "control.command.execute.10",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_legacy_repair",
        "stream_url": "/api/workflow-runs/dispatch_legacy_repair/stream",
        "status_url": "/api/workflow-runs/dispatch_legacy_repair/status",
    }

    assert recorder.request_calls == [
        {
            "command_type": "workflow.retry",
            "requested_by_kind": "chat",
            "requested_by_ref": "chat.workspace",
            "idempotency_key": recorder.request_calls[0]["idempotency_key"],
            "payload": {"run_id": "dispatch_chat_retry", "label": "build_a"},
        },
        {
            "command_type": "workflow.cancel",
            "requested_by_kind": "chat",
            "requested_by_ref": "chat.workspace",
            "idempotency_key": recorder.request_calls[1]["idempotency_key"],
            "payload": {"run_id": "dispatch_chat_cancel", "include_running": True},
        },
    ]

    assert recorder.execute_calls == [
        {
            "command_type": "workflow.retry",
            "requested_by_kind": "mcp",
            "requested_by_ref": "praxis_workflow.retry",
            "approved_by": "mcp.praxis_workflow.retry",
            "idempotency_key": recorder.execute_calls[0]["idempotency_key"],
            "payload": {"run_id": "dispatch_mcp_retry", "label": "build_b"},
        },
        {
            "command_type": "workflow.cancel",
            "requested_by_kind": "mcp",
            "requested_by_ref": "praxis_workflow.cancel",
            "approved_by": "mcp.praxis_workflow.cancel",
            "idempotency_key": recorder.execute_calls[1]["idempotency_key"],
            "payload": {"run_id": "dispatch_mcp_cancel", "include_running": True},
        },
        {
            "command_type": "sync.repair",
            "requested_by_kind": "mcp",
            "requested_by_ref": "praxis_workflow.repair",
            "approved_by": "mcp.praxis_workflow.repair",
            "idempotency_key": recorder.execute_calls[2]["idempotency_key"],
            "payload": {"run_id": "dispatch_mcp_repair"},
        },
        {
            "command_type": "workflow.cancel",
            "requested_by_kind": "http",
            "requested_by_ref": "queue_cancel",
            "approved_by": "http.queue_cancel",
            "idempotency_key": "workflow.cancel.http.42",
            "payload": {"run_id": "dispatch_http_cancel", "include_running": True},
        },
        {
            "command_type": "workflow.retry",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.retry",
            "approved_by": "cli.workflow.retry",
            "idempotency_key": "workflow.retry.cli.dispatch_cli_retry.build_c",
            "payload": {"run_id": "dispatch_cli_retry", "label": "build_c"},
        },
        {
            "command_type": "workflow.cancel",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.cancel",
            "approved_by": "cli.workflow.cancel",
            "idempotency_key": "workflow.cancel.cli.dispatch_cli_cancel",
            "payload": {"run_id": "dispatch_cli_cancel", "include_running": True},
        },
        {
            "command_type": "sync.repair",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.repair",
            "approved_by": "cli.workflow.repair",
            "idempotency_key": "sync.repair.cli.dispatch_cli_repair",
            "payload": {"run_id": "dispatch_cli_repair"},
        },
        {
            "command_type": "workflow.retry",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.retry",
            "approved_by": "cli.workflow.retry",
            "idempotency_key": "workflow.retry.cli.dispatch_legacy_retry.build_d",
            "payload": {"run_id": "dispatch_legacy_retry", "label": "build_d"},
        },
        {
            "command_type": "workflow.cancel",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.cancel",
            "approved_by": "cli.workflow.cancel",
            "idempotency_key": "workflow.cancel.cli.dispatch_legacy_cancel",
            "payload": {"run_id": "dispatch_legacy_cancel", "include_running": True},
        },
        {
            "command_type": "sync.repair",
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.repair",
            "approved_by": "cli.workflow.repair",
            "idempotency_key": "sync.repair.cli.dispatch_legacy_repair",
            "payload": {"run_id": "dispatch_legacy_repair"},
        },
    ]


def test_cancel_surfaces_fail_closed_when_durable_cancel_proof_disagrees(monkeypatch) -> None:
    class _DisputedCancelConn:
        def execute(self, query: str, *params: Any):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT run_id FROM workflow_jobs"):
                assert params == ("42",)
                return [{"run_id": "dispatch_cancel_disputed"}]
            if "UPDATE workflow_jobs" in normalized:
                raise AssertionError("API cancel should not mutate workflow_jobs directly")
            raise AssertionError(f"unexpected query: {query}")

    command = _fake_command(
        command_id="control.command.cancel.shared",
        command_type="workflow.cancel",
        command_status="succeeded",
        requested_by_kind="shared",
        requested_by_ref="shared",
        idempotency_key="workflow.cancel.shared",
        payload={"run_id": "dispatch_cancel_disputed", "include_running": True},
        result_ref="workflow_run:dispatch_cancel_disputed",
    )
    cancel_proof = {
        "cancelled_jobs": 0,
        "labels": [],
        "run_status": "running",
        "terminal_reason": None,
    }

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "execute_control_intent", lambda *_args, **_kwargs: command)
    monkeypatch.setattr(control_commands, "workflow_cancel_proof", lambda _conn, _run_id: dict(cancel_proof))
    monkeypatch.setattr(
        unified_dispatch,
        "cancel_run",
        _fail_direct_mutation("workflow cancel should not mutate directly when proof disagrees"),
    )
    monkeypatch.setattr(
        "storage.postgres.connection.SyncPostgresConnection",
        lambda: _DisputedCancelConn(),
    )
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: _DisputedCancelConn())
    monkeypatch.setattr(
        mcp_workflow,
        "_subs",
        SimpleNamespace(get_pg_conn=lambda: object()),
    )
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())

    mcp_cancel = mcp_workflow.tool_praxis_workflow(
        {"action": "cancel", "run_id": "dispatch_cancel_disputed"},
    )
    api_cancel_response = rest.cancel_queue_job("42")
    api_cancel = json.loads(api_cancel_response.body)
    cli_stdout = StringIO()
    with redirect_stdout(cli_stdout):
        cli_exit = workflow_cli.cmd_cancel(SimpleNamespace(run_id="dispatch_cancel_disputed"))
    cli_cancel = json.loads(cli_stdout.getvalue())

    expected = {
        "run_id": "dispatch_cancel_disputed",
        "status": "failed",
        "approval_required": False,
        "command_id": "control.command.cancel.shared",
        "command_status": "succeeded",
        "result_ref": "workflow_run:dispatch_cancel_disputed",
        "stream_url": "/api/workflow-runs/dispatch_cancel_disputed/stream",
        "status_url": "/api/workflow-runs/dispatch_cancel_disputed/status",
        "error": "workflow cancel did not reach the cancelled state",
        "error_code": "control.command.workflow_cancel_incomplete",
        "error_detail": "workflow cancel did not reach the cancelled state",
        "command": command.to_json(),
        "cancelled_jobs": 0,
        "labels": [],
        "run_status": "running",
        "terminal_reason": None,
        "proof": cancel_proof,
    }

    assert mcp_cancel == expected
    assert api_cancel_response.status_code == 409
    assert api_cancel == {**expected, "job_id": "42"}
    assert cli_exit == 1
    assert cli_cancel == expected


def test_legacy_cli_cancel_frontdoor_delegates_to_the_bus_backed_cli(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_cmd_cancel(args):
        captured["run_id"] = args.run_id
        print(
            json.dumps(
                {
                    "run_id": args.run_id,
                    "status": "cancelled",
                    "approval_required": False,
                    "command_id": "control.command.execute.fake",
                    "command_status": "succeeded",
                },
                indent=2,
            )
        )
        return 0

    monkeypatch.setattr(workflow_cli, "cmd_cancel", _fake_cmd_cancel)

    stdout = StringIO()
    exit_code = workflow_commands._cancel_command(["dispatch_legacy_cancel"], stdout=stdout)
    result = json.loads(stdout.getvalue())

    assert exit_code == 0
    assert captured["run_id"] == "dispatch_legacy_cancel"
    assert result == {
        "run_id": "dispatch_legacy_cancel",
        "status": "cancelled",
        "approval_required": False,
        "command_id": "control.command.execute.fake",
        "command_status": "succeeded",
    }


def test_legacy_cli_retry_frontdoor_delegates_to_the_bus_backed_cli(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_cmd_retry(args):
        captured["run_id"] = args.run_id
        captured["label"] = args.label
        print(
            json.dumps(
                {
                    "run_id": args.run_id,
                    "label": args.label,
                    "status": "requeued",
                    "approval_required": False,
                    "command_id": "control.command.execute.fake",
                    "command_status": "succeeded",
                },
                indent=2,
            )
        )
        return 0

    monkeypatch.setattr(workflow_cli, "cmd_retry", _fake_cmd_retry)

    stdout = StringIO()
    exit_code = workflow_commands._retry_command(["dispatch_legacy_retry", "build_x"], stdout=stdout)
    result = json.loads(stdout.getvalue())

    assert exit_code == 0
    assert captured["run_id"] == "dispatch_legacy_retry"
    assert captured["label"] == "build_x"
    assert result == {
        "run_id": "dispatch_legacy_retry",
        "label": "build_x",
        "status": "requeued",
        "approval_required": False,
        "command_id": "control.command.execute.fake",
        "command_status": "succeeded",
    }
