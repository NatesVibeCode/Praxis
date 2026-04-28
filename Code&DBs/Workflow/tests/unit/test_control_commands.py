from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import runtime.control_commands as control_commands


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.control_commands_by_id: dict[str, dict[str, object]] = {}
        self.control_commands_by_key: dict[str, dict[str, object]] = {}
        self.system_events: list[dict[str, object]] = []
        self.script_calls: list[str] = []
        self.workflow_runs: dict[str, dict[str, object]] = {}
        self.workflow_jobs: dict[tuple[str, str], dict[str, object]] = {
            ("run-7", "build_a"): {
                "id": 17,
                "run_id": "run-7",
                "label": "build_a",
                "status": "failed",
                "attempt": 2,
            }
        }

    def execute_script(self, sql: str) -> None:
        self.script_calls.append(sql)

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())

        if "INSERT INTO control_commands" in normalized:
            return self._insert_control_command(args)
        if "UPDATE control_commands" in normalized and "RETURNING command_id, command_type" in normalized:
            return self._update_control_command(args)
        if "FROM control_commands WHERE command_id = $1 LIMIT 1" in normalized:
            row = self.control_commands_by_id.get(str(args[0]))
            return [dict(row)] if row else []
        if "FROM control_commands WHERE idempotency_key = $1 LIMIT 1" in normalized:
            row = self.control_commands_by_key.get(str(args[0]))
            return [dict(row)] if row else []
        if normalized.startswith("SELECT id, run_id, label, status, attempt FROM workflow_jobs"):
            row = self.workflow_jobs.get((str(args[0]), str(args[1])))
            return [dict(row)] if row else []
        if normalized.startswith("SELECT * FROM workflow_runs WHERE run_id = $1"):
            row = self.workflow_runs.get(str(args[0]))
            return [dict(row)] if row else []
        if "FROM control_commands" in normalized and normalized.startswith("SELECT command_id, command_type, command_status"):
            return self._list_control_commands(normalized, args)
        if "INSERT INTO system_events" in normalized:
            payload = args[3]
            if isinstance(payload, str):
                payload = json.loads(payload)
            self.system_events.append(
                {
                    "event_type": args[0],
                    "source_id": args[1],
                    "source_type": args[2],
                    "payload": payload,
                }
            )
            return []

        return []

    def _insert_control_command(self, args):
        row = {
            "command_id": args[0],
            "command_type": args[1],
            "command_status": args[2],
            "requested_by_kind": args[3],
            "requested_by_ref": args[4],
            "requested_at": args[5],
            "approved_at": args[6],
            "approved_by": args[7],
            "idempotency_key": args[8],
            "risk_level": args[9],
            "payload": json.loads(args[10]) if isinstance(args[10], str) else args[10],
            "result_ref": args[11],
            "error_code": args[12],
            "error_detail": args[13],
            "created_at": args[14],
            "updated_at": args[15],
        }
        existing = self.control_commands_by_key.get(str(row["idempotency_key"]))
        if existing is not None:
            return []
        self.control_commands_by_id[str(row["command_id"])] = row
        self.control_commands_by_key[str(row["idempotency_key"])] = row
        return [dict(row)]

    def _update_control_command(self, args):
        command_id = str(args[0])
        row = self.control_commands_by_id.get(command_id)
        if row is None:
            return []
        row = dict(row)
        row["command_status"] = args[1]
        row["approved_at"] = args[2]
        row["approved_by"] = args[3]
        row["payload"] = json.loads(args[4]) if isinstance(args[4], str) else args[4]
        row["result_ref"] = args[5]
        row["error_code"] = args[6]
        row["error_detail"] = args[7]
        row["updated_at"] = datetime(2026, 4, 8, 12, 30, tzinfo=timezone.utc)
        self.control_commands_by_id[command_id] = row
        self.control_commands_by_key[str(row["idempotency_key"])] = row
        return [dict(row)]

    def _list_control_commands(self, normalized: str, args: tuple[object, ...]):
        rows = list(self.control_commands_by_id.values())
        idx = 0
        if "command_type = $1" in normalized:
            rows = [row for row in rows if row["command_type"] == args[idx]]
            idx += 1
        if "command_status = $" in normalized:
            rows = [row for row in rows if row["command_status"] == args[idx]]
            idx += 1
        if "requested_by_kind = $" in normalized:
            rows = [row for row in rows if row["requested_by_kind"] == args[idx]]
            idx += 1
        rows.sort(
            key=lambda row: (
                row["requested_at"],
                row["created_at"],
                row["command_id"],
            ),
            reverse=True,
        )
        limit = int(args[idx]) if idx < len(args) else len(rows)
        return [dict(row) for row in rows[:limit]]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)


def _submit_intent(*, risk_level: str | None = None) -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        idempotency_key="idem.submit.1",
        risk_level=risk_level,
        payload={"repo_root": "/repo", "spec_path": "spec.queue.json"},
    )


def _api_submit_intent() -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="http",
        requested_by_ref="api.workflow_run",
        idempotency_key="idem.submit.api.1",
        payload={"repo_root": "/repo", "spec_path": "spec.queue.json"},
    )


def _mcp_submit_intent() -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="mcp",
        requested_by_ref="praxis_workflow.run",
        idempotency_key="idem.submit.mcp.1",
        payload={"repo_root": "/repo", "spec_path": "spec.queue.json"},
    )


def _inline_submit_intent() -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="chat",
        requested_by_ref="chat.workspace",
        idempotency_key="idem.submit.inline.1",
        payload={
            "spec": {
                "name": "chat inline",
                "objective": "Run the thing",
                "outcome_goal": "Run the thing",
                "phase": "build",
                "jobs": [
                    {
                        "label": "job-a",
                        "agent": "openai/gpt-5.4-mini",
                        "prompt": "Run the thing.",
                    }
                ],
            }
        },
    )


def _retry_intent() -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_RETRY,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        idempotency_key="idem.retry.1",
        payload={
            "run_id": "run-7",
            "label": "build_a",
            "previous_failure": "receipt:run-7:build_a:2 failed with provider.capacity",
            "retry_delta": "retry after provider slot repair",
            "retry_guard": {
                "run_id": "run-7",
                "label": "build_a",
                "job_id": 17,
                "status": "failed",
                "attempt": 2,
            },
        },
    )


def _cancel_intent() -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_CANCEL,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        idempotency_key="idem.cancel.1",
        payload={"run_id": "run-7"},
    )


def _repair_intent() -> control_commands.ControlIntent:
    return control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.SYNC_REPAIR,
        requested_by_kind="system",
        requested_by_ref="system.scheduler",
        idempotency_key="idem.repair.1",
        payload={"run_id": "run-9"},
    )


def test_control_command_lifecycle_transitions_and_cancel_handler(monkeypatch):
    conn = _FakeConn()
    intent = _cancel_intent()

    called: list[tuple[str, str, bool]] = []

    def _cancel_run(_conn, run_id, *, include_running=False):
        called.append(("cancel", run_id, include_running))
        return {
            "run_id": run_id,
            "cancelled_jobs": 2,
            "labels": ["build_a", "build_b"],
            "run_status": "cancelled",
        }

    monkeypatch.setattr(control_commands.unified_dispatch, "cancel_run", _cancel_run)

    created = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.cancel.1",
        requested_at=_fixed_clock(),
        auto_execute=False,
    )
    approved = control_commands.accept_control_command(
        conn,
        created.command_id,
        approved_by="operator.console",
        approved_at=_fixed_clock(),
    )
    final = control_commands.execute_control_command(conn, approved.command_id)
    loaded = control_commands.load_control_command(conn, created.command_id)

    assert called == [("cancel", "run-7", True)]
    assert created.command_status == "requested"
    assert approved.command_status == "accepted"
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run:run-7"
    assert loaded is not None
    assert loaded.command_status == "succeeded"
    assert loaded.result_ref == "workflow_run:run-7"
    assert loaded.error_code is None
    assert loaded.error_detail is None
    assert [event["event_type"] for event in conn.system_events] == [
        "control.command.requested",
        "control.command.accepted",
        "control.command.started",
        "control.command.completed",
    ]


def test_control_command_idempotency_replays_existing_row_and_conflicts() -> None:
    conn = _FakeConn()
    intent = _submit_intent()

    first = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.1",
        requested_at=_fixed_clock(),
        auto_execute=False,
    )
    second = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.2",
        requested_at=_fixed_clock(),
        auto_execute=False,
    )

    assert first.command_id == "control.command.submit.1"
    assert second.command_id == first.command_id

    conflicting_intent = control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        idempotency_key=intent.idempotency_key,
        payload={"repo_root": "/repo", "spec_path": "other.queue.json"},
    )

    with pytest.raises(control_commands.ControlCommandIdempotencyConflict) as exc_info:
        control_commands.create_control_command(
            conn,
            conflicting_intent,
            command_id="control.command.submit.3",
            requested_at=_fixed_clock(),
            auto_execute=False,
        )

    assert exc_info.value.idempotency_key == intent.idempotency_key
    assert exc_info.value.existing_command_id == first.command_id


def test_request_workflow_submit_command_persists_lineage_payload() -> None:
    conn = _FakeConn()

    command = control_commands.request_workflow_submit_command(
        conn,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        spec_path="spec.queue.json",
        repo_root="/repo",
        run_id="run-child",
        parent_run_id="run-parent",
        parent_job_label="phase.dispatch",
        dispatch_reason="phase.spawn",
        lineage_depth=2,
        idempotency_key="idem.submit.lineage.1",
    )

    payload = command.payload
    assert payload["run_id"] == "run-child"
    assert payload["parent_run_id"] == "run-parent"
    assert payload["parent_job_label"] == "phase.dispatch"
    assert payload["dispatch_reason"] == "phase.spawn"
    assert payload["lineage_depth"] == 2
    assert len([event for event in conn.system_events if event["event_type"] == "control.command.requested"]) == 1


def test_unified_dispatch_proxy_deletes_override_without_loading_module() -> None:
    def _handler(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

    setattr(control_commands.unified_dispatch, "submit_workflow", _handler)
    assert control_commands.unified_dispatch.submit_workflow is _handler

    delattr(control_commands.unified_dispatch, "submit_workflow")

    missing = control_commands.unified_dispatch.submit_workflow
    assert isinstance(
        missing,
        control_commands._LazyUnifiedDispatchProxy._MissingDispatchAttribute,
    )


def test_request_workflow_submit_command_bootstraps_and_shapes_intent(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(control_conn, intent, **kwargs):
        captured["conn"] = control_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.submit.99",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:run-99",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.submit.99",
                "command_status": "succeeded",
                "result_ref": "workflow_run:run-99",
            },
        )

    monkeypatch.setattr(control_commands, "request_control_command", _request)

    command = control_commands.request_workflow_submit_command(
        conn,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        spec_path="spec.queue.json",
        repo_root="/repo",
    )

    assert captured["conn"] is conn
    intent = captured["intent"]
    assert intent.command_type == control_commands.ControlCommandType.WORKFLOW_SUBMIT
    assert intent.requested_by_kind == "operator"
    assert intent.requested_by_ref == "operator.console"
    assert intent.payload == {"spec_path": "spec.queue.json", "repo_root": "/repo"}
    assert str(intent.idempotency_key).startswith("workflow.submit.operator.")
    assert captured["kwargs"] == {"command_id": None, "requested_at": None}
    assert command.result_ref == "workflow_run:run-99"


def test_request_workflow_submit_command_preserves_preassigned_run_id(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(control_conn, intent, **kwargs):
        captured["conn"] = control_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.submit.100",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:dispatch_preassigned",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.submit.100",
                "command_status": "succeeded",
                "result_ref": "workflow_run:dispatch_preassigned",
            },
        )

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", _request)

    control_commands.request_workflow_submit_command(
        conn,
        requested_by_kind="cli",
        requested_by_ref="workflow_cli.run",
        spec_path="spec.queue.json",
        repo_root="/repo",
        run_id="dispatch_preassigned",
    )

    assert captured["conn"] is conn
    assert captured["kwargs"] == {"command_id": None, "requested_at": None}
    intent = captured["intent"]
    assert intent.payload == {
        "spec_path": "spec.queue.json",
        "repo_root": "/repo",
        "run_id": "dispatch_preassigned",
    }


def test_request_workflow_submit_command_records_force_fresh_run(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(control_conn, intent, **kwargs):
        captured["conn"] = control_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.submit.100b",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:dispatch_fresh",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.submit.100b",
                "command_status": "succeeded",
                "result_ref": "workflow_run:dispatch_fresh",
            },
        )

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", _request)

    control_commands.request_workflow_submit_command(
        conn,
        requested_by_kind="cli",
        requested_by_ref="workflow_cli.run",
        spec_path="spec.queue.json",
        repo_root="/repo",
        force_fresh_run=True,
    )

    assert captured["conn"] is conn
    intent = captured["intent"]
    assert intent.payload == {
        "spec_path": "spec.queue.json",
        "repo_root": "/repo",
        "force_fresh_run": True,
    }


def test_request_workflow_submit_command_preserves_explicit_idempotency_key(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(control_conn, intent, **kwargs):
        captured["conn"] = control_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.submit.101",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:dispatch_explicit",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.submit.101",
                "command_status": "succeeded",
                "result_ref": "workflow_run:dispatch_explicit",
            },
        )

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", _request)

    control_commands.request_workflow_submit_command(
        conn,
        requested_by_kind="system",
        requested_by_ref="workflow_chain:workflow_chain_123",
        spec_path="spec.queue.json",
        repo_root="/repo",
        run_id="dispatch_explicit",
        idempotency_key="workflow.chain.submit.workflow_chain_123.wave_a.001",
    )

    assert captured["conn"] is conn
    intent = captured["intent"]
    assert intent.idempotency_key == "workflow.chain.submit.workflow_chain_123.wave_a.001"


def test_request_workflow_submit_command_shapes_inline_spec_intent(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(control_conn, intent, **kwargs):
        captured["conn"] = control_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.submit.102",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:dispatch_inline",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.submit.102",
                "command_status": "succeeded",
                "result_ref": "workflow_run:dispatch_inline",
            },
        )

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", _request)

    control_commands.request_workflow_submit_command(
        conn,
        requested_by_kind="http",
        requested_by_ref="queue_submit",
        inline_spec={
            "name": "Queue Report",
            "phase": "build",
            "jobs": [{"label": "build_a", "agent": "auto/build", "prompt": "Build it."}],
        },
        run_id="dispatch_inline",
    )

    assert captured["conn"] is conn
    intent = captured["intent"]
    assert intent.payload == {
        "inline_spec": {
            "name": "Queue Report",
            "phase": "build",
            "jobs": [{"label": "build_a", "agent": "auto/build", "prompt": "Build it."}],
        },
        "run_id": "dispatch_inline",
    }
    assert captured["kwargs"] == {"command_id": None, "requested_at": None}


def test_request_workflow_spawn_command_shapes_intent(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(control_conn, intent, **kwargs):
        captured["conn"] = control_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.spawn.102",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_run:dispatch_spawn",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.spawn.102",
                "command_status": "succeeded",
                "result_ref": "workflow_run:dispatch_spawn",
            },
        )

    monkeypatch.setattr(control_commands, "bootstrap_control_commands_schema", lambda _conn: None)
    monkeypatch.setattr(control_commands, "request_control_command", _request)

    control_commands.request_workflow_spawn_command(
        conn,
        requested_by_kind="cli",
        requested_by_ref="workflow_cli.spawn",
        parent_run_id="run-parent",
        parent_job_label="phase.dispatch",
        dispatch_reason="phase.spawn",
        spec_path="child.queue.json",
        repo_root="/repo",
        lineage_depth=2,
    )

    assert captured["conn"] is conn
    intent = captured["intent"]
    assert intent.command_type == control_commands.ControlCommandType.WORKFLOW_SPAWN
    assert intent.payload == {
        "parent_run_id": "run-parent",
        "dispatch_reason": "phase.spawn",
        "parent_job_label": "phase.dispatch",
        "spec_path": "child.queue.json",
        "repo_root": "/repo",
        "lineage_depth": 2,
    }
    assert str(intent.idempotency_key).startswith("workflow.spawn.cli.")
    assert captured["kwargs"] == {"command_id": None, "requested_at": None}


def test_request_workflow_chain_submit_command_bootstraps_and_shapes_intent(monkeypatch):
    conn = _FakeConn()
    captured: dict[str, object] = {}

    def _request(chain_conn, intent, **kwargs):
        captured["conn"] = chain_conn
        captured["intent"] = intent
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            command_id="control.command.chain.submit.99",
            command_status="succeeded",
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload=dict(intent.payload),
            result_ref="workflow_chain:workflow_chain_99",
            error_code=None,
            error_detail=None,
            to_json=lambda: {
                "command_id": "control.command.chain.submit.99",
                "command_status": "succeeded",
                "result_ref": "workflow_chain:workflow_chain_99",
            },
        )

    monkeypatch.setattr(control_commands, "request_control_command", _request)

    command = control_commands.request_workflow_chain_submit_command(
        conn,
        requested_by_kind="cli",
        requested_by_ref="workflow_cli.chain",
        coordination_path="config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
        repo_root="/repo",
        adopt_active=False,
    )

    assert captured["conn"] is conn
    intent = captured["intent"]
    assert intent.command_type == control_commands.ControlCommandType.WORKFLOW_CHAIN_SUBMIT
    assert intent.requested_by_kind == "cli"
    assert intent.requested_by_ref == "workflow_cli.chain"
    assert intent.payload == {
        "coordination_path": "config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
        "repo_root": "/repo",
        "adopt_active": False,
    }
    assert str(intent.idempotency_key).startswith("workflow.chain.submit.cli.")
    assert captured["kwargs"] == {"command_id": None, "requested_at": None}
    assert command.result_ref == "workflow_chain:workflow_chain_99"


@pytest.mark.parametrize(
    "intent_factory, expected_safe",
    [
        (_submit_intent, True),
        (_api_submit_intent, True),
        (_mcp_submit_intent, True),
        (_cancel_intent, False),
    ],
)
def test_control_command_policy_helpers(intent_factory, expected_safe):
    intent = intent_factory()

    assert control_commands.is_safe_to_auto_execute(intent) is expected_safe
    assert control_commands.requires_confirmation(intent) is (not expected_safe)
    assert control_commands.classify_control_intent(intent).auto_execute is expected_safe


@pytest.mark.parametrize(
    "intent_factory, handler_attr, handler_return, command_id, expected_result_ref",
    [
        (
            _retry_intent,
            "retry_job",
            {"run_id": "run-7", "label": "build_a", "status": "requeued"},
            "control.command.retry.1",
            "workflow_run:run-7",
        ),
        (
            _cancel_intent,
            "cancel_run",
            {"run_id": "run-7", "cancelled_jobs": 1, "labels": ["build_a"], "run_status": "cancelled"},
            "control.command.cancel.2",
            "workflow_run:run-7",
        ),
    ],
)
def test_control_command_confirmation_required_handlers_execute_after_approval(
    monkeypatch,
    intent_factory,
    handler_attr,
    handler_return,
    command_id,
    expected_result_ref,
):
    conn = _FakeConn()
    intent = intent_factory()
    called: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _handler(*args, **kwargs):
        called.append((args, kwargs))
        return handler_return

    monkeypatch.setattr(control_commands.unified_dispatch, handler_attr, _handler)

    requested = control_commands.create_control_command(
        conn,
        intent,
        command_id=command_id,
        requested_at=_fixed_clock(),
        auto_execute=True,
    )
    assert requested.command_status == "requested"
    assert called == []

    approved = control_commands.accept_control_command(
        conn,
        requested.command_id,
        approved_by="operator.console",
        approved_at=_fixed_clock(),
    )
    final = control_commands.execute_control_command(conn, approved.command_id)
    loaded = control_commands.load_control_command(conn, approved.command_id)

    assert approved.command_status == "accepted"
    assert final.command_status == "succeeded"
    assert final.result_ref == expected_result_ref
    assert loaded is not None
    assert loaded.result_ref == expected_result_ref
    assert loaded.error_code is None
    assert loaded.error_detail is None
    assert called
    if handler_attr == "cancel_run":
        assert called == [((conn, "run-7"), {"include_running": True})]
    else:
        assert called == [((conn, "run-7", "build_a"), {})]


def test_workflow_retry_idempotency_key_tracks_current_job_attempt() -> None:
    conn = _FakeConn()

    payload_a = control_commands.workflow_retry_payload_with_guard(
        conn,
        {
            "run_id": "run-7",
            "label": "build_a",
            "previous_failure": "receipt:run-7:build_a:2 failed with provider.capacity",
            "retry_delta": "retry after provider slot repair",
        },
    )
    key_a = control_commands.workflow_retry_idempotency_key(
        requested_by_kind="cli",
        payload=payload_a,
    )

    conn.workflow_jobs[("run-7", "build_a")]["status"] = "cancelled"
    conn.workflow_jobs[("run-7", "build_a")]["attempt"] = 3
    payload_b = control_commands.workflow_retry_payload_with_guard(
        conn,
        {
            "run_id": "run-7",
            "label": "build_a",
            "previous_failure": "receipt:run-7:build_a:2 failed with provider.capacity",
            "retry_delta": "retry after provider slot repair",
        },
    )
    key_b = control_commands.workflow_retry_idempotency_key(
        requested_by_kind="cli",
        payload=payload_b,
    )

    assert payload_a["retry_guard"]["status"] == "failed"
    assert payload_a["retry_guard"]["attempt"] == 2
    assert payload_b["retry_guard"]["status"] == "cancelled"
    assert payload_b["retry_guard"]["attempt"] == 3
    assert key_a != key_b
    assert key_a.startswith("workflow.retry.cli.run-7.failed.2.")
    assert key_b.startswith("workflow.retry.cli.run-7.cancelled.3.")


def test_workflow_retry_guard_falls_back_to_graph_evidence(monkeypatch) -> None:
    from runtime.workflow import _status as workflow_status

    conn = _FakeConn()
    conn.workflow_jobs.clear()
    conn.workflow_runs["run-graph"] = {
        "run_id": "run-graph",
        "current_state": "failed",
        "request_envelope": {"nodes": [], "edges": []},
    }

    monkeypatch.setattr(
        workflow_status,
        "_graph_job_rows_from_evidence",
        lambda *, run_row, run_id: [
            {
                "id": 2,
                "label": "graph_build",
                "status": "failed",
                "attempt": 1,
            }
        ],
    )

    payload = control_commands.workflow_retry_payload_with_guard(
        conn,
        {
            "run_id": "run-graph",
            "label": "graph_build",
            "previous_failure": "receipt:run-graph:16 failed with sandbox_error",
            "retry_delta": "retry after graph shard root repair",
        },
    )

    assert payload["retry_guard"] == {
        "run_id": "run-graph",
        "label": "graph_build",
        "job_id": 2,
        "status": "failed",
        "attempt": 1,
    }


def test_workflow_retry_intent_requires_failure_and_delta() -> None:
    with pytest.raises(control_commands.ControlCommandError) as exc_info:
        control_commands.ControlIntent(
            command_type=control_commands.ControlCommandType.WORKFLOW_RETRY,
            requested_by_kind="cli",
            requested_by_ref="workflow_cli.retry",
            idempotency_key="idem.retry.missing-explanation",
            payload={
                "run_id": "run-7",
                "label": "build_a",
                "retry_guard": {
                    "run_id": "run-7",
                    "label": "build_a",
                    "job_id": 17,
                    "status": "failed",
                    "attempt": 2,
                },
            },
        )

    assert exc_info.value.reason_code == "control.command.invalid_value"
    assert "payload.previous_failure" in str(exc_info.value)


def test_workflow_retry_command_fails_when_retry_guard_is_stale(monkeypatch) -> None:
    conn = _FakeConn()
    intent = _retry_intent()

    monkeypatch.setattr(
        control_commands.unified_dispatch,
        "retry_job",
        lambda *_args, **_kwargs: pytest.fail("stale retry guard must block retry execution"),
    )

    requested = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.retry.stale",
        requested_at=_fixed_clock(),
        auto_execute=False,
    )
    approved = control_commands.accept_control_command(
        conn,
        requested.command_id,
        approved_by="operator.console",
        approved_at=_fixed_clock(),
    )

    conn.workflow_jobs[("run-7", "build_a")]["status"] = "running"
    conn.workflow_jobs[("run-7", "build_a")]["attempt"] = 3

    final = control_commands.execute_control_command(conn, approved.command_id)
    loaded = control_commands.load_control_command(conn, approved.command_id)

    assert final.command_status == "failed"
    assert final.error_code == "control.command.workflow_retry_guard_stale"
    assert loaded is not None
    assert loaded.command_status == "failed"
    assert loaded.error_code == "control.command.workflow_retry_guard_stale"


def test_control_command_cancel_handler_fails_closed_when_run_state_is_not_cancelled(monkeypatch):
    conn = _FakeConn()
    intent = _cancel_intent()

    def _cancel_run(_conn, run_id, *, include_running=False):
        assert include_running is True
        return {
            "run_id": run_id,
            "cancelled_jobs": 1,
            "labels": ["build_a"],
            "run_status": "failed",
        }

    monkeypatch.setattr(control_commands.unified_dispatch, "cancel_run", _cancel_run)

    created = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.cancel.fail.closed.1",
        requested_at=_fixed_clock(),
        auto_execute=False,
    )
    approved = control_commands.accept_control_command(
        conn,
        created.command_id,
        approved_by="operator.console",
        approved_at=_fixed_clock(),
    )
    final = control_commands.execute_control_command(conn, approved.command_id)

    assert final.command_status == "failed"
    assert final.error_code == "control.command.workflow_cancel_incomplete"
    assert "cancelled state" in (final.error_detail or "")


def test_execute_control_intent_explicitly_approves_and_runs_confirm_required_handler(
    monkeypatch,
):
    conn = _FakeConn()
    intent = _cancel_intent()
    called: list[str] = []

    def _cancel_run(_conn, run_id, *, include_running=False):
        assert include_running is True
        called.append(run_id)
        return {
            "run_id": run_id,
            "cancelled_jobs": 1,
            "labels": ["build_a"],
            "run_status": "cancelled",
        }

    monkeypatch.setattr(control_commands.unified_dispatch, "cancel_run", _cancel_run)

    final = control_commands.execute_control_intent(
        conn,
        intent,
        approved_by="mcp.praxis_workflow",
        command_id="control.command.cancel.explicit.1",
        requested_at=_fixed_clock(),
        approved_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, final.command_id)

    assert called == ["run-7"]
    assert final.command_status == "succeeded"
    assert final.approved_by == "mcp.praxis_workflow"
    assert final.result_ref == "workflow_run:run-7"
    assert loaded is not None
    assert loaded.command_status == "succeeded"
    assert loaded.approved_by == "mcp.praxis_workflow"
    assert [event["event_type"] for event in conn.system_events] == [
        "control.command.requested",
        "control.command.accepted",
        "control.command.started",
        "control.command.completed",
    ]


def test_control_command_submit_auto_execute_persists_result_ref(monkeypatch):
    conn = _FakeConn()
    intent = _submit_intent()
    called: list[tuple[object, ...]] = []

    def _submit_workflow(
        _conn,
        spec_path,
        repo_root,
        run_id=None,
        force_fresh_run=False,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        lineage_depth=None,
    ):
        called.append((spec_path, repo_root, run_id))
        return {"run_id": "run-123", "status": "running", "spec_name": "spec"}

    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow", _submit_workflow)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.10",
        requested_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, final.command_id)

    assert called == [("spec.queue.json", "/repo", None)]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run:run-123"
    assert loaded is not None
    assert loaded.result_ref == "workflow_run:run-123"
    assert loaded.command_status == "succeeded"
    assert loaded.error_code is None
    assert loaded.error_detail is None


def test_control_command_spawn_auto_execute_persists_result_ref(monkeypatch):
    conn = _FakeConn()
    intent = control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SPAWN,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        idempotency_key="idem.spawn.1",
        payload={
            "repo_root": "/repo",
            "spec_path": "child.queue.json",
            "parent_run_id": "run-parent",
            "parent_job_label": "phase.dispatch",
            "dispatch_reason": "phase.spawn",
        },
    )
    called: list[tuple[object, ...]] = []

    def _submit_workflow(
        _conn,
        spec_path,
        repo_root,
        run_id=None,
        force_fresh_run=False,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        lineage_depth=None,
    ):
        called.append(
            (
                spec_path,
                repo_root,
                run_id,
                parent_run_id,
                parent_job_label,
                dispatch_reason,
                lineage_depth,
            )
        )
        return {"run_id": "run-spawn-123", "status": "running", "spec_name": "child"}

    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow", _submit_workflow)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.spawn.10",
        requested_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, final.command_id)

    assert called == [
        ("child.queue.json", "/repo", None, "run-parent", "phase.dispatch", "phase.spawn", None)
    ]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run:run-spawn-123"
    assert loaded is not None
    assert loaded.result_ref == "workflow_run:run-spawn-123"
    assert loaded.command_status == "succeeded"


def test_control_command_submit_auto_execute_passes_force_fresh_run(monkeypatch):
    conn = _FakeConn()
    intent = control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        idempotency_key="idem.submit.fresh.1",
        payload={
            "repo_root": "/repo",
            "spec_path": "spec.queue.json",
            "force_fresh_run": True,
        },
    )
    called: list[tuple[object, ...]] = []

    def _submit_workflow(
        _conn,
        spec_path,
        repo_root,
        run_id=None,
        force_fresh_run=False,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        lineage_depth=None,
    ):
        called.append((spec_path, repo_root, run_id, force_fresh_run))
        return {"run_id": "run-fresh-123", "status": "running", "spec_name": "spec"}

    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow", _submit_workflow)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.fresh.10",
        requested_at=_fixed_clock(),
    )

    assert called == [("spec.queue.json", "/repo", None, True)]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run:run-fresh-123"


def test_control_command_chain_submit_auto_execute_persists_result_ref(monkeypatch):
    conn = _FakeConn()
    intent = control_commands.ControlIntent(
        command_type=control_commands.ControlCommandType.WORKFLOW_CHAIN_SUBMIT,
        requested_by_kind="cli",
        requested_by_ref="workflow_cli.chain",
        idempotency_key="idem.chain.submit.1",
        payload={
            "coordination_path": "config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
            "repo_root": "/repo",
            "adopt_active": True,
        },
    )
    called: list[tuple[object, ...]] = []

    def _submit_workflow_chain(
        _conn,
        *,
        coordination_path,
        repo_root,
        requested_by_kind,
        requested_by_ref,
        adopt_active=True,
        chain_id=None,
        command_id=None,
    ):
        called.append(
            (
                coordination_path,
                repo_root,
                requested_by_kind,
                requested_by_ref,
                adopt_active,
                chain_id,
                command_id,
            )
        )
        return "workflow_chain_123"

    monkeypatch.setattr("runtime.workflow_chain.submit_workflow_chain", _submit_workflow_chain)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.chain.submit.10",
        requested_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, final.command_id)

    assert called == [
        (
            "config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
            "/repo",
            "cli",
            "workflow_cli.chain",
            True,
            None,
            "control.command.chain.submit.10",
        )
    ]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_chain:workflow_chain_123"
    assert loaded is not None
    assert loaded.result_ref == "workflow_chain:workflow_chain_123"
    assert loaded.command_status == "succeeded"
    assert loaded.error_code is None
    assert loaded.error_detail is None


def test_control_command_submit_saved_spec_path_never_falls_back_to_inline_submit(monkeypatch):
    conn = _FakeConn()
    intent = _submit_intent()
    called: list[tuple[object, ...]] = []

    def _submit_workflow(
        _conn,
        spec_path,
        repo_root,
        run_id=None,
        force_fresh_run=False,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        lineage_depth=None,
    ):
        called.append((spec_path, repo_root, run_id))
        return {"run_id": "run-124", "status": "running", "spec_name": "spec"}

    def _fail_inline_submit(*_args, **_kwargs):
        raise AssertionError("saved spec_path submit should not fall back to submit_workflow_inline")

    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow", _submit_workflow)
    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow_inline", _fail_inline_submit)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.10a",
        requested_at=_fixed_clock(),
    )

    assert called == [("spec.queue.json", "/repo", None)]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run:run-124"


def test_control_command_submit_failure_is_reported_with_explicit_reason(monkeypatch):
    conn = _FakeConn()
    intent = _submit_intent()

    def _submit_workflow(*_args, **_kwargs):
        raise RuntimeError("workflow submit failed closed while resolving write-scope authority: step compiler is unavailable")

    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow", _submit_workflow)

    failed = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.11",
        requested_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, failed.command_id)

    assert failed.command_status == "failed"
    assert failed.result_ref is None
    assert failed.error_code == "control.command.workflow_submit_failed"
    assert "write-scope authority" in (failed.error_detail or "")
    assert loaded is not None
    assert loaded.command_status == "failed"
    assert loaded.result_ref is None
    assert loaded.error_code == "control.command.workflow_submit_failed"
    assert "write-scope authority" in (loaded.error_detail or "")


def test_control_command_submit_failure_is_reported_when_submit_returns_failed_status(monkeypatch):
    conn = _FakeConn()
    intent = _submit_intent()

    monkeypatch.setattr(
        control_commands.unified_dispatch,
        "submit_workflow",
        lambda *_args, **_kwargs: {
            "run_id": "dispatch_failed_012",
            "status": "failed",
            "error": "persistent evidence proof append failed",
            "execution_mode": "graph_runtime",
        },
    )

    failed = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.11b",
        requested_at=_fixed_clock(),
    )

    assert failed.command_status == "failed"
    assert failed.result_ref == "workflow_run:dispatch_failed_012"
    assert failed.error_code == "control.command.workflow_submit_failed"
    assert failed.error_detail == "graph_runtime submit failed: persistent evidence proof append failed"


def test_render_workflow_submit_response_uses_canonical_async_envelope() -> None:
    command = SimpleNamespace(
        command_id="control.command.submit.12",
        command_status="succeeded",
        result_ref="workflow_run:dispatch_012",
        to_json=lambda: {
            "command_id": "control.command.submit.12",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_012",
        },
    )

    payload = control_commands.render_workflow_submit_response(
        command,
        spec_name="sample",
        total_jobs=3,
    )

    assert payload == {
        "run_id": "dispatch_012",
        "status": "queued",
        "spec_name": "sample",
        "total_jobs": 3,
        "command_id": "control.command.submit.12",
        "command_status": "succeeded",
        "approval_required": False,
        "stream_url": "/api/workflow-runs/dispatch_012/stream",
        "status_url": "/api/workflow-runs/dispatch_012/status",
        "result_ref": "workflow_run:dispatch_012",
    }


def test_submit_workflow_command_surfaces_live_run_metrics(monkeypatch) -> None:
    command = SimpleNamespace(
        command_id="control.command.submit.12b",
        command_status="succeeded",
        result_ref="workflow_run:dispatch_live_012",
        to_json=lambda: {
            "command_id": "control.command.submit.12b",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_live_012",
        },
    )

    monkeypatch.setattr(
        control_commands,
        "request_workflow_submit_command",
        lambda *_args, **_kwargs: command,
    )
    monkeypatch.setattr(
        control_commands,
        "_workflow_submit_run_snapshot",
        lambda *_args, **_kwargs: {
            "status": "claim_accepted",
            "completed_jobs": 0,
            "total_jobs": 2,
            "elapsed_seconds": 1.5,
            "total_cost_usd": 0.25,
            "total_duration_ms": 2300,
            "total_tokens_in": 123,
            "total_tokens_out": 45,
            "jobs": [
                {"status": "pending"},
                {"status": "pending"},
            ],
            "health": {"state": "healthy"},
        },
    )

    payload = control_commands.submit_workflow_command(
        _FakeConn(),
        requested_by_kind="cli",
        requested_by_ref="tests.control_commands",
        spec_path="spec.queue.json",
        repo_root="/repo",
        spec_name="sample",
        total_jobs=2,
    )

    assert payload["run_id"] == "dispatch_live_012"
    assert payload["status"] == "claim_accepted"
    assert payload["run_status"] == "claim_accepted"
    assert payload["status_source"] == "live_snapshot"
    assert payload["run_metrics"] == {
        "completed_jobs": 0,
        "total_jobs": 2,
        "elapsed_seconds": 1.5,
        "health_state": "healthy",
        "job_status_counts": {"pending": 2},
        "total_cost_usd": 0.25,
        "total_duration_ms": 2300,
        "total_tokens_in": 123,
        "total_tokens_out": 45,
    }


def test_workflow_submit_run_snapshot_waits_for_terminal_details(monkeypatch) -> None:
    snapshots = [
        {
            "status": "succeeded",
            "completed_jobs": 1,
            "total_jobs": 2,
            "jobs": [{"status": "succeeded"}, {"status": "pending"}],
            "health": {"state": "unknown"},
            "terminal_reason": "",
        },
        {
            "status": "succeeded",
            "completed_jobs": 2,
            "total_jobs": 2,
            "jobs": [{"status": "succeeded"}, {"status": "succeeded"}],
            "health": {"state": "healthy"},
            "terminal_reason": "runtime.workflow_succeeded",
        },
    ]
    sleep_calls: list[float] = []

    def _fake_get_run_status(_conn, _run_id):
        return snapshots.pop(0)

    monkeypatch.setattr("runtime.workflow._status.get_run_status", _fake_get_run_status)
    monkeypatch.setattr(control_commands.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    snapshot = control_commands._workflow_submit_run_snapshot(_FakeConn(), "run-live-terminal")

    assert snapshot is not None
    assert snapshot["completed_jobs"] == 2
    assert snapshot["terminal_reason"] == "runtime.workflow_succeeded"
    assert sleep_calls == [0.05]


def test_render_workflow_submit_response_fails_closed_without_run_id() -> None:
    command = SimpleNamespace(
        command_id="control.command.submit.13",
        command_status="succeeded",
        result_ref=None,
        to_json=lambda: {
            "command_id": "control.command.submit.13",
            "command_status": "succeeded",
            "result_ref": None,
        },
    )

    payload = control_commands.render_workflow_submit_response(
        command,
        spec_name="sample",
        total_jobs=3,
    )

    assert payload == {
        "status": "failed",
        "command_status": "succeeded",
        "approval_required": False,
        "error": "workflow submit command did not produce a workflow run",
        "error_code": "control.command.workflow_submit_missing_run_id",
        "error_detail": "workflow submit command did not produce a workflow run",
        "command": {
            "command_id": "control.command.submit.13",
            "command_status": "succeeded",
            "result_ref": None,
        },
        "command_id": "control.command.submit.13",
        "spec_name": "sample",
        "total_jobs": 3,
    }


def test_render_workflow_spawn_response_uses_canonical_async_envelope() -> None:
    command = SimpleNamespace(
        command_id="control.command.spawn.12",
        command_status="succeeded",
        result_ref="workflow_run:dispatch_spawn_012",
        to_json=lambda: {
            "command_id": "control.command.spawn.12",
            "command_status": "succeeded",
            "result_ref": "workflow_run:dispatch_spawn_012",
        },
    )

    payload = control_commands.render_workflow_spawn_response(
        command,
        spec_name="child",
        total_jobs=2,
    )

    assert payload == {
        "run_id": "dispatch_spawn_012",
        "status": "queued",
        "spec_name": "child",
        "total_jobs": 2,
        "command_id": "control.command.spawn.12",
        "command_status": "succeeded",
        "approval_required": False,
        "stream_url": "/api/workflow-runs/dispatch_spawn_012/stream",
        "status_url": "/api/workflow-runs/dispatch_spawn_012/status",
        "result_ref": "workflow_run:dispatch_spawn_012",
    }


def test_render_workflow_chain_submit_response_uses_chain_envelope(monkeypatch) -> None:
    command = SimpleNamespace(
        command_id="control.command.chain.submit.12",
        command_status="succeeded",
        result_ref="workflow_chain:workflow_chain_012",
        to_json=lambda: {
            "command_id": "control.command.chain.submit.12",
            "command_status": "succeeded",
            "result_ref": "workflow_chain:workflow_chain_012",
        },
    )

    monkeypatch.setattr(
        "runtime.workflow_chain.get_workflow_chain_status",
        lambda _conn, chain_id: {
            "chain_id": chain_id,
            "program": "phase_1_pre_ui_execution_spine_convergence",
            "coordination_path": "config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
            "status": "running",
            "current_wave": "phase1_wave_a_identity_foundation",
            "waves": [
                {"wave_id": "phase1_wave_a_identity_foundation", "status": "running"},
                {"wave_id": "phase1_wave_b_runtime_contract_cleanup", "status": "pending"},
            ],
        },
    )

    payload = control_commands.render_workflow_chain_submit_response(
        object(),
        command,
        coordination_path="config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
    )

    assert payload == {
        "status": "running",
        "command_status": "succeeded",
        "command_id": "control.command.chain.submit.12",
        "approval_required": False,
        "chain_id": "workflow_chain_012",
        "program": "phase_1_pre_ui_execution_spine_convergence",
        "coordination_path": "config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
        "current_wave": "phase1_wave_a_identity_foundation",
        "waves_total": 2,
        "waves_completed": 0,
        "result_ref": "workflow_chain:workflow_chain_012",
    }


def test_bootstrap_control_commands_schema_applies_type_cutover(monkeypatch):
    conn = _FakeConn()
    requested_filenames: list[str] = []

    def _fake_workflow_migration_statements(filename: str) -> tuple[str, ...]:
        requested_filenames.append(filename)
        return (f"-- {filename} begin", f"-- {filename} end")

    monkeypatch.setattr(control_commands, "workflow_migration_statements", _fake_workflow_migration_statements)
    control_commands._schema_statements.cache_clear()

    control_commands.bootstrap_control_commands_schema(conn)

    assert requested_filenames == [
        "040_control_commands.sql",
        "042_workflow_control_command_types.sql",
    ]
    assert len(conn.script_calls) == 1
    assert "-- 040_control_commands.sql begin" in conn.script_calls[0]
    assert "-- 042_workflow_control_command_types.sql end" in conn.script_calls[0]


def test_control_command_submit_inline_spec_routes_through_inline_submit_authority(monkeypatch):
    conn = _FakeConn()
    intent = _inline_submit_intent()
    called: list[tuple[object, ...]] = []

    def _fail_saved_spec_submit(*_args, **_kwargs):
        raise AssertionError("inline workflow submit should not fall back to saved spec_path submission")

    def _submit_inline(
        _conn,
        spec,
        run_id=None,
        force_fresh_run=False,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        trigger_depth=0,
        lineage_depth=None,
        packet_provenance=None,
    ):
        called.append((spec, run_id, trigger_depth, packet_provenance))
        return {"run_id": "run-inline-1", "status": "queued", "spec_name": spec["name"]}

    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow", _fail_saved_spec_submit)
    monkeypatch.setattr(control_commands.unified_dispatch, "submit_workflow_inline", _submit_inline)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.submit.inline.1",
        requested_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, final.command_id)

    assert called == [(dict(intent.payload["spec"]), None, 0, None)]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run:run-inline-1"
    assert final.error_code is None
    assert loaded is not None
    assert loaded.command_status == "succeeded"
    assert loaded.result_ref == "workflow_run:run-inline-1"
    assert loaded.error_code is None


def test_control_command_sync_repair_auto_execute_persists_result_ref(monkeypatch):
    conn = _FakeConn()
    intent = _repair_intent()
    called: list[tuple[str | None, object | None]] = []

    def _repair_dispatch(run_id=None, *, conn=None, repo_root=None):
        called.append((run_id, repo_root))
        return SimpleNamespace(run_id="run-9", sync_status="succeeded", sync_cycle_id="cycle-9", sync_error_count=0)

    import runtime.command_handlers as command_handlers

    monkeypatch.setattr(command_handlers.post_workflow_sync, "repair_workflow_run_sync", _repair_dispatch)

    final = control_commands.create_control_command(
        conn,
        intent,
        command_id="control.command.repair.1",
        requested_at=_fixed_clock(),
    )
    loaded = control_commands.load_control_command(conn, final.command_id)

    assert called == [("run-9", None)] or called == [("run-9", None)]
    assert final.command_status == "succeeded"
    assert final.result_ref == "workflow_run_sync_status:run-9"
    assert loaded is not None
    assert loaded.result_ref == "workflow_run_sync_status:run-9"
    assert loaded.command_status == "succeeded"
    assert loaded.error_code is None
    assert loaded.error_detail is None
