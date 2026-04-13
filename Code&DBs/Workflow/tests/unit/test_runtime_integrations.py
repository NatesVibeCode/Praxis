from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from runtime.integrations import execute_integration
from runtime.integrations import workflow as workflow_integration


class _FakeConn:
    def __init__(self, row: dict) -> None:
        self._row = row

    def execute(self, query: str, *params):
        if "FROM integration_registry" in query:
            return [self._row]
        raise AssertionError(f"Unexpected SQL: {query}")


class _NoQueryConn:
    def execute(self, query: str, *params):
        raise AssertionError(f"Unexpected SQL: {query}")


def test_execute_integration_dispatches_catalog_backed_mcp_action(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "surfaces.mcp.catalog.get_tool_catalog",
        lambda: {
            "praxis_bugs": SimpleNamespace(
                selector_field="action",
                action_enum=("list", "file", "search"),
                view_enum=(),
            ),
        },
    )

    def _handler(params: dict) -> dict:
        captured.update(params)
        return {"count": 1}

    monkeypatch.setattr(
        "surfaces.mcp.catalog.resolve_tool_entry",
        lambda name: (_handler, {}),
    )

    conn = _FakeConn(
        {
            "id": "praxis_bugs",
            "name": "Praxis Bugs",
            "description": "Bug tracker tool.",
            "provider": "mcp",
            "capabilities": [
                {"action": "list"},
                {"action": "file"},
                {"action": "search"},
            ],
            "auth_status": "connected",
            "icon": "tool",
            "mcp_server_id": "praxis-workflow-mcp",
            "catalog_dispatch": True,
        }
    )

    result = execute_integration(
        "praxis_bugs",
        "file",
        {"title": "wire it", "severity": "P1"},
        conn,
    )

    assert result["status"] == "succeeded"
    assert captured == {
        "action": "file",
        "title": "wire it",
        "severity": "P1",
    }


def test_execute_integration_dispatches_catalog_backed_mcp_view_selector(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "surfaces.mcp.catalog.get_tool_catalog",
        lambda: {
            "praxis_operator_view": SimpleNamespace(
                selector_field="view",
                action_enum=(),
                view_enum=("status", "scoreboard", "graph"),
            ),
        },
    )

    def _handler(params: dict) -> dict:
        captured.update(params)
        return {"message": "ok"}

    monkeypatch.setattr(
        "surfaces.mcp.catalog.resolve_tool_entry",
        lambda name: (_handler, {}),
    )

    conn = _FakeConn(
        {
            "id": "praxis_operator_view",
            "name": "Praxis Operator View",
            "description": "Operator read views.",
            "provider": "mcp",
            "capabilities": [
                {"action": "status", "selectorField": "view"},
                {"action": "scoreboard", "selectorField": "view"},
                {"action": "graph", "selectorField": "view"},
            ],
            "auth_status": "connected",
            "icon": "tool",
            "mcp_server_id": "praxis-workflow-mcp",
            "catalog_dispatch": True,
        }
    )

    result = execute_integration(
        "praxis_operator_view",
        "graph",
        {},
        conn,
    )

    assert result["status"] == "succeeded"
    assert captured == {"view": "graph"}


def test_execute_integration_dispatch_job_exposes_async_links(monkeypatch) -> None:
    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        return {
            "run_id": "workflow_async_123",
            "status": "queued",
            "total_jobs": len(spec_dict.get("jobs", [])),
            "spec_name": spec_dict.get("name", ""),
            "workflow_id": spec_dict.get("workflow_id", ""),
            "replayed_jobs": [],
        }

    monkeypatch.setattr(
        "runtime.workflow.unified.submit_workflow_inline",
        _submit,
    )

    conn = _FakeConn(
        {
            "id": "dag-dispatch",
            "name": "DAG Dispatch",
            "description": "Dispatch bridge.",
            "provider": "internal",
            "capabilities": [
                {"action": "dispatch_job"},
                {"action": "check_status"},
            ],
            "auth_status": "connected",
            "icon": "tool",
            "mcp_server_id": "",
        }
    )

    result = execute_integration(
        "dag-dispatch",
        "dispatch_job",
        {"prompt": "wire the seam", "label": "child"},
        conn,
    )

    assert result["status"] == "succeeded"
    assert result["data"]["run_id"] == "workflow_async_123"
    assert result["data"]["status"] == "queued"
    assert result["data"]["stream_url"] == "/api/workflow-runs/workflow_async_123/stream"
    assert result["data"]["status_url"] == "/api/workflow-runs/workflow_async_123/status"
    assert "poll" in result["summary"]


def test_execute_integration_check_status_exposes_explicit_recovery(monkeypatch) -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    fake_status = {
        "run_id": "dispatch_idle",
        "status": "running",
        "spec_name": "test-queue",
        "total_jobs": 1,
        "completed_jobs": 0,
        "created_at": now - timedelta(seconds=2000),
        "finished_at": None,
        "total_cost_usd": 0.0,
        "total_tokens_in": 0,
        "total_tokens_out": 0,
        "total_duration_ms": 0,
        "jobs": [
            {
                "id": 1,
                "label": "job-a",
                "agent_slug": "agent-a",
                "resolved_agent": "agent-a",
                "status": "pending",
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
                "created_at": now - timedelta(seconds=2000),
                "ready_at": now - timedelta(seconds=1200),
                "claimed_at": None,
                "started_at": None,
                "finished_at": None,
                "heartbeat_at": None,
                "next_retry_at": None,
                "claimed_by": None,
            }
        ],
    }

    monkeypatch.setattr(
        "runtime.workflow.unified.get_run_status",
        lambda _conn, _run_id: fake_status,
    )

    conn = _FakeConn(
        {
            "id": "dag-dispatch",
            "name": "DAG Dispatch",
            "description": "Dispatch bridge.",
            "provider": "internal",
            "capabilities": [
                {"action": "dispatch_job"},
                {"action": "check_status"},
            ],
            "auth_status": "connected",
            "icon": "tool",
            "mcp_server_id": "",
        }
    )

    result = execute_integration(
        "dag-dispatch",
        "check_status",
        {"run_id": "dispatch_idle"},
        conn,
    )

    assert result["status"] == "succeeded"
    assert result["data"]["run_id"] == "dispatch_idle"
    assert result["data"]["status"] == "running"
    assert result["data"]["stream_url"] == "/api/workflow-runs/dispatch_idle/stream"
    assert result["data"]["status_url"] == "/api/workflow-runs/dispatch_idle/status"
    assert result["data"]["recovery"]["mode"] == "kill_if_idle"
    assert result["data"]["recovery"]["recommended_tool"]["arguments"]["kill_if_idle"] is True


def test_execute_integration_search_receipts_returns_evidence(monkeypatch) -> None:
    class _Record:
        id = 11
        label = "deploy-check"
        agent = "openai/gpt-5.4"
        status = "succeeded"
        failure_code = ""
        timestamp = datetime(2026, 4, 8, 18, 45, tzinfo=timezone.utc)

        def to_dict(self) -> dict:
            return {
                "run_id": "workflow_run_123",
                "workflow_name": "Deploy Check",
                "current_state": "succeeded",
                "outputs": {"note": "receipt evidence"},
            }

    monkeypatch.setattr(
        "runtime.receipt_store.search_receipts",
        lambda query, *, limit=10: [_Record()] if query == "receipt evidence" else [],
    )

    conn = _FakeConn(
        {
            "id": "dag-dispatch",
            "name": "DAG Dispatch",
            "description": "Dispatch bridge.",
            "provider": "internal",
            "capabilities": [
                {"action": "dispatch_job"},
                {"action": "check_status"},
                {"action": "search_receipts"},
            ],
            "auth_status": "connected",
            "icon": "tool",
            "mcp_server_id": "",
        }
    )

    result = execute_integration(
        "dag-dispatch",
        "search_receipts",
        {"query": "receipt evidence", "limit": 10},
        conn,
    )

    assert result["status"] == "succeeded"
    assert result["data"]["count"] == 1
    run = result["data"]["runs"][0]
    assert run["receipt"]["outputs"]["note"] == "receipt evidence"
    assert result["summary"] == "Found 1 receipt evidence matches for 'receipt evidence'"


def test_invoke_workflow_requires_workflow_id_and_does_not_fallback_to_name() -> None:
    conn = _NoQueryConn()

    result = workflow_integration.invoke_workflow({"workflow_name": "Inbox Triage"}, conn)

    assert result["status"] == "failed"
    assert result["error"] == "missing_workflow_id"
    assert result["summary"] == "workflow_id required."


def test_invoke_workflow_returns_child_packet_reuse_provenance(monkeypatch) -> None:
    class _WorkflowInvokeConn:
        def __init__(self) -> None:
            self.updated_ids: list[str] = []

        def execute(self, query: str, *params):
            normalized = " ".join(query.split())
            if "FROM workflows WHERE id = $1" in normalized:
                return [
                    {
                        "id": "wf_123",
                        "name": "Inbox Triage",
                        "definition": {"definition_revision": "def_123"},
                        "compiled_spec": {"definition_revision": "def_123", "plan_revision": "plan_123"},
                        "invocation_count": 2,
                        "last_invoked_at": datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
                    }
                ]
            if normalized.startswith("UPDATE workflows SET invocation_count = invocation_count + 1"):
                self.updated_ids.append(str(params[0]))
                return []
            if "SELECT current_state FROM workflow_runs" in normalized:
                return [{"current_state": "succeeded"}]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in normalized:
                return [
                    {
                        "label": "search-inbox",
                        "status": "succeeded",
                        "stdout_preview": "done",
                        "duration_ms": 42,
                        "cost_usd": 0.0,
                    }
                ]
            raise AssertionError(f"Unexpected SQL: {normalized}")

    captured_submit: dict[str, object] = {}
    expected_spec = {
        "name": "Inbox Triage",
        "workflow_id": "wf_123",
        "phase": "build",
        "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
        "outcome_goal": "triage the inbox",
        "definition_revision": "def_123",
    }

    monkeypatch.setattr(
        "runtime.operating_model_planner.current_compiled_spec",
        lambda definition, compiled_spec: dict(expected_spec),
    )

    def _submit(_conn, spec_raw, run_id=None, parent_run_id=None, trigger_depth=0, packet_provenance=None):
        captured_submit["spec_raw"] = spec_raw
        captured_submit["packet_provenance"] = packet_provenance
        return {
            "run_id": "workflow_child_123",
            "status": "queued",
            "total_jobs": 1,
            "spec_name": spec_raw.get("name", ""),
            "workflow_id": spec_raw.get("workflow_id", ""),
            "replayed_jobs": [],
            "packet_reuse_provenance": {
                "artifact_kind": "packet_lineage",
                "decision": "reused",
                "reason_code": "packet.compile.exact_input_match",
                "input_fingerprint": "packet-input.alpha",
            },
        }

    monkeypatch.setattr("runtime.workflow.unified.submit_workflow_inline", _submit)
    monkeypatch.setattr(workflow_integration.time, "sleep", lambda _seconds: None)

    conn = _WorkflowInvokeConn()
    result = workflow_integration.invoke_workflow(
        {
            "workflow_id": "wf_123",
            "inputs": {"ticket": "T-1"},
            "parent_run_id": "workflow_parent_1",
            "trigger_depth": 1,
        },
        conn,
    )

    assert result["status"] == "succeeded"
    assert result["data"]["packet_reuse_provenance"] == {
        "artifact_kind": "packet_lineage",
        "decision": "reused",
        "reason_code": "packet.compile.exact_input_match",
        "input_fingerprint": "packet-input.alpha",
    }
    assert captured_submit["packet_provenance"] == {
        "source_kind": "workflow_invoke",
        "workflow_row": {
            "id": "wf_123",
            "name": "Inbox Triage",
            "definition": {"definition_revision": "def_123"},
            "compiled_spec": {"definition_revision": "def_123", "plan_revision": "plan_123"},
            "invocation_count": 2,
            "last_invoked_at": datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
        },
        "definition_row": {"definition_revision": "def_123"},
        "compiled_spec_row": expected_spec,
        "file_inputs": {"inputs": {"ticket": "T-1"}},
    }
    assert conn.updated_ids == ["wf_123"]


def test_invoke_workflow_surfaces_child_packet_reuse_provenance(monkeypatch) -> None:
    definition = {
        "definition_revision": "def_123",
        "source_prose": "Handle the inbox",
        "compiled_prose": "Handle the inbox",
        "references": [],
        "narrative_blocks": [],
        "draft_flow": [],
        "trigger_intent": [],
        "compile_provenance": {
            "artifact_kind": "definition",
            "input_fingerprint": "definition.input.123",
        },
    }

    monkeypatch.setattr(
        "runtime.operating_model_planner._plan_surface_revision",
        lambda: "surface_plan_123",
    )
    from runtime.operating_model_planner import plan_definition

    compiled_spec = plan_definition(definition, title="Inbox Triage")["compiled_spec"]

    class _WorkflowConn:
        def execute(self, query: str, *params):
            if "FROM workflows WHERE id = $1" in query:
                return [
                    {
                        "id": "wf_123",
                        "name": "Inbox Triage",
                        "definition": definition,
                        "compiled_spec": compiled_spec,
                        "invocation_count": 0,
                        "last_invoked_at": None,
                    }
                ]
            if "UPDATE workflows SET invocation_count = invocation_count + 1" in query:
                return []
            if "SELECT current_state FROM workflow_runs WHERE run_id = $1" in query:
                return [{"current_state": "succeeded"}]
            if "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in query:
                return [{"label": "search-inbox", "status": "succeeded", "stdout_preview": "ok", "duration_ms": 1}]
            raise AssertionError(f"Unexpected SQL: {query}")

    monkeypatch.setattr(
        workflow_integration,
        "MAX_WAIT_SECONDS",
        1,
    )
    monkeypatch.setattr(
        workflow_integration,
        "POLL_INTERVAL",
        0,
    )
    monkeypatch.setattr(
        "runtime.workflow.unified.submit_workflow_inline",
        lambda *args, **kwargs: {
            "run_id": "workflow_child_123",
            "status": "queued",
            "total_jobs": 1,
            "spec_name": "Inbox Triage",
            "workflow_id": "wf_123",
            "replayed_jobs": [],
            "packet_reuse_provenance": {
                "decision": "reused",
                "reason_code": "packet.compile.exact_input_match",
                "artifact_ref": "packet_1234567890abcdef:1",
            },
        },
    )

    result = workflow_integration.invoke_workflow(
        {"workflow_id": "wf_123", "inputs": {"ticket_id": "T-1"}},
        _WorkflowConn(),
    )

    assert result["status"] == "succeeded"
    assert result["data"]["child_run_id"] == "workflow_child_123"
    assert result["data"]["packet_reuse_provenance"] == {
        "decision": "reused",
        "reason_code": "packet.compile.exact_input_match",
        "artifact_ref": "packet_1234567890abcdef:1",
    }
