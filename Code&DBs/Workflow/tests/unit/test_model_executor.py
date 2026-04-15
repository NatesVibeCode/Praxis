from __future__ import annotations

from types import SimpleNamespace

import runtime.model_executor as model_executor
import runtime.workflow.unified as workflow_unified
from registry import agent_config as agent_config_mod


def test_execute_action_routes_mcp_transport_through_cli_with_tool_permissions(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute_cli(agent_config, prompt: str, workdir: str, execution_bundle=None):
        captured["agent_config"] = agent_config
        captured["prompt"] = prompt
        captured["workdir"] = workdir
        captured["execution_bundle"] = execution_bundle
        return {
            "status": "succeeded",
            "stdout": "ok",
            "stderr": "",
            "error_code": "",
        }

    monkeypatch.setattr(workflow_unified, "_execute_cli", _fake_execute_cli)
    monkeypatch.setattr(
        workflow_unified,
        "_execute_api",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("API path should not be used for MCP transport")),
    )
    monkeypatch.setattr(model_executor, "resolve_execution_transport", lambda _agent_config: SimpleNamespace(transport_kind="mcp", sandbox_provider="cloudflare_remote"))
    monkeypatch.setattr(model_executor, "_collect_upstream_outputs", lambda _conn, _run_id, _card_id: {})
    monkeypatch.setattr(
        agent_config_mod.AgentRegistry,
        "load_from_postgres",
        lambda _conn: SimpleNamespace(
            get=lambda _slug: SimpleNamespace(
                provider="anthropic",
                model="claude-3.7-sonnet",
                timeout_seconds=30,
                wrapper_command="claude --print",
            )
        ),
    )

    card = {
        "id": "card-1",
        "kind": "action",
        "executor": {
            "kind": "agent",
            "detail": "anthropic/claude-3.7-sonnet",
            "name": "claude",
        },
        "task": "Summarize the latest receipts",
        "toolPermissions": ["praxis_query", "praxis_discover"],
    }

    result = model_executor._execute_action(object(), "run-123", card, "/repo/root")

    assert result["status"] == "succeeded"
    assert result["outputs"]["resolved_agent"] == "anthropic/claude-3.7-sonnet"
    assert result["outputs"]["execution_transport"] == "mcp"
    assert captured["workdir"] == "/repo/root"
    assert captured["execution_bundle"] == {
        "run_id": "run-123",
        "job_label": "card-1",
        "mcp_tool_names": ["praxis_query", "praxis_discover"],
    }
