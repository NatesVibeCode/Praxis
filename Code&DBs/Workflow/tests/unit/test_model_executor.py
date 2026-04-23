from __future__ import annotations

from types import SimpleNamespace

import runtime.model_executor as model_executor
import runtime.workflow._status as workflow_status
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


def test_start_model_run_is_retired() -> None:
    try:
        model_executor.start_model_run(object(), {"cards": [], "edges": []})
    except model_executor.LegacyModelRuntimeRetiredError as exc:
        assert "unified workflow front door" in str(exc)
    else:  # pragma: no cover - fail closed if the stale creation lane reopens
        raise AssertionError("model-card run creation should be retired")


class _ReleaseConn:
    def __init__(self) -> None:
        self.nodes = {
            "success": {"current_state": "succeeded"},
            "fallback": {"current_state": "waiting"},
            "downstream": {"current_state": "waiting"},
        }
        self.edges = {
            "success_edge": {
                "run_edge_id": "success_edge",
                "edge_id": "success_edge",
                "from_node_id": "success",
                "to_node_id": "downstream",
                "edge_type": "proceeds_to",
                "release_state": "pending",
            },
            "fallback_edge": {
                "run_edge_id": "fallback_edge",
                "edge_id": "fallback_edge",
                "from_node_id": "success",
                "to_node_id": "downstream",
                "edge_type": "alternate_route",
                "release_state": "pending",
            },
        }

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT current_state FROM run_nodes"):
            return [{"current_state": self.nodes[args[1]]["current_state"]}]
        if normalized.startswith("SELECT run_edge_id"):
            return [
                dict(edge)
                for edge in self.edges.values()
                if edge["from_node_id"] == args[1]
            ]
        if normalized.startswith("UPDATE run_edges SET release_state='released'"):
            self.edges[args[0]]["release_state"] = "released"
            return []
        if normalized.startswith("UPDATE run_edges SET release_state='skipped'"):
            if self.edges[args[0]]["release_state"] == "pending":
                self.edges[args[0]]["release_state"] = "skipped"
            return []
        if normalized.startswith("SELECT count(*) as cnt FROM run_edges"):
            return [
                {
                    "cnt": sum(
                        1
                        for edge in self.edges.values()
                        if edge["to_node_id"] == args[1]
                        and edge["release_state"] == "pending"
                    )
                }
            ]
        if normalized.startswith("UPDATE run_nodes SET current_state='ready'"):
            self.nodes[args[1]]["current_state"] = "ready"
            return [{"node_id": args[1]}]
        if normalized.startswith("SELECT count(*) as cnt FROM run_nodes"):
            return [
                {
                    "cnt": sum(
                        1
                        for node in self.nodes.values()
                        if node["current_state"] not in ("succeeded", "failed")
                    )
                }
            ]
        raise AssertionError(f"Unexpected query: {normalized}")


def test_release_downstream_skips_unselected_alternate_edges() -> None:
    conn = _ReleaseConn()

    released = model_executor.release_downstream(conn, "run-1", "success")

    assert released == ["downstream"]
    assert conn.edges["success_edge"]["release_state"] == "released"
    assert conn.edges["fallback_edge"]["release_state"] == "skipped"
    assert conn.nodes["downstream"]["current_state"] == "ready"


class _StatusConn:
    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM run_nodes WHERE run_id=$1" in normalized:
            return [
                {
                    "node_id": "card.review",
                    "node_type": "card_decision",
                    "current_state": "awaiting_human",
                    "started_at": None,
                    "finished_at": None,
                    "output_payload": {"reason": "Human approval required"},
                    "failure_code": "",
                }
            ]
        raise AssertionError(f"Unexpected query: {normalized}")


def test_model_run_status_delegates_to_canonical_status(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_status,
        "get_run_status",
        lambda _conn, run_id: {
            "run_id": run_id,
            "status": "running",
            "jobs": [],
            "data_quality": [{"reason_code": "status.from.canonical"}],
        },
    )

    status = model_executor.get_run_status(_StatusConn(), "run-1")

    assert status["status"] == "running"
    assert status["data_quality"] == [{"reason_code": "status.from.canonical"}]
    assert status["cards"]["card.review"]["status"] == "awaiting_human"
    assert status["model_runtime_status_projection"] == "run_nodes_compatibility"


class _ApprovalConn:
    def __init__(self) -> None:
        self.node_state = "awaiting_human"
        self.terminal_update_args: tuple[object, ...] | None = None

    def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE run_nodes SET current_state='running'"):
            if self.node_state != "awaiting_human":
                return None
            self.node_state = "running"
            return "rn-1"
        raise AssertionError(f"Unexpected fetchval: {normalized}")

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE run_nodes SET current_state = $2"):
            self.terminal_update_args = args
            if self.node_state != args[5]:
                return []
            self.node_state = str(args[1])
            return [{"run_node_id": args[0]}]
        raise AssertionError(f"Unexpected query: {normalized}")


def test_approve_card_claims_awaiting_human_before_terminal_write(monkeypatch) -> None:
    conn = _ApprovalConn()
    released: list[tuple[str, str]] = []
    monkeypatch.setattr(
        model_executor,
        "write_run_node_receipt",
        lambda _conn, **_kwargs: "receipt:approval",
    )
    monkeypatch.setattr(
        model_executor,
        "release_downstream",
        lambda _conn, run_id, card_id: released.append((run_id, card_id)) or ["next"],
    )

    result = model_executor.approve_card(conn, "run-1", "card.review", "approved", "go")

    assert result == {"status": "approved", "released_cards": ["next"]}
    assert conn.node_state == "succeeded"
    assert conn.terminal_update_args is not None
    assert conn.terminal_update_args[5] == "running"
    assert released == [("run-1", "card.review")]


def test_approve_card_rejects_already_claimed_human_node(monkeypatch) -> None:
    conn = _ApprovalConn()
    conn.node_state = "running"
    monkeypatch.setattr(
        model_executor,
        "write_run_node_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("approval receipt should not be written without a claim")
        ),
    )

    try:
        model_executor.approve_card(conn, "run-1", "card.review", "approved", "")
    except RuntimeError as exc:
        assert "No claimable awaiting_human run_node" in str(exc)
    else:  # pragma: no cover - fail closed if double approval is accepted
        raise AssertionError("already claimed approval should fail")
