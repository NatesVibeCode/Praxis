from __future__ import annotations

from typing import Any

from surfaces.mcp.tools import moon


def test_praxis_moon_compose_dispatches_compile_materialize_then_reads_build(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def _dispatch(_subsystems: object, *, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        calls.append((operation_name, dict(payload)))
        if operation_name == "compile_materialize":
            return {
                "ok": True,
                "workflow_id": "wf_moon",
                "graph_summary": {"node_count": 2, "edge_count": 1},
                "operation_receipt": {"receipt_id": "receipt-materialize"},
            }
        if operation_name == "workflow_build_get":
            return {
                "ok": True,
                "workflow_id": payload["workflow_id"],
                "build_graph": {
                    "nodes": [{"node_id": "n1"}, {"node_id": "n2"}],
                    "edges": [{"edge_id": "e1", "source": "n1", "target": "n2"}],
                },
                "operation_receipt": {"receipt_id": "receipt-build-get"},
            }
        raise AssertionError(f"unexpected operation: {operation_name}")

    monkeypatch.setattr(moon, "execute_operation_from_subsystems", _dispatch)

    result = moon.tool_praxis_moon(
        {
            "action": "compose",
            "intent": "Search issues, draft summary, notify Slack",
            "plan_name": "Moon delivery",
            "enable_llm": True,
            "enable_full_compose": True,
        }
    )

    assert [call[0] for call in calls] == ["compile_materialize", "workflow_build_get"]
    assert calls[0][1] == {
        "intent": "Search issues, draft summary, notify Slack",
        "title": "Moon delivery",
        "enable_llm": True,
        "enable_full_compose": True,
    }
    assert calls[1][1] == {"workflow_id": "wf_moon"}
    assert result["ok"] is True
    assert result["workflow_id"] == "wf_moon"
    assert result["graph_summary"] == {"node_count": 2, "edge_count": 1}
    assert result["operation_receipt"]["receipt_id"] == "receipt-materialize"
    assert result["build"]["operation_receipt"]["receipt_id"] == "receipt-build-get"
