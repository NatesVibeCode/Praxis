from __future__ import annotations

from typing import Any

import runtime.operation_catalog_gateway as gateway
from surfaces.mcp.tools import workflow


def test_praxis_generate_plan_materialize_dispatches_compile_then_reads_build(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    monkeypatch.setattr(workflow._subs, "get_pg_conn", lambda: object())

    def _dispatch(_subsystems: object, *, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        calls.append((operation_name, dict(payload)))
        if operation_name == "compile_materialize":
            return {
                "ok": True,
                "workflow_id": "wf_generated",
                "graph_summary": {"node_count": 3, "edge_count": 2},
                "operation_receipt": {"receipt_id": "receipt-materialize"},
            }
        if operation_name == "workflow_build_get":
            return {
                "ok": True,
                "workflow_id": payload["workflow_id"],
                "build_graph": {"nodes": [{"node_id": "n1"}], "edges": []},
                "operation_receipt": {"receipt_id": "receipt-build"},
            }
        raise AssertionError(f"unexpected operation: {operation_name}")

    monkeypatch.setattr(gateway, "execute_operation_from_subsystems", _dispatch)

    result = workflow.tool_praxis_generate_plan(
        {
            "action": "materialize_plan",
            "intent": "Generate a Moon workflow",
            "workflow_id": "wf_generated",
            "title": "Generated plan",
            "enable_llm": True,
            "enable_full_compose": True,
        }
    )

    assert [call[0] for call in calls] == ["compile_materialize", "workflow_build_get"]
    assert calls[0][1] == {
        "intent": "Generate a Moon workflow",
        "match_limit": 5,
        "workflow_id": "wf_generated",
        "title": "Generated plan",
        "enable_llm": True,
        "enable_full_compose": True,
    }
    assert calls[1][1] == {"workflow_id": "wf_generated"}
    assert result["ok"] is True
    assert result["action"] == "materialize_plan"
    assert result["workflow_id"] == "wf_generated"
    assert result["operation_receipt"]["receipt_id"] == "receipt-materialize"
    assert result["build"]["operation_receipt"]["receipt_id"] == "receipt-build"
