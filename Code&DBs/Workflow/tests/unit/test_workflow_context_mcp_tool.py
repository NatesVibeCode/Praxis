from __future__ import annotations

from surfaces.mcp.tools import workflow_context


def test_workflow_context_mcp_tools_use_gateway(monkeypatch) -> None:
    captured: list[tuple[str, dict]] = []

    def _execute(*, env, operation_name: str, payload: dict):
        captured.append((operation_name, payload))
        return {"ok": True, "operation": operation_name}

    monkeypatch.setattr(workflow_context, "execute_operation_from_env", _execute)

    assert workflow_context.tool_praxis_workflow_context_compile(
        {"intent": "renewal risk", "context_mode": "synthetic", "ignored": None}
    ) == {"ok": True, "operation": "workflow_context_compile"}
    assert workflow_context.tool_praxis_workflow_context_read(
        {"context_ref": "workflow_context:1"}
    ) == {"ok": True, "operation": "workflow_context_read"}
    assert workflow_context.tool_praxis_workflow_context_transition(
        {
            "context_ref": "workflow_context:1",
            "to_truth_state": "verified",
            "transition_reason": "verifier passed",
        }
    ) == {"ok": True, "operation": "workflow_context_transition"}
    assert workflow_context.tool_praxis_workflow_context_bind(
        {
            "context_ref": "workflow_context:1",
            "entity_ref": "entity:1",
            "target_ref": "object_truth:1",
        }
    ) == {"ok": True, "operation": "workflow_context_bind"}
    assert workflow_context.tool_praxis_workflow_context_guardrail_check(
        {"context_ref": "workflow_context:1", "target_truth_state": "promoted"}
    ) == {"ok": True, "operation": "workflow_context_guardrail_check"}
    assert workflow_context.tool_praxis_object_truth_latest_version_read(
        {"system_ref": "Salesforce", "object_ref": "Account"}
    ) == {"ok": True, "operation": "object_truth_latest_version_read"}

    assert [name for name, _payload in captured] == [
        "workflow_context_compile",
        "workflow_context_read",
        "workflow_context_transition",
        "workflow_context_bind",
        "workflow_context_guardrail_check",
        "object_truth_latest_version_read",
    ]
    assert captured[0][1] == {"intent": "renewal risk", "context_mode": "synthetic"}
