from __future__ import annotations

import runtime.workflow_builder as workflow_builder


def test_workflow_step_adapter_default_is_resolved_lazily(monkeypatch) -> None:
    monkeypatch.setattr(workflow_builder, "default_llm_adapter_type", lambda: "llm_task")

    step = workflow_builder.WorkflowStep(name="review", prompt="Review this")

    assert step.adapter_type == "llm_task"
