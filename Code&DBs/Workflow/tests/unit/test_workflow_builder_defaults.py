from __future__ import annotations

import runtime.workflow_builder as workflow_builder


def test_workflow_step_adapter_default_is_resolved_lazily(monkeypatch) -> None:
    seen_provider_slugs: list[str | None] = []

    def _default_adapter(provider_slug: str | None = None) -> str:
        seen_provider_slugs.append(provider_slug)
        return "llm_task" if provider_slug == "cursor" else "cli_llm"

    monkeypatch.setattr(workflow_builder, "resolve_default_adapter_type", _default_adapter)

    step = workflow_builder.WorkflowStep(name="review", prompt="Review this", provider_slug="cursor")
    request = workflow_builder.build_workflow_request(
        [step],
        workspace_ref="workspace.test",
        runtime_profile_ref="runtime.test",
    )

    assert step.adapter_type is None
    assert request.nodes[0].adapter_type == "llm_task"
    assert seen_provider_slugs == ["cursor"]
