from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.workflow._result_shaping import (
    project_single_workflow_result,
    shape_pipeline_workflow_result,
)
from runtime.workflow._workflow_execution import WorkflowExecutionContext
from runtime.workflow.orchestrator import WorkflowSpec


def _context() -> WorkflowExecutionContext:
    return WorkflowExecutionContext(
        provider_slug="anthropic",
        model_slug="claude-test",
        adapter_type="cli_llm",
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        start_ns=0,
    )


def test_project_single_dispatch_result_wraps_projection(monkeypatch):
    from runtime.workflow import _result_shaping as shaping_module

    projected = {
        "run_id": "run_123",
        "status": "succeeded",
        "reason_code": "runtime.workflow_succeeded",
        "completion": "done",
        "outputs": {"structured_output": {"has_code": True}},
        "evidence_count": 4,
        "started_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 4, 8, 12, 0, 5, tzinfo=timezone.utc),
        "latency_ms": 5000,
        "provider_slug": "openai",
        "model_slug": "gpt-5.4",
        "adapter_type": "cli_llm",
        "failure_code": None,
        "label": "job-a",
        "capabilities": ["ops"],
        "author_model": "openai/gpt-5.4",
        "reviews_workflow_id": "review_run",
        "review_target_modules": ["a.py"],
    }
    calls = []

    def _project(**kwargs):
        calls.append(kwargs)
        return projected

    monkeypatch.setattr(shaping_module, "project_workflow_result", _project)

    spec = WorkflowSpec(
        prompt="test",
        provider_slug="openai",
        model_slug="gpt-5.4",
        label="job-a",
        capabilities=["ops"],
        reviews_workflow_id="review_run",
        review_target_modules=["a.py"],
    )
    evidence_writer = SimpleNamespace(evidence_timeline=lambda run_id: ("evt1", "evt2"))
    result = project_single_workflow_result(
        spec=spec,
        intake_outcome=SimpleNamespace(run_id="run_123"),
        evidence_writer=evidence_writer,
    )

    assert calls == [
        {
            "run_id": "run_123",
            "timeline": ("evt1", "evt2"),
            "spec_provider_slug": "openai",
            "spec_model_slug": "gpt-5.4",
            "spec_adapter_type": "cli_llm",
            "spec_label": "job-a",
            "spec_capabilities": ["ops"],
            "spec_reviews_dispatch_id": "review_run",
            "spec_review_target_modules": ["a.py"],
        }
    ]
    assert result.run_id == "run_123"
    assert result.author_model == "openai/gpt-5.4"


def test_shape_pipeline_dispatch_result_returns_node_not_found_failure():
    result = shape_pipeline_workflow_result(
        steps=[SimpleNamespace(prompt="one"), SimpleNamespace(prompt="two")],
        intake_outcome=SimpleNamespace(run_id="run_missing"),
        execution_result=SimpleNamespace(node_results=[], terminal_reason_code="runtime.workflow_failed"),
        evidence_writer=SimpleNamespace(evidence_timeline=lambda run_id: [1, 2, 3]),
        context=_context(),
    )

    assert result.run_id == "run_missing"
    assert result.reason_code == "dispatch.node_not_found"
    assert result.failure_code == "dispatch.node_not_found"
    assert result.evidence_count == 3


def test_shape_pipeline_dispatch_result_applies_fan_out(monkeypatch):
    fan_out_calls = []

    import runtime.fan_out as fan_out_module

    monkeypatch.setattr(
        fan_out_module,
        "fan_out_from_completion",
        lambda completion, *, prompt_template, tier, max_parallel: fan_out_calls.append(
            {
                "completion": completion,
                "prompt_template": prompt_template,
                "tier": tier,
                "max_parallel": max_parallel,
            }
        )
        or ["a", "b"],
    )
    monkeypatch.setattr(
        fan_out_module,
        "aggregate_fan_out_results",
        lambda results: {"completions": ["done-a", "done-b"], "results": results},
    )

    steps = [
        SimpleNamespace(prompt="first"),
        SimpleNamespace(
            prompt="fan this",
            fan_out=True,
            fan_out_prompt="rewrite {item}",
            fan_out_max_parallel=7,
            tier="frontier",
        ),
    ]
    execution_result = SimpleNamespace(
        terminal_reason_code="runtime.workflow_succeeded",
        node_results=[
            SimpleNamespace(node_id="node_1", status="succeeded", outputs={"completion": "seed"}, failure_code=None)
        ],
    )

    result = shape_pipeline_workflow_result(
        steps=steps,
        intake_outcome=SimpleNamespace(run_id="run_fan"),
        execution_result=execution_result,
        evidence_writer=SimpleNamespace(evidence_timeline=lambda run_id: [1, 2]),
        context=_context(),
    )

    assert fan_out_calls == [
        {
            "completion": "seed",
            "prompt_template": "rewrite {item}",
            "tier": "frontier",
            "max_parallel": 7,
        }
    ]
    assert json.loads(result.completion) == ["done-a", "done-b"]
    assert result.outputs["fan_out"] == {"completions": ["done-a", "done-b"], "results": ["a", "b"]}
    assert result.outputs["fan_out_source_completion"] == "seed"
