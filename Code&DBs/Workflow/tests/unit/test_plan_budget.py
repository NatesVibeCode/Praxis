from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.plan_budget import (
    JobBudgetEstimate,
    PlanBudgetProjection,
    project_plan_budget,
)


class _FakeProposedPlan:
    """Mimics ProposedPlan for budget projection without importing the full type."""

    def __init__(self, *, spec_dict: dict, preview: dict | None = None) -> None:
        self.spec_dict = spec_dict
        self.preview = preview or {"jobs": []}


def test_project_plan_budget_per_job_and_rollup() -> None:
    # Two jobs, different stages, different prompt sizes.
    spec_dict = {
        "jobs": [
            {"label": "pkt-1", "prompt": "x" * 400, "task_type": "build"},  # 400 chars → 100 tokens
            {"label": "pkt-2", "prompt": "y" * 200, "task_type": "test"},   # 200 chars → 50 tokens
        ]
    }
    preview = {
        "jobs": [
            {"label": "pkt-1", "resolved_agent": "openai/gpt-5.4"},
            {"label": "pkt-2", "resolved_agent": "openai/gpt-5.4-mini"},
        ]
    }
    proposed = _FakeProposedPlan(spec_dict=spec_dict, preview=preview)

    projection = project_plan_budget(proposed)

    assert isinstance(projection, PlanBudgetProjection)
    assert len(projection.jobs) == 2

    job_a, job_b = projection.jobs
    assert job_a.label == "pkt-1"
    assert job_a.prompt_chars == 400
    assert job_a.estimated_prompt_tokens == 100  # 400 / 4
    assert job_a.estimated_output_tokens == 4000  # build stage
    assert job_a.resolved_agent == "openai/gpt-5.4"
    assert job_a.estimated_total_tokens == 100 + 4000

    assert job_b.prompt_chars == 200
    assert job_b.estimated_prompt_tokens == 50
    assert job_b.estimated_output_tokens == 2500  # test stage
    assert job_b.resolved_agent == "openai/gpt-5.4-mini"

    assert projection.total_prompt_chars == 600
    assert projection.total_estimated_prompt_tokens == 150
    assert projection.total_estimated_output_tokens == 4000 + 2500
    assert projection.total_estimated_tokens == 150 + 6500
    assert projection.warnings == []


def test_project_plan_budget_label_override_wins() -> None:
    spec_dict = {
        "jobs": [{"label": "pkt-1", "prompt": "abcdefgh", "task_type": "build"}]
    }
    proposed = _FakeProposedPlan(spec_dict=spec_dict)

    projection = project_plan_budget(
        proposed,
        output_tokens_by_label={"pkt-1": 9999},
    )
    assert projection.jobs[0].estimated_output_tokens == 9999


def test_project_plan_budget_stage_override() -> None:
    spec_dict = {
        "jobs": [{"label": "pkt-1", "prompt": "x", "task_type": "review"}]
    }
    proposed = _FakeProposedPlan(spec_dict=spec_dict)

    projection = project_plan_budget(
        proposed,
        output_tokens_by_stage={"review": 500},
    )
    assert projection.jobs[0].estimated_output_tokens == 500


def test_project_plan_budget_unknown_stage_falls_back_with_warning() -> None:
    spec_dict = {
        "jobs": [{"label": "pkt-1", "prompt": "x", "task_type": "weird_stage"}]
    }
    proposed = _FakeProposedPlan(spec_dict=spec_dict)

    projection = project_plan_budget(proposed)
    assert projection.jobs[0].estimated_output_tokens == 3000  # fallback
    assert any("weird_stage" in w for w in projection.warnings)


def test_project_plan_budget_ceils_prompt_tokens() -> None:
    """Odd character counts round UP to a whole token estimate."""
    spec_dict = {
        "jobs": [{"label": "pkt-1", "prompt": "x" * 5, "task_type": "build"}]
    }
    proposed = _FakeProposedPlan(spec_dict=spec_dict)

    projection = project_plan_budget(proposed)
    # 5 / 4 = 1.25 → ceil to 2
    assert projection.jobs[0].estimated_prompt_tokens == 2


def test_project_plan_budget_empty_plan_returns_zero_totals() -> None:
    proposed = _FakeProposedPlan(spec_dict={"jobs": []})
    projection = project_plan_budget(proposed)
    assert projection.jobs == []
    assert projection.total_estimated_tokens == 0


def test_project_plan_budget_resolved_agent_optional() -> None:
    """Jobs with no matching preview entry get resolved_agent=None, not crash."""
    spec_dict = {
        "jobs": [{"label": "pkt-1", "prompt": "x", "task_type": "build"}]
    }
    # Preview has no jobs list at all.
    proposed = _FakeProposedPlan(spec_dict=spec_dict, preview={})
    projection = project_plan_budget(proposed)
    assert projection.jobs[0].resolved_agent is None


def test_job_budget_estimate_to_dict_round_trips() -> None:
    spec_dict = {
        "jobs": [{"label": "pkt-1", "prompt": "hello world", "task_type": "build"}]
    }
    proposed = _FakeProposedPlan(spec_dict=spec_dict)
    projection = project_plan_budget(proposed)
    payload = projection.to_dict()
    assert payload["jobs"][0]["label"] == "pkt-1"
    assert payload["jobs"][0]["estimated_total_tokens"] > 0
    assert payload["total_estimated_tokens"] > 0
