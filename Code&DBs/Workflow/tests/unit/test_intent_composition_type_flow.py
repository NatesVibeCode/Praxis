"""Tests for Phase 1.2.b type-flow validation in compose_plan_from_intent.

Honors architecture-policy::platform-architecture::fail-closed-at-compile-
no-silent-defaults. compose_plan_from_intent surfaces type-flow errors in
the ProposedPlan's warnings list so callers see them before approving or
launching. Canvas commitDefinition independently blocks at save via 1.2.a.
"""
from __future__ import annotations

from runtime.intent_composition import (
    _adapt_plan_jobs_to_type_flow_request,
    _validate_composed_plan_type_flow,
)
from runtime.spec_materializer import ProposedPlan


def _make_proposed(jobs: list[dict]) -> ProposedPlan:
    return ProposedPlan(
        spec_dict={"name": "test", "workflow_id": "wf", "jobs": jobs},
        preview={},
        warnings=[],
        workflow_id="wf",
        spec_name="test",
        total_jobs=len(jobs),
        packet_declarations=[],
        binding_summary={"totals": {"bound": 0, "ambiguous": 0, "unbound": 0}},
        unresolved_routes=[],
    )


# ---------------------------------------------------------------------------
# _adapt_plan_jobs_to_type_flow_request
# ---------------------------------------------------------------------------


def test_adapter_empty_spec_returns_empty_nodes_edges():
    request = _adapt_plan_jobs_to_type_flow_request({})
    assert request == {"nodes": [], "edges": []}


def test_adapter_jobs_to_nodes_uses_label_and_task_type():
    spec = {
        "jobs": [
            {"label": "j1", "task_type": "trigger"},
            {"label": "j2", "task_type": "research"},
        ]
    }
    request = _adapt_plan_jobs_to_type_flow_request(spec)
    assert request["nodes"] == [
        {"node_id": "j1", "route": "trigger"},
        {"node_id": "j2", "route": "research"},
    ]
    assert request["edges"] == []


def test_adapter_depends_on_becomes_edges():
    spec = {
        "jobs": [
            {"label": "j1", "task_type": "trigger"},
            {"label": "j2", "task_type": "research", "depends_on": ["j1"]},
            {"label": "j3", "task_type": "analyze", "depends_on": ["j2", "j1"]},
        ]
    }
    request = _adapt_plan_jobs_to_type_flow_request(spec)
    assert {"from_node_id": "j1", "to_node_id": "j2"} in request["edges"]
    assert {"from_node_id": "j2", "to_node_id": "j3"} in request["edges"]
    assert {"from_node_id": "j1", "to_node_id": "j3"} in request["edges"]
    assert len(request["edges"]) == 3


def test_adapter_skips_jobs_without_label():
    spec = {
        "jobs": [
            {"label": "", "task_type": "trigger"},  # blank label skipped for edges
            {"label": "j2", "task_type": "research", "depends_on": [""]},  # blank dep
        ]
    }
    request = _adapt_plan_jobs_to_type_flow_request(spec)
    # Nodes still include the blank-label entry (node_id is blank str).
    assert len(request["nodes"]) == 2
    # Edges skip when either side is blank.
    assert request["edges"] == []


# ---------------------------------------------------------------------------
# _validate_composed_plan_type_flow
# ---------------------------------------------------------------------------


def test_validator_empty_plan_returns_empty():
    proposed = _make_proposed([])
    assert _validate_composed_plan_type_flow(proposed) == []


def test_validator_single_trigger_is_satisfied():
    """A single trigger node produces input_text ambient; nothing consumes
    anything that isn't ambient — the flow is satisfied."""
    proposed = _make_proposed([{"label": "j1", "task_type": "trigger"}])
    errors = _validate_composed_plan_type_flow(proposed)
    assert errors == []


def test_validator_trigger_to_research_is_satisfied():
    """Trigger produces input_text, research consumes_any input_text →
    flow satisfied."""
    proposed = _make_proposed(
        [
            {"label": "j1", "task_type": "trigger"},
            {"label": "j2", "task_type": "research", "depends_on": ["j1"]},
        ]
    )
    errors = _validate_composed_plan_type_flow(proposed)
    assert errors == []


def test_validator_returns_list_on_degraded_substrate(monkeypatch):
    """When the type_contracts module cannot be imported, validator returns
    [] — compose_plan_from_intent must not block on optional substrate."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "runtime.workflow_type_contracts":
            raise ImportError("type_contracts unavailable (simulated)")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    proposed = _make_proposed([{"label": "j1", "task_type": "trigger"}])
    result = _validate_composed_plan_type_flow(proposed)
    assert result == []


def test_validator_always_returns_list_never_none():
    proposed = _make_proposed([{"label": "j1", "task_type": "trigger"}])
    result = _validate_composed_plan_type_flow(proposed)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# compose_plan_from_intent integration (mocked propose_plan)
# ---------------------------------------------------------------------------


def test_compose_attaches_type_flow_errors_to_warnings(monkeypatch):
    """When validate returns errors, they append to ProposedPlan.warnings."""
    from runtime import intent_composition

    def fake_validate(proposed):
        return ["workflow.type_flow.unsatisfied_inputs:j1:research_findings"]

    monkeypatch.setattr(
        intent_composition,
        "_validate_composed_plan_type_flow",
        fake_validate,
    )
    monkeypatch.setattr(
        intent_composition,
        "_best_effort_emit",
        lambda *a, **k: None,
    )

    from dataclasses import replace
    from runtime.spec_materializer import ProposedPlan

    def fake_propose(plan_dict, *, conn, workdir=None):
        return ProposedPlan(
            spec_dict={"name": "n", "workflow_id": "wf", "jobs": []},
            preview={},
            warnings=["existing warning"],
            workflow_id="wf",
            spec_name="n",
            total_jobs=0,
            packet_declarations=[],
            binding_summary={"totals": {"bound": 0, "ambiguous": 0, "unbound": 0}},
            unresolved_routes=[],
        )

    monkeypatch.setattr(intent_composition, "propose_plan", fake_propose)

    # Also stub decompose + packets_from_steps
    from runtime.intent_decomposition import DecomposedIntent, StepIntent

    def fake_decompose(intent, *, allow_single_step=False):
        return DecomposedIntent(
            intent=intent,
            steps=[StepIntent(index=0, text=intent, stage_hint=None, raw_marker="")],
            detection_mode="single",
            rationale="",
        )

    monkeypatch.setattr(intent_composition, "decompose_intent", fake_decompose)

    result = intent_composition.compose_plan_from_intent(
        "Do a thing",
        conn=object(),
        allow_single_step=True,
    )
    assert "existing warning" in result.warnings
    assert (
        "workflow.type_flow.unsatisfied_inputs:j1:research_findings"
        in result.warnings
    )


def test_compose_no_errors_leaves_warnings_unchanged(monkeypatch):
    """When validate returns [], warnings list is untouched (no phantom
    entries appended)."""
    from runtime import intent_composition

    monkeypatch.setattr(
        intent_composition,
        "_validate_composed_plan_type_flow",
        lambda proposed: [],
    )
    monkeypatch.setattr(
        intent_composition,
        "_best_effort_emit",
        lambda *a, **k: None,
    )

    from runtime.spec_materializer import ProposedPlan

    baseline_warnings = ["baseline warning"]

    def fake_propose(plan_dict, *, conn, workdir=None):
        return ProposedPlan(
            spec_dict={"name": "n", "workflow_id": "wf", "jobs": []},
            preview={},
            warnings=list(baseline_warnings),
            workflow_id="wf",
            spec_name="n",
            total_jobs=0,
            packet_declarations=[],
            binding_summary={"totals": {"bound": 0, "ambiguous": 0, "unbound": 0}},
            unresolved_routes=[],
        )

    monkeypatch.setattr(intent_composition, "propose_plan", fake_propose)

    from runtime.intent_decomposition import DecomposedIntent, StepIntent

    monkeypatch.setattr(
        intent_composition,
        "decompose_intent",
        lambda intent, *, allow_single_step=False: DecomposedIntent(
            intent=intent,
            steps=[StepIntent(index=0, text=intent, stage_hint=None, raw_marker="")],
            detection_mode="single",
            rationale="",
        ),
    )

    result = intent_composition.compose_plan_from_intent(
        "Do a thing",
        conn=object(),
        allow_single_step=True,
    )
    assert result.warnings == baseline_warnings
