from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.operations.commands import plan_orchestration
from runtime.spec_materializer import LaunchReceipt, ProposedPlan


class _Subsystems:
    def __init__(self) -> None:
        self.conn = object()

    def get_pg_conn(self):
        return self.conn


def test_compose_plan_handler_hoists_workflow_id_into_authority_event_payload(monkeypatch):
    import runtime.intent_composition as intent_composition

    def _fake_compose_plan_from_intent(*_args, **_kwargs):
        return ProposedPlan(
            spec_dict={"workflow_id": "plan.alpha", "jobs": []},
            preview={},
            warnings=["narrow write scope before launch"],
            workflow_id="plan.alpha",
            spec_name="alpha",
            total_jobs=2,
            packet_declarations=[{"label": "step_1"}, {"label": "step_2"}],
            binding_summary={"unbound_refs": []},
            unresolved_routes=[{"label": "step_2"}],
        )

    monkeypatch.setattr(
        intent_composition,
        "compose_plan_from_intent",
        _fake_compose_plan_from_intent,
    )

    result = plan_orchestration.handle_compose_plan(
        plan_orchestration.ComposePlanCommand(intent="1. Build alpha"),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["event_payload"]["workflow_id"] == "plan.alpha"
    assert result["event_payload"]["spec_name"] == "alpha"
    assert result["event_payload"]["total_jobs"] == 2
    assert result["event_payload"]["warning_count"] == 1
    assert result["event_payload"]["unresolved_route_count"] == 1


def test_launch_plan_handler_hoists_run_fields_into_authority_event_payload(monkeypatch):
    import runtime.spec_materializer as spec_materializer

    def _fake_launch_plan(*_args, **_kwargs):
        return LaunchReceipt(
            run_id="workflow_run_123",
            workflow_id="plan.alpha",
            spec_name="alpha",
            total_jobs=2,
            packet_map=[{"label": "step_1"}, {"label": "step_2"}],
            warnings=[],
        )

    monkeypatch.setattr(spec_materializer, "launch_plan", _fake_launch_plan)

    result = plan_orchestration.handle_launch_plan(
        plan_orchestration.LaunchPlanCommand(
            plan={"name": "alpha", "packets": []},
            requested_by_kind="workflow",
            requested_by_ref="ci@praxis",
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["mode"] == "submitted"
    assert result["event_payload"]["workflow_id"] == "plan.alpha"
    assert result["event_payload"]["run_id"] == "workflow_run_123"
    assert result["event_payload"]["spec_name"] == "alpha"
    assert result["event_payload"]["packet_labels"] == ["step_1", "step_2"]
    assert result["event_payload"]["requested_by_ref"] == "ci@praxis"
