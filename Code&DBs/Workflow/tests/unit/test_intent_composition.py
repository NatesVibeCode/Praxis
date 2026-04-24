from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime import spec_compiler
from runtime.intent_composition import (
    ComposeAndLaunchBlocked,
    compose_and_launch,
    compose_plan_from_intent,
    packets_from_steps,
)
from runtime.intent_decomposition import (
    DecompositionRequiresLLMError,
    StepIntent,
)
from runtime.spec_compiler import CompiledSpec, LaunchReceipt, PlanPacket


class _FakeConn:
    pass


def _stub_compile_spec(intent_dict, *, conn):
    label = intent_dict.get("label") or intent_dict["description"].split()[0].lower()
    return (
        CompiledSpec(
            prompt=f"PROMPT({intent_dict['description']})",
            scope_write=list(intent_dict.get("write") or []),
            capabilities=["cap"],
            tier="mid",
            label=label,
            task_type=intent_dict["stage"],
            verify_refs=["v"],
        ),
        [],
    )


def _install_quiet_preview_and_binding(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    import runtime.intent_binding as intent_binding_mod

    def _fake_bind(intent, *, conn, object_kinds=None):
        return intent_binding_mod.BoundIntent(intent=intent)

    monkeypatch.setattr(intent_binding_mod, "bind_data_pills", _fake_bind)

    import runtime.workflow._admission as admission_mod

    def _fake_preview(conn, *, inline_spec, **_kwargs):
        return {
            "action": "preview",
            "jobs": [
                {
                    "label": job["label"],
                    "requested_agent": job.get("agent", "auto/build"),
                    "resolved_agent": "openai/gpt-5.4-mini",
                    "route_status": "resolved",
                }
                for job in inline_spec["jobs"]
            ],
            "warnings": [],
        }

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _fake_preview)


def test_packets_from_steps_translates_each_step() -> None:
    steps = [
        StepIntent(index=0, text="Add timezone column", raw_marker="1", stage_hint="build"),
        StepIntent(index=1, text="Verify migration", raw_marker="2", stage_hint="test"),
        StepIntent(index=2, text="Refactor UI", raw_marker="3", stage_hint=None),
    ]
    packets = packets_from_steps(steps, default_stage="build")

    assert len(packets) == 3
    assert all(isinstance(p, PlanPacket) for p in packets)
    assert [p.label for p in packets] == ["step_1", "step_2", "step_3"]
    assert [p.stage for p in packets] == ["build", "test", "build"]
    assert [p.write for p in packets] == [["."], ["."], ["."]]
    assert packets[0].description == "Add timezone column"


def test_packets_from_steps_honors_per_step_write_scope() -> None:
    steps = [
        StepIntent(index=0, text="A", raw_marker="1", stage_hint="build"),
        StepIntent(index=1, text="B", raw_marker="2", stage_hint="fix"),
    ]
    packets = packets_from_steps(
        steps,
        write_scope_per_step=[
            ["src/a.py"],
            ["src/b.py", "tests/test_b.py"],
        ],
    )
    assert packets[0].write == ["src/a.py"]
    assert packets[1].write == ["src/b.py", "tests/test_b.py"]


def test_packets_from_steps_rejects_mismatched_write_scope_length() -> None:
    steps = [StepIntent(index=0, text="A", raw_marker="1", stage_hint=None)]
    with pytest.raises(ValueError, match="counts must match"):
        packets_from_steps(steps, write_scope_per_step=[["a"], ["b"]])


def test_packets_from_steps_rejects_empty_steps() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        packets_from_steps([])


def test_compose_plan_from_intent_happy_path(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    intent = (
        "1. Add a timezone column to users.\n"
        "2. Backfill existing rows with UTC.\n"
        "3. Update the profile UI."
    )
    proposed = compose_plan_from_intent(
        intent,
        conn=_FakeConn(),
        plan_name="timezone_rollout",
        why="Personalization support.",
        workdir="/repo",
    )

    assert proposed.spec_name == "timezone_rollout"
    assert proposed.total_jobs == 3
    assert proposed.spec_dict["why"] == "Personalization support."
    labels = [job["label"] for job in proposed.spec_dict["jobs"]]
    assert labels == ["step_1", "step_2", "step_3"]
    # The 'Add' verb maps to 'build'; Backfill/Update fall through to the
    # default stage since they aren't in the conservative verb map.
    stages = [job["task_type"] for job in proposed.spec_dict["jobs"]]
    assert stages == ["build", "build", "build"]
    # Workspace-root default triggers the broad-scope warning.
    assert any("workspace root" in w for w in proposed.warnings) is False  # not a from_* plan
    # Preview resolved every step — no unresolved_routes.
    assert proposed.unresolved_routes == []


def test_compose_plan_from_intent_auto_plan_name_when_absent(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    intent = "1. Do thing one.\n2. Do thing two."
    proposed = compose_plan_from_intent(intent, conn=_FakeConn(), workdir="/repo")

    # Auto name encodes detection_mode + step count.
    assert proposed.spec_name.startswith("compose_plan.numbered_list")
    assert "2_steps" in proposed.spec_name


def test_compose_plan_from_intent_free_prose_fails_closed(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    intent = "Make the dashboard faster by reducing API calls."
    with pytest.raises(DecompositionRequiresLLMError, match="no explicit step markers"):
        compose_plan_from_intent(intent, conn=_FakeConn(), workdir="/repo")


def test_compose_plan_from_intent_allow_single_step_escape(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    intent = "Investigate the staging checkout regression and write up findings."
    proposed = compose_plan_from_intent(
        intent,
        conn=_FakeConn(),
        allow_single_step=True,
        workdir="/repo",
    )
    assert proposed.total_jobs == 1
    assert proposed.spec_dict["jobs"][0]["label"] == "step_1"
    # First verb 'Investigate' → research stage.
    assert proposed.spec_dict["jobs"][0]["task_type"] == "research"


def test_compose_plan_from_intent_honors_per_step_write_scope(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    intent = (
        "1. Update the users schema.\n"
        "2. Migrate existing rows.\n"
        "3. Update the UI."
    )
    proposed = compose_plan_from_intent(
        intent,
        conn=_FakeConn(),
        write_scope_per_step=[
            ["Code&DBs/Databases/migrations/"],
            ["Code&DBs/Workflow/scripts/backfill.py"],
            ["Code&DBs/Workflow/surfaces/app/src/"],
        ],
        workdir="/repo",
    )
    scopes = [job["write_scope"] for job in proposed.spec_dict["jobs"]]
    assert scopes == [
        ["Code&DBs/Databases/migrations/"],
        ["Code&DBs/Workflow/scripts/backfill.py"],
        ["Code&DBs/Workflow/surfaces/app/src/"],
    ]


def _install_submit_command_stub(monkeypatch, *, run_id: str = "workflow_composed_abc"):
    """Wire submit_workflow_command to a stub so compose_and_launch can complete."""

    captured: dict[str, object] = {}

    def _fake_submit(conn, **kwargs):
        captured["kwargs"] = kwargs
        return {
            "run_id": run_id,
            "status": "queued",
            "total_jobs": kwargs["total_jobs"],
            "spec_name": kwargs["spec_name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit)
    return captured


def test_compose_and_launch_happy_path(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)
    captured = _install_submit_command_stub(monkeypatch)

    receipt = compose_and_launch(
        "1. Add timezone column\n2. Backfill existing rows\n3. Update UI",
        conn=_FakeConn(),
        approved_by="ci@praxis",
        approval_note="CI flow",
        plan_name="tz_rollout",
    )

    assert isinstance(receipt, LaunchReceipt)
    assert receipt.run_id == "workflow_composed_abc"
    assert receipt.spec_name == "tz_rollout"
    # Audit trail distinguishes end-to-end pipeline from plain launch_approved.
    assert captured["kwargs"]["requested_by_kind"] == "compose_and_launch"
    # approved_by threads into requested_by_ref for the audit trail.
    assert captured["kwargs"]["requested_by_ref"] == "ci@praxis"


def test_compose_and_launch_refuses_unresolved_routes_by_default(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    import runtime.intent_binding as intent_binding_mod

    monkeypatch.setattr(
        intent_binding_mod,
        "bind_data_pills",
        lambda intent, *, conn, object_kinds=None: intent_binding_mod.BoundIntent(intent=intent),
    )

    import runtime.workflow._admission as admission_mod

    def _unresolved_preview(conn, *, inline_spec, **_kwargs):
        return {
            "action": "preview",
            "jobs": [
                {
                    "label": job["label"],
                    "requested_agent": "auto/build",
                    "resolved_agent": None,
                    "route_status": "unresolved",
                    "route_reason": "no admitted route",
                }
                for job in inline_spec["jobs"]
            ],
            "warnings": [],
        }

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _unresolved_preview)

    # Submit must NOT be called — compose_and_launch fails closed first.
    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit should not run when routes are unresolved")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    with pytest.raises(ComposeAndLaunchBlocked) as exc_info:
        compose_and_launch(
            "1. Step one\n2. Step two",
            conn=_FakeConn(),
            approved_by="ci@praxis",
        )
    reasons = exc_info.value.reasons
    assert any(entry["kind"] == "unresolved_routes" for entry in reasons)


def test_compose_and_launch_refuses_unbound_pills_by_default(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    # Override binding to surface an unbound pill on one step.
    import runtime.intent_binding as intent_binding_mod

    def _fake_bind_with_unbound(intent, *, conn, object_kinds=None):
        return intent_binding_mod.BoundIntent(
            intent=intent,
            unbound=[
                intent_binding_mod.UnboundCandidate(
                    matched_span="users.first_nm",
                    object_kind="users",
                    field_path="first_nm",
                    reason="field_path_not_in_object",
                )
            ],
        )

    monkeypatch.setattr(intent_binding_mod, "bind_data_pills", _fake_bind_with_unbound)

    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit should not run when pills are unbound")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    with pytest.raises(ComposeAndLaunchBlocked) as exc_info:
        compose_and_launch(
            "1. Copy users.first_nm somewhere\n2. Verify it copied",
            conn=_FakeConn(),
            approved_by="ci@praxis",
        )
    reasons = exc_info.value.reasons
    assert any(entry["kind"] == "unbound_pills" for entry in reasons)


def test_compose_and_launch_refuses_over_budget(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit should not run when budget cap is exceeded")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    # 3 build-stage jobs × 4000 estimated output tokens each = 12000. Cap at
    # 5000 trips the budget block.
    with pytest.raises(ComposeAndLaunchBlocked) as exc_info:
        compose_and_launch(
            "1. Do one\n2. Do two\n3. Do three",
            conn=_FakeConn(),
            approved_by="ci@praxis",
            budget_cap_tokens=5000,
        )
    reasons = exc_info.value.reasons
    budget_reason = next(
        entry for entry in reasons if entry["kind"] == "budget_exceeded"
    )
    assert budget_reason["cap"] == 5000
    assert budget_reason["estimated_total_tokens"] > 5000


def test_compose_and_launch_requires_approved_by(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    with pytest.raises(ValueError, match="approved_by is required"):
        compose_and_launch(
            "1. A\n2. B",
            conn=_FakeConn(),
            approved_by="",
        )


def test_compose_and_launch_allows_explicit_safety_override(monkeypatch) -> None:
    """Caller can disable a safety check explicitly — but not by default."""
    _install_quiet_preview_and_binding(monkeypatch)

    # Simulate unbound pills but caller explicitly opts out of the refusal.
    import runtime.intent_binding as intent_binding_mod

    def _fake_bind_with_unbound(intent, *, conn, object_kinds=None):
        return intent_binding_mod.BoundIntent(
            intent=intent,
            unbound=[
                intent_binding_mod.UnboundCandidate(
                    matched_span="users.x",
                    object_kind="users",
                    field_path="x",
                    reason="field_path_not_in_object",
                )
            ],
        )

    monkeypatch.setattr(intent_binding_mod, "bind_data_pills", _fake_bind_with_unbound)
    _install_submit_command_stub(monkeypatch)

    receipt = compose_and_launch(
        "1. Touch users.x\n2. Verify users.x",
        conn=_FakeConn(),
        approved_by="ci@praxis",
        refuse_unbound_pills=False,  # explicit opt-out
    )
    assert isinstance(receipt, LaunchReceipt)


def _install_event_capture(monkeypatch) -> list[dict[str, object]]:
    """Wire emit_system_event to a list sink so tests can assert emissions."""
    events: list[dict[str, object]] = []

    def _fake_emit(conn, *, event_type, source_id, source_type, payload):
        events.append(
            {
                "event_type": event_type,
                "source_id": source_id,
                "source_type": source_type,
                "payload": dict(payload),
            }
        )

    import runtime.intent_composition as composition_mod

    monkeypatch.setattr(composition_mod, "emit_system_event", _fake_emit)
    return events


def test_compose_plan_emits_composed_event(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)
    events = _install_event_capture(monkeypatch)

    compose_plan_from_intent(
        "1. Step one\n2. Step two",
        conn=_FakeConn(),
        workdir="/repo",
    )

    composed = [e for e in events if e["event_type"] == "plan.composed"]
    assert len(composed) == 1
    payload = composed[0]["payload"]
    assert payload["detection_mode"] == "numbered_list"
    assert payload["step_count"] == 2
    assert payload["total_jobs"] == 2
    assert payload["has_unresolved_routes"] is False
    assert payload["unbound_pill_count"] == 0


def test_compose_and_launch_emits_approved_then_launched(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)
    _install_submit_command_stub(monkeypatch, run_id="workflow_events_001")
    events = _install_event_capture(monkeypatch)

    compose_and_launch(
        "1. Step one\n2. Step two",
        conn=_FakeConn(),
        approved_by="ci@praxis",
    )

    ordered_types = [e["event_type"] for e in events]
    assert ordered_types == ["plan.composed", "plan.approved", "plan.launched"]

    approved_payload = events[1]["payload"]
    assert approved_payload["approved_by"] == "ci@praxis"
    assert approved_payload["proposal_hash"]

    launched_payload = events[2]["payload"]
    assert launched_payload["run_id"] == "workflow_events_001"
    assert launched_payload["approved_by"] == "ci@praxis"


def test_compose_and_launch_blocked_emits_blocked_event(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)
    events = _install_event_capture(monkeypatch)

    # Force budget cap overrun — 3 build-stage jobs ≈ 12000 output tokens.
    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit should not run when blocked")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    with pytest.raises(ComposeAndLaunchBlocked):
        compose_and_launch(
            "1. One\n2. Two\n3. Three",
            conn=_FakeConn(),
            approved_by="ci@praxis",
            budget_cap_tokens=500,
        )

    ordered_types = [e["event_type"] for e in events]
    # composed fires, then blocked; no approved / launched because the
    # pipeline stopped at the safeguard.
    assert ordered_types == ["plan.composed", "plan.blocked"]
    blocked_payload = events[1]["payload"]
    assert blocked_payload["approved_by_attempted"] == "ci@praxis"
    assert any(
        entry["kind"] == "budget_exceeded"
        for entry in blocked_payload["blocked_reasons"]
    )


def test_event_emission_failures_do_not_break_primary_flow(monkeypatch) -> None:
    _install_quiet_preview_and_binding(monkeypatch)

    def _failing_emit(*_args, **_kwargs):
        raise RuntimeError("event bus is down")

    import runtime.intent_composition as composition_mod

    monkeypatch.setattr(composition_mod, "emit_system_event", _failing_emit)

    # Primary flow must still complete even though every emit raises.
    proposed = compose_plan_from_intent(
        "1. Step one\n2. Step two",
        conn=_FakeConn(),
        workdir="/repo",
    )
    assert proposed.total_jobs == 2
