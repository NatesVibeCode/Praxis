from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime import spec_compiler
from runtime.intent_composition import (
    compose_plan_from_intent,
    packets_from_steps,
)
from runtime.intent_decomposition import (
    DecompositionRequiresLLMError,
    StepIntent,
)
from runtime.spec_compiler import CompiledSpec, PlanPacket


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
