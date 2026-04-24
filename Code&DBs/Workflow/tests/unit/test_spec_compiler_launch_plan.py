from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime import spec_compiler
from runtime.spec_compiler import (
    CompiledSpec,
    LaunchReceipt,
    Plan,
    PlanPacket,
    ProposedPlan,
    compile_plan,
    launch_plan,
    launch_proposed,
    propose_plan,
)


def _stub_compile_spec(intent_dict, *, conn):
    label = intent_dict.get("label") or intent_dict["description"].split()[0].lower()
    return (
        CompiledSpec(
            prompt=f"PROMPT({intent_dict['description']})",
            scope_write=list(intent_dict.get("write") or []),
            scope_read=intent_dict.get("read"),
            capabilities=["capability.code.python"],
            tier="mid",
            label=f"{intent_dict['stage']}:{label}",
            task_type=intent_dict["stage"],
            verify_refs=[f"verify.{label}"],
            workspace_ref="workspace.default",
            runtime_profile_ref="runtime.default",
        ),
        [],
    )


class _FakeConn:
    pass


def test_compile_plan_translates_packets_into_multi_job_spec(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    plan = {
        "name": "wave_0_authority",
        "why": "fix bug tracker before burning down dependent bugs",
        "packets": [
            {
                "description": "fix bug evidence authority so FIXED requires verifier linkage",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "bug-authority",
                "bug_ref": "BUG-175EB9F3",
            },
            {
                "description": "require superseding evidence before FIXED transitions",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "fixed-transition-evidence",
                "bug_ref": "BUG-9B812B32",
                "depends_on": ["bug-authority"],
            },
        ],
    }

    spec_dict, warnings = compile_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert warnings == []
    assert spec_dict["name"] == "wave_0_authority"
    assert spec_dict["why"] == "fix bug tracker before burning down dependent bugs"
    assert spec_dict["workflow_id"].startswith("plan.")
    assert spec_dict["phase"] == "build"
    assert spec_dict["workdir"] == "/repo"
    assert len(spec_dict["jobs"]) == 2

    first, second = spec_dict["jobs"]
    assert first["label"] == "bug-authority"
    assert first["agent"] == "auto/build"
    assert first["write_scope"] == ["Code&DBs/Workflow/runtime/bugs.py"]
    assert first["workdir"] == "/repo"
    assert first["task_type"] == "build"
    assert first["verify_refs"] == ["verify.bug-authority"]
    assert first["bug_ref"] == "BUG-175EB9F3"
    assert "depends_on" not in first

    assert second["label"] == "fixed-transition-evidence"
    assert second["depends_on"] == ["bug-authority"]
    assert second["bug_ref"] == "BUG-9B812B32"


def test_launch_plan_routes_through_command_bus(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    captured: dict[str, object] = {}

    def _fake_submit_command(conn, **kwargs):
        captured["conn"] = conn
        captured["kwargs"] = kwargs
        return {
            "run_id": "workflow_abc123",
            "status": "queued",
            "total_jobs": len(kwargs["inline_spec"]["jobs"]),
            "spec_name": kwargs["inline_spec"]["name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit_command)

    plan = Plan(
        name="bug_burn_wave_0",
        packets=[
            PlanPacket(
                description="fix bug authority",
                write=["Code&DBs/Workflow/runtime/bugs.py"],
                stage="build",
                label="bug-authority",
                bug_ref="BUG-175EB9F3",
            ),
        ],
    )

    receipt = launch_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert isinstance(receipt, LaunchReceipt)
    assert receipt.run_id == "workflow_abc123"
    assert receipt.spec_name == "bug_burn_wave_0"
    assert receipt.total_jobs == 1
    assert receipt.packet_map == [
        {
            "label": "bug-authority",
            "bug_ref": "BUG-175EB9F3",
            "agent": "auto/build",
            "stage": "build",
        }
    ]

    command_kwargs = captured["kwargs"]
    assert command_kwargs["requested_by_kind"] == "launch_plan"
    assert command_kwargs["requested_by_ref"] == "bug_burn_wave_0"
    assert command_kwargs["spec_name"] == "bug_burn_wave_0"
    assert command_kwargs["total_jobs"] == 1
    assert command_kwargs["dispatch_reason"] == "launch_plan:bug_burn_wave_0"
    inline_spec = command_kwargs["inline_spec"]
    assert inline_spec["name"] == "bug_burn_wave_0"
    assert inline_spec["jobs"][0]["prompt"].startswith("PROMPT(")


def test_launch_plan_rejects_empty_packets() -> None:
    with pytest.raises(ValueError, match="at least one packet"):
        compile_plan({"name": "empty", "packets": []}, conn=_FakeConn())


def test_launch_plan_deduplicates_colliding_labels(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    plan = {
        "name": "same_label_twice",
        "packets": [
            {"description": "first pass", "write": ["a.py"], "stage": "build", "label": "do-it"},
            {"description": "second pass", "write": ["b.py"], "stage": "build", "label": "do-it"},
        ],
    }
    spec_dict, _ = compile_plan(plan, conn=_FakeConn(), workdir="/repo")
    labels = [job["label"] for job in spec_dict["jobs"]]
    assert labels == ["do-it", "do-it__2"]


def test_propose_plan_returns_spec_preview_and_declarations_without_submit(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    # Preview stub — replicates the preview_workflow_execution payload shape
    # we care about without needing a real Postgres connection.
    def _fake_preview(conn, *, inline_spec, **_kwargs):
        return {
            "action": "preview",
            "jobs": [
                {
                    "label": job["label"],
                    "resolved_agent": "openai/gpt-5.4-mini",
                    "route_status": "resolved",
                }
                for job in inline_spec["jobs"]
            ],
            "warnings": [],
        }

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _fake_preview)

    # Submit must NOT be called in preview mode. If it is, the test fails.
    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit_workflow_command should not run in preview")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    plan = {
        "name": "preview_wave",
        "packets": [
            {
                "description": "fix bug evidence authority",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "bug-authority",
                "bug_ref": "BUG-175EB9F3",
            }
        ],
    }

    proposed = propose_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert isinstance(proposed, ProposedPlan)
    assert proposed.spec_name == "preview_wave"
    assert proposed.total_jobs == 1
    assert proposed.spec_dict["jobs"][0]["label"] == "bug-authority"
    assert proposed.preview["jobs"][0]["resolved_agent"] == "openai/gpt-5.4-mini"

    # packet_declarations expose what the caller declared so Moon / CLI
    # can render declared-vs-derived side by side.
    declaration = proposed.packet_declarations[0]
    assert declaration["label"] == "bug-authority"
    assert declaration["declared_description"] == "fix bug evidence authority"
    assert declaration["declared_write"] == ["Code&DBs/Workflow/runtime/bugs.py"]
    assert declaration["declared_stage"] == "build"
    assert declaration["declared_bug_ref"] == "BUG-175EB9F3"


def test_launch_proposed_submits_previously_built_spec(monkeypatch) -> None:
    monkeypatch.setattr(spec_compiler, "compile_spec", _stub_compile_spec)

    def _fake_preview(conn, *, inline_spec, **_kwargs):
        return {"action": "preview", "jobs": [], "warnings": []}

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _fake_preview)

    proposed = propose_plan(
        {
            "name": "two_phase",
            "packets": [
                {"description": "do a thing", "write": ["x.py"], "stage": "build", "label": "thing-1"},
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )

    captured: dict[str, object] = {}

    def _fake_submit_command(conn, **kwargs):
        captured["kwargs"] = kwargs
        return {
            "run_id": "workflow_def456",
            "status": "queued",
            "total_jobs": kwargs["total_jobs"],
            "spec_name": kwargs["spec_name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit_command)

    receipt = launch_proposed(proposed, conn=_FakeConn())

    assert isinstance(receipt, LaunchReceipt)
    assert receipt.run_id == "workflow_def456"
    assert receipt.spec_name == "two_phase"
    assert receipt.total_jobs == 1
    assert captured["kwargs"]["dispatch_reason"] == "launch_proposed:two_phase"
    assert captured["kwargs"]["inline_spec"] is proposed.spec_dict
