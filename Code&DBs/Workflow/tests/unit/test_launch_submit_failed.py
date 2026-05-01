"""Regression tests: launch_* must raise LaunchSubmitFailedError on submit failure.

Before this fix, when ``submit_workflow_command`` returned status='failed' or
'approval_required' (no run_id), the launch_* helpers coerced the missing
``run_id`` to an empty string and constructed a LaunchReceipt anyway. The
caller had no signal that nothing executed — ok:True with empty run_id.

The fix routes the failed submit through ``LaunchSubmitFailedError`` so every
wrapper above (CLI, MCP, HTTP) gets a structured error_code instead of a lie.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime import spec_materializer
from runtime.spec_materializer import (
    ApprovedPlan,
    MaterializedSpec,
    LaunchSubmitFailedError,
    Plan,
    PlanPacket,
    ProposedPlan,
    _ensure_run_id_or_raise,
    approve_proposed_plan,
    launch_approved,
    launch_plan,
    launch_proposed,
    propose_plan,
)


def _stub_compile_spec(intent_dict, *, conn):
    label = intent_dict.get("label") or intent_dict["description"].split()[0].lower()
    return (
        MaterializedSpec(
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


def _make_plan() -> Plan:
    return Plan(
        name="failing_submit_probe",
        packets=[
            PlanPacket(
                description="probe a failing submit",
                write=["artifacts/workflow/"],
                stage="build",
                label="probe",
            ),
        ],
    )


def test_ensure_run_id_or_raise_passes_on_queued() -> None:
    submit_result = {
        "status": "queued",
        "run_id": "workflow_real_run",
        "total_jobs": 1,
    }
    assert _ensure_run_id_or_raise(submit_result, spec_name="ok") == "workflow_real_run"


def test_ensure_run_id_or_raise_passes_on_running() -> None:
    submit_result = {
        "status": "running",
        "run_id": "workflow_real_run",
        "total_jobs": 1,
    }
    assert _ensure_run_id_or_raise(submit_result, spec_name="ok") == "workflow_real_run"


def test_ensure_run_id_or_raise_rejects_failed_status() -> None:
    submit_result = {
        "status": "failed",
        "error_code": "control.command.workflow_submit_missing_run_id",
        "error_detail": "no run materialized",
    }
    with pytest.raises(LaunchSubmitFailedError) as excinfo:
        _ensure_run_id_or_raise(submit_result, spec_name="probe")
    assert excinfo.value.status == "failed"
    assert excinfo.value.error_code == "control.command.workflow_submit_missing_run_id"
    assert excinfo.value.spec_name == "probe"


def test_ensure_run_id_or_raise_rejects_approval_required() -> None:
    submit_result = {
        "status": "approval_required",
        "command_id": "cmd-x",
    }
    with pytest.raises(LaunchSubmitFailedError) as excinfo:
        _ensure_run_id_or_raise(submit_result, spec_name="probe")
    assert excinfo.value.status == "approval_required"


def test_ensure_run_id_or_raise_rejects_empty_run_id() -> None:
    submit_result = {"status": "queued", "run_id": ""}
    with pytest.raises(LaunchSubmitFailedError):
        _ensure_run_id_or_raise(submit_result, spec_name="probe")


def test_ensure_run_id_or_raise_rejects_missing_run_id() -> None:
    submit_result = {"status": "queued"}
    with pytest.raises(LaunchSubmitFailedError):
        _ensure_run_id_or_raise(submit_result, spec_name="probe")


def test_launch_plan_raises_on_failed_submit(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "compile_spec", _stub_compile_spec)

    def _failing_submit(conn, **kwargs):
        return {
            "status": "failed",
            "error_code": "control.command.workflow_submit_missing_run_id",
            "error_detail": "no run materialized",
            "command": {"command_id": "cmd-failing"},
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(
        control_commands_mod, "submit_workflow_command", _failing_submit
    )

    with pytest.raises(LaunchSubmitFailedError) as excinfo:
        launch_plan(_make_plan(), conn=_FakeConn(), workdir="/repo")
    err = excinfo.value
    assert err.status == "failed"
    assert err.error_code == "control.command.workflow_submit_missing_run_id"
    assert err.spec_name == "failing_submit_probe"


def test_launch_proposed_raises_on_failed_submit(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "compile_spec", _stub_compile_spec)

    def _failing_submit(conn, **kwargs):
        return {"status": "approval_required", "command_id": "cmd-needs-approval"}

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(
        control_commands_mod, "submit_workflow_command", _failing_submit
    )

    proposed = propose_plan(_make_plan(), conn=_FakeConn(), workdir="/repo")
    with pytest.raises(LaunchSubmitFailedError) as excinfo:
        launch_proposed(proposed, conn=_FakeConn())
    assert excinfo.value.status == "approval_required"


def test_launch_approved_raises_on_failed_submit(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "compile_spec", _stub_compile_spec)

    def _failing_submit(conn, **kwargs):
        return {
            "status": "failed",
            "error_code": "control.command.workflow_submit_missing_run_id",
            "error_detail": "dispatch path could not materialize run",
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(
        control_commands_mod, "submit_workflow_command", _failing_submit
    )

    proposed = propose_plan(_make_plan(), conn=_FakeConn(), workdir="/repo")
    approved = approve_proposed_plan(proposed, approved_by="probe@praxis")
    with pytest.raises(LaunchSubmitFailedError) as excinfo:
        launch_approved(approved, conn=_FakeConn())
    assert excinfo.value.status == "failed"


def test_launch_submit_failed_error_carries_full_submit_result() -> None:
    submit_result = {
        "status": "failed",
        "error_code": "control.command.workflow_submit_missing_run_id",
        "error_detail": "details here",
        "command_id": "cmd-x",
        "extra": "preserved",
    }
    err = LaunchSubmitFailedError(submit_result, spec_name="probe")
    assert err.submit_result == submit_result
    assert err.submit_result is not submit_result  # defensive copy
    assert "probe" in str(err)
    assert "control.command.workflow_submit_missing_run_id" in str(err)
