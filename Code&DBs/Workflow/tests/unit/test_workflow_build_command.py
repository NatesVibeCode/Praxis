from types import SimpleNamespace
from unittest.mock import patch

from runtime.operations.commands.workflow_build import (
    MutateWorkflowBuildCommand,
    WorkflowBuildMutationDeprecatedError,
    handle_mutate_workflow_build,
)


def test_handle_mutate_workflow_build_uses_runtime_build_moment() -> None:
    command = MutateWorkflowBuildCommand(
        workflow_id="wf_build",
        subpath="attachments",
        body={"node_id": "step-001", "authority_kind": "reference", "authority_ref": "@gmail/search"},
    )
    conn = object()
    subsystems = SimpleNamespace(get_pg_conn=lambda: conn)
    runtime_result = {
        "row": {"id": "wf_build", "name": "Build Workflow", "version": 1},
        "definition": {"type": "operating_model"},
        "materialized_spec": {"compiled": True},
        "build_bundle": {"projection_status": {"state": "ready"}},
        "planning_notes": ["ready"],
        "intent_brief": {"goal": "Do the thing"},
        "execution_manifest": {"execution_manifest_ref": "execution_manifest:wf_build:def_001:1"},
        "progressive_build": {"mode": "one_unit_at_a_time"},
        "undo_receipt": {"receipt_id": "receipt-123"},
        "mutation_event_id": 77,
    }
    built_payload = {"workflow": {"id": "wf_build"}, "build_state": "ready"}

    with (
        patch("runtime.canonical_workflows.mutate_workflow_build", return_value=runtime_result) as mutate_mock,
        patch(
            "runtime.operations.commands.workflow_build.build_workflow_build_moment",
            return_value=built_payload,
        ) as build_mock,
    ):
        result = handle_mutate_workflow_build(command, subsystems)

    mutate_mock.assert_called_once_with(
        conn,
        workflow_id="wf_build",
        subpath="attachments",
        body=command.body,
    )
    build_mock.assert_called_once_with(
        runtime_result["row"],
        conn=conn,
        definition=runtime_result["definition"],
        materialized_spec=runtime_result["materialized_spec"],
        build_bundle=runtime_result["build_bundle"],
        planning_notes=runtime_result["planning_notes"],
        intent_brief=runtime_result["intent_brief"],
        execution_manifest=runtime_result["execution_manifest"],
        progressive_build=runtime_result["progressive_build"],
        undo_receipt=runtime_result["undo_receipt"],
        mutation_event_id=runtime_result["mutation_event_id"],
    )
    assert result == built_payload


def test_handle_mutate_workflow_build_rejects_bootstrap_surface() -> None:
    command = MutateWorkflowBuildCommand(
        workflow_id="wf_build",
        subpath="bootstrap",
        body={"prose": "make a workflow"},
    )

    try:
        handle_mutate_workflow_build(command, SimpleNamespace(get_pg_conn=lambda: object()))
    except WorkflowBuildMutationDeprecatedError as exc:
        assert exc.reason_code == "workflow_build.bootstrap.deprecated"
        assert exc.details["migration_hint"] == "/api/compile/materialize"
    else:  # pragma: no cover - explicit fail branch for assertion clarity
        raise AssertionError("bootstrap surface should be deprecated")
