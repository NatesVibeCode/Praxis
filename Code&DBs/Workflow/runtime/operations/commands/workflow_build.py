from typing import Any
from pydantic import BaseModel

from runtime.workflow_build_moment import build_workflow_build_moment


class WorkflowBuildMutationDeprecatedError(RuntimeError):
    def __init__(self, message: str, *, details: dict[str, Any]) -> None:
        super().__init__(message)
        self.reason_code = "workflow_build.bootstrap.deprecated"
        self.details = details


class MutateWorkflowBuildCommand(BaseModel):
    workflow_id: str
    subpath: str
    body: dict[str, Any]


def handle_mutate_workflow_build(command: MutateWorkflowBuildCommand, subsystems: Any) -> dict[str, Any]:
    from runtime.canonical_workflows import mutate_workflow_build

    if command.subpath == "bootstrap":
        raise WorkflowBuildMutationDeprecatedError(
            "workflow build bootstrap is deprecated; use compile_materialize",
            details={
                "workflow_id": command.workflow_id,
                "migration_hint": "/api/compile/materialize",
                "operation_name": "compile_materialize",
            },
        )

    conn = subsystems.get_pg_conn()

    result = mutate_workflow_build(
        conn,
        workflow_id=command.workflow_id,
        subpath=command.subpath,
        body=command.body,
    )

    return build_workflow_build_moment(
        result["row"],
        conn=conn,
        definition=result["definition"],
        materialized_spec=result["materialized_spec"],
        build_bundle=result["build_bundle"],
        planning_notes=result["planning_notes"],
        intent_brief=result.get("intent_brief"),
        execution_manifest=result.get("execution_manifest"),
        progressive_build=result.get("progressive_build"),
        undo_receipt=result.get("undo_receipt"),
        mutation_event_id=result.get("mutation_event_id"),
    )
