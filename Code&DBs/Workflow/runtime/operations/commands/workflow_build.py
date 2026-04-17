from typing import Any
from pydantic import BaseModel

class MutateWorkflowBuildCommand(BaseModel):
    workflow_id: str
    subpath: str
    body: dict[str, Any]

def handle_mutate_workflow_build(command: MutateWorkflowBuildCommand, subsystems: Any) -> dict[str, Any]:
    from runtime.canonical_workflows import mutate_workflow_build
    from surfaces.api.handlers.workflow_query import _workflow_build_payload
    
    conn = subsystems.get_pg_conn()
    
    result = mutate_workflow_build(
        conn,
        workflow_id=command.workflow_id,
        subpath=command.subpath,
        body=command.body,
    )
    
    return _workflow_build_payload(
        result["row"],
        conn=conn,
        definition=result["definition"],
        compiled_spec=result["compiled_spec"],
        build_bundle=result["build_bundle"],
        planning_notes=result["planning_notes"],
        intent_brief=result.get("intent_brief"),
        execution_manifest=result.get("execution_manifest"),
        undo_receipt=result.get("undo_receipt"),
        mutation_event_id=result.get("mutation_event_id"),
    )
