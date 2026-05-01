"""Gateway-friendly query handler for ``workflow_build.get``.

Loads the workflow row and composes the canonical ``BuildPayload`` shape
used by Moon and any other authoring surface. Same payload the
``GET /api/workflows/{workflow_id}/build`` HTTP route returns, but
dispatched through the CQRS gateway so the read records its own receipt
and is reachable from CLI / MCP / chat tools as a registered operation.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from runtime.workflow_build_moment import build_workflow_build_moment


class GetWorkflowBuildCommand(BaseModel):
    workflow_id: str


def handle_get_workflow_build(
    command: GetWorkflowBuildCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    row = conn.fetchrow(
        "SELECT id, name, description, definition, materialized_spec, version, updated_at "
        "FROM public.workflows WHERE id = $1",
        command.workflow_id,
    )
    if row is None:
        raise RuntimeError(f"workflow_build.get: workflow not found: {command.workflow_id}")
    return build_workflow_build_moment(dict(row), conn=conn)


__all__ = [
    "GetWorkflowBuildCommand",
    "handle_get_workflow_build",
]
