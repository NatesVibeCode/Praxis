"""Gateway-friendly command for creating a minimal workflow draft.

Wraps ``runtime.canonical_workflows.save_workflow`` for the
``canvas_compose`` chat tool (and any other authoring agent) so
"build me a workflow" can land on a real, editable workflow row in one
chained call: workflow_create_draft -> workflow_build_mutate(subpath=
'bootstrap').

The handler accepts an optional ``name`` and creates a row with an empty
definition. The new workflow_id is returned so the caller can immediately
bootstrap prose into it via ``workflow_build_mutate``.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from runtime.canonical_workflows import save_workflow


class CreateWorkflowDraftCommand(BaseModel):
    name: str | None = None
    description: str | None = None


def handle_create_workflow_draft(
    command: CreateWorkflowDraftCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    body: dict[str, Any] = {
        "name": command.name or "Chat draft",
        "definition": {},
    }
    if command.description:
        body["description"] = command.description
    row = save_workflow(conn, workflow_id=None, body=body)
    persisted = dict(row)
    return {
        "ok": True,
        "workflow_id": persisted.get("id"),
        "name": persisted.get("name"),
        "version": persisted.get("version"),
        "workflow": persisted,
    }


__all__ = [
    "CreateWorkflowDraftCommand",
    "handle_create_workflow_draft",
]
