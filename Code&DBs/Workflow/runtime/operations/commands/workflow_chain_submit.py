"""Gateway-dispatched command wrapper for workflow chain submission.

Submits a multi-wave workflow chain by reading a coordination JSON file
and dispatching it through the service bus. Wraps the existing
`runtime.control_commands.request_workflow_chain_submit_command` so the
operation routes through the CQRS gateway with receipt + event.

Real authority — workflow_chain has dedicated migrations
(087_workflow_chain_authority.sql, 088_workflow_chain_dependency_and_adoption_authority.sql,
090_workflow_chain_cancellation_and_alignment.sql) and live consumers in
runtime/scheduler.py, runtime/control_commands.py, runtime/command_handlers.py.

Brings the chain submit lane in line with the dogfooding principle
(`project_dogfooding_principle.md`).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class WorkflowChainSubmitCommand(BaseModel):
    """Input for `workflow_chain.submit` — submit a multi-wave coordination program."""

    coordination_path: str = Field(
        ...,
        description="Path to the workflow chain coordination JSON spec (required).",
    )
    adopt_active: bool = Field(
        default=True,
        description=(
            "If true (default), adopt any in-flight runs of the same chain "
            "rather than starting a duplicate. Set false to force a fresh "
            "submission alongside existing runs."
        ),
    )
    requested_by_kind: str = Field(
        default="mcp",
        description="Origin tag for the chain submit (e.g. 'mcp', 'cli', 'http').",
    )
    requested_by_ref: str = Field(
        default="praxis_workflow.chain",
        description="Detail ref identifying the specific caller within the kind.",
    )


def handle_workflow_chain_submit(
    command: WorkflowChainSubmitCommand, subsystems: Any
) -> dict[str, Any]:
    """Dispatch workflow_chain submit through the gateway.

    Emits `workflow_chain.submitted` on the receipt and chains into the
    existing service-bus submission path. The underlying runtime helpers
    handle the multi-wave coordination + adoption logic; this wrapper is
    the gateway-friendly seam.
    """

    from runtime.control_commands import (
        render_workflow_chain_submit_response,
        request_workflow_chain_submit_command,
    )
    from runtime.workspace_paths import repo_root

    conn = subsystems.get_pg_conn()
    try:
        chain_command = request_workflow_chain_submit_command(
            conn,
            requested_by_kind=command.requested_by_kind,
            requested_by_ref=command.requested_by_ref,
            coordination_path=command.coordination_path,
            repo_root=str(repo_root()),
            adopt_active=command.adopt_active,
        )
        return render_workflow_chain_submit_response(
            conn,
            chain_command,
            coordination_path=command.coordination_path,
        )
    except ValueError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "workflow_chain.submit.invalid",
        }
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "workflow_chain.coordination_path.missing",
        }
