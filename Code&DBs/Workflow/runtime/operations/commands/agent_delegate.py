"""agent.delegate — parent agent spawns a bounded child workflow.

The delegation broker is the only first-class way for an agent_principal
(or its in-flight workflow run) to spawn child work. It:

  1. validates the parent caller is a known agent_principal
  2. materialises an agent_delegations row with admitted_tools,
     admitted_integrations, write_envelope, network_policy=praxis_only
  3. compiles the child trust envelope (re-uses runtime.agent_context for
     prompt + scope shape)
  4. launches a child workflow run with requested_by_kind='agent' and
     requested_by_ref=<parent_agent_ref> so receipts attribute back
  5. stamps child_run_id back on the delegation row

The child run inherits the parent agent's standing_order_keys but the
admitted tool list narrows further to what the parent explicitly
admitted. Network policy defaults to praxis_only — child workers cannot
browse the open web unless the parent explicitly elevates the policy.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class AgentDelegateCommand(BaseModel):
    """Parent agent delegates a bounded child task."""

    parent_agent_ref: str = Field(..., min_length=1)
    child_task: str = Field(..., min_length=1, description="Short slug for the child task.")
    child_intent: str = Field(
        ..., min_length=1, description="Plain-prose intent the child worker is asked to satisfy."
    )
    parent_run_id: str | None = None
    parent_job_id: str | None = None
    parent_wake_id: str | None = None
    admitted_tools: list[str] = Field(default_factory=list)
    admitted_integrations: list[str] = Field(default_factory=list)
    write_envelope: list[str] = Field(default_factory=list)
    network_policy: Literal["disabled", "provider_only", "praxis_only", "enabled"] = (
        "praxis_only"
    )
    timeout_ms: int = Field(default=600_000, ge=1_000, le=7_200_000)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("parent_agent_ref", "child_task", "child_intent")
    @classmethod
    def _strip(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must be non-empty")
        return stripped


def _build_child_inline_spec(
    *,
    parent_agent_ref: str,
    parent_agent_row: dict[str, Any],
    command: AgentDelegateCommand,
    delegation_id: str,
) -> dict[str, Any]:
    """Build the child workflow's inline spec.

    The child inherits the parent's standing_order_keys but its
    admitted_tools list is intersected with what the parent explicitly
    admitted on this delegation. write_envelope and network_policy are
    set per the delegation row, not the parent's defaults.
    """
    parent_standing_orders = parent_agent_row.get("standing_order_keys") or []
    if isinstance(parent_standing_orders, str):
        try:
            parent_standing_orders = json.loads(parent_standing_orders)
        except (json.JSONDecodeError, TypeError):
            parent_standing_orders = []

    prompt = (
        f"You are a child worker delegated by {parent_agent_ref} "
        f"(delegation_id={delegation_id}).\n"
        f"\n"
        f"# Bounded task\n"
        f"task: {command.child_task}\n"
        f"intent:\n{command.child_intent}\n"
        f"\n"
        f"# Scope cage (enforced)\n"
        f"admitted_tools: {command.admitted_tools or '(none — read-only Praxis surface)'}\n"
        f"admitted_integrations: {command.admitted_integrations or '(none)'}\n"
        f"write_envelope: {command.write_envelope or '(empty — read-only)'}\n"
        f"network_policy: {command.network_policy}\n"
        f"\n"
        f"# Standing orders inherited from {parent_agent_ref}\n"
        f"{list(parent_standing_orders) if parent_standing_orders else '(none)'}\n"
        f"\n"
        f"# Tool gaps\n"
        f"If you find Praxis lacks a capability you need, file an "
        f"agent_tool_gap via agent_tool_gap.file with reporter_delegation_id="
        f"{delegation_id}. Then return what you can and stop. Do NOT improvise "
        f"around a missing capability.\n"
        f"\n"
        f"When the bounded task is complete, return a structured result and stop. "
        f"The parent will read your child_run_id off the delegation row.\n"
    )

    return {
        "name": f"agent_delegation::{parent_agent_ref}::{command.child_task}",
        "metadata": {
            "delegation_id": delegation_id,
            "parent_agent_ref": parent_agent_ref,
            "parent_run_id": command.parent_run_id,
            "parent_job_id": command.parent_job_id,
            "parent_wake_id": command.parent_wake_id,
            "execution_bundle": {
                "access_policy": {
                    "write_scope": list(command.write_envelope),
                    "network_policy": command.network_policy,
                    "integration_refs": list(command.admitted_integrations),
                    "allowed_tools": list(command.admitted_tools),
                }
            },
        },
        "jobs": [
            {
                "label": "delegation_child",
                "prompt": prompt,
                "task_type": "agent_delegation_child",
                "max_iterations": 12,
                "metadata": {
                    "delegation_id": delegation_id,
                    "parent_agent_ref": parent_agent_ref,
                    "child_task": command.child_task,
                },
            }
        ],
    }


def handle_agent_delegate(
    command: AgentDelegateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.control_commands import submit_workflow_command

    conn = subsystems.get_pg_conn()

    parent_rows = conn.execute(
        """SELECT
               agent_principal_ref,
               status,
               write_envelope,
               capability_refs,
               integration_refs,
               standing_order_keys,
               allowed_tools,
               network_policy
           FROM agent_registry
           WHERE agent_principal_ref = $1""",
        command.parent_agent_ref,
    )
    if not parent_rows:
        return {
            "ok": False,
            "operation": "agent.delegate",
            "error_code": "agent_principal.parent_not_found",
            "parent_agent_ref": command.parent_agent_ref,
        }
    parent = dict(parent_rows[0])
    if str(parent.get("status") or "").strip() != "active":
        return {
            "ok": False,
            "operation": "agent.delegate",
            "error_code": "agent_principal.parent_inactive",
            "parent_agent_ref": command.parent_agent_ref,
            "parent_status": parent.get("status"),
        }

    rows = conn.execute(
        """INSERT INTO agent_delegations (
               parent_agent_ref,
               parent_run_id,
               parent_job_id,
               parent_wake_id,
               child_task,
               child_intent,
               admitted_tools,
               admitted_integrations,
               write_envelope,
               network_policy,
               timeout_ms,
               status
           )
           VALUES ($1, $2, $3, $4::uuid, $5, $6,
                   $7::jsonb, $8::jsonb, $9::jsonb, $10, $11, 'requested')
           RETURNING delegation_id, requested_at""",
        command.parent_agent_ref,
        command.parent_run_id,
        command.parent_job_id,
        command.parent_wake_id,
        command.child_task,
        command.child_intent,
        json.dumps(command.admitted_tools),
        json.dumps(command.admitted_integrations),
        json.dumps(command.write_envelope),
        command.network_policy,
        int(command.timeout_ms),
    )
    if not rows:
        return {
            "ok": False,
            "operation": "agent.delegate",
            "error_code": "agent_delegations.insert_returned_no_row",
        }
    delegation_id = str(rows[0]["delegation_id"])

    inline_spec = _build_child_inline_spec(
        parent_agent_ref=command.parent_agent_ref,
        parent_agent_row=parent,
        command=command,
        delegation_id=delegation_id,
    )

    result = submit_workflow_command(
        conn,
        requested_by_kind="agent",
        requested_by_ref=command.parent_agent_ref,
        inline_spec=inline_spec,
        parent_run_id=command.parent_run_id,
        dispatch_reason=f"agent.delegate.{command.child_task}",
        spec_name=str(inline_spec.get("name")),
        total_jobs=1,
    )
    if result.get("error") or not result.get("run_id"):
        conn.execute(
            """UPDATE agent_delegations
                  SET status = 'failed',
                      error_code = 'submit_workflow_failed',
                      error_message = $2,
                      completed_at = now()
                WHERE delegation_id = $1::uuid""",
            delegation_id,
            str(result.get("error") or "no run_id returned"),
        )
        return {
            "ok": False,
            "operation": "agent.delegate",
            "error_code": "submit_workflow_failed",
            "delegation_id": delegation_id,
            "submit_result": result,
        }

    child_run_id = str(result["run_id"])
    conn.execute(
        """UPDATE agent_delegations
              SET child_run_id = $2,
                  status = 'launched',
                  launched_at = now()
            WHERE delegation_id = $1::uuid""",
        delegation_id,
        child_run_id,
    )

    return {
        "ok": True,
        "operation": "agent.delegate",
        "delegation_id": delegation_id,
        "parent_agent_ref": command.parent_agent_ref,
        "child_run_id": child_run_id,
        "child_task": command.child_task,
        "network_policy": command.network_policy,
        "admitted_tools_count": len(command.admitted_tools),
        "admitted_integrations_count": len(command.admitted_integrations),
    }


__all__ = ["AgentDelegateCommand", "handle_agent_delegate"]
