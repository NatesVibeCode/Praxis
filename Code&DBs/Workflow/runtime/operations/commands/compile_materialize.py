"""Gateway-dispatched command wrapper for compile materialize.

Lets `praxis_compile action=materialize` dispatch through
`operation_catalog_gateway.execute_operation_*` so the gateway records an
`authority_operation_receipts` row + emits `compile.materialized` to
`authority_events`. Brings compile materialize in line with the dogfooding
principle (`project_dogfooding_principle.md`) — every internal operation
should route through the gateway.

The underlying business logic still lives in
`runtime.compile_cqrs.materialize_workflow`. This module is the
gateway-friendly seam (Pydantic input + (command, subsystems) handler).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CompileMaterializeCommand(BaseModel):
    """Input model for the compile materialize gateway operation.

    Mirrors the parameters accepted by `materialize_workflow` so the MCP
    `praxis_compile` tool can construct a typed payload and dispatch via
    the gateway instead of calling materialize_workflow directly.
    """

    intent: str = Field(
        ...,
        description="Operator prose. Will be compiled into a workflow definition.",
    )
    workflow_id: str | None = Field(
        default=None,
        description="Existing or desired workflow id. Omit to auto-generate.",
    )
    title: str | None = Field(
        default=None,
        description="Workflow title. Omit to derive from intent.",
    )
    enable_llm: bool | None = Field(
        default=None,
        description="Override compiler LLM policy. Omit to use compiler default.",
    )
    enable_full_compose: bool | None = Field(
        default=None,
        description=(
            "Pipeline selector for materialize. True (default) routes through "
            "compose_plan_via_llm (synthesis + N-way fork-out). False routes "
            "through compile_prose (compile_synthesize → compile_pill_match → "
            "compile_author → compile_finalize sub-tasks)."
        ),
    )
    match_limit: int = Field(
        default=5,
        description="Maximum authority candidates per recognized span.",
    )


def handle_compile_materialize(
    command: CompileMaterializeCommand, subsystems: Any
) -> dict[str, Any]:
    """Dispatch `runtime.compile_cqrs.materialize_workflow` through the gateway.

    The gateway wraps this call in an authority operation receipt + emits
    `compile.materialized` to `authority_events` (event_required=TRUE on the
    operation_catalog_registry row registered for this operation).

    Known input failures are returned as `{ok: False, ...}` dicts so callers
    preserve the structured reason_code; unknown exceptions raise and the
    gateway records execution_status=failed with the traceback in the receipt.
    """

    from runtime.compile_cqrs import materialize_workflow

    conn = subsystems.get_pg_conn()
    try:
        return materialize_workflow(
            command.intent,
            conn=conn,
            workflow_id=command.workflow_id,
            title=command.title,
            enable_llm=command.enable_llm,
            enable_full_compose=command.enable_full_compose,
            match_limit=command.match_limit,
        )
    except ValueError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "compile.intent.invalid",
        }
