"""Gateway-dispatched command wrappers for launch_plan and compose_plan.

These let `launch_plan` and `compose_plan_from_intent` dispatch through
`operation_catalog_gateway.execute_operation_*` so the gateway auto-generates
`event_ids` on completed `authority_operation_receipts` and emits
`plan.launched` / `plan.composed` to `authority_events`.

The underlying business logic still lives in `runtime.spec_compiler.launch_plan`
and `runtime.intent_composition.compose_plan_from_intent`. This module is the
gateway-friendly seam (Pydantic input + (command, subsystems) handler).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class LaunchPlanCommand(BaseModel):
    plan: dict[str, Any] = Field(default_factory=dict)
    workdir: str | None = None
    requested_by_kind: str = "workflow"
    requested_by_ref: str | None = None


class ComposePlanCommand(BaseModel):
    intent: str
    plan_name: str | None = None
    why: str | None = None
    workdir: str | None = None
    allow_single_step: bool = False
    write_scope_per_step: list[list[str]] | None = None
    default_write_scope: list[str] | None = None
    default_stage: str = "build"
    serialize_scope_conflicts: bool = False


def handle_launch_plan(command: LaunchPlanCommand, subsystems: Any) -> dict[str, Any]:
    """Dispatch ``runtime.spec_compiler.launch_plan`` through the catalog gateway.

    The gateway wraps this call in an authority operation receipt + emits
    ``plan.launched`` to ``authority_events`` (event_required=TRUE on the
    operation_catalog_registry row registered in migration 234).

    Known structured failures are returned as ``{ok: False, ...}`` dicts so
    callers preserve the rich error metadata (submit_status, submit_result)
    that the gateway's generic exception handler would otherwise flatten to a
    string. Unknown exceptions raise; the gateway records execution_status=
    failed and the receipt reflects the error.
    """

    from runtime.spec_compiler import LaunchSubmitFailedError, launch_plan

    conn = subsystems.get_pg_conn()
    try:
        receipt = launch_plan(
            command.plan,
            conn=conn,
            workdir=command.workdir,
            requested_by_kind=command.requested_by_kind,
            requested_by_ref=command.requested_by_ref,
        )
    except LaunchSubmitFailedError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "launch.submit_failed",
            "submit_status": exc.status,
            "submit_error_code": exc.error_code,
            "submit_error_detail": exc.error_detail,
            "spec_name": exc.spec_name,
            "submit_result": exc.submit_result,
        }
    except ValueError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "plan.invalid",
        }
    payload = receipt.to_dict()
    payload["ok"] = True
    payload["mode"] = "submitted"
    return payload


def handle_compose_plan(command: ComposePlanCommand, subsystems: Any) -> dict[str, Any]:
    """Dispatch ``runtime.intent_composition.compose_plan_from_intent`` through
    the catalog gateway. Emission of ``plan.composed`` flows through the
    receipt path on completed runs.

    Known structured failures (DecompositionRequiresLLMError, ValueError) are
    returned as ``{ok: False, ...}`` dicts so the rich error metadata is
    preserved across the gateway boundary.
    """

    from runtime.intent_composition import compose_plan_from_intent
    from runtime.intent_decomposition import DecompositionRequiresLLMError

    conn = subsystems.get_pg_conn()
    try:
        proposed = compose_plan_from_intent(
            command.intent,
            conn=conn,
            plan_name=command.plan_name,
            why=command.why,
            workdir=command.workdir,
            allow_single_step=command.allow_single_step,
            write_scope_per_step=command.write_scope_per_step,
            default_write_scope=command.default_write_scope,
            default_stage=command.default_stage,
            serialize_scope_conflicts=command.serialize_scope_conflicts,
        )
    except DecompositionRequiresLLMError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "decomposition.requires_llm",
        }
    except ValueError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "compose.invalid",
        }
    payload = proposed.to_dict()
    payload["ok"] = True
    return payload


__all__ = [
    "LaunchPlanCommand",
    "ComposePlanCommand",
    "handle_launch_plan",
    "handle_compose_plan",
]
