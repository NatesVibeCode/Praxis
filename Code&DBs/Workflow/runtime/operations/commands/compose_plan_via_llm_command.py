"""Gateway-friendly Pydantic command + handler for ``compose_plan_via_llm``.

Migration 248 registered the ``compose-plan-via-llm`` operation in
``operation_catalog_registry`` but pointed ``input_model_ref`` at a class
that did not exist (``runtime.compose_plan_via_llm.ComposePlanViaLLMCommand``)
and ``handler_ref`` at the bare function whose signature
``(intent, *, conn, plan_name, why, concurrency, hydrate_env, llm_overrides)``
does not match the gateway's ``(command, subsystems)`` calling convention.
The registration was never reachable through ``execute_operation_from_subsystems``
as a result.

Migration 275 re-points the registration at this module. The
``ComposeExperimentCommand`` handler dispatches each child compose call
through the gateway so every child gets its own ``plan.composed``
receipt + event for replay parity with single-compose runs.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ComposePlanViaLLMCommand(BaseModel):
    """Input contract for ``compose_plan_via_llm``.

    Mirrors the ``compose_plan_via_llm`` function keyword arguments. The
    runtime function's positional ``intent`` parameter becomes a required
    field here; ``conn`` comes from ``subsystems.get_pg_conn()``.
    """

    intent: str
    plan_name: str | None = None
    why: str | None = None
    concurrency: int = 20
    llm_overrides: dict[str, Any] | None = None
    caller_ref: str = "platform.compose_plan_via_llm"


def handle_compose_plan_via_llm(
    command: ComposePlanViaLLMCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Adapt the gateway's ``(command, subsystems)`` calling convention to
    the underlying function. Returns a dict the gateway wraps into the
    receipt + ``plan.composed`` event."""
    from runtime.compose_plan_via_llm import compose_plan_via_llm

    conn = subsystems.get_pg_conn()
    result = compose_plan_via_llm(
        command.intent,
        conn=conn,
        plan_name=command.plan_name,
        why=command.why,
        concurrency=int(command.concurrency or 20),
        llm_overrides=command.llm_overrides,
    )
    payload = result.to_dict()
    # Hoist a compact "what happened" summary onto the event payload field
    # so the conceptual event carries decision-relevant data, not just
    # input/output hashes. The gateway reads this when present.
    usage = result.usage_summary()
    payload["event_payload"] = {
        "intent_fingerprint": payload.get("intent"),
        "ok": result.ok,
        "reason_code": result.reason_code,
        "packet_count": len(result.plan_packets or []),
        "synthesis_seeds": (
            len(result.synthesis.packet_seeds) if result.synthesis is not None else 0
        ),
        "validation_passed": getattr(result.validation, "passed", None),
        "validation_findings_count": len(
            getattr(result.validation, "findings", []) or []
        ),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "cached_tokens": usage.get("cached_tokens"),
        "calls": usage.get("calls"),
        "caller_ref": command.caller_ref,
    }
    payload["caller_ref"] = command.caller_ref
    return payload


__all__ = [
    "ComposePlanViaLLMCommand",
    "handle_compose_plan_via_llm",
]
