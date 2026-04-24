"""Compose Layers 2 → 1 → 5 of the planning stack in one call.

This is the end-to-end shortcut that turns a prose intent into a
:class:`ProposedPlan` without the caller stitching layers together:

  1. :func:`runtime.intent_decomposition.decompose_intent` splits the
     prose into ordered :class:`StepIntent` records.
  2. :func:`packets_from_steps` translates each step into a
     :class:`runtime.spec_compiler.PlanPacket`.
  3. :func:`runtime.spec_compiler.propose_plan` compiles + previews
     the resulting packet list (which also runs bind_data_pills per
     packet description).

Honest scope: this composer does not introduce any new planning
intelligence. If a layer fails (e.g. free-prose intent with no step
markers), composition fails closed with the same error the layer
raises. The caller still owns Layer 3 (data-flow reorder) and the
richer parts of Layer 4 (prompt authoring beyond the stage template
shim).
"""

from __future__ import annotations

from typing import Any

from runtime.intent_decomposition import (
    DecompositionRequiresLLMError,
    StepIntent,
    decompose_intent,
)
from runtime.spec_compiler import (
    PlanPacket,
    ProposedPlan,
    propose_plan,
)


def packets_from_steps(
    steps: list[StepIntent],
    *,
    write_scope_per_step: list[list[str]] | None = None,
    default_write_scope: list[str] | None = None,
    default_stage: str = "build",
) -> list[PlanPacket]:
    """Translate a list of :class:`StepIntent` records into PlanPackets.

    Args:
        steps: decomposition output from ``decompose_intent``.
        write_scope_per_step: optional list the same length as ``steps``;
            each entry is the write scope for the corresponding step.
            When absent, ``default_write_scope`` is used for every step.
        default_write_scope: fallback write scope when no per-step scope
            is provided. Defaults to workspace root ``["."]`` — ProposedPlan
            will surface a warning so the caller can narrow before launch.
        default_stage: stage to use when a step has no ``stage_hint``.
            Defaults to ``"build"``.

    Returns:
        One PlanPacket per step, in order. Labels are ``step_<index>``;
        depends_on is NOT wired here — that's layer 3's job.
    """
    if not steps:
        raise ValueError("steps must be non-empty")

    if write_scope_per_step is not None and len(write_scope_per_step) != len(steps):
        raise ValueError(
            f"write_scope_per_step has {len(write_scope_per_step)} entries "
            f"but there are {len(steps)} steps — counts must match"
        )

    fallback_scope = list(default_write_scope or ["."])
    packets: list[PlanPacket] = []
    for step in steps:
        scope = (
            list(write_scope_per_step[step.index])
            if write_scope_per_step is not None
            else fallback_scope
        )
        if not scope:
            scope = ["."]
        stage = (step.stage_hint or default_stage).strip() or default_stage
        label = f"step_{step.index + 1}"
        packets.append(
            PlanPacket(
                description=step.text,
                write=scope,
                stage=stage,
                label=label,
            )
        )
    return packets


def compose_plan_from_intent(
    intent: str,
    *,
    conn: Any,
    plan_name: str | None = None,
    why: str | None = None,
    workdir: str | None = None,
    allow_single_step: bool = False,
    write_scope_per_step: list[list[str]] | None = None,
    default_write_scope: list[str] | None = None,
    default_stage: str = "build",
) -> ProposedPlan:
    """Turn a prose intent into a :class:`ProposedPlan` end-to-end.

    Chains decompose → packets_from_steps → propose_plan in one call.
    No submission, no approval — just the translated + previewed
    ProposedPlan ready for caller approval and launch.

    Raises:
        ValueError: when the intent is empty or write_scope_per_step
            length disagrees with the decomposed step count.
        DecompositionRequiresLLMError: when the prose has no explicit
            step markers and ``allow_single_step`` is False.
    """
    decomposed = decompose_intent(intent, allow_single_step=allow_single_step)
    packets = packets_from_steps(
        decomposed.steps,
        write_scope_per_step=write_scope_per_step,
        default_write_scope=default_write_scope,
        default_stage=default_stage,
    )

    resolved_name = (
        str(plan_name or "").strip()
        or f"compose_plan.{decomposed.detection_mode}.{len(decomposed.steps)}_steps"
    )
    plan_dict: dict[str, Any] = {
        "name": resolved_name,
        "packets": [
            {
                "description": p.description,
                "write": list(p.write),
                "stage": p.stage,
                "label": p.label,
            }
            for p in packets
        ],
    }
    if why:
        plan_dict["why"] = str(why)

    return propose_plan(plan_dict, conn=conn, workdir=workdir)


__all__ = [
    "compose_plan_from_intent",
    "packets_from_steps",
]
