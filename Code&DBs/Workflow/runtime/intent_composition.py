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

from dataclasses import dataclass, replace
from typing import Any

from runtime.intent_decomposition import (
    DecompositionRequiresLLMError,
    StepIntent,
    decompose_intent,
)
from runtime.spec_compiler import (
    LaunchReceipt,
    PlanPacket,
    ProposedPlan,
    approve_proposed_plan,
    launch_approved,
    propose_plan,
)
from runtime.system_events import emit_system_event


def _best_effort_emit(
    conn: Any,
    *,
    event_type: str,
    source_id: str,
    payload: dict[str, Any],
) -> None:
    """Emit a plan-lifecycle system event, never failing the primary flow.

    Observability should never break planning. A degraded event bus (or a
    test using a conn stub that doesn't implement record_system_event) is
    allowed to silently skip the emit — primary flow continues.
    """
    try:
        emit_system_event(
            conn,
            event_type=event_type,
            source_id=source_id,
            source_type="plan",
            payload=payload,
        )
    except Exception:
        pass


def _adapt_plan_jobs_to_type_flow_request(spec_dict: dict[str, Any]) -> dict[str, Any]:
    """Convert a jobs-based spec_dict into a ``{nodes, edges}`` request that
    ``validate_workflow_request_type_flow`` can evaluate.

    Each job is a node (``node_id``=label, ``route``=task_type). Each
    ``depends_on`` entry on a job becomes a ``from_node_id`` → ``to_node_id``
    edge. The adapter does not add types or rewrite routes; it surfaces
    what the compiler already produced so the validator can walk it.
    """
    jobs = list(spec_dict.get("jobs") or [])
    nodes: list[dict[str, str]] = [
        {
            "node_id": str(job.get("label") or ""),
            "route": str(job.get("task_type") or ""),
        }
        for job in jobs
    ]
    edges: list[dict[str, str]] = []
    for job in jobs:
        to_id = str(job.get("label") or "")
        if not to_id:
            continue
        for dep in job.get("depends_on") or ():
            from_id = str(dep).strip()
            if from_id and to_id:
                edges.append({"from_node_id": from_id, "to_node_id": to_id})
    return {"nodes": nodes, "edges": edges}


def _validate_composed_plan_type_flow(proposed: ProposedPlan) -> list[str]:
    """Run the type-flow validator against a ProposedPlan's spec_dict.

    Returns the raw error list from
    ``runtime.workflow_type_contracts.validate_workflow_request_type_flow``
    (empty when flow is satisfied). Honors architecture-policy::platform-
    architecture::fail-closed-at-compile-no-silent-defaults: ``compose_plan_
    from_intent`` folds the errors into the ProposedPlan's warnings list
    so callers see them before approving or launching the plan. The
    Moon commit path independently rejects at save time (Phase 1.2.a).

    Degraded substrate (type_contracts module unavailable) returns [] —
    the compose path must not block on optional validation infrastructure.
    """
    try:
        from runtime.workflow_type_contracts import (
            validate_workflow_request_type_flow,
        )
    except Exception:
        return []
    request = _adapt_plan_jobs_to_type_flow_request(proposed.spec_dict)
    if not request["nodes"]:
        return []
    return list(validate_workflow_request_type_flow(request) or [])


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
    serialize_scope_conflicts: bool = False,
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
    if serialize_scope_conflicts:
        packets = reorder_packets_by_write_conflicts(packets)

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
                **({"depends_on": list(p.depends_on)} if p.depends_on else {}),
            }
            for p in packets
        ],
    }
    if why:
        plan_dict["why"] = str(why)

    proposed = propose_plan(plan_dict, conn=conn, workdir=workdir)

    # Type-flow validation per architecture-policy::platform-architecture::
    # fail-closed-at-compile-no-silent-defaults. Errors surface in warnings
    # (visible, not silent). Moon commitDefinition independently rejects at
    # the save boundary via Phase 1.2.a — this layer exposes the failure at
    # preview/propose time so callers don't approve a plan that can't save.
    type_flow_errors = _validate_composed_plan_type_flow(proposed)
    if type_flow_errors:
        proposed = replace(
            proposed,
            warnings=list(proposed.warnings) + type_flow_errors,
        )
        # Also promote to durable typed_gap.created events — observers
        # see the type-flow failure at event stream level, not only in
        # the ProposedPlan warnings list. Best-effort: emission failures
        # never block the primary compose flow.
        try:
            from runtime.typed_gap_events import (
                emit_typed_gaps_for_type_flow_errors,
            )

            emit_typed_gaps_for_type_flow_errors(
                conn,
                type_flow_errors,
                source_ref=f"compose_plan_from_intent:{proposed.spec_name}",
            )
        except Exception:
            pass

    _best_effort_emit(
        conn,
        event_type="plan.composed",
        source_id=proposed.workflow_id,
        payload={
            "spec_name": proposed.spec_name,
            "total_jobs": proposed.total_jobs,
            "detection_mode": decomposed.detection_mode,
            "step_count": len(decomposed.steps),
            "has_unresolved_routes": bool(proposed.unresolved_routes),
            "unbound_pill_count": len(
                (proposed.binding_summary or {}).get("unbound_refs") or []
            ),
            "type_flow_error_count": len(type_flow_errors),
        },
    )
    return proposed


def _scopes_overlap(a_scope: list[str], b_scope: list[str]) -> bool:
    """Return True when two file-path scopes share any common path.

    Honest-scope match — two scopes overlap when one is equal to the other,
    one is a directory prefix of the other, or both resolve to the same
    workspace root. Normalizes trailing slashes and ``./`` prefixes.
    """

    def _normalize(entry: str) -> str:
        text = (entry or "").strip()
        if text.startswith("./"):
            text = text[2:]
        return text.rstrip("/")

    a_norm = {_normalize(p) for p in (a_scope or []) if p}
    b_norm = {_normalize(p) for p in (b_scope or []) if p}
    if not a_norm or not b_norm:
        return False
    for a in a_norm:
        for b in b_norm:
            if a == b:
                return True
            # Directory-prefix match — 'src/' covers 'src/foo.py'.
            if a and b and (
                (a + "/").startswith(b + "/") or (b + "/").startswith(a + "/")
            ):
                return True
            # Workspace-root ('.') covers every other scope.
            if a == "." or b == ".":
                return True
    return False


def reorder_packets_by_write_conflicts(
    packets: list[PlanPacket],
) -> list[PlanPacket]:
    """Add depends_on edges to serialize packets with conflicting scopes.

    Honest, deterministic Layer 3. For each packet pair (i, j) with i < j:

      - Write-write conflict: both packets' write scopes overlap. Packet
        j's ``depends_on`` gains packet i's label so both can't run in
        parallel against the same files.
      - Write-read conflict: packet j's read scope overlaps packet i's
        write scope. Packet j depends on packet i so j reads what i
        produced, not what existed before.

    Caller-supplied ``depends_on`` is never overwritten — new edges are
    merged in additively, deduplicated, and order-preserved. Packets are
    returned in their input order; execution ordering is expressed via
    depends_on for the workflow engine to honor.

    No cycle detection needed: all new edges point strictly backward
    (i < j). Caller-supplied forward edges are the caller's contract.
    """
    if not packets:
        return []

    # Quick lookup from label to original index so we can preserve the
    # caller's manual edges and only add new ones.
    label_at: dict[str, int] = {}
    for idx, packet in enumerate(packets):
        label = packet.label or f"packet_{idx}"
        label_at.setdefault(label, idx)

    reordered: list[PlanPacket] = []
    for j, packet in enumerate(packets):
        existing_deps = list(packet.depends_on or [])
        new_deps: list[str] = list(existing_deps)

        for i in range(j):
            earlier = packets[i]
            earlier_label = earlier.label or f"packet_{i}"
            if earlier_label in new_deps:
                continue
            write_conflict = _scopes_overlap(earlier.write, packet.write)
            read_conflict = _scopes_overlap(earlier.write, packet.read or [])
            if write_conflict or read_conflict:
                new_deps.append(earlier_label)

        if new_deps == existing_deps:
            reordered.append(packet)
            continue
        reordered.append(
            PlanPacket(
                description=packet.description,
                write=list(packet.write),
                stage=packet.stage,
                label=packet.label,
                read=list(packet.read) if packet.read else None,
                depends_on=new_deps,
                bug_ref=packet.bug_ref,
                bug_refs=list(packet.bug_refs) if packet.bug_refs else None,
                agent=packet.agent,
                complexity=packet.complexity,
            )
        )
    return reordered


class ComposeAndLaunchBlocked(ValueError):
    """Raised when compose_and_launch refuses to launch for safety reasons.

    ``reasons`` carries the structured list: unresolved_routes and
    unbound_pills. Callers render the blocked reasons to the operator;
    fixing any of them requires re-composing or explicitly disabling the
    check.
    """

    def __init__(self, reasons: list[dict[str, Any]]) -> None:
        self.reasons = reasons
        summary = ", ".join(str(entry.get("kind") or "?") for entry in reasons)
        super().__init__(
            f"compose_and_launch blocked by {len(reasons)} check(s): {summary}. "
            "Inspect receipt.reasons for the structured detail."
        )


def compose_and_launch(
    intent: str,
    *,
    conn: Any,
    approved_by: str,
    approval_note: str | None = None,
    plan_name: str | None = None,
    why: str | None = None,
    workdir: str | None = None,
    allow_single_step: bool = False,
    write_scope_per_step: list[list[str]] | None = None,
    default_write_scope: list[str] | None = None,
    default_stage: str = "build",
    refuse_unresolved_routes: bool = True,
    refuse_unbound_pills: bool = True,
    serialize_scope_conflicts: bool = False,
) -> LaunchReceipt:
    """End-to-end: prose intent → ProposedPlan → ApprovedPlan → LaunchReceipt.

    Wraps :func:`compose_plan_from_intent`, :func:`approve_proposed_plan`,
    and :func:`launch_approved` into one call for trusted automation (CI,
    scripts, experienced operators).

    Bulletproof defaults — any of these fail closed with a structured
    :class:`ComposeAndLaunchBlocked` unless the caller explicitly disables
    the check:

      - ``refuse_unresolved_routes``: block if any job has an unresolved
        auto-route (i.e. no admitted provider for the requested stage).
      - ``refuse_unbound_pills``: block if any packet description
        references an object.field that doesn't exist in the data
        dictionary (typo or hallucination).

    ``approved_by`` is required — no anonymous automation. The approval is
    still hash-bound to the exact spec_dict, so any tampering between the
    compose step and submit will still fail at launch_approved.
    """
    proposed = compose_plan_from_intent(
        intent,
        conn=conn,
        plan_name=plan_name,
        why=why,
        workdir=workdir,
        allow_single_step=allow_single_step,
        write_scope_per_step=write_scope_per_step,
        default_write_scope=default_write_scope,
        default_stage=default_stage,
        serialize_scope_conflicts=serialize_scope_conflicts,
    )

    blocked: list[dict[str, Any]] = []

    if refuse_unresolved_routes and proposed.unresolved_routes:
        blocked.append(
            {
                "kind": "unresolved_routes",
                "count": len(proposed.unresolved_routes),
                "detail": list(proposed.unresolved_routes),
            }
        )

    if refuse_unbound_pills:
        unbound = proposed.binding_summary.get("unbound_refs") if isinstance(
            proposed.binding_summary, dict
        ) else None
        if unbound:
            blocked.append(
                {
                    "kind": "unbound_pills",
                    "count": len(unbound),
                    "detail": list(unbound),
                }
            )

    if blocked:
        _best_effort_emit(
            conn,
            event_type="plan.blocked",
            source_id=proposed.workflow_id,
            payload={
                "spec_name": proposed.spec_name,
                "approved_by_attempted": approved_by,
                "blocked_reasons": blocked,
            },
        )
        raise ComposeAndLaunchBlocked(blocked)

    approved = approve_proposed_plan(
        proposed,
        approved_by=approved_by,
        approval_note=approval_note,
    )
    _best_effort_emit(
        conn,
        event_type="plan.approved",
        source_id=proposed.workflow_id,
        payload={
            "spec_name": proposed.spec_name,
            "approved_by": approved.approved_by,
            "approved_at": approved.approved_at,
            "proposal_hash": approved.proposal_hash,
        },
    )
    receipt = launch_approved(
        approved,
        conn=conn,
        requested_by_kind="workflow",
    )
    _best_effort_emit(
        conn,
        event_type="plan.launched",
        source_id=proposed.workflow_id,
        payload={
            "spec_name": proposed.spec_name,
            "run_id": receipt.run_id,
            "total_jobs": receipt.total_jobs,
            "approved_by": approved.approved_by,
        },
    )
    return receipt


@dataclass(frozen=True)
class PlanLifecycleEvent:
    """One plan.* event from the system_events log."""

    event_id: int | None
    event_type: str
    created_at: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "created_at": self.created_at,
            "payload": dict(self.payload),
        }


@dataclass(frozen=True)
class PlanLifecycle:
    """Ordered read of every plan.* system_event for one workflow_id.

    Q-side of the planning stack's CQRS pattern: the C path emits
    plan.composed / plan.approved / plan.launched / plan.blocked via
    compose_and_launch; this dataclass pulls them back for Moon, CLI, or
    ad-hoc inspection.
    """

    workflow_id: str
    events: list[PlanLifecycleEvent]

    @property
    def latest_event_type(self) -> str | None:
        return self.events[-1].event_type if self.events else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "events": [event.to_dict() for event in self.events],
            "latest_event_type": self.latest_event_type,
        }


def get_plan_lifecycle(workflow_id: str, *, conn: Any) -> PlanLifecycle:
    """Read every plan.* system_event for one workflow_id in order.

    Returns :class:`PlanLifecycle` with events sorted oldest → newest so a
    reader sees compose → approve → launch (or blocked) in the order they
    fired. Q-side read — no mutations, no side effects.
    """
    normalized = (workflow_id or "").strip()
    if not normalized:
        raise ValueError("workflow_id is required")

    rows = conn.execute(
        "SELECT id, event_type, payload, created_at "
        "FROM system_events "
        "WHERE source_type = 'plan' AND source_id = $1 "
        "ORDER BY created_at ASC, id ASC",
        normalized,
    )

    events: list[PlanLifecycleEvent] = []
    for row in rows or []:
        row_dict = dict(row)
        payload = row_dict.get("payload") or {}
        if isinstance(payload, str):
            import json as _json

            try:
                payload = _json.loads(payload)
            except (TypeError, ValueError):
                payload = {}
        created_at = row_dict.get("created_at")
        if hasattr(created_at, "isoformat"):
            created_at = created_at.isoformat()
        events.append(
            PlanLifecycleEvent(
                event_id=row_dict.get("id"),
                event_type=str(row_dict.get("event_type") or ""),
                created_at=str(created_at or ""),
                payload=payload if isinstance(payload, dict) else {},
            )
        )
    return PlanLifecycle(workflow_id=normalized, events=events)


__all__ = [
    "ComposeAndLaunchBlocked",
    "PlanLifecycle",
    "PlanLifecycleEvent",
    "compose_and_launch",
    "compose_plan_from_intent",
    "get_plan_lifecycle",
    "packets_from_steps",
    "reorder_packets_by_write_conflicts",
]
