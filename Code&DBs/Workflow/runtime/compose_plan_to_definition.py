"""Translator: authored plan_packets → workflow definition for the build canvas.

The Praxis "Describe it" UI compile path historically called
``runtime.materializer.materialize_prose`` which produces a ``definition`` whose
``references`` + ``draft_flow`` are auto-converted to a ``build_graph`` and
``binding_ledger`` by ``runtime.build_authority.build_authority_bundle``.

Per operator standing order ``feedback_fanout_by_work_volume`` (2026-04-26),
the canonical compile path is now ``runtime.compose_plan_via_llm`` —
synthesis (1 Pro call → packet seeds) + N parallel fork-out author calls
(default concurrency=20). That orchestrator returns ``authored.packets`` —
typed, structured, depends_on-linked, with consumes/produces/gates.

This module bridges the two: takes a ``ComposeViaLLMResult`` and emits a
workflow ``definition`` shape that ``build_authority_bundle`` can fold into
``build_graph`` + ``binding_ledger`` + ``import_snapshots``. The build canvas
already renders that shape.

Anchored to: feedback_fanout_by_work_volume + autonomous-first compile gate.
"""
from __future__ import annotations

from typing import Any, Iterable
from uuid import uuid4

from runtime.compose_plan_via_llm import ComposeViaLLMResult


def _slug_kind(slug: str) -> str:
    if slug.startswith("@"):
        return "integration"
    if slug.startswith("#"):
        return "object"
    if "/" in slug and not slug.startswith(("@", "#")):
        return "agent"
    return "reference"


def _slug_display(slug: str) -> str:
    raw = slug.strip()
    if raw.startswith(("@", "#")):
        return raw[1:]
    return raw


def _label_title(value: str) -> str:
    label = value.strip().replace("_", " ").replace("-", " ")
    if not label:
        return "Step"
    return " ".join(part.capitalize() for part in label.split())


def _reference_id(slug: str, *, taken: set[str]) -> str:
    base = f"ref-{_slug_display(slug).replace('/', '-').replace('.', '-')}"
    candidate = base
    counter = 1
    while candidate in taken:
        counter += 1
        candidate = f"{base}-{counter}"
    taken.add(candidate)
    return candidate


def _ensure_reference(
    *,
    slug: str,
    references: list[dict[str, Any]],
    by_slug: dict[str, dict[str, Any]],
    taken_ids: set[str],
) -> dict[str, Any]:
    """Return the existing reference for ``slug`` or insert a new one."""
    existing = by_slug.get(slug)
    if existing is not None:
        return existing
    ref_id = _reference_id(slug, taken=taken_ids)
    entry = {
        "id": ref_id,
        "type": _slug_kind(slug),
        "slug": slug,
        "raw": slug,
        "display_name": _slug_display(slug),
        "span": None,
        "config": {},
    }
    references.append(entry)
    by_slug[slug] = entry
    return entry


def _packet_step(
    *,
    packet: dict[str, Any],
    reference_slugs_for_step: Iterable[str],
) -> dict[str, Any]:
    step_id = str(packet.get("label") or f"step-{uuid4().hex[:8]}")
    return {
        "id": step_id,
        "kind": "agent",
        "title": _label_title(step_id),
        "summary": str(packet.get("description") or ""),
        "stage": str(packet.get("stage") or "build"),
        "route": str(packet.get("agent") or packet.get("task_type") or "auto/build"),
        "task_type": str(packet.get("task_type") or "auto/build"),
        "agent": str(packet.get("agent") or ""),
        "capabilities": list(packet.get("capabilities") or []),
        "consumes": list(packet.get("consumes") or []),
        "produces": list(packet.get("produces") or []),
        "depends_on": list(packet.get("depends_on") or []),
        "gates": [dict(g) for g in (packet.get("gates") or []) if isinstance(g, dict)],
        "parameters": dict(packet.get("parameters") or {}),
        "reference_slugs": sorted(set(reference_slugs_for_step)),
        "prompt": str(packet.get("prompt") or ""),
        "write_scope": list(packet.get("write") or []),
    }


def _default_result_kind(packet: dict[str, Any]) -> str:
    produces = {str(value).strip() for value in packet.get("produces") or [] if str(value).strip()}
    if "code_change_candidate" in produces or "diff" in produces:
        return "code_change_candidate"
    stage = str(packet.get("stage") or packet.get("task_type") or "").strip().lower()
    if stage == "research":
        return "research_result"
    return "artifact_bundle"


def _default_submit_tool(result_kind: str) -> str:
    if result_kind == "code_change_candidate":
        return "praxis_submit_code_change_candidate"
    if result_kind == "research_result":
        return "praxis_submit_research_result"
    return "praxis_submit_artifact_bundle"


def _default_tool_plan(packet: dict[str, Any]) -> dict[str, Any]:
    params = packet.get("parameters") if isinstance(packet.get("parameters"), dict) else {}
    explicit = params.get("agent_tool_plan")
    if isinstance(explicit, dict):
        return dict(explicit)
    stage = str(packet.get("stage") or packet.get("task_type") or "").strip().lower()
    if stage == "research":
        tool_name = "praxis_search"
        operation = "search"
        focus = "Run focused discovery passes, one query shape at a time."
    elif stage == "review":
        tool_name = "praxis_search"
        operation = "verify"
        focus = "Check claims and blockers against receipts, decisions, and source context."
    else:
        tool_name = _default_submit_tool(_default_result_kind(packet))
        operation = "submit"
        focus = "Produce the durable packet result and seal it through the submit tool."
    return {
        "tool_name": tool_name,
        "operation": operation,
        "repeats": 4,
        "cadence": "sequential",
        "focus": focus,
        "target_fields": ["prompt", "outputs", "completion_contract", "handoff_target"],
    }


def _capability_slug(value: Any) -> str:
    return str(value or "").strip()


def _capability_object(slug: str) -> dict[str, Any]:
    display = _slug_display(slug)
    signals = list(dict.fromkeys([display, slug]))
    return {
        "slug": slug,
        "label": _label_title(display),
        "route": slug,
        "signals": signals,
        "reference_slugs": [],
    }


def _packet_phase(packet: dict[str, Any], *, index: int, total: int) -> dict[str, Any]:
    step_id = str(packet.get("label") or f"step-{index:03d}")
    result_kind = _default_result_kind(packet)
    submit_tool = _default_submit_tool(result_kind)
    integration_args_raw = packet.get("integration_args")
    integration_args: dict[str, Any] = (
        dict(integration_args_raw) if isinstance(integration_args_raw, dict) else {}
    )
    phase: dict[str, Any] = {
        "phase_id": f"phase-{index:03d}",
        "step_id": step_id,
        "kind": str(packet.get("stage") or "build"),
        "title": _label_title(step_id),
        "purpose": str(packet.get("description") or ""),
        "agent_route": str(packet.get("agent") or packet.get("task_type") or "auto/build"),
        "agent": str(packet.get("agent") or ""),
        "task_type": str(packet.get("task_type") or packet.get("stage") or "build"),
        "system_prompt": str(packet.get("prompt") or ""),
        "required_inputs": list(packet.get("consumes") or []),
        "outputs": list(packet.get("produces") or []),
        "persistence_targets": list(packet.get("write") or []),
        "handoff_target": None if index >= total else "next",
        "integration_args": integration_args,
        "capabilities": list(packet.get("capabilities") or []),
        "write_scope": list(packet.get("write") or []),
        "agent_tool_plan": _default_tool_plan(packet),
        "completion_contract": {
            "result_kind": result_kind,
            "submit_tool_names": [submit_tool],
            "submission_required": True,
            "verification_required": bool(packet.get("gates")),
        },
    }
    integration_id = packet.get("integration_id")
    integration_action = packet.get("integration_action")
    if isinstance(integration_id, str) and integration_id:
        phase["integration_id"] = integration_id
    if isinstance(integration_action, str) and integration_action:
        phase["integration_action"] = integration_action
    return phase


def packets_to_definition(
    *,
    workflow_id: str,
    intent: str,
    compose_result: ComposeViaLLMResult,
) -> dict[str, Any]:
    """Translate a ComposeViaLLMResult into a workflow ``definition``.

    The returned dict is shaped so ``build_authority_bundle`` will fold its
    ``draft_flow`` + ``references`` into the live ``build_graph`` and
    ``binding_ledger`` the React build canvas already renders.
    """
    plan_packets = compose_result.plan_packets or []
    references: list[dict[str, Any]] = []
    by_slug: dict[str, dict[str, Any]] = {}
    taken_ids: set[str] = set()
    draft_flow: list[dict[str, Any]] = []
    phases: list[dict[str, Any]] = []

    for packet_index, packet in enumerate(plan_packets, start=1):
        if not isinstance(packet, dict):
            continue
        slugs_for_step: list[str] = []
        # consumes/produces become reference rows so binding_ledger picks them up
        for raw_slug in (packet.get("consumes") or []) + (packet.get("produces") or []):
            slug = str(raw_slug or "").strip()
            if not slug:
                continue
            _ensure_reference(slug=slug, references=references, by_slug=by_slug, taken_ids=taken_ids)
            slugs_for_step.append(slug)
        # The agent itself is a reference too (so its routing is bindable).
        agent_slug = str(packet.get("agent") or "").strip()
        if agent_slug:
            agent_ref_slug = agent_slug if agent_slug.startswith(("@", "#")) else agent_slug
            _ensure_reference(slug=agent_ref_slug, references=references, by_slug=by_slug, taken_ids=taken_ids)
            slugs_for_step.append(agent_ref_slug)
        draft_flow.append(_packet_step(packet=packet, reference_slugs_for_step=slugs_for_step))
        phases.append(_packet_phase(packet, index=packet_index, total=len(plan_packets)))

    capability_slugs: list[str] = sorted({
        cap
        for packet in plan_packets
        if isinstance(packet, dict)
        for cap in (packet.get("capabilities") or [])
        if isinstance(cap, str) and cap.strip()
    })
    capabilities = [_capability_object(slug) for slug in capability_slugs]

    synthesis_dict = compose_result.synthesis.to_dict() if compose_result.synthesis else None
    validation_dict = compose_result.validation.to_dict() if compose_result.validation else None
    pill_triage_dict = compose_result.pill_triage.to_dict() if compose_result.pill_triage else None

    return {
        "workflow_id": workflow_id,
        "type": "operating_model",
        "source_prose": intent,
        "materialized_prose": intent,  # compose_plan_via_llm doesn't rewrite prose
        "narrative_blocks": [],
        "references": references,
        "capabilities": capabilities,
        "authority": "",
        "sla": {},
        "trigger_intent": "",
        "draft_flow": draft_flow,
        "definition_revision": str(uuid4()),
        "execution_setup": {
            "phase_count": len(phases),
            "phases": phases,
            "agent_contract": {
                "field_edit_grain": "one_node_one_field",
                "tool_rhythm": "one_tool_repeated_for_similar_actions",
                "human_role": "review_and_approve",
            },
        },
        "surface_manifest": {},
        "build_receipt": {},
        # Compose-specific provenance — the build canvas can show "compiled
        # via fork-out (N packets, M cached tokens)" etc.
        "compose_provenance": {
            "ok": compose_result.ok,
            "intent": compose_result.intent,
            "synthesis": synthesis_dict,
            "validation": validation_dict,
            "pill_triage": pill_triage_dict,
            "usage_summary": compose_result.usage_summary(),
            "notes": list(compose_result.notes or []),
            "reason_code": compose_result.reason_code,
            "error": compose_result.error,
        },
    }


__all__ = ["packets_to_definition"]
